// Package nodepool implements the multi-node backend pool shared by the
// claudecode and codex agents: one bot frontend, N execution backends
// (local or reached over SSH), with session affinity, round-robin
// distribution for new sessions, and failover.
//
// Session affinity is the core invariant: each agent keeps its session
// state (claude --resume transcripts, codex rollout files) on the backend
// host under that backend's $HOME, so a session must stay on the backend
// it started on for resume to work. Bindings are lost when cc-connect
// restarts (in-memory); the session files survive on their backends.
package nodepool

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

// Backend describes one execution target in a multi-node pool. Host == ""
// means the local machine (no SSH); any other value is reached over SSH.
type Backend struct {
	Name    string   // /node reference name
	Host    string   // "" = local exec; "user@host" for SSH
	Port    int      // SSH port (0 = 22)
	SSHKey  string   // SSH identity file ("" = default)
	SSHOpts []string // extra ssh(1) flags
	WorkDir string   // working directory on the backend
	Cmd     string   // agent binary path/name on the backend

	// Up tracks lazy health: flipped false on dial/spawn failure, true on a
	// successful spawn. No background probing — selection skips down backends
	// and retries them on the next round-robin cycle.
	Up atomic.Bool
}

// IsLocal reports whether this backend runs the agent on the cc-connect host
// itself (no SSH).
func (b *Backend) IsLocal() bool { return b == nil || b.Host == "" }

// String renders a human label for /node output.
func (b *Backend) String() string {
	if b.IsLocal() {
		return fmt.Sprintf("%s (local%s)", b.Name, workDirSuffix(b.WorkDir))
	}
	port := ""
	if b.Port != 0 && b.Port != 22 {
		port = fmt.Sprintf(":%d", b.Port)
	}
	return fmt.Sprintf("%s (%s%s%s)", b.Name, b.Host, port, workDirSuffix(b.WorkDir))
}

func workDirSuffix(w string) string {
	if w == "" {
		return ""
	}
	return " " + w
}

// Pool holds the backend list plus per-agent selection state.
type Pool struct {
	Backends []*Backend

	// Binding maps sessionID → chosen backend, for session affinity.
	Binding sync.Map // map[string]*Backend

	// RR is the round-robin cursor for new sessions with no binding.
	RR atomic.Uint64

	// PendingNode holds a backend name chosen via /node that applies to the
	// next StartSession for a given sessionID.
	PendingNode sync.Map // map[string]string
}

// ParseBackends reads the optional [[projects.agent.options.hosts]] array from
// the agent opts map. Returns nil when no hosts are configured, preserving the
// legacy single-local-process behavior. Accepts both []any (map-of-any style,
// e.g. from tests) and []map[string]any (what a TOML unmarshal produces).
func ParseBackends(opts map[string]any, defaultWorkDir, defaultCmd string) ([]*Backend, error) {
	raw, ok := opts["hosts"]
	if !ok || raw == nil {
		return nil, nil
	}
	var arr []any
	switch typed := raw.(type) {
	case []any:
		arr = typed
	case []map[string]any:
		arr = make([]any, len(typed))
		for i, m := range typed {
			arr[i] = m
		}
	default:
		return nil, fmt.Errorf("nodepool: option \"hosts\" must be an array of tables")
	}
	if len(arr) == 0 {
		return nil, nil
	}
	out := make([]*Backend, 0, len(arr))
	seen := make(map[string]bool)
	for i, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("nodepool: hosts[%d] must be a table", i)
		}
		be := &Backend{Up: atomic.Bool{}}
		be.Up.Store(true) // optimistic; flipped on first failure
		be.Name, _ = m["name"].(string)
		if be.Name == "" {
			return nil, fmt.Errorf("nodepool: hosts[%d] requires a \"name\"", i)
		}
		if seen[be.Name] {
			return nil, fmt.Errorf("nodepool: duplicate host name %q", be.Name)
		}
		seen[be.Name] = true
		be.Host, _ = m["host"].(string)
		if p, ok := m["port"].(int); ok {
			be.Port = p
		} else if p, ok := m["port"].(int64); ok {
			be.Port = int(p)
		} else if p, ok := m["port"].(float64); ok {
			be.Port = int(p)
		}
		be.SSHKey, _ = m["ssh_key"].(string)
		be.WorkDir, _ = m["work_dir"].(string)
		be.Cmd, _ = m["cmd"].(string)
		// Fall back to the agent-level defaults so a backend only needs to
		// specify what differs (typically just host + work_dir).
		if be.WorkDir == "" {
			be.WorkDir = defaultWorkDir
		}
		if be.Cmd == "" {
			be.Cmd = defaultCmd
		}
		if opts, ok := m["ssh_opts"].([]any); ok {
			for _, o := range opts {
				if s, ok := o.(string); ok {
					be.SSHOpts = append(be.SSHOpts, s)
				}
			}
		} else if opts, ok := m["ssh_opts"].([]string); ok {
			be.SSHOpts = append(be.SSHOpts, opts...)
		}
		out = append(out, be)
	}
	return out, nil
}

// SelectBackend picks the backend for a session, in priority order:
//  1. pending /node selection for this sessionID (if the named backend is up)
//  2. existing binding (session affinity)
//  3. round-robin across healthy backends
//
// Returns the chosen backend and whether it was a failover (the bound backend
// was down and another was chosen instead).
//
// An empty sessionID (the first turn of every fresh session) is never bound:
// it is shared by all fresh sessions, so a binding on "" would pin every new
// conversation to one backend. Affinity for later turns comes from
// RegisterSessionChain instead.
func (p *Pool) SelectBackend(sessionID string) (*Backend, bool) {
	// 1. /node override
	if sessionID != "" {
		if name, ok := p.PendingNode.LoadAndDelete(sessionID); ok {
			if n, ok := name.(string); ok {
				for _, be := range p.Backends {
					if be.Name == n && be.Up.Load() {
						p.Binding.Store(sessionID, be)
						return be, false
					}
				}
			}
		}
	}
	// 2. affinity — reuse the bound backend if it is still up; otherwise failover.
	if sessionID != "" {
		if v, ok := p.Binding.Load(sessionID); ok {
			be := v.(*Backend)
			if be.Up.Load() {
				return be, false
			}
			// bound backend is down → fall through to round-robin failover
			p.Binding.Delete(sessionID)
		}
	}
	// 3. round-robin over healthy backends
	n := len(p.Backends)
	for i := 0; i < n; i++ {
		idx := int(p.RR.Add(1)-1) % n
		be := p.Backends[idx]
		if be.Up.Load() {
			if sessionID != "" {
				p.Binding.Store(sessionID, be)
			}
			return be, false
		}
	}
	// No healthy backend; return the first (will likely fail, but surface
	// the error rather than panic). Mark a fresh round-robin attempt.
	be := p.Backends[0]
	if sessionID != "" {
		p.Binding.Store(sessionID, be)
	}
	return be, true
}

// RegisterSessionChain records that agentSessionID lives on be. Agents call
// this when the backend process reports a NEW session/thread id (claude forks
// a fresh id on every --resume, codex on every exec resume), because the id
// the engine passes to the next StartSession is exactly this new id — binding
// it here is what keeps the session on the backend that holds its transcript.
func (p *Pool) RegisterSessionChain(agentSessionID string, be *Backend) {
	if p == nil || be == nil || agentSessionID == "" {
		return
	}
	p.Binding.Store(agentSessionID, be)
}

// Bind forces a specific backend for a session (used by /node migration).
func (p *Pool) Bind(sessionID string, be *Backend) {
	p.Binding.Store(sessionID, be)
}

// Bound returns the backend currently bound to a session, or nil.
func (p *Pool) Bound(sessionID string) *Backend {
	if v, ok := p.Binding.Load(sessionID); ok {
		return v.(*Backend)
	}
	return nil
}

// ClearBinding drops the session→backend binding (used on migration/failover).
func (p *Pool) ClearBinding(sessionID string) {
	p.Binding.Delete(sessionID)
}

// MarkDown flips a backend's health to down.
func (p *Pool) MarkDown(be *Backend) {
	if be != nil {
		be.Up.Store(false)
	}
}

// MarkUp flips a backend's health to up (called after a successful spawn).
func (p *Pool) MarkUp(be *Backend) {
	if be != nil {
		be.Up.Store(true)
	}
}

// SetPendingNode records a /node selection to apply on the next StartSession.
func (p *Pool) SetPendingNode(sessionID, name string) {
	p.PendingNode.Store(sessionID, name)
}

// ExpandPath tilde-expands a leading ~ in a path. SSH identity files are
// conventionally given as ~/.ssh/id_rsa; exec.Command needs the expanded form.
func ExpandPath(p string) string {
	if p == "" || !strings.HasPrefix(p, "~") {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return p
	}
	if p == "~" {
		return home
	}
	if strings.HasPrefix(p, "~/") {
		return filepath.Join(home, p[2:])
	}
	return p // ~user form; leave to the shell
}

// BuildRemoteSSHArgs constructs the argv for spawning the agent binary on a
// remote backend over SSH. The returned slice is the args to
// exec.Command("ssh", ...).
//
// Layout: ssh -q -o BatchMode=yes [-p port] [-i key] [sshOpts...] host --
//
//	sh -c 'cd "<workDir>" && exec env VAR=val ... <cmd> <args...>'
//
// The inner command is built first and then shell-quoted as ONE argument to
// sh -c. Nesting must go through shellQuote — a hand-written `sh -c '...'`
// wrapper would be closed prematurely by the first single quote of any inner
// value, and the remote login shell would execute a truncated command with
// the tail as stray arguments (seen live as `env` printing the environment
// with no command to run).
//
// Env vars ride the `env` command — `exec VAR=val cmd` is NOT shell
// assignment syntax: the shell would try to execute a file literally named
// "VAR=val" (resolved against the workdir, thanks to URL slashes), the
// "No such file or directory" failure from the first live run.
//
// No PTY is allocated: stdio JSON is line-oriented and a PTY would inject
// terminal escapes. BatchMode fails fast on a missing key instead of hanging
// on a password prompt.
func BuildRemoteSSHArgs(be *Backend, cliBin string, allArgs []string, extraEnv []string) []string {
	args := []string{"-q", "-o", "BatchMode=yes"}
	if be.Port != 0 && be.Port != 22 {
		args = append(args, "-p", fmt.Sprintf("%d", be.Port))
	}
	if be.SSHKey != "" {
		args = append(args, "-i", ExpandPath(be.SSHKey))
	}
	args = append(args, be.SSHOpts...)
	args = append(args, be.Host, "--")

	// Build the inner command. We exec the agent binary so it replaces the
	// shell (clean process-group kill when ssh closes the connection).
	var inner strings.Builder
	if be.WorkDir != "" {
		inner.WriteString("cd \"")
		inner.WriteString(strings.ReplaceAll(be.WorkDir, "\"", "\\\""))
		inner.WriteString("\" && ")
	}
	inner.WriteString("exec")
	if len(extraEnv) > 0 {
		inner.WriteString(" env")
	}
	for _, e := range extraEnv {
		inner.WriteString(" ")
		inner.WriteString(shellQuoteEnv(e))
	}
	inner.WriteString(" ")
	inner.WriteString(shellQuote(cliBin))
	for _, a := range allArgs {
		inner.WriteString(" ")
		inner.WriteString(shellQuote(a))
	}

	// The remote login shell (zsh, bash, ...) parses the ssh command line
	// first; sh -c must receive the whole inner command as ONE argument.
	args = append(args, "sh", "-c", shellQuote(inner.String()))
	return args
}

// shellQuote single-quotes a string for safe inclusion in a sh -c command.
// Single quotes inside are escaped as '\''.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

// shellQuoteEnv quotes the value part of KEY=VALUE for the remote env command.
// The KEY is passed through verbatim (env var names are [A-Za-z_][A-Za-z0-9_]*);
// the value is single-quoted.
func shellQuoteEnv(kv string) string {
	eq := strings.IndexByte(kv, '=')
	if eq < 0 {
		return shellQuote(kv)
	}
	return kv[:eq+1] + shellQuote(kv[eq+1:])
}
