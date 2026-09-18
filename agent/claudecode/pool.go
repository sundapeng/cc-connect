package claudecode

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

// backend describes one Claude Code execution target in a multi-node pool.
// host == "" means the local machine (no SSH); any other value is reached
// over SSH. Each backend runs its own claude process with its own local
// ~/.claude/projects session store, so a cc-connect session bound to a
// backend must stay on that backend for --resume to work.
type backend struct {
	name    string   // /node reference name
	host    string   // "" = local exec; "user@host" for SSH
	port    int      // SSH port (0 = 22)
	sshKey  string   // SSH identity file ("" = default)
	sshOpts []string // extra ssh(1) flags
	workDir string   // working directory on the backend
	cmd     string   // claude binary path/name on the backend

	// up tracks lazy health: flipped false on dial/spawn failure, true on a
	// successful spawn. No background probing — selection skips down backends
	// and retries them on the next round-robin cycle.
	up atomic.Bool
}

// isLocal reports whether this backend runs claude on the cc-connect host
// itself (no SSH).
func (b *backend) isLocal() bool { return b == nil || b.host == "" }

// String renders a human label for /node output.
func (b *backend) String() string {
	if b.isLocal() {
		return fmt.Sprintf("%s (local%s)", b.name, workDirSuffix(b.workDir))
	}
	port := ""
	if b.port != 0 && b.port != 22 {
		port = fmt.Sprintf(":%d", b.port)
	}
	return fmt.Sprintf("%s (%s%s%s)", b.name, b.host, port, workDirSuffix(b.workDir))
}

func workDirSuffix(w string) string {
	if w == "" {
		return ""
	}
	return " " + w
}

// pool holds the backend list plus per-agent selection state.
type pool struct {
	backends []*backend

	// binding maps sessionID → chosen backend, for session affinity.
	binding sync.Map // map[string]*backend

	// rr is the round-robin cursor for new sessions with no binding.
	rr atomic.Uint64

	// pendingNode holds a backend name chosen via /node that applies to the
	// next StartSession for a given sessionID.
	pendingNode sync.Map // map[string]string
}

// parseBackends reads the optional [[projects.agent.options.hosts]] array from
// the agent opts map. Returns nil when no hosts are configured, preserving the
// legacy single-local-process behavior.
func parseBackends(opts map[string]any, defaultWorkDir, defaultCmd string) ([]*backend, error) {
	raw, ok := opts["hosts"]
	if !ok || raw == nil {
		return nil, nil
	}
	arr, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("claudecode: option \"hosts\" must be an array of tables")
	}
	if len(arr) == 0 {
		return nil, nil
	}
	out := make([]*backend, 0, len(arr))
	seen := make(map[string]bool)
	for i, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("claudecode: hosts[%d] must be a table", i)
		}
		be := &backend{up: atomic.Bool{}}
		be.up.Store(true) // optimistic; flipped on first failure
		be.name, _ = m["name"].(string)
		if be.name == "" {
			return nil, fmt.Errorf("claudecode: hosts[%d] requires a \"name\"", i)
		}
		if seen[be.name] {
			return nil, fmt.Errorf("claudecode: duplicate host name %q", be.name)
		}
		seen[be.name] = true
		be.host, _ = m["host"].(string)
		if p, ok := m["port"].(int); ok {
			be.port = p
		} else if p, ok := m["port"].(int64); ok {
			be.port = int(p)
		} else if p, ok := m["port"].(float64); ok {
			be.port = int(p)
		}
		be.sshKey, _ = m["ssh_key"].(string)
		be.workDir, _ = m["work_dir"].(string)
		be.cmd, _ = m["cmd"].(string)
		// Fall back to the agent-level defaults so a backend only needs to
		// specify what differs (typically just host + work_dir).
		if be.workDir == "" {
			be.workDir = defaultWorkDir
		}
		if be.cmd == "" {
			be.cmd = defaultCmd
		}
		if opts, ok := m["ssh_opts"].([]any); ok {
			for _, o := range opts {
				if s, ok := o.(string); ok {
					be.sshOpts = append(be.sshOpts, s)
				}
			}
		}
		out = append(out, be)
	}
	return out, nil
}

// selectBackend picks the backend for a session, in priority order:
//  1. pending /node selection for this sessionID (if the named backend is up)
//  2. existing binding (session affinity)
//  3. round-robin across healthy backends
//
// Returns the chosen backend and whether it was a failover (the bound backend
// was down and another was chosen instead).
func (p *pool) selectBackend(sessionID string) (*backend, bool) {
	// 1. /node override
	if name, ok := p.pendingNode.LoadAndDelete(sessionID); ok {
		if n, ok := name.(string); ok {
			for _, be := range p.backends {
				if be.name == n && be.up.Load() {
					p.binding.Store(sessionID, be)
					return be, false
				}
			}
		}
	}
	// 2. affinity — reuse the bound backend if it is still up; otherwise failover.
	if v, ok := p.binding.Load(sessionID); ok {
		be := v.(*backend)
		if be.up.Load() {
			return be, false
		}
		// bound backend is down → fall through to round-robin failover
		p.binding.Delete(sessionID)
	}
	// 3. round-robin over healthy backends
	n := len(p.backends)
	for i := 0; i < n; i++ {
		idx := int(p.rr.Add(1)-1) % n
		be := p.backends[idx]
		if be.up.Load() {
			p.binding.Store(sessionID, be)
			return be, false
		}
	}
	// No healthy backend; return the first (will likely fail, but surface
	// the error rather than panic). Mark a fresh round-robin attempt.
	be := p.backends[0]
	p.binding.Store(sessionID, be)
	return be, true
}

// bind forces a specific backend for a session (used by /node migration).
func (p *pool) bind(sessionID string, be *backend) {
	p.binding.Store(sessionID, be)
}

// bound returns the backend currently bound to a session, or nil.
func (p *pool) bound(sessionID string) *backend {
	if v, ok := p.binding.Load(sessionID); ok {
		return v.(*backend)
	}
	return nil
}

// clearBinding drops the session→backend binding (used on migration/failover).
func (p *pool) clearBinding(sessionID string) {
	p.binding.Delete(sessionID)
}

// markDown flips a backend's health to down.
func (p *pool) markDown(be *backend) {
	if be != nil {
		be.up.Store(false)
	}
}

// markUp flips a backend's health to up (called after a successful spawn).
func (p *pool) markUp(be *backend) {
	if be != nil {
		be.up.Store(true)
	}
}

// setPendingNode records a /node selection to apply on the next StartSession.
func (p *pool) setPendingNode(sessionID, name string) {
	p.pendingNode.Store(sessionID, name)
}

// expandPath tilde-expands a leading ~ in a path. SSH identity files are
// conventionally given as ~/.ssh/id_rsa; exec.Command needs the expanded form.
func expandPath(p string) string {
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

// buildRemoteSSHArgs constructs the argv for spawning claude on a remote
// backend over SSH. The returned slice is the args to exec.Command("ssh", ...).
//
// Layout: ssh -q -o BatchMode=yes [-p port] [-i key] [sshOpts...] host --
//   sh -c 'cd "<workDir>" && exec env VAR=val ... <cmd> <args...>'
//
// No PTY is allocated: stream-json is line-oriented JSON over stdio and a PTY
// would inject terminal escapes. stdin/stdout/stderr are piped by cc-connect
// and forwarded transparently by ssh. BatchMode fails fast on a missing key
// instead of hanging on a password prompt.
func buildRemoteSSHArgs(be *backend, cliBin string, allArgs []string, extraEnv []string) []string {
	args := []string{"-q", "-o", "BatchMode=yes"}
	if be.port != 0 && be.port != 22 {
		args = append(args, "-p", fmt.Sprintf("%d", be.port))
	}
	if be.sshKey != "" {
		args = append(args, "-i", expandPath(be.sshKey))
	}
	args = append(args, be.sshOpts...)
	args = append(args, be.host, "--")

	// Build the remote shell command. We exec claude so it replaces the shell
	// (clean process-group kill when ssh closes the connection).
	var sb strings.Builder
	sb.WriteString("sh -c '")
	if be.workDir != "" {
		sb.WriteString("cd \"")
		sb.WriteString(strings.ReplaceAll(be.workDir, "\"", "\\\""))
		sb.WriteString("\" && ")
	}
	sb.WriteString("exec")
	for _, e := range extraEnv {
		// env VAR=val tokens; quote the value for the remote shell.
		sb.WriteString(" ")
		sb.WriteString(shellQuoteEnv(e))
	}
	sb.WriteString(" ")
	sb.WriteString(shellQuote(cliBin))
	for _, a := range allArgs {
		sb.WriteString(" ")
		sb.WriteString(shellQuote(a))
	}
	sb.WriteString("'")
	args = append(args, sb.String())
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
