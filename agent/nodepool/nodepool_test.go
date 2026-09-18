package nodepool

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
)

// TestParseBackends_FromTOML locks the real config path: a TOML array of
// tables decodes into []map[string]any (not []any), and the old code rejected
// exactly that shape.
func TestParseBackends_FromTOML(t *testing.T) {
	var doc map[string]any
	if _, err := toml.Decode(`
hosts = [
  {name = "n1", host = "admin@localhost", port = 2024, work_dir = "/home/admin/ws", cmd = "/home/admin/node-tools/bin/claude"},
  {name = "local"},
]
`, &doc); err != nil {
		t.Fatalf("toml decode: %v", err)
	}
	got, err := ParseBackends(doc, "/root/ws", "claude")
	if err != nil {
		t.Fatalf("ParseBackends: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 backends, got %d", len(got))
	}
	if got[0].Host != "admin@localhost" || got[0].Port != 2024 || got[0].Cmd != "/home/admin/node-tools/bin/claude" {
		t.Errorf("backend 0 wrong: %+v", got[0])
	}
	if got[1].Name != "local" || !got[1].IsLocal() || got[1].WorkDir != "/root/ws" {
		t.Errorf("backend 1 defaults wrong: %+v", got[1])
	}
}

func TestParseBackends_None(t *testing.T) {
	got, err := ParseBackends(map[string]any{}, "/root/ws", "claude")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil backends when no hosts option, got %v", got)
	}
}

func TestParseBackends_Defaults(t *testing.T) {
	opts := map[string]any{
		"hosts": []any{
			map[string]any{"name": "local"},
			map[string]any{"name": "remote", "host": "admin@localhost", "port": int64(2024)},
		},
	}
	got, err := ParseBackends(opts, "/root/ws", "claude")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 backends, got %d", len(got))
	}
	if got[0].Name != "local" || !got[0].IsLocal() {
		t.Errorf("backend 0: want local, got %v", got[0])
	}
	if got[0].WorkDir != "/root/ws" || got[0].Cmd != "claude" {
		t.Errorf("backend 0: defaults not applied: WorkDir=%q Cmd=%q", got[0].WorkDir, got[0].Cmd)
	}
	if got[1].Host != "admin@localhost" || got[1].Port != 2024 || got[1].IsLocal() {
		t.Errorf("backend 1: want remote admin@localhost:2024, got %v", got[1])
	}
}

func TestParseBackends_DuplicateName(t *testing.T) {
	opts := map[string]any{
		"hosts": []any{
			map[string]any{"name": "dup"},
			map[string]any{"name": "dup", "host": "x"},
		},
	}
	if _, err := ParseBackends(opts, "", ""); err == nil {
		t.Fatal("expected error for duplicate name, got nil")
	}
}

func TestPool_SelectBackend_RoundRobin(t *testing.T) {
	p := &Pool{Backends: []*Backend{{Name: "a"}, {Name: "b"}, {Name: "c"}}}
	for _, be := range p.Backends {
		be.Up.Store(true)
	}
	// Three new sessions should distribute across all three backends.
	seen := map[string]int{}
	for i := 0; i < 3; i++ {
		be, _ := p.SelectBackend("sess-" + string(rune('A'+i)))
		seen[be.Name]++
	}
	if len(seen) != 3 {
		t.Errorf("expected 3 distinct backends via round-robin, got %v", seen)
	}
}

func TestPool_SelectBackend_Affinity(t *testing.T) {
	p := &Pool{Backends: []*Backend{{Name: "a"}, {Name: "b"}}}
	for _, be := range p.Backends {
		be.Up.Store(true)
	}
	first, _ := p.SelectBackend("sess-1")
	for i := 0; i < 5; i++ {
		next, _ := p.SelectBackend("sess-1")
		if next != first {
			t.Errorf("affinity broken: first=%s, then got %s", first.Name, next.Name)
		}
	}
}

func TestPool_SelectBackend_EmptySessionIDRoundRobinOnly(t *testing.T) {
	p := &Pool{Backends: []*Backend{{Name: "a"}, {Name: "b"}}}
	for _, be := range p.Backends {
		be.Up.Store(true)
	}
	// Two fresh sessions (both "") must round-robin, and "" must never bind.
	for i := 0; i < 4; i++ {
		be, _ := p.SelectBackend("")
		if be == nil {
			t.Fatalf("no backend selected")
		}
	}
	if _, ok := p.Binding.Load(""); ok {
		t.Errorf("empty sessionID must never be bound (would pin all fresh sessions)")
	}
}

func TestPool_RegisterSessionChain(t *testing.T) {
	bA := &Backend{Name: "a"}
	p := &Pool{Backends: []*Backend{bA}}
	// A fresh turn picks some backend; the session it spawns reports its new
	// id — the chain registration must bind that id to the spawn backend.
	p.RegisterSessionChain("", bA) // no-op, must not panic or bind
	if _, ok := p.Binding.Load(""); ok {
		t.Errorf("empty id must not bind")
	}
	p.RegisterSessionChain("u1", bA)
	if got := p.Bound("u1"); got != bA {
		t.Errorf("expected u1 bound to a, got %v", got)
	}
	// Next turn's StartSession("u1") must hit affinity, not round-robin.
	be, _ := p.SelectBackend("u1")
	if be != bA {
		t.Errorf("expected affinity for u1, got %s", be.Name)
	}
}

func TestPool_SelectBackend_Failover(t *testing.T) {
	bA := &Backend{Name: "a"}
	bB := &Backend{Name: "b"}
	bA.Up.Store(true)
	bB.Up.Store(true)
	p := &Pool{Backends: []*Backend{bA, bB}}
	// Bind sess-1 to A.
	be, _ := p.SelectBackend("sess-1")
	if be != bA {
		t.Fatalf("expected initial bind to A, got %s", be.Name)
	}
	// A goes down → next selection for sess-1 must failover to B.
	bA.Up.Store(false)
	be, _ = p.SelectBackend("sess-1")
	if be != bB {
		t.Errorf("expected failover to B, got %s", be.Name)
	}
	// sess-1 should now be rebound to B.
	if p.Bound("sess-1") != bB {
		t.Errorf("expected sess-1 rebound to B after failover")
	}
}

func TestPool_SelectBackend_PendingNode(t *testing.T) {
	bA := &Backend{Name: "a"}
	bB := &Backend{Name: "b"}
	bA.Up.Store(true)
	bB.Up.Store(true)
	p := &Pool{Backends: []*Backend{bA, bB}}
	// sess-1 binds to A initially.
	be, _ := p.SelectBackend("sess-1")
	if be != bA && be != bB {
		t.Fatalf("unexpected backend %s", be.Name)
	}
	// /node b override → next selection picks B regardless of affinity.
	p.SetPendingNode("sess-1", "b")
	be, _ = p.SelectBackend("sess-1")
	if be != bB {
		t.Errorf("expected /node override to B, got %s", be.Name)
	}
}

func TestBuildRemoteSSHArgs(t *testing.T) {
	be := &Backend{
		Name:    "sandbox",
		Host:    "admin@localhost",
		Port:    2024,
		SSHKey:  "~/.ssh/id_rsa",
		WorkDir: "/home/admin/ws",
		Cmd:     "claude",
	}
	args := BuildRemoteSSHArgs(be, "claude", []string{"--model", "glm-5.2"}, []string{"ANTHROPIC_BASE_URL=http://x:15443", "FOO=bar baz"})
	joined := strings.Join(args, " ")
	// Must use BatchMode, the port, the expanded key, exec env, and quoted vars.
	checks := []string{
		"-q", "-o", "BatchMode=yes", "-p", "2024",
		"-i", strings.ReplaceAll("~/.ssh/id_rsa", "~", ""), // expanded
		"admin@localhost", "--",
		"sh -c", "cd \"/home/admin/ws\"", "exec env",
		"ANTHROPIC_BASE_URL='http://x:15443'",
		"FOO='bar baz'",
		"'claude'", "--model", "glm-5.2",
	}
	for _, c := range checks {
		if !strings.Contains(joined, c) {
			t.Errorf("expected SSH args to contain %q; got: %s", c, joined)
		}
	}
	// No env vars → no `env` prefix (exec straight to the binary).
	joined = strings.Join(BuildRemoteSSHArgs(be, "claude", []string{"-p"}, nil), " ")
	if strings.Contains(joined, "exec env") {
		t.Errorf("no extraEnv must not inject an env command: %s", joined)
	}
	if !strings.Contains(joined, "exec 'claude'") {
		t.Errorf("expected direct exec of the binary: %s", joined)
	}
}

// TestBuildRemoteSSHArgsLive runs the REAL generated argv over ssh against a
// live backend (guarded: set MAOMAO_E2E=1 and MAOMAO_E2E_PORT). It exists so
// a bug like the missing `env` command — where the remote shell executed
// "NOELLE_BASE_URL=https://..." as a file path — cannot pass review again:
// the command under test is byte-for-byte what cc-connect spawns.
func TestBuildRemoteSSHArgsLive(t *testing.T) {
	port := os.Getenv("MAOMAO_E2E_PORT")
	if os.Getenv("MAOMAO_E2E") == "" || port == "" {
		t.Skip("set MAOMAO_E2E=1 and MAOMAO_E2E_PORT to run")
	}
	be := &Backend{
		Name:    "live",
		Host:    "admin@localhost",
		Port:    2024,
		SSHKey:  os.Getenv("MAOMAO_E2E_KEY"),
		SSHOpts: []string{"-o", "UserKnownHostsFile=" + os.Getenv("MAOMAO_E2E_KH")},
		WorkDir: os.Getenv("MAOMAO_E2E_WORKDIR"),
	}
	// Env values with URL slashes and spaces — the exact shapes that explode
	// when the `env` command is missing.
	sshArgs := BuildRemoteSSHArgs(be, os.Getenv("MAOMAO_E2E_BIN"), []string{"exec", "--skip-git-repo-check", "--dangerously-bypass-approvals-and-sandbox", "-"},
		[]string{"NOELLE_BASE_URL=https://dlf-noelle.aliyun-inc.com", "FOO=bar baz"})
	cmd := exec.Command("ssh", sshArgs...)
	cmd.Stdin = strings.NewReader("Reply with exactly: OK")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ssh run failed: %v\noutput: %s", err, out)
	}
	if !strings.Contains(string(out), "OK") {
		t.Fatalf("expected OK in output, got: %s", out)
	}
}

func TestShellQuote(t *testing.T) {
	cases := []struct{ in, want string }{
		{"simple", "'simple'"},
		{"with space", "'with space'"},
		{"with'quote", "'with'\\''quote'"},
	}
	for _, c := range cases {
		if got := shellQuote(c.in); got != c.want {
			t.Errorf("shellQuote(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestShellQuoteEnv(t *testing.T) {
	got := shellQuoteEnv("FOO=bar baz")
	want := "FOO='bar baz'"
	if got != want {
		t.Errorf("shellQuoteEnv = %q, want %q", got, want)
	}
	// Value with a single quote must be escaped.
	got = shellQuoteEnv("X=a'b")
	if !strings.Contains(got, "'\\''") {
		t.Errorf("expected escaped quote in %q", got)
	}
}
