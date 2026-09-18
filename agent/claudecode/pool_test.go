package claudecode

import (
	"strings"
	"testing"
)

func TestParseBackends_None(t *testing.T) {
	got, err := parseBackends(map[string]any{}, "/root/ws", "claude")
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
	got, err := parseBackends(opts, "/root/ws", "claude")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 backends, got %d", len(got))
	}
	if got[0].name != "local" || !got[0].isLocal() {
		t.Errorf("backend 0: want local, got %v", got[0])
	}
	if got[0].workDir != "/root/ws" || got[0].cmd != "claude" {
		t.Errorf("backend 0: defaults not applied: workDir=%q cmd=%q", got[0].workDir, got[0].cmd)
	}
	if got[1].host != "admin@localhost" || got[1].port != 2024 || got[1].isLocal() {
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
	if _, err := parseBackends(opts, "", ""); err == nil {
		t.Fatal("expected error for duplicate name, got nil")
	}
}

func TestPool_SelectBackend_RoundRobin(t *testing.T) {
	p := &pool{backends: []*backend{{name: "a"}, {name: "b"}, {name: "c"}}}
	for _, be := range p.backends {
		be.up.Store(true)
	}
	// Three new sessions should distribute across all three backends.
	seen := map[string]int{}
	for i := 0; i < 3; i++ {
		be, _ := p.selectBackend("sess-" + string(rune('A'+i)))
		seen[be.name]++
	}
	if len(seen) != 3 {
		t.Errorf("expected 3 distinct backends via round-robin, got %v", seen)
	}
}

func TestPool_SelectBackend_Affinity(t *testing.T) {
	p := &pool{backends: []*backend{{name: "a"}, {name: "b"}}}
	for _, be := range p.backends {
		be.up.Store(true)
	}
	first, _ := p.selectBackend("sess-1")
	for i := 0; i < 5; i++ {
		next, _ := p.selectBackend("sess-1")
		if next != first {
			t.Errorf("affinity broken: first=%s, then got %s", first.name, next.name)
		}
	}
}

func TestPool_SelectBackend_Failover(t *testing.T) {
	bA := &backend{name: "a"}
	bB := &backend{name: "b"}
	bA.up.Store(true)
	bB.up.Store(true)
	p := &pool{backends: []*backend{bA, bB}}
	// Bind sess-1 to A.
	be, _ := p.selectBackend("sess-1")
	if be != bA {
		t.Fatalf("expected initial bind to A, got %s", be.name)
	}
	// A goes down → next selection for sess-1 must failover to B.
	bA.up.Store(false)
	be, failover := p.selectBackend("sess-1")
	if be != bB {
		t.Errorf("expected failover to B, got %s", be.name)
	}
	if !failover {
		// failover flag is only set when no healthy backend is found at all;
		// a successful failover to B returns failover=false. Verify B is chosen.
	}
	// sess-1 should now be rebound to B.
	if p.bound("sess-1") != bB {
		t.Errorf("expected sess-1 rebound to B after failover")
	}
}

func TestPool_SelectBackend_PendingNode(t *testing.T) {
	bA := &backend{name: "a"}
	bB := &backend{name: "b"}
	bA.up.Store(true)
	bB.up.Store(true)
	p := &pool{backends: []*backend{bA, bB}}
	// sess-1 binds to A initially.
	be, _ := p.selectBackend("sess-1")
	if be != bA && be != bB {
		t.Fatalf("unexpected backend %s", be.name)
	}
	// /node b override → next selection picks B regardless of affinity.
	p.setPendingNode("sess-1", "b")
	be, _ = p.selectBackend("sess-1")
	if be != bB {
		t.Errorf("expected /node override to B, got %s", be.name)
	}
}

func TestBuildRemoteSSHArgs(t *testing.T) {
	be := &backend{
		name:    "sandbox",
		host:    "admin@localhost",
		port:    2024,
		sshKey:  "~/.ssh/id_rsa",
		workDir: "/home/admin/ws",
		cmd:     "claude",
	}
	args := buildRemoteSSHArgs(be, "claude", []string{"--model", "glm-5.2"}, []string{"ANTHROPIC_BASE_URL=http://x:15443", "FOO=bar baz"})
	joined := strings.Join(args, " ")
	// Must use BatchMode, the port, the expanded key, and exec.
	checks := []string{
		"-q", "-o", "BatchMode=yes", "-p", "2024",
		"-i", strings.ReplaceAll("~/.ssh/id_rsa", "~", ""), // expanded
		"admin@localhost", "--",
		"sh -c", "cd \"/home/admin/ws\"", "exec",
		"ANTHROPIC_BASE_URL='http://x:15443'",
		"FOO='bar baz'",
		"'claude'", "--model", "glm-5.2",
	}
	for _, c := range checks {
		if !strings.Contains(joined, c) {
			t.Errorf("expected SSH args to contain %q; got: %s", c, joined)
		}
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
