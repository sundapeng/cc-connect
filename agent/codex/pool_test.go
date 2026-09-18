package codex

import (
	"context"
	"strings"
	"testing"

	"github.com/chenhg5/cc-connect/agent/nodepool"
)

func TestPoolBackendRefusedWithAppServer(t *testing.T) {
	opts := map[string]any{
		"backend": "app_server",
		"hosts": []any{
			map[string]any{"name": "n1", "host": "admin@localhost", "port": int64(2024)},
		},
	}
	if _, err := New(opts); err == nil {
		t.Fatal("expected error: hosts pool requires exec backend")
	}
}

// codex exec reads the prompt from stdin ("-" argv terminator), so the SSH
// command line only carries flags — but the -c config overrides embed
// double quotes and must survive the sh -c quoting.
func TestSpawnRemoteArgQuoting(t *testing.T) {
	be := &nodepool.Backend{Name: "n1", Host: "admin@localhost", Port: 2024, WorkDir: "/home/admin/ws"}
	cs, err := newCodexSession(context.Background(), "codex", nil, "/local/stage", "gpt-5.6-sol", "ultra", "yolo", "", "", nil, "", "", "", be, nil)
	if err != nil {
		t.Fatalf("newCodexSession: %v", err)
	}

	// Mirror Send's arg construction for a fresh turn (no resume).
	args := cs.buildExecArgs("prompt goes to stdin", nil)
	if got := args[len(args)-1]; got != "-" {
		t.Fatalf("expected prompt via stdin (trailing \"-\"), got last arg %q", got)
	}
	if strings.Contains(strings.Join(args, " "), "prompt goes to stdin") {
		t.Errorf("prompt must not leak into argv (stdin transport)")
	}
	sshArgs := nodepool.BuildRemoteSSHArgs(be, "codex", args, []string{"CODEX_API_KEY=QTlmb2s="})
	joined := strings.Join(sshArgs, " ")
	// With proper quote nesting each inner element appears as '\''...'\''.
	for _, frag := range []string{
		"cd \"/home/admin/ws\"", "exec env", "CODEX_API_KEY=",
		`model_reasoning_effort="ultra"`, "--json",
	} {
		if !strings.Contains(joined, frag) {
			t.Errorf("expected %q to survive SSH quoting, got: %s", frag, joined)
		}
	}
	// Exactly one sh -c, and the whole inner command is its single argument
	// (the tail of the string closes the wrapper quote).
	if strings.Count(joined, "sh -c ") != 1 || !strings.HasSuffix(joined, "'") {
		t.Errorf("expected one sh -c wrapping the whole command, got: %s", joined)
	}
}

func TestTrackThreadIDRegistersPoolBinding(t *testing.T) {
	be := &nodepool.Backend{Name: "n2", Host: "admin@localhost"}
	p := &nodepool.Pool{Backends: []*nodepool.Backend{be}}
	cs, err := newCodexSession(context.Background(), "codex", nil, "/tmp", "m", "high", "yolo", "", "", nil, "", "", "", be, p)
	if err != nil {
		t.Fatalf("newCodexSession: %v", err)
	}
	cs.trackThreadID("thread-42")
	if got := p.Bound("thread-42"); got != be {
		t.Errorf("expected thread-42 bound to n2, got %v", got)
	}
	if cs.CurrentSessionID() != "thread-42" {
		t.Errorf("expected session id thread-42, got %q", cs.CurrentSessionID())
	}
}
