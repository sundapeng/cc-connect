package claudecode

import (
	"context"
	"os"
	"testing"
	"strings"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

// TestClaudePoolE2E drives the REAL agent path — New(opts with hosts) →
// StartSession → Send — against a live SSH backend, exactly as cc-connect
// runs it. The config env carries URL-valued variables, the shape that broke
// remote spawn twice (missing `env`, broken quote nesting). Guarded: set
// MAOMAO_E2E=1 plus the MAOMAO_E2E_* vars.
func TestClaudePoolE2E(t *testing.T) {
	if os.Getenv("MAOMAO_E2E") == "" {
		t.Skip("set MAOMAO_E2E=1 plus MAOMAO_E2E_{KEY,KH} to run")
	}
	key := os.Getenv("MAOMAO_E2E_KEY")
	kh := os.Getenv("MAOMAO_E2E_KH")
	if key == "" || kh == "" {
		t.Fatal("MAOMAO_E2E_KEY and MAOMAO_E2E_KH are required")
	}
	opts := map[string]any{
		"work_dir": "/tmp",
		"cc_data_dir": "/tmp/maomao-e2e-ccdata",
		"cmd":      "claude --fallback-model claude-opus-4-6",
		"model":    "claude-opus-4-6",
		"mode":     "bypassPermissions",
		"env": map[string]any{
			"IS_SANDBOX":       "1",
			"NOELLE_BASE_URL":  "https://dlf-noelle.aliyun-inc.com",
			"NOELLE_TOKEN":     "e2e-dummy",
			"ANTHROPIC_MODEL":  "claude-opus-5",
		},
		"hosts": []map[string]any{{
			"name":     "e2e",
			"host":     "admin@localhost",
			"port":     2024,
			"ssh_key":  key,
			"ssh_opts": []string{"-o", "UserKnownHostsFile=" + kh},
			"work_dir": "/home/admin/ws-maomao1",
			"cmd":      "/home/admin/node-tools/bin/claude",
		}},
	}
	agent, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	s, err := agent.StartSession(ctx, "")
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	defer s.Close()
	if err := s.Send("Reply with exactly one word: pong", "e2e-msg-1", nil, nil); err != nil {
		t.Fatalf("Send: %v", err)
	}

	deadline := time.After(150 * time.Second)
	for {
		select {
		case evt, ok := <-s.Events():
			if !ok {
				t.Fatal("event channel closed before a text reply")
			}
			if evt.Type == core.EventText && evt.Content != "" {
				t.Logf("reply: %s", evt.Content)
				if !strings.Contains(evt.Content, "pong") {
					t.Fatalf("expected pong, got: %s", evt.Content)
				}
				return
			}
			if evt.Type == core.EventError {
				t.Fatalf("agent error: %v: %s", evt.Error, evt.Content)
			}
		case <-deadline:
			t.Fatal("timed out waiting for the reply")
		}
	}
}
