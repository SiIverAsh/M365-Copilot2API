package web

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"m365-copilot2api/internal/auth"
	"m365-copilot2api/internal/chathub"
)

// The production transport cooldown is 30s. It must fit inside the resolver's
// recovery budget instead of immediately turning a transient write timeout
// into a 503 that requires a manual rerun.
func TestResolveWaitsForTransportRecovery(t *testing.T) {
	s := &Server{tokens: testAccountFiles(t), accountPool: newAccountHealth(), accountConcurrency: newAccountConcurrency()}
	for _, acc := range s.tokens.List() {
		s.accountPool.MarkFailure(acc.ID, &chathub.DialError{Kind: "WS_WRITE_TIMEOUT"}, 0)
	}
	until := s.accountPool.EarliestRecovery()
	ctx, cancel := context.WithTimeout(context.Background(), resolveTransportWaitWindow+time.Second)
	defer cancel()
	start := time.Now()
	acc, err := s.resolveAccountCtx(ctx, "")
	if err != nil {
		t.Fatalf("resolveAccountCtx returned %v after %v; want automatic transport recovery", err, time.Since(start))
	}
	if acc.ID == "" {
		t.Fatal("no account selected after recovery")
	}
	if time.Now().Before(until) {
		t.Fatal("returned before the transport cooldown expired")
	}
	if time.Since(start) > resolveTransportWaitWindow+time.Second {
		t.Fatal("transport recovery exceeded the 30s wait budget")
	}
}

func TestResolveTransportWaitHonorsCancellation(t *testing.T) {
	s := &Server{tokens: testAccountFiles(t), accountPool: newAccountHealth(), accountConcurrency: newAccountConcurrency()}
	for _, acc := range s.tokens.List() {
		s.accountPool.MarkFailure(acc.ID, &chathub.DialError{Kind: "WS_WRITE_TIMEOUT"}, 0)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := s.resolveAccountCtx(ctx, "")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline exceeded", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("account selection did not stop promptly on cancellation")
	}
}

// When every account is momentarily cooling down, account selection must wait
// for the earliest recovery and retry rather than returning a rate-limit error
// that forces the client to implement its own backoff.
func TestResolveWaitsForImminentRecovery(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("M365_SESSION_CACHE", filepath.Join(dir, "sessions.json"))
	t.Setenv("M365_CONVERSATION_CACHE", filepath.Join(dir, "conversations.json"))
	t.Setenv("M365_USER_SESSION_CACHE", filepath.Join(dir, "users.json"))
	t.Setenv("M365_DATA_DIR", dir)

	store, err := auth.OpenStore(filepath.Join(dir, "accounts.json"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := store.Upsert(auth.TokenSet{
		HomeOID:      "u-1",
		Email:        "one@example.com",
		AccessToken:  "tok",
		RefreshToken: "r",
		ExpiresAt:    time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	s := &Server{
		tokens:             store,
		accountPool:        newAccountHealth(),
		accountConcurrency: newAccountConcurrency(),
		chat:               chathub.NewClient(),
	}

	const cooldown = 250 * time.Millisecond
	s.accountPool.MarkCall("u-1")
	s.accountPool.MarkFailure("u-1", &UpstreamHTTPError{Status: 429}, 0)
	s.accountPool.mu.Lock()
	s.accountPool.cooldown["u-1"] = time.Now().Add(cooldown)
	s.accountPool.mu.Unlock()

	start := time.Now()
	tok, err := s.resolveAccount("")
	if err != nil {
		t.Fatalf("resolveAccount returned %v; want it to wait for recovery", err)
	}
	if tok.ID != "u-1" {
		t.Fatalf("resolved %q, want u-1", tok.ID)
	}
	if elapsed := time.Since(start); elapsed < cooldown {
		t.Fatalf("returned after %v; expected to wait at least %v", elapsed, cooldown)
	}
}
