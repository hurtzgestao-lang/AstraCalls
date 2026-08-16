package main

import (
	"context"
	"database/sql"
	"log/slog"
	"path/filepath"
	"testing"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
	_ "modernc.org/sqlite"
)

func newTestManager(t *testing.T) (*SessionManager, *sqlstore.Container) {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "mgr_test.db")
	db, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	container := sqlstore.NewWithDB(db, "sqlite3", waLog.Noop)
	if err := container.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	manager := newSessionManager(ctx, nil, NewBroker(), nil, waLog.Noop, slog.Default(), 0)
	return manager, container
}

func addUnconnected(manager *SessionManager, container *sqlstore.Container, name string) *Session {
	id := newSessionID()
	client := whatsmeow.NewClient(container.NewDevice(), waLog.Noop)
	session := newSession(manager, id, name, client)
	manager.register(session)
	return session
}

func TestSessionManagerRegistry(t *testing.T) {
	manager, container := newTestManager(t)

	if len(manager.infos()) != 0 {
		t.Fatal("expected no sessions when empty")
	}

	first := addUnconnected(manager, container, "Account A")
	second := addUnconnected(manager, container, "Account B")

	infos := manager.infos()
	if len(infos) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(infos))
	}
	if infos[0].Name != "Account A" || infos[1].Name != "Account B" {
		t.Fatalf("registration order not preserved: %+v", infos)
	}
	if infos[0].Paired {
		t.Fatal("unconnected session should not report paired")
	}
	if got, ok := manager.Get(first.id); !ok || got != first {
		t.Fatal("Get did not return registered session")
	}

	manager.unregister(second.id)
	if _, ok := manager.Get(second.id); ok {
		t.Fatal("second session should be gone after unregister")
	}
	if len(manager.infos()) != 1 {
		t.Fatal("expected 1 session after unregister")
	}
}
