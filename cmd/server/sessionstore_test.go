package main

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSessionStoreRoundtrip(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS sessions").WillReturnResult(sqlmock.NewResult(0, 0))
	for range 8 {
		mock.ExpectExec("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	}
	for range 2 {
		mock.ExpectExec("CREATE UNIQUE INDEX IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	}
	store, err := newSessionStore(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	id := newSessionID()
	if len(id) != 32 {
		t.Fatalf("session id should be 32 hex chars, got %d", len(id))
	}
	params := SessionCreateParams{Name: "Account A"}
	mock.ExpectExec("INSERT INTO sessions").
		WithArgs(id, "Account A", nil, nil, nil, nil, nil).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := store.insert(ctx, id, params); err != nil {
		t.Fatal(err)
	}

	rows := sqlmock.NewRows([]string{"id", "name", "jid", "webhook", "chatwoot", "tenant_key", "account_id", "inbox_id", "idempotency_key", "pairing_expires_at"}).
		AddRow(id, "Account A", "", "", "", "", 0, 0, "", nil)
	mock.ExpectQuery("SELECT id, name").WillReturnRows(rows)
	listed, err := store.list(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].ID != id || listed[0].Name != "Account A" || listed[0].JID != "" {
		t.Fatalf("unexpected rows after insert: %+v", listed)
	}

	mock.ExpectExec("UPDATE sessions SET jid").WithArgs("5511999999999:1@s.whatsapp.net", id).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.setJID(ctx, id, "5511999999999:1@s.whatsapp.net"); err != nil {
		t.Fatal(err)
	}

	mock.ExpectExec("UPDATE sessions").
		WithArgs(id, "account:115:inbox:153", 115, 153).
		WillReturnResult(sqlmock.NewResult(0, 1))
	claimed, err := store.claimTenant(ctx, id, "account:115:inbox:153", 115, 153)
	if err != nil || !claimed {
		t.Fatalf("legacy session tenant claim failed: claimed=%v err=%v", claimed, err)
	}

	mock.ExpectExec("DELETE FROM sessions").WithArgs(id).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
