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
	for range 3 {
		mock.ExpectExec("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	}
	store, err := newSessionStore(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	id := newSessionID()
	if len(id) != 32 {
		t.Fatalf("session id should be 32 hex chars, got %d", len(id))
	}
	mock.ExpectExec("INSERT INTO sessions").WithArgs(id, "Account A").WillReturnResult(sqlmock.NewResult(1, 1))
	if err := store.insert(ctx, id, "Account A"); err != nil {
		t.Fatal(err)
	}

	rows := sqlmock.NewRows([]string{"id", "name", "jid", "webhook", "chatwoot"}).
		AddRow(id, "Account A", "", "", "")
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

	mock.ExpectExec("DELETE FROM sessions").WithArgs(id).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
