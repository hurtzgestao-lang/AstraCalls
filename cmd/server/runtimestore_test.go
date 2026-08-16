package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestExpireOutboxRemovesDeliveredAndUndeliveredRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	deliveredDir := filepath.Join(t.TempDir(), "delivered")
	undeliveredDir := filepath.Join(t.TempDir(), "undelivered")
	for _, dir := range []string{deliveredDir, undeliveredDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "call.ogg"), []byte("recording"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	query := regexp.QuoteMeta(`DELETE FROM webhook_outbox
		WHERE expires_at <= now()
		RETURNING id, COALESCE(file_path,''), delivered_at IS NOT NULL`)
	mock.ExpectQuery(query).WillReturnRows(
		sqlmock.NewRows([]string{"id", "file_path", "delivered"}).
			AddRow("delivered-event", filepath.Join(deliveredDir, "call.ogg"), true).
			AddRow("expired-event", filepath.Join(undeliveredDir, "call.ogg"), false),
	)

	var logs bytes.Buffer
	store := &runtimeStore{db: db, log: slog.New(slog.NewTextHandler(&logs, nil))}
	if err := store.expireOutbox(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(deliveredDir); !os.IsNotExist(err) {
		t.Fatalf("delivered recording directory still exists: %v", err)
	}
	if _, err := os.Stat(undeliveredDir); !os.IsNotExist(err) {
		t.Fatalf("undelivered recording directory still exists: %v", err)
	}
	if strings.Contains(logs.String(), "delivered-event") {
		t.Fatalf("delivered event was logged as expired: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "expired-event") {
		t.Fatalf("undelivered expiration was not logged: %s", logs.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
