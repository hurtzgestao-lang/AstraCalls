package main

import (
	"net/url"
	"testing"
)

func TestPostgresDatabaseNamesAndDSN(t *testing.T) {
	provider := &dbProvider{
		ns:   "hurtz_calls",
		base: mustParseURL(t, "postgres://astracalls:secret@127.0.0.1:55433/postgres?sslmode=disable"),
	}

	if got := provider.mainDBName(); got != "hurtz_calls_main" {
		t.Fatalf("unexpected main database name: %q", got)
	}
	if got := provider.sessionDBName("abc123"); got != "hurtz_calls_abc123" {
		t.Fatalf("unexpected session database name: %q", got)
	}
	want := "postgres://astracalls:secret@127.0.0.1:55433/hurtz_calls_abc123?sslmode=disable"
	if got := provider.dsnFor(provider.sessionDBName("abc123")); got != want {
		t.Fatalf("unexpected session DSN: %q", got)
	}
}

func mustParseURL(t *testing.T, value string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestQuoteIdentEscapesDoubleQuotes(t *testing.T) {
	if got := quoteIdent(`hurtz"calls`); got != `"hurtz""calls"` {
		t.Fatalf("unexpected quoted identifier: %q", got)
	}
}
