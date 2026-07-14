package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseStoredWebhookSupportsLegacyURL(t *testing.T) {
	config := parseStoredWebhook("https://crm.example/webhooks/astracalls")
	if config.URL != "https://crm.example/webhooks/astracalls" {
		t.Fatalf("unexpected URL: %q", config.URL)
	}
	if config.Secret != "" || len(config.Events) != 0 {
		t.Fatal("legacy URL must not invent signing configuration")
	}
}

func TestWebhookConfigMatchesCallWildcard(t *testing.T) {
	config := WebhookConfig{Events: []string{"call.*"}}
	if !config.matches("call.connected") {
		t.Fatal("call.* must match call.connected")
	}
	if config.matches("message.received") {
		t.Fatal("call.* must not match message.received")
	}
}

func TestSendWebhookSignsTimestampAndBody(t *testing.T) {
	const secret = "test-secret"
	const timestamp = int64(1720000000000)
	body := []byte(`{"event":"call.connected"}`)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		gotBody, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(gotBody) != string(body) {
			t.Fatalf("unexpected body: %s", gotBody)
		}
		if got := r.Header.Get("X-AstraCalls-Timestamp"); got != "1720000000000" {
			t.Fatalf("unexpected timestamp header: %q", got)
		}

		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write([]byte("1720000000000."))
		_, _ = mac.Write(body)
		want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		if got := r.Header.Get("X-AstraCalls-Signature"); got != want {
			t.Fatalf("unexpected signature: %q", got)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	session := &Session{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := session.sendWebhook(WebhookConfig{URL: server.URL, Secret: secret}, timestamp, body); err != nil {
		t.Fatal(err)
	}
}
