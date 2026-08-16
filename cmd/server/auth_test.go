package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWithAuthRejectsAPIKeyInQueryString(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	handler := withAuth(next, "strong-key")

	queryRequest := httptest.NewRequest(http.MethodGet, "/api/events?apiKey=strong-key", nil)
	queryResponse := httptest.NewRecorder()
	handler.ServeHTTP(queryResponse, queryRequest)
	if queryResponse.Code != http.StatusUnauthorized {
		t.Fatalf("query credential returned %d, want %d", queryResponse.Code, http.StatusUnauthorized)
	}

	headerRequest := httptest.NewRequest(http.MethodGet, "/api/events", nil)
	headerRequest.Header.Set("X-API-Key", "strong-key")
	headerResponse := httptest.NewRecorder()
	handler.ServeHTTP(headerResponse, headerRequest)
	if headerResponse.Code != http.StatusNoContent {
		t.Fatalf("header credential returned %d, want %d", headerResponse.Code, http.StatusNoContent)
	}
}

func TestCallsCanBeDisabledWithoutDisablingHealth(t *testing.T) {
	t.Setenv("ASTRACALLS_ENABLED", "false")
	response := httptest.NewRecorder()
	if requireCallsEnabled(response) {
		t.Fatal("calls should be disabled")
	}
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}
