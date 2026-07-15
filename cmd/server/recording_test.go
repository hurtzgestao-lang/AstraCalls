package main

import (
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestCallRecorderFinalizeProducesOgg(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available")
	}

	recorder, err := newCallRecorder("call-test", slog.Default())
	if err != nil {
		t.Fatalf("newCallRecorder: %v", err)
	}

	started := time.Now().Add(-1500 * time.Millisecond)
	recorder.recordingStartedAt = &started
	recorder.AddAgentFrame(make([]float32, 960))
	recorder.AddPeerFrame(make([]float32, 960))

	info, filePath, err := recorder.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if info == nil {
		t.Fatal("expected recording info")
	}
	if filePath == "" {
		t.Fatal("expected recording path")
	}
	if info.ContentType != "audio/ogg" {
		t.Fatalf("unexpected content type: %s", info.ContentType)
	}
	if info.DurationMs < 1000 {
		t.Fatalf("unexpected duration: %d", info.DurationMs)
	}
	if info.SizeBytes <= 0 {
		t.Fatalf("unexpected file size: %d", info.SizeBytes)
	}

	t.Cleanup(func() {
		_ = os.RemoveAll(filepath.Dir(filePath))
	})
}

func TestCallRecorderSkipsShortRecordings(t *testing.T) {
	recorder, err := newCallRecorder("call-short", slog.Default())
	if err != nil {
		t.Fatalf("newCallRecorder: %v", err)
	}

	info, filePath, err := recorder.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if info != nil || filePath != "" {
		t.Fatalf("expected no artifact for short recording, got %#v %q", info, filePath)
	}
}
