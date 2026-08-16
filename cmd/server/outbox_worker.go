package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func (s *server) startWorkers(ctx context.Context) {
	go s.runOutboxWorker(ctx)
	go s.runPairingCleanup(ctx)
}

func (s *server) runPairingCleanup(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sessions.cleanupExpiredPairings(ctx)
		}
	}
}

func (s *server) runOutboxWorker(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.deliverDueOutbox(ctx)
		}
	}
}

func (s *server) deliverDueOutbox(ctx context.Context) {
	rows, err := s.runtime.dueOutbox(ctx, 20)
	if err != nil {
		s.log.Error("loading webhook outbox failed", "err", err)
		return
	}
	for _, row := range rows {
		if err := s.deliverOutboxRow(row); err != nil {
			attempts := row.Attempts + 1
			_ = s.runtime.markOutboxFailed(ctx, row.ID, attempts, err)
			s.log.Warn("webhook delivery failed", "event_id", row.ID, "event", row.Event, "attempts", attempts, "err", err)
			continue
		}
		if err := s.runtime.markOutboxDelivered(ctx, row.ID); err != nil {
			s.log.Error("marking webhook delivered failed", "event_id", row.ID, "err", err)
			continue
		}
		if row.FilePath != "" {
			_ = os.RemoveAll(filepath.Dir(row.FilePath))
		}
	}
	if err := s.runtime.expireOutbox(ctx); err != nil {
		s.log.Error("expiring webhook outbox failed", "err", err)
	}
}

func (s *server) deliverOutboxRow(row outboxRow) error {
	session, ok := s.sessions.Get(row.SessionID)
	if !ok {
		return fmt.Errorf("session %s is not loaded", row.SessionID)
	}
	if row.FilePath != "" {
		if row.FileInfo == nil {
			return fmt.Errorf("recording metadata is missing")
		}
		return session.sendWebhookFile(row.Config, row.Payload, row.FilePath, row.FileInfo)
	}
	return session.sendWebhook(row.Config, row.Timestamp, row.Payload)
}
