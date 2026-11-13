package main

import (
	"context"
	"log/slog"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
)

// WhatsApp refuses clients whose web version is too old (405 "client outdated"), and the
// version built into whatsmeow goes stale within a few months. The server therefore uses the
// version web.whatsapp.com currently publishes, refreshed periodically, and keeps the previous
// one when the lookup fails.
const waVersionRefreshInterval = 6 * time.Hour

func refreshWAVersion(ctx context.Context, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	latest, err := whatsmeow.GetLatestVersion(ctx, nil)
	if err != nil {
		log.Warn("could not fetch the current WhatsApp Web version, keeping the previous one", "err", err)
		return
	}
	store.SetWAVersion(*latest)
	log.Info("WhatsApp Web version set", "version", latest.String())
}

// keepWAVersionFresh sets the current version before any session connects and refreshes it
// in the background until ctx ends.
func keepWAVersionFresh(ctx context.Context, log *slog.Logger) {
	refreshWAVersion(ctx, log)
	go func() {
		ticker := time.NewTicker(waVersionRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				refreshWAVersion(ctx, log)
			}
		}
	}()
}
