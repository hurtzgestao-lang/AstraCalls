package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"time"
)

type sessionRow struct {
	ID               string
	Name             string
	JID              string
	Webhook          string
	Chatwoot         string
	TenantKey        string
	AccountID        int
	InboxID          int
	IdempotencyKey   string
	PairingExpiresAt *time.Time
}

type SessionCreateParams struct {
	Name             string
	TenantKey        string
	AccountID        int
	InboxID          int
	IdempotencyKey   string
	PairingExpiresAt *time.Time
}

type sessionStore struct{ db *sql.DB }

// newSessionStore cria a tabela de config das sessões no banco PRINCIPAL.
// (O store do whatsmeow de cada sessão fica em um banco separado — ver db.go.)
func newSessionStore(ctx context.Context, db *sql.DB) (*sessionStore, error) {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS sessions (
		id         TEXT PRIMARY KEY,
		name       TEXT NOT NULL,
		jid        TEXT,
		webhook    TEXT,
		chatwoot   TEXT,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`)
	if err != nil {
		return nil, err
	}
	// migração p/ bancos antigos (Postgres aceita IF NOT EXISTS no ADD COLUMN)
	_, _ = db.ExecContext(ctx, `ALTER TABLE sessions ADD COLUMN IF NOT EXISTS webhook TEXT`)
	_, _ = db.ExecContext(ctx, `ALTER TABLE sessions ADD COLUMN IF NOT EXISTS chatwoot TEXT`)
	_, _ = db.ExecContext(ctx, `ALTER TABLE sessions ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()`)
	_, _ = db.ExecContext(ctx, `ALTER TABLE sessions ADD COLUMN IF NOT EXISTS tenant_key TEXT`)
	_, _ = db.ExecContext(ctx, `ALTER TABLE sessions ADD COLUMN IF NOT EXISTS account_id INTEGER`)
	_, _ = db.ExecContext(ctx, `ALTER TABLE sessions ADD COLUMN IF NOT EXISTS inbox_id INTEGER`)
	_, _ = db.ExecContext(ctx, `ALTER TABLE sessions ADD COLUMN IF NOT EXISTS idempotency_key TEXT`)
	_, _ = db.ExecContext(ctx, `ALTER TABLE sessions ADD COLUMN IF NOT EXISTS pairing_expires_at TIMESTAMPTZ`)
	if _, err = db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS index_sessions_on_tenant_key
		ON sessions(tenant_key) WHERE tenant_key IS NOT NULL AND tenant_key <> ''`); err != nil {
		return nil, err
	}
	if _, err = db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS index_sessions_on_idempotency_key
		ON sessions(idempotency_key) WHERE idempotency_key IS NOT NULL AND idempotency_key <> ''`); err != nil {
		return nil, err
	}
	return &sessionStore{db: db}, nil
}

func newSessionID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *sessionStore) list(ctx context.Context) ([]sessionRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, COALESCE(jid, ''), COALESCE(webhook, ''), COALESCE(chatwoot, ''),
		COALESCE(tenant_key, ''), COALESCE(account_id, 0), COALESCE(inbox_id, 0),
		COALESCE(idempotency_key, ''), pairing_expires_at FROM sessions ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []sessionRow
	for rows.Next() {
		var r sessionRow
		var pairingExpiresAt sql.NullTime
		if err := rows.Scan(&r.ID, &r.Name, &r.JID, &r.Webhook, &r.Chatwoot, &r.TenantKey,
			&r.AccountID, &r.InboxID, &r.IdempotencyKey, &pairingExpiresAt); err != nil {
			return nil, err
		}
		if pairingExpiresAt.Valid {
			r.PairingExpiresAt = &pairingExpiresAt.Time
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *sessionStore) insert(ctx context.Context, id string, params SessionCreateParams) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO sessions
		(id, name, jid, tenant_key, account_id, inbox_id, idempotency_key, pairing_expires_at)
		VALUES ($1,$2,NULL,$3,$4,$5,$6,$7)`, id, params.Name, nullableString(params.TenantKey),
		nullableInt(params.AccountID), nullableInt(params.InboxID), nullableString(params.IdempotencyKey), params.PairingExpiresAt)
	return err
}

func (s *sessionStore) findByIdempotency(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", nil
	}

	var id string
	err := s.db.QueryRowContext(ctx, `SELECT id FROM sessions WHERE idempotency_key = $1`, key).Scan(&id)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return id, err
}

func nullableInt(value int) any {
	if value == 0 {
		return nil
	}
	return value
}

func (s *sessionStore) setJID(ctx context.Context, id, jid string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sessions SET jid = $1,
		pairing_expires_at = CASE WHEN $1 = '' THEN pairing_expires_at ELSE NULL END WHERE id = $2`, jid, id)
	return err
}

func (s *sessionStore) setWebhook(ctx context.Context, id, url string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sessions SET webhook = $1 WHERE id = $2`, url, id)
	return err
}

func (s *sessionStore) setChatwoot(ctx context.Context, id, cfgJSON string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sessions SET chatwoot = $1 WHERE id = $2`, cfgJSON, id)
	return err
}

func (s *sessionStore) claimTenant(ctx context.Context, id, tenantKey string, accountID, inboxID int) (bool, error) {
	result, err := s.db.ExecContext(ctx, `UPDATE sessions
		SET tenant_key=$2, account_id=$3, inbox_id=$4
		WHERE id=$1 AND (
			(COALESCE(tenant_key, '')='' AND COALESCE(account_id, 0)=0 AND COALESCE(inbox_id, 0)=0)
			OR (tenant_key=$2 AND account_id=$3 AND inbox_id=$4)
		)`, id, tenantKey, accountID, inboxID)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	return rows == 1, err
}

func (s *sessionStore) delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE id = $1`, id)
	return err
}
