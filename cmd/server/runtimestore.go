package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

const outboxRetention = 7 * 24 * time.Hour

type runtimeStore struct {
	db  *sql.DB
	log *slog.Logger
}

type outboxRow struct {
	ID        string
	SessionID string
	Event     string
	Timestamp int64
	Payload   []byte
	Config    WebhookConfig
	FilePath  string
	FileInfo  *RecordingInfo
	Attempts  int
	CreatedAt time.Time
}

func newRuntimeStore(ctx context.Context, db *sql.DB, log *slog.Logger) (*runtimeStore, error) {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS calls (
			call_id TEXT PRIMARY KEY,
			session_id TEXT NOT NULL,
			owner TEXT,
			direction TEXT NOT NULL,
			peer TEXT,
			phone TEXT,
			started_at BIGINT NOT NULL,
			connected_at BIGINT,
			status TEXT NOT NULL,
			ended_at BIGINT,
			end_reason TEXT,
			metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
			idempotency_key TEXT,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`ALTER TABLE calls ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb`,
		`ALTER TABLE calls ADD COLUMN IF NOT EXISTS idempotency_key TEXT`,
		`ALTER TABLE calls ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`,
		`CREATE UNIQUE INDEX IF NOT EXISTS index_calls_on_session_idempotency
			ON calls(session_id, idempotency_key) WHERE idempotency_key IS NOT NULL AND idempotency_key <> ''`,
		`CREATE INDEX IF NOT EXISTS index_calls_on_session_status ON calls(session_id, status)`,
		`CREATE TABLE IF NOT EXISTS webhook_outbox (
			id TEXT PRIMARY KEY,
			session_id TEXT NOT NULL,
			event TEXT NOT NULL,
			event_timestamp BIGINT NOT NULL,
			payload JSONB NOT NULL,
			config JSONB NOT NULL,
			file_path TEXT,
			file_info JSONB,
			attempts INTEGER NOT NULL DEFAULT 0,
			next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			delivered_at TIMESTAMPTZ,
			expires_at TIMESTAMPTZ NOT NULL,
			last_error TEXT,
			claimed_until TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`ALTER TABLE webhook_outbox ADD COLUMN IF NOT EXISTS claimed_until TIMESTAMPTZ`,
		`CREATE INDEX IF NOT EXISTS index_webhook_outbox_due
			ON webhook_outbox(next_attempt_at) WHERE delivered_at IS NULL`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return nil, err
		}
	}
	return &runtimeStore{db: db, log: log}, nil
}

func newEventID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *runtimeStore) upsertCall(ctx context.Context, record CallRecord) error {
	metadata, _ := json.Marshal(record.Metadata)
	var owner any
	if record.Owner != nil {
		owner = *record.Owner
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO calls (
		call_id, session_id, owner, direction, peer, phone, started_at, connected_at,
		status, ended_at, end_reason, metadata, idempotency_key, updated_at
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,now())
	ON CONFLICT (call_id) DO UPDATE SET
		owner=EXCLUDED.owner, peer=EXCLUDED.peer, phone=EXCLUDED.phone,
		connected_at=EXCLUDED.connected_at, status=EXCLUDED.status,
		ended_at=EXCLUDED.ended_at, end_reason=EXCLUDED.end_reason,
		metadata=EXCLUDED.metadata, idempotency_key=COALESCE(calls.idempotency_key, EXCLUDED.idempotency_key),
		updated_at=now()`,
		record.CallID, record.SessionID, owner, record.Direction, record.Peer, record.Phone,
		record.StartedAt, record.ConnectedAt, record.Status, record.EndedAt, record.EndReason,
		string(metadata), record.IdempotencyKey)
	return err
}

func (s *runtimeStore) reserveCall(ctx context.Context, record CallRecord) (*CallRecord, bool, error) {
	metadata, _ := json.Marshal(record.Metadata)
	var owner any
	if record.Owner != nil {
		owner = *record.Owner
	}
	var insertedID string
	err := s.db.QueryRowContext(ctx, `INSERT INTO calls (
		call_id, session_id, owner, direction, peer, phone, started_at, connected_at,
		status, ended_at, end_reason, metadata, idempotency_key, updated_at
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,now())
	ON CONFLICT DO NOTHING RETURNING call_id`,
		record.CallID, record.SessionID, owner, record.Direction, record.Peer, record.Phone,
		record.StartedAt, record.ConnectedAt, record.Status, record.EndedAt, record.EndReason,
		string(metadata), record.IdempotencyKey).Scan(&insertedID)
	if err == nil {
		return &record, false, nil
	}
	if err != sql.ErrNoRows {
		return nil, false, err
	}
	if record.IdempotencyKey != "" {
		existing, lookupErr := s.callByIdempotency(ctx, record.SessionID, record.IdempotencyKey)
		if lookupErr != nil {
			return nil, false, lookupErr
		}
		if existing != nil {
			return existing, true, nil
		}
	}
	return nil, false, fmt.Errorf("call reservation conflicted without an idempotent match")
}

func (s *runtimeStore) callByIdempotency(ctx context.Context, sessionID, key string) (*CallRecord, error) {
	if key == "" {
		return nil, nil
	}
	row := s.db.QueryRowContext(ctx, `SELECT session_id, call_id, owner, direction, peer, phone,
		started_at, connected_at, status, ended_at, end_reason, metadata, idempotency_key
		FROM calls WHERE session_id=$1 AND idempotency_key=$2`, sessionID, key)
	record, err := scanCall(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return record, err
}

func (s *runtimeStore) historyRows(ctx context.Context, sessionID string, limit int) ([]CallRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT session_id, call_id, owner, direction, peer, phone,
		started_at, connected_at, status, ended_at, end_reason, metadata, idempotency_key
		FROM calls WHERE ($1='' OR session_id=$1) ORDER BY started_at DESC LIMIT $2`, sessionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]CallRecord, 0, limit)
	for rows.Next() {
		record, scanErr := scanCall(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, *record)
	}
	return out, rows.Err()
}

type callScanner interface {
	Scan(dest ...any) error
}

func scanCall(row callScanner) (*CallRecord, error) {
	var record CallRecord
	var owner sql.NullString
	var connectedAt, endedAt sql.NullInt64
	var metadata []byte
	if err := row.Scan(&record.SessionID, &record.CallID, &owner, &record.Direction, &record.Peer,
		&record.Phone, &record.StartedAt, &connectedAt, &record.Status, &endedAt,
		&record.EndReason, &metadata, &record.IdempotencyKey); err != nil {
		return nil, err
	}
	if owner.Valid {
		record.Owner = &owner.String
	}
	if connectedAt.Valid {
		record.ConnectedAt = &connectedAt.Int64
	}
	if endedAt.Valid {
		record.EndedAt = &endedAt.Int64
	}
	_ = json.Unmarshal(metadata, &record.Metadata)
	return &record, nil
}

func (s *runtimeStore) reconcileInterruptedCalls(ctx context.Context) error {
	now := time.Now().UnixMilli()
	_, err := s.db.ExecContext(ctx, `UPDATE calls SET status='ended', ended_at=$1,
		end_reason='server_restart', updated_at=now() WHERE status <> 'ended'`, now)
	return err
}

func (s *runtimeStore) enqueueWebhook(ctx context.Context, id, sessionID, event string, timestamp int64, payload []byte,
	config WebhookConfig, filePath string, info *RecordingInfo) (string, error) {
	if id == "" {
		id = newEventID()
	}
	configJSON, _ := json.Marshal(config)
	var infoJSON any
	if info != nil {
		encoded, _ := json.Marshal(info)
		infoJSON = string(encoded)
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO webhook_outbox
		(id, session_id, event, event_timestamp, payload, config, file_path, file_info, expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (id) DO NOTHING`,
		id, sessionID, event, timestamp, string(payload), string(configJSON), nullableString(filePath), infoJSON, time.Now().Add(outboxRetention))
	return id, err
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func (s *runtimeStore) dueOutbox(ctx context.Context, limit int) ([]outboxRow, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `SELECT id, session_id, event, event_timestamp, payload,
		config, COALESCE(file_path,''), file_info, attempts, created_at
		FROM webhook_outbox
		WHERE delivered_at IS NULL AND next_attempt_at <= now() AND expires_at > now()
		  AND (claimed_until IS NULL OR claimed_until <= now())
		ORDER BY next_attempt_at, created_at LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []outboxRow{}
	for rows.Next() {
		var row outboxRow
		var configJSON, infoJSON []byte
		if err := rows.Scan(&row.ID, &row.SessionID, &row.Event, &row.Timestamp, &row.Payload,
			&configJSON, &row.FilePath, &infoJSON, &row.Attempts, &row.CreatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(configJSON, &row.Config); err != nil {
			return nil, err
		}
		if len(infoJSON) > 0 {
			row.FileInfo = &RecordingInfo{}
			if err := json.Unmarshal(infoJSON, row.FileInfo); err != nil {
				return nil, err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE webhook_outbox SET claimed_until = now() + interval '10 minutes' WHERE id = $1`, row.ID); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *runtimeStore) markOutboxDelivered(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE webhook_outbox SET delivered_at=now(), claimed_until=NULL, last_error=NULL WHERE id=$1`, id)
	return err
}

func (s *runtimeStore) markOutboxFailed(ctx context.Context, id string, attempts int, deliveryErr error) error {
	delay := time.Duration(1<<minInt(attempts, 8)) * time.Second
	if delay > 5*time.Minute {
		delay = 5 * time.Minute
	}
	_, err := s.db.ExecContext(ctx, `UPDATE webhook_outbox SET attempts=$2, last_error=$3,
		next_attempt_at=$4, claimed_until=NULL WHERE id=$1`, id, attempts, deliveryErr.Error(), time.Now().Add(delay))
	return err
}

func (s *runtimeStore) expireOutbox(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `DELETE FROM webhook_outbox
		WHERE expires_at <= now()
		RETURNING id, COALESCE(file_path,''), delivered_at IS NOT NULL`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, filePath string
		var delivered bool
		if err := rows.Scan(&id, &filePath, &delivered); err != nil {
			return err
		}
		if filePath != "" {
			_ = os.RemoveAll(filepath.Dir(filePath))
		}
		if !delivered {
			s.log.Error("webhook outbox expired without delivery", "event_id", id)
		}
	}
	return rows.Err()
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (s *runtimeStore) pendingOutboxCount(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM webhook_outbox WHERE delivered_at IS NULL`).Scan(&count)
	return count, err
}

func (r outboxRow) String() string {
	return fmt.Sprintf("%s/%s", r.SessionID, r.Event)
}
