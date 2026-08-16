package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

var (
	errSessionNotFound       = errors.New("session not found")
	errSessionTenantConflict = errors.New("session belongs to another tenant")
)

type SessionManager struct {
	appCtx   context.Context
	db       *dbProvider
	broker   *Broker
	store    *sessionStore
	waLogger waLog.Logger
	log      *slog.Logger
	maxCalls int

	mu       sync.RWMutex
	createMu sync.Mutex
	sessions map[string]*Session
	order    []string
}

func newSessionManager(ctx context.Context, db *dbProvider, broker *Broker, store *sessionStore, waLogger waLog.Logger, log *slog.Logger, maxCalls int) *SessionManager {
	return &SessionManager{
		appCtx:   ctx,
		db:       db,
		broker:   broker,
		store:    store,
		waLogger: waLogger,
		log:      log,
		maxCalls: maxCalls,
		sessions: map[string]*Session{},
	}
}

func (m *SessionManager) register(s *Session) {
	m.mu.Lock()
	m.sessions[s.id] = s
	m.order = append(m.order, s.id)
	m.mu.Unlock()
}

func (m *SessionManager) unregister(id string) {
	m.mu.Lock()
	delete(m.sessions, id)
	for i, x := range m.order {
		if x == id {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
	m.mu.Unlock()
}

func (m *SessionManager) sessionForChatwootAccount(accountID int) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, s := range m.sessions {
		if c := s.getChatwoot(); c.valid() && c.AccountID == accountID {
			return s
		}
	}
	return nil
}

// sessionForChatwootInbox: sessão amarrada à conta E à caixa (inbox) específica.
// Usada para que o widget só apareça/ligue na caixa que tem WhatsApp conectado.
func (m *SessionManager) sessionForChatwootInbox(accountID, inboxID int) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, s := range m.sessions {
		if c := s.getChatwoot(); c.valid() && c.AccountID == accountID && c.InboxID == inboxID {
			return s
		}
	}
	return nil
}

func (m *SessionManager) jidAvailable(sessionID, jid string) bool {
	if jid == "" {
		return false
	}
	candidate, err := types.ParseJID(jid)
	if err != nil {
		return false
	}
	canonicalJID := sessionInfoJID(candidate)
	m.mu.RLock()
	defer m.mu.RUnlock()
	for id, session := range m.sessions {
		if id == sessionID || session.client.Store.ID == nil {
			continue
		}
		if sessionInfoJID(*session.client.Store.ID) == canonicalJID {
			return false
		}
	}
	return true
}

func (m *SessionManager) Get(id string) (*Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[id]
	return s, ok
}

func (m *SessionManager) ClaimTenant(ctx context.Context, id, tenantKey string, accountID, inboxID int) (*Session, error) {
	m.createMu.Lock()
	defer m.createMu.Unlock()

	session, ok := m.Get(id)
	if !ok {
		return nil, errSessionNotFound
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.tenantKey == tenantKey && session.accountID == accountID && session.inboxID == inboxID {
		return session, nil
	}
	if session.tenantKey != "" || session.accountID != 0 || session.inboxID != 0 {
		return nil, errSessionTenantConflict
	}
	claimed, err := m.store.claimTenant(ctx, id, tenantKey, accountID, inboxID)
	if err != nil || !claimed {
		return nil, errSessionTenantConflict
	}
	session.tenantKey = tenantKey
	session.accountID = accountID
	session.inboxID = inboxID
	return session, nil
}

func (m *SessionManager) infos() []SessionInfo {
	m.mu.RLock()
	ordered := make([]*Session, 0, len(m.order))
	for _, id := range m.order {
		if s, ok := m.sessions[id]; ok {
			ordered = append(ordered, s)
		}
	}
	m.mu.RUnlock()
	out := make([]SessionInfo, 0, len(ordered))
	for _, s := range ordered {
		out = append(out, s.info())
	}
	return out
}

func (m *SessionManager) snapshotEvents() []any {
	return []any{map[string]any{"type": "session-list", "sessions": m.infos()}}
}

func (m *SessionManager) Restore(ctx context.Context) error {
	rows, err := m.store.list(ctx)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.JID != "" {
			if _, err := types.ParseJID(row.JID); err != nil {
				m.log.Warn("dropping session with unparseable jid", "session", row.ID, "jid", row.JID)
				_ = m.db.dropSessionDB(ctx, row.ID)
				_ = m.store.delete(ctx, row.ID)
				continue
			}
		}
		container, db, err := m.db.openSessionContainer(ctx, row.ID)
		if err != nil {
			m.log.Error("opening session database failed", "session", row.ID, "err", err)
			continue
		}
		device := container.NewDevice()
		if row.JID != "" {
			device, err = container.GetFirstDevice(ctx)
			if err != nil || device == nil || device.ID == nil {
				m.log.Warn("dropping session with no stored device", "session", row.ID, "jid", row.JID, "err", err)
				_ = db.Close()
				_ = m.db.dropSessionDB(ctx, row.ID)
				_ = m.store.delete(ctx, row.ID)
				continue
			}
		}
		client := whatsmeow.NewClient(device, m.waLogger)
		s := newSession(m, row.ID, row.Name, client)
		s.tenantKey = row.TenantKey
		s.accountID = row.AccountID
		s.inboxID = row.InboxID
		s.pairingExpiresAt = row.PairingExpiresAt
		s.waContainer = container
		s.waDB = db
		s.setWebhook(parseStoredWebhook(row.Webhook))
		if row.Chatwoot != "" {
			var cfg ChatwootConfig
			if json.Unmarshal([]byte(row.Chatwoot), &cfg) == nil {
				s.setChatwoot(cfg)
			}
		}
		m.register(s)
		if row.JID == "" {
			if err := s.startPairing(ctx); err != nil {
				m.log.Error("restored session pairing failed", "session", row.ID, "err", err)
			}
			continue
		}
		if err := s.connect(ctx); err != nil {
			m.log.Error("session connect failed", "session", row.ID, "err", err)
		}
	}
	m.broker.emitSessionList(m.infos())
	m.log.Info("sessions restored", "count", len(m.infos()))
	return nil
}

func (m *SessionManager) Create(params SessionCreateParams) (string, error) {
	m.createMu.Lock()
	defer m.createMu.Unlock()
	if params.IdempotencyKey != "" {
		if id, err := m.store.findByIdempotency(m.appCtx, params.IdempotencyKey); err != nil {
			return "", err
		} else if id != "" {
			if _, ok := m.Get(id); ok {
				return id, nil
			}
			return "", fmt.Errorf("idempotent session %s is not loaded", id)
		}
	}
	if params.TenantKey != "" {
		m.mu.RLock()
		for _, existing := range m.sessions {
			if existing.tenantKey == params.TenantKey {
				id := existing.id
				m.mu.RUnlock()
				return id, nil
			}
		}
		m.mu.RUnlock()
	}
	id := newSessionID()
	if params.Name == "" {
		params.Name = "Session"
	}
	if err := m.store.insert(m.appCtx, id, params); err != nil {
		return "", err
	}
	container, db, err := m.db.openSessionContainer(m.appCtx, id)
	if err != nil {
		_ = m.store.delete(m.appCtx, id)
		_ = m.db.dropSessionDB(m.appCtx, id)
		return "", fmt.Errorf("create session store: %w", err)
	}
	device := container.NewDevice()
	client := whatsmeow.NewClient(device, m.waLogger)
	s := newSession(m, id, params.Name, client)
	s.tenantKey = params.TenantKey
	s.accountID = params.AccountID
	s.inboxID = params.InboxID
	s.pairingExpiresAt = params.PairingExpiresAt
	s.waContainer = container
	s.waDB = db
	m.register(s)
	m.broker.emitSessionList(m.infos())
	if err := s.startPairing(m.appCtx); err != nil {
		m.log.Error("start pairing failed", "session", id, "err", err)
		return "", fmt.Errorf("start pairing: %w", err)
	}
	m.log.Info("session created", "session", id, "name", params.Name, "tenant", params.TenantKey)
	return id, nil
}

func (m *SessionManager) Delete(ctx context.Context, id string) error {
	s, ok := m.Get(id)
	if !ok {
		return fmt.Errorf("no session %s", id)
	}
	if s.client.Store.ID != nil {
		if err := s.client.Logout(ctx); err != nil {
			m.log.Warn("logout failed; deleting locally", "session", id, "err", err)
		}
	}
	s.client.Disconnect()
	s.teardownAllCalls()
	// o store da sessão é um banco inteiro só dela: fecha a conexão e derruba.
	if s.waDB != nil {
		_ = s.waDB.Close()
	}
	if err := m.db.dropSessionDB(ctx, id); err != nil {
		m.log.Warn("drop session database failed", "session", id, "err", err)
	}
	m.unregister(id)
	_ = m.store.delete(ctx, id)
	m.broker.emitSessionList(m.infos())
	m.log.Info("session deleted", "session", id)
	return nil
}

func (m *SessionManager) Logout(ctx context.Context, id string) error {
	s, ok := m.Get(id)
	if !ok {
		return fmt.Errorf("no session %s", id)
	}
	if s.client.Store.ID != nil {
		if err := s.client.Logout(ctx); err != nil {
			m.log.Warn("logout failed", "session", id, "err", err)
		}
	}
	s.replaceClient(whatsmeow.NewClient(s.waContainer.NewDevice(), m.waLogger))
	_ = m.store.setJID(ctx, id, "")
	s.setAuth(AuthSnapshot{State: "logged_out", Paired: false})
	m.log.Info("session disconnected", "session", id)
	return nil
}

func (m *SessionManager) Pair(id string) error {
	s, ok := m.Get(id)
	if !ok {
		return fmt.Errorf("no session %s", id)
	}
	if s.client.Store.ID != nil {
		return fmt.Errorf("session already paired")
	}
	s.replaceClient(whatsmeow.NewClient(s.waContainer.NewDevice(), m.waLogger))
	if err := s.startPairing(m.appCtx); err != nil {
		return fmt.Errorf("start pairing: %w", err)
	}
	m.broker.emitSessionList(m.infos())
	m.log.Info("session re-pairing", "session", id)
	return nil
}

func (m *SessionManager) disconnectAll() {
	m.mu.RLock()
	all := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		all = append(all, s)
	}
	m.mu.RUnlock()
	for _, s := range all {
		s.shutdown()
	}
}

func (m *SessionManager) cleanupExpiredPairings(ctx context.Context) {
	now := time.Now()
	m.mu.RLock()
	ids := []string{}
	for _, session := range m.sessions {
		info := session.info()
		if !info.Paired && info.PairingExpiresAt != nil && !info.PairingExpiresAt.After(now) {
			ids = append(ids, session.id)
		}
	}
	m.mu.RUnlock()
	for _, id := range ids {
		if err := m.Delete(ctx, id); err != nil {
			m.log.Warn("expired pairing cleanup failed", "session", id, "err", err)
		}
	}
}
