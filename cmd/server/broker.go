package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"
)

var (
	errOperatorBusy         = errors.New("operator already on a call")
	errSessionCapacity      = errors.New("session call capacity reached")
	errGlobalCapacity       = errors.New("global call capacity reached")
	errIdempotencyForbidden = errors.New("idempotency key belongs to another operator")
)

type CallStatus string

const (
	StatusStarting  CallStatus = "starting"
	StatusRinging   CallStatus = "ringing"
	StatusConnected CallStatus = "connected"
	StatusEnded     CallStatus = "ended"
)

type CallRecord struct {
	SessionID      string         `json:"sessionId"`
	CallID         string         `json:"callId"`
	Owner          *string        `json:"owner"`
	Direction      string         `json:"direction"`
	Peer           string         `json:"peer"`
	Phone          string         `json:"phone,omitempty"`
	StartedAt      int64          `json:"startedAt"`
	ConnectedAt    *int64         `json:"connectedAt,omitempty"`
	Status         CallStatus     `json:"status"`
	EndedAt        *int64         `json:"endedAt,omitempty"`
	EndReason      string         `json:"endReason,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
	IdempotencyKey string         `json:"-"`
}

type AuthSnapshot struct {
	State  string `json:"state"`
	Paired bool   `json:"paired"`
	QR     string `json:"qr,omitempty"`
}

type SessionInfo struct {
	ID               string     `json:"id"`
	Name             string     `json:"name"`
	JID              string     `json:"jid"`
	State            string     `json:"state"`
	Paired           bool       `json:"paired"`
	QR               string     `json:"qr,omitempty"`
	TenantKey        string     `json:"tenant_key,omitempty"`
	AccountID        int        `json:"account_id,omitempty"`
	InboxID          int        `json:"inbox_id,omitempty"`
	PairingExpiresAt *time.Time `json:"pairing_expires_at,omitempty"`
}

type subscriber struct {
	clientID string
	ch       chan []byte
}

type Broker struct {
	mu      sync.RWMutex
	subs    map[*subscriber]struct{}
	calls   map[string]*CallRecord
	history []CallRecord
	store   *runtimeStore

	SnapshotFn func() []any
}

func (b *Broker) setRuntimeStore(store *runtimeStore) {
	b.mu.Lock()
	b.store = store
	b.mu.Unlock()
}

func (b *Broker) runtimeStore() *runtimeStore {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.store
}

func NewBroker() *Broker {
	return &Broker{
		subs:  map[*subscriber]struct{}{},
		calls: map[string]*CallRecord{},
	}
}

func (b *Broker) subscribe(clientID string) *subscriber {
	s := &subscriber{clientID: clientID, ch: make(chan []byte, 32)}
	b.mu.Lock()
	b.subs[s] = struct{}{}
	b.mu.Unlock()
	return s
}

func (b *Broker) unsubscribe(s *subscriber) {
	b.mu.Lock()
	delete(b.subs, s)
	b.mu.Unlock()
	close(s.ch)
}

func (b *Broker) broadcast(ev any) {
	data, err := json.Marshal(ev)
	if err != nil {
		return
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for s := range b.subs {
		select {
		case s.ch <- data:
		default:
		}
	}
}

func (b *Broker) emitAuthState(sessionID string, a AuthSnapshot) {
	b.broadcast(map[string]any{
		"type": "auth-state", "sessionId": sessionID,
		"paired": a.Paired, "state": a.State, "qr": a.QR,
	})
}

func (b *Broker) emitSessionList(sessions []SessionInfo) {
	b.broadcast(map[string]any{"type": "session-list", "sessions": sessions})
}

func (b *Broker) emitSessionQR(sessionID, qr string) {
	b.broadcast(map[string]any{"type": "session-qr", "sessionId": sessionID, "qr": qr})
}

func (b *Broker) upsertCall(r CallRecord) {
	b.mu.Lock()
	cp := r
	b.calls[r.CallID] = &cp
	store := b.store
	b.mu.Unlock()
	if store != nil {
		if err := store.upsertCall(context.Background(), r); err != nil {
			store.log.Error("persisting call failed", "call_id", r.CallID, "err", err)
		}
	}
	b.broadcastCall(r)
}

func (b *Broker) reserveOutgoingCall(r CallRecord, sessionLimit, globalLimit int) (*CallRecord, bool, string, error) {
	b.mu.Lock()
	for _, call := range b.calls {
		if r.IdempotencyKey != "" && call.SessionID == r.SessionID && call.IdempotencyKey == r.IdempotencyKey {
			if call.Owner == nil || r.Owner == nil || *call.Owner != *r.Owner {
				b.mu.Unlock()
				return nil, false, call.CallID, errIdempotencyForbidden
			}
			copy := *call
			b.mu.Unlock()
			return &copy, true, "", nil
		}
	}
	if b.store != nil && r.IdempotencyKey != "" {
		existing, err := b.store.callByIdempotency(context.Background(), r.SessionID, r.IdempotencyKey)
		if err != nil {
			b.mu.Unlock()
			return nil, false, "", err
		}
		if existing != nil {
			if existing.Owner == nil || r.Owner == nil || *existing.Owner != *r.Owner {
				b.mu.Unlock()
				return nil, false, existing.CallID, errIdempotencyForbidden
			}
			b.mu.Unlock()
			return existing, true, "", nil
		}
	}

	activeTotal := 0
	activeSession := 0
	for _, call := range b.calls {
		if call.Status == StatusEnded {
			continue
		}
		activeTotal++
		if call.SessionID == r.SessionID {
			activeSession++
		}
		if call.Owner != nil && r.Owner != nil && *call.Owner == *r.Owner {
			conflictID := call.CallID
			b.mu.Unlock()
			return nil, false, conflictID, errOperatorBusy
		}
	}
	if sessionLimit > 0 && activeSession >= sessionLimit {
		b.mu.Unlock()
		return nil, false, "", errSessionCapacity
	}
	if globalLimit > 0 && activeTotal >= globalLimit {
		b.mu.Unlock()
		return nil, false, "", errGlobalCapacity
	}

	if b.store != nil {
		reserved, reused, err := b.store.reserveCall(context.Background(), r)
		if err != nil {
			b.mu.Unlock()
			return nil, false, "", err
		}
		if reused {
			if reserved.Owner == nil || r.Owner == nil || *reserved.Owner != *r.Owner {
				b.mu.Unlock()
				return nil, false, reserved.CallID, errIdempotencyForbidden
			}
			b.mu.Unlock()
			return reserved, true, "", nil
		}
	}
	copy := r
	b.calls[r.CallID] = &copy
	b.mu.Unlock()
	b.broadcastCall(r)
	return &copy, false, "", nil
}

func (b *Broker) broadcastCall(r CallRecord) {
	b.broadcastCallList()
	b.broadcast(map[string]any{
		"type": "call-status", "sessionId": r.SessionID, "id": r.CallID, "owner": r.Owner,
		"status": r.Status, "peer": r.Peer, "startedAt": r.StartedAt,
	})
}

func (b *Broker) getCall(id string) (*CallRecord, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	c, ok := b.calls[id]
	if !ok {
		return nil, false
	}
	cp := *c
	return &cp, true
}

func (b *Broker) setOwner(id, owner string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.calls[id]
	if !ok {
		return false
	}
	if c.Owner != nil && *c.Owner != owner {
		return false
	}
	c.Owner = &owner
	return true
}

func (b *Broker) ownerMatches(id, owner string) bool {
	if owner == "" {
		return false
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	c, ok := b.calls[id]
	return ok && c.Owner != nil && *c.Owner == owner
}

func (b *Broker) ownerActiveCall(owner string) string {
	if owner == "" {
		return ""
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for id, c := range b.calls {
		if c.Owner != nil && *c.Owner == owner && c.Status != StatusEnded {
			return id
		}
	}
	return ""
}

func (b *Broker) activeCallCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	count := 0
	for _, call := range b.calls {
		if call.Status != StatusEnded {
			count++
		}
	}
	return count
}

func (b *Broker) claimOrAuthorize(id, owner string) bool {
	if owner == "" {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	call, ok := b.calls[id]
	if !ok {
		return false
	}
	if call.Owner != nil && *call.Owner != owner {
		return false
	}
	call.Owner = &owner
	return true
}

func (b *Broker) endCall(id, reason string) {
	b.mu.Lock()
	c, ok := b.calls[id]
	if !ok {
		b.mu.Unlock()
		return
	}
	now := time.Now().UnixMilli()
	c.Status = StatusEnded
	c.EndedAt = &now
	c.EndReason = reason
	ended := *c
	delete(b.calls, id)
	b.history = append(b.history, ended)
	owner := c.Owner
	sessionID := c.SessionID
	store := b.store
	b.mu.Unlock()
	if store != nil {
		if err := store.upsertCall(context.Background(), ended); err != nil {
			store.log.Error("persisting ended call failed", "call_id", id, "err", err)
		}
	}

	b.broadcast(map[string]any{
		"type": "call-ended", "sessionId": sessionID, "id": id, "owner": owner, "reason": reason, "endedAt": now,
	})
	b.broadcastCallList()
}

func (b *Broker) broadcastCallList() {
	b.mu.RLock()
	list := make([]CallRecord, 0, len(b.calls))
	for _, c := range b.calls {
		list = append(list, *c)
	}
	b.mu.RUnlock()
	b.broadcast(map[string]any{"type": "call-list", "calls": list})
}

func (b *Broker) emitIncoming(sessionID, id, peer string) {
	b.broadcast(map[string]any{
		"type": "incoming", "sessionId": sessionID, "id": id, "peer": peer, "offeredAt": time.Now().UnixMilli(),
	})
}

func (b *Broker) emitIncomingClaimed(sessionID, id, owner string) {
	b.broadcast(map[string]any{"type": "incoming-claimed", "sessionId": sessionID, "id": id, "owner": owner})
}

func (b *Broker) historyRows(sessionID string, limit int) []CallRecord {
	b.mu.RLock()
	store := b.store
	b.mu.RUnlock()
	if store != nil {
		if rows, err := store.historyRows(context.Background(), sessionID, limit); err == nil {
			return rows
		} else {
			store.log.Error("loading call history failed", "session", sessionID, "err", err)
		}
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	rows := make([]CallRecord, 0, limit)
	for i := len(b.history) - 1; i >= 0 && len(rows) < limit; i-- {
		if sessionID == "" || b.history[i].SessionID == sessionID {
			rows = append(rows, b.history[i])
		}
	}
	return rows
}

func (b *Broker) serveSSE(w http.ResponseWriter, r *http.Request, clientID string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	sub := b.subscribe(clientID)
	defer b.unsubscribe(sub)

	if b.SnapshotFn != nil {
		for _, ev := range b.SnapshotFn() {
			writeSSE(w, flusher, ev)
		}
	}
	b.broadcastCallList()

	keepalive := time.NewTicker(20 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case data := <-sub.ch:
			if _, err := w.Write(append(append([]byte("data: "), data...), '\n', '\n')); err != nil {
				return
			}
			flusher.Flush()
		case <-keepalive.C:
			w.Write([]byte(": ping\n\n"))
			flusher.Flush()
		}
	}
}

func writeSSE(w http.ResponseWriter, f http.Flusher, ev any) {
	data, _ := json.Marshal(ev)
	w.Write(append(append([]byte("data: "), data...), '\n', '\n'))
	f.Flush()
}
