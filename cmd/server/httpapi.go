package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
	"wacalls/internal/voip/signaling"

	"go.mau.fi/whatsmeow/types"
)

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.HandleFunc("GET /api/config", s.handleConfig)
	mux.HandleFunc("GET /api/sessions", s.handleSessionList)
	mux.HandleFunc("POST /api/sessions", s.handleSessionCreate)
	mux.HandleFunc("GET /api/sessions/{sid}/calls", s.handleSessionCalls)
	mux.HandleFunc("DELETE /api/sessions/{sid}", s.handleSessionDelete)
	mux.HandleFunc("PATCH /api/sessions/{sid}/tenant", s.handleSessionTenantClaim)
	mux.HandleFunc("POST /api/sessions/{sid}/logout", s.handleSessionLogout)
	mux.HandleFunc("POST /api/sessions/{sid}/pair", s.handleSessionPair)
	mux.HandleFunc("POST /api/sessions/{sid}/calls", s.handleStartCall)
	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/webrtc", s.handleWebRTC)
	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/accept", s.handleAccept)
	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/reject", s.handleReject)
	mux.HandleFunc("DELETE /api/sessions/{sid}/calls/{id}", s.handleEndCall)
	mux.HandleFunc("GET /api/sessions/{sid}/history", s.handleHistory)

	if envBool("WACALLS_ENABLE_MESSAGES", true) {
		mux.HandleFunc("POST /api/sessions/{sid}/messages/text", s.handleSendText)
		mux.HandleFunc("POST /api/sessions/{sid}/messages/image", s.handleSendImage)
		mux.HandleFunc("POST /api/sessions/{sid}/messages/audio", s.handleSendAudio)
		mux.HandleFunc("POST /api/sessions/{sid}/messages/video", s.handleSendVideo)
		mux.HandleFunc("POST /api/sessions/{sid}/messages/document", s.handleSendDocument)
	}

	// Webhook por sessão (recebimento -> Chatwoot etc.)
	mux.HandleFunc("POST /api/sessions/{sid}/webhook", s.handleSetWebhook)
	mux.HandleFunc("GET /api/sessions/{sid}/webhook", s.handleGetWebhook)
	mux.HandleFunc("DELETE /api/sessions/{sid}/webhook", s.handleDeleteWebhook)

	if envBool("WACALLS_ENABLE_CHATWOOT_MESSAGES", true) {
		mux.HandleFunc("POST /api/sessions/{sid}/chatwoot", s.handleSetChatwoot)
		mux.HandleFunc("GET /api/sessions/{sid}/chatwoot", s.handleGetChatwoot)
		mux.HandleFunc("DELETE /api/sessions/{sid}/chatwoot", s.handleDeleteChatwoot)
		mux.HandleFunc("POST /api/sessions/{sid}/chatwoot/webhook", s.handleChatwootWebhook)
		mux.HandleFunc("GET /api/chatwoot/resolve", s.handleChatwootResolve)
	}

	mux.HandleFunc("GET /api/events", s.handleEvents)

	if s.staticDir != "" {
		if _, err := os.Stat(s.staticDir); err == nil {
			mux.Handle("/", http.FileServer(http.Dir(s.staticDir)))
		}
	}
	var handler http.Handler = mux
	if key := os.Getenv("WACALLS_API_KEY"); key != "" {
		handler = withAuth(handler, key)
	}
	return withCORS(handler, os.Getenv("WACALLS_ALLOWED_ORIGINS"))
}

func envBool(key string, defaultValue bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	return strings.EqualFold(value, "true") || value == "1"
}

func withCORS(h http.Handler, rawOrigins string) http.Handler {
	allowed := map[string]bool{}
	for _, origin := range strings.Split(rawOrigins, ",") {
		if origin = strings.TrimSpace(origin); origin != "" {
			allowed[origin] = true
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && !allowed[origin] {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "origin not allowed"})
			return
		}
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
		}
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Client-Id, X-API-Key")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func callsEnabled() bool {
	return envBool("ASTRACALLS_ENABLED", true)
}

func requireCallsEnabled(w http.ResponseWriter) bool {
	if callsEnabled() {
		return true
	}

	writeJSON(w, http.StatusServiceUnavailable, map[string]string{
		"code":    "ASTRACALLS_DISABLED",
		"message": "AstraCalls is temporarily disabled",
	})
	return false
}

// withAuth protege as rotas /api/* com uma API key enviada somente por header.
// Credenciais na query string vazam para historicos, proxies e logs.
// Os arquivos estáticos do painel permanecem públicos; toda a API é protegida.
func withAuth(h http.Handler, key string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		guarded := strings.HasPrefix(p, "/api/")
		if guarded {
			got := r.Header.Get("X-API-Key")
			if got != key {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}
		}
		h.ServeHTTP(w, r)
	})
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) handleReady(w http.ResponseWriter, r *http.Request) {
	if err := s.sessions.store.db.PingContext(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "sessions": len(s.sessions.infos())})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func clientID(r *http.Request) string {
	return strings.TrimSpace(r.Header.Get("X-Client-Id"))
}

func (s *server) sessionByID(w http.ResponseWriter, sid string) *Session {
	sess, ok := s.sessions.Get(sid)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such session"})
		return nil
	}
	return sess
}

func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	s.broker.serveSSE(w, r, clientID(r))
}

func (s *server) handleConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"maxCallsPerSession": s.sessions.maxCalls,
		"maxGlobalCalls":     s.maxGlobalCalls,
	})
}

func (s *server) handleSessionList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"sessions": s.sessions.infos()})
}

func (s *server) handleSessionCalls(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r.PathValue("sid"))
	if sess == nil {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"active":             sess.reg.count(),
		"maxCallsPerSession": s.sessions.maxCalls,
	})
}

func (s *server) handleSessionCreate(w http.ResponseWriter, r *http.Request) {
	if !requireCallsEnabled(w) {
		return
	}
	var body struct {
		Name           string `json:"name"`
		TenantKey      string `json:"tenant_key"`
		AccountID      int    `json:"account_id"`
		InboxID        int    `json:"inbox_id"`
		IdempotencyKey string `json:"idempotency_key"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	name := strings.TrimSpace(body.Name)
	if name == "" {
		name = "Session"
	}
	tenantKey := strings.TrimSpace(body.TenantKey)
	if body.AccountID != 0 || body.InboxID != 0 || tenantKey != "" {
		if body.AccountID <= 0 || body.InboxID <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "account_id and inbox_id are required for tenant sessions"})
			return
		}
		expected := fmt.Sprintf("account:%d:inbox:%d", body.AccountID, body.InboxID)
		if tenantKey == "" {
			tenantKey = expected
		} else if tenantKey != expected {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tenant_key does not match account and inbox"})
			return
		}
	}
	var expiresAt *time.Time
	if tenantKey != "" {
		expires := time.Now().Add(10 * time.Minute)
		expiresAt = &expires
	}
	id, err := s.sessions.Create(SessionCreateParams{
		Name: name, TenantKey: tenantKey, AccountID: body.AccountID, InboxID: body.InboxID,
		IdempotencyKey: strings.TrimSpace(body.IdempotencyKey), PairingExpiresAt: expiresAt,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	session, _ := s.sessions.Get(id)
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "session": session.info()})
}

func (s *server) handleSessionDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.sessions.Delete(r.Context(), r.PathValue("sid")); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleSessionTenantClaim(w http.ResponseWriter, r *http.Request) {
	var body struct {
		TenantKey string `json:"tenant_key"`
		AccountID int    `json:"account_id"`
		InboxID   int    `json:"inbox_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.AccountID <= 0 || body.InboxID <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "INVALID_TENANT", "error": "account_id and inbox_id are required"})
		return
	}
	expected := fmt.Sprintf("account:%d:inbox:%d", body.AccountID, body.InboxID)
	if body.TenantKey != "" && body.TenantKey != expected {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "INVALID_TENANT", "error": "tenant_key does not match account and inbox"})
		return
	}
	session, err := s.sessions.ClaimTenant(r.Context(), r.PathValue("sid"), expected, body.AccountID, body.InboxID)
	if err != nil {
		status := http.StatusConflict
		code := "TENANT_CONFLICT"
		if errors.Is(err, errSessionNotFound) {
			status = http.StatusNotFound
			code = "SESSION_NOT_FOUND"
		}
		writeJSON(w, status, map[string]string{"code": code, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": session.id, "session": session.info()})
}

func (s *server) handleSessionLogout(w http.ResponseWriter, r *http.Request) {
	if err := s.sessions.Logout(r.Context(), r.PathValue("sid")); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleSessionPair(w http.ResponseWriter, r *http.Request) {
	if !requireCallsEnabled(w) {
		return
	}
	if err := s.sessions.Pair(r.PathValue("sid")); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleStartCall(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doStartCall(sess, w, r)
	}
}

func (s *server) handleWebRTC(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doWebRTC(sess, w, r)
	}
}

func (s *server) handleAccept(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doAccept(sess, w, r)
	}
}

func (s *server) handleReject(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doReject(sess, w, r)
	}
}

func (s *server) handleEndCall(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doEndCall(sess, w, r)
	}
}

func (s *server) handleHistory(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		writeJSON(w, http.StatusOK, map[string]any{"rows": s.broker.historyRows(sess.id, 50)})
	}
}

func (s *server) doStartCall(sess *Session, w http.ResponseWriter, r *http.Request) {
	if !requireCallsEnabled(w) {
		return
	}
	if sess.client.Store.ID == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not paired"})
		return
	}
	var body struct {
		Phone          string         `json:"phone"`
		DurationMs     int            `json:"duration_ms"`
		Record         bool           `json:"record"`
		IdempotencyKey string         `json:"idempotency_key"`
		Metadata       map[string]any `json:"metadata"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.Phone) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "phone required"})
		return
	}
	owner := clientID(r)
	if owner == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "OWNER_REQUIRED", "error": "client owner required"})
		return
	}
	body.IdempotencyKey = strings.TrimSpace(body.IdempotencyKey)
	peer := types.NewJID(normalizePhone(body.Phone), types.DefaultUserServer)

	shouldRecord := body.Record || sess.recordingEnabled()
	callID := signaling.GenerateCallID()
	record, reused, conflictID, reserveErr := s.broker.reserveOutgoingCall(CallRecord{
		SessionID: sess.id, CallID: callID, Owner: &owner, Direction: "outbound", Peer: peer.String(),
		Phone: peer.User, StartedAt: time.Now().UnixMilli(), Status: StatusStarting,
		Metadata: body.Metadata, IdempotencyKey: body.IdempotencyKey,
	}, s.sessions.maxCalls, s.maxGlobalCalls)
	if reserveErr != nil {
		switch {
		case errors.Is(reserveErr, errOperatorBusy):
			writeJSON(w, http.StatusConflict, map[string]string{"code": "OPERATOR_BUSY", "error": "operator already on a call", "call_id": conflictID})
		case errors.Is(reserveErr, errSessionCapacity):
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"code": "SESSION_CAPACITY", "error": "max concurrent calls"})
		case errors.Is(reserveErr, errGlobalCapacity):
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"code": "GLOBAL_CAPACITY", "error": "global call capacity reached"})
		case errors.Is(reserveErr, errIdempotencyForbidden):
			writeJSON(w, http.StatusForbidden, map[string]string{"code": "CALL_FORBIDDEN", "error": "call belongs to another operator"})
		default:
			s.log.Error("reserving call failed", "session", sess.id, "err", reserveErr)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"code": "CALL_RESERVATION_FAILED", "error": "unable to reserve call"})
		}
		return
	}
	if reused {
		writeJSON(w, http.StatusOK, map[string]any{"call": map[string]any{"callId": record.CallID, "status": record.Status}})
		return
	}
	err := sess.startOutgoing(r.Context(), callID, peer, false, shouldRecord)
	if err != nil {
		s.broker.endCall(callID, "start_failed")
		s.log.Error("starting call failed", "call_id", callID, "err", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"code": "CALL_START_FAILED", "error": "unable to start call", "call_id": callID})
		return
	}

	switch status := sess.waitOutgoingOffer(callID, 3*time.Second); status {
	case StatusEnded:
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": "O WhatsApp recusou a chamada antes de tocar. Tente por outro número ou faça o primeiro contato por mensagem.",
		})
		return
	case StatusRinging, StatusConnected:
		if record, ok := s.broker.getCall(callID); ok {
			record.Status = status
			s.broker.upsertCall(*record)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"call": map[string]string{"callId": callID}})
}

func (s *server) doWebRTC(sess *Session, w http.ResponseWriter, r *http.Request) {
	if !requireCallsEnabled(w) {
		return
	}
	callID := r.PathValue("id")
	if !s.broker.ownerMatches(callID, clientID(r)) {
		writeJSON(w, http.StatusForbidden, map[string]string{"code": "CALL_FORBIDDEN", "error": "call belongs to another operator"})
		return
	}
	ac, ok := sess.reg.get(callID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such call"})
		return
	}
	var body struct {
		SDPOffer string `json:"sdp_offer"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.SDPOffer == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sdp_offer required"})
		return
	}
	bridge, answer, err := NewBridge(body.SDPOffer, s.log)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	browserOpus, ocErr := media.NewOpusCodec(48000, 960)
	if ocErr != nil {
		s.log.Warn("browser Opus codec unavailable — call audio disabled", "err", ocErr)
		browserOpus = nil
	}
	bridge.OnBrowserRTP = func(payload []byte) {
		if browserOpus == nil {
			return
		}
		pcm48, err := browserOpus.Decode(payload)
		if err != nil {
			return
		}
		pcm16 := media.Downsample48to16(pcm48)
		if ac.recorder != nil {
			ac.recorder.AddAgentFrame(pcm16)
		}
		ac.cm.FeedCapturedPCM(pcm16)
	}
	bridge.OnTerminalICE = func() {
		go sess.terminateCall(callID, core.EndCallReasonUserEnded)
	}
	sess.setBridge(callID, bridge, browserOpus)
	writeJSON(w, http.StatusOK, map[string]string{"sdp_answer": answer})
}

func (s *server) doAccept(sess *Session, w http.ResponseWriter, r *http.Request) {
	if !requireCallsEnabled(w) {
		return
	}
	id := r.PathValue("id")
	ac, ok := sess.reg.get(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such call"})
		return
	}
	owner := clientID(r)
	if owner == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "OWNER_REQUIRED", "error": "client owner required"})
		return
	}
	if other := s.broker.ownerActiveCall(owner); other != "" && other != id {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "operator already on a call"})
		return
	}
	if !s.broker.setOwner(id, owner) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "claimed by another client"})
		return
	}
	s.broker.emitIncomingClaimed(sess.id, id, owner)
	if record, found := s.broker.getCall(id); found {
		record.Owner = &owner
		s.broker.upsertCall(*record)
		sess.dispatchCallWebhook("call.claimed", *record)
	}
	if err := ac.cm.AcceptCall(r.Context(), id); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"call": map[string]string{"callId": id}})
}

func (s *server) doReject(sess *Session, w http.ResponseWriter, r *http.Request) {
	if !requireCallsEnabled(w) {
		return
	}
	id := r.PathValue("id")
	if !s.broker.claimOrAuthorize(id, clientID(r)) {
		writeJSON(w, http.StatusForbidden, map[string]string{"code": "CALL_FORBIDDEN", "error": "call belongs to another operator"})
		return
	}
	if ac, ok := sess.reg.get(id); ok {
		_ = ac.cm.RejectCall(r.Context(), id, core.EndCallReasonDeclined)
	}
	sess.dispatchCallEnded(id, string(core.EndCallReasonDeclined))
	sess.removeCall(id)
	s.broker.endCall(id, string(core.EndCallReasonDeclined))
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) doEndCall(sess *Session, w http.ResponseWriter, r *http.Request) {
	if !requireCallsEnabled(w) {
		return
	}
	id := r.PathValue("id")
	if !s.broker.ownerMatches(id, clientID(r)) {
		writeJSON(w, http.StatusForbidden, map[string]string{"code": "CALL_FORBIDDEN", "error": "call belongs to another operator"})
		return
	}
	if ac, ok := sess.reg.get(id); ok {
		_ = ac.cm.EndCall(r.Context(), core.EndCallReasonUserEnded)
	}
	sess.dispatchCallEnded(id, string(core.EndCallReasonUserEnded))
	sess.removeCall(id)
	s.broker.endCall(id, string(core.EndCallReasonUserEnded))
	w.WriteHeader(http.StatusNoContent)
}

func normalizePhone(p string) string {
	p = strings.TrimSpace(p)
	p = strings.TrimPrefix(p, "+")
	var b strings.Builder
	for _, c := range p {
		if c >= '0' && c <= '9' {
			b.WriteRune(c)
		}
	}
	return b.String()
}
