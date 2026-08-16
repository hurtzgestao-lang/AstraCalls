package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/encoding/protojson"
)

var webhookClient = &http.Client{Timeout: 10 * time.Second}

type WebhookConfig struct {
	URL    string   `json:"url"`
	Secret string   `json:"secret,omitempty"`
	Events []string `json:"events,omitempty"`
}

type CallWebhookData struct {
	CallID      string         `json:"call_id"`
	Direction   string         `json:"direction"`
	Phone       string         `json:"phone"`
	Peer        string         `json:"peer"`
	Status      string         `json:"status"`
	Owner       *string        `json:"owner,omitempty"`
	StartedAt   int64          `json:"started_at"`
	ConnectedAt *int64         `json:"connected_at,omitempty"`
	EndedAt     *int64         `json:"ended_at,omitempty"`
	EndReason   string         `json:"end_reason,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

func parseStoredWebhook(raw string) WebhookConfig {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return WebhookConfig{}
	}
	var config WebhookConfig
	if json.Unmarshal([]byte(raw), &config) == nil && config.URL != "" {
		return config
	}
	return WebhookConfig{URL: raw}
}

func (c WebhookConfig) storedValue() string {
	if strings.TrimSpace(c.URL) == "" {
		return ""
	}
	data, _ := json.Marshal(c)
	return string(data)
}

func (c WebhookConfig) matches(event string) bool {
	if len(c.Events) == 0 {
		return true
	}
	for _, pattern := range c.Events {
		pattern = strings.TrimSpace(pattern)
		if pattern == event || (strings.HasSuffix(pattern, ".*") && strings.HasPrefix(event, strings.TrimSuffix(pattern, "*"))) {
			return true
		}
	}
	return false
}

// dispatchWebhook envia um evento para a URL de webhook da sessão (se houver),
// de forma assíncrona. Formato: {session, event, timestamp, data}.
func (s *Session) dispatchWebhook(event string, data any) {
	config := s.getWebhook()
	if config.URL == "" || !config.matches(event) {
		return
	}
	timestamp := time.Now().UnixMilli()
	eventID := newEventID()
	body, err := json.Marshal(map[string]any{
		"event_id":  eventID,
		"session":   s.id,
		"event":     event,
		"timestamp": timestamp,
		"data":      data,
	})
	if err != nil {
		return
	}
	if s.mgr != nil && s.mgr.broker != nil {
		if store := s.mgr.broker.runtimeStore(); store != nil {
			if _, err := store.enqueueWebhook(context.Background(), eventID, s.id, event, timestamp, body, config, "", nil); err != nil {
				s.log.Error("queueing webhook failed", "event", event, "event_id", eventID, "err", err)
			}
			return
		}
	}
	go func() {
		for attempt := 1; attempt <= 3; attempt++ {
			if s.sendWebhook(config, timestamp, body) == nil {
				return
			}
			if attempt < 3 {
				time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
			}
		}
		s.log.Warn("webhook delivery failed", "event", event, "attempts", 3)
	}()
}

func (s *Session) sendWebhook(config WebhookConfig, timestamp int64, body []byte) error {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, config.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-AstraCalls-Timestamp", fmt.Sprintf("%d", timestamp))
	if config.Secret != "" {
		mac := hmac.New(sha256.New, []byte(config.Secret))
		_, _ = mac.Write([]byte(fmt.Sprintf("%d.", timestamp)))
		_, _ = mac.Write(body)
		req.Header.Set("X-AstraCalls-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}
	resp, err := webhookClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func (s *Session) dispatchCallWebhook(event string, record CallRecord) {
	phone := digitsOnly(record.Phone)
	if phone == "" {
		if jid, err := types.ParseJID(record.Peer); err == nil {
			phone = s.realPhone(jid)
		} else {
			phone = digitsOnly(record.Peer)
		}
	}
	s.dispatchWebhook(event, CallWebhookData{
		CallID: record.CallID, Direction: record.Direction, Phone: phone, Peer: record.Peer,
		Status: string(record.Status), Owner: record.Owner, StartedAt: record.StartedAt,
		ConnectedAt: record.ConnectedAt, EndedAt: record.EndedAt, EndReason: record.EndReason, Metadata: record.Metadata,
	})
}

func (s *Session) dispatchCallEnded(callID, reason string) {
	record, ok := s.mgr.broker.getCall(callID)
	if !ok || record.Status == StatusEnded {
		return
	}
	now := time.Now().UnixMilli()
	record.Status = StatusEnded
	record.EndedAt = &now
	record.EndReason = reason
	s.mgr.broker.upsertCall(*record)
	s.dispatchCallWebhook("call.ended", *record)
}

// summarizeMessage extrai os campos úteis de uma mensagem recebida e inclui o
// payload bruto (protojson) para integrações que precisem de mais detalhes.
func summarizeMessage(evt *events.Message) map[string]any {
	info := evt.Info
	out := map[string]any{
		"id":        info.ID,
		"chat":      info.Chat.String(),
		"sender":    info.Sender.String(),
		"fromMe":    info.IsFromMe,
		"pushName":  info.PushName,
		"timestamp": info.Timestamp.UnixMilli(),
		"isGroup":   info.IsGroup,
		"type":      messageType(evt.Message),
		"text":      messageText(evt.Message),
	}
	if raw, err := protojson.Marshal(evt.Message); err == nil {
		out["raw"] = json.RawMessage(raw)
	}
	return out
}

func messageText(m *waE2E.Message) string {
	switch {
	case m.GetConversation() != "":
		return m.GetConversation()
	case m.GetExtendedTextMessage() != nil:
		return m.GetExtendedTextMessage().GetText()
	case m.GetImageMessage() != nil:
		return m.GetImageMessage().GetCaption()
	case m.GetVideoMessage() != nil:
		return m.GetVideoMessage().GetCaption()
	}
	return ""
}

func messageType(m *waE2E.Message) string {
	switch {
	case m.GetConversation() != "" || m.GetExtendedTextMessage() != nil:
		return "text"
	case m.GetImageMessage() != nil:
		return "image"
	case m.GetAudioMessage() != nil:
		return "audio"
	case m.GetVideoMessage() != nil:
		return "video"
	case m.GetDocumentMessage() != nil:
		return "document"
	case m.GetStickerMessage() != nil:
		return "sticker"
	case m.GetLocationMessage() != nil:
		return "location"
	case m.GetContactMessage() != nil:
		return "contact"
	}
	return "unknown"
}
