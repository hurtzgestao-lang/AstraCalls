package main

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/types"
)

const (
	recordingSampleRate    = 16000
	recordingMinDuration   = time.Second
	recordingMaxDuration   = 2 * time.Hour
	recordingMaxTempBytes  = 300 << 20
	recordingOpusBitrate   = "32k"
	recordingTempDirectory = "/var/lib/astracalls/recordings"
)

var webhookUploadClient = &http.Client{Timeout: 2 * time.Minute}

type RecordingInfo struct {
	Format      string `json:"format"`
	ContentType string `json:"content_type"`
	Channels    string `json:"channels"`
	DurationMs  int64  `json:"duration_ms"`
	SizeBytes   int64  `json:"size_bytes"`
	SHA256      string `json:"sha256"`
	FileName    string `json:"file_name"`
}

type RecordingWebhookData struct {
	CallID         string         `json:"call_id"`
	Direction      string         `json:"direction"`
	Phone          string         `json:"phone"`
	Peer           string         `json:"peer"`
	Status         string         `json:"status"`
	Owner          *string        `json:"owner,omitempty"`
	StartedAt      int64          `json:"started_at"`
	ConnectedAt    *int64         `json:"connected_at,omitempty"`
	EndedAt        *int64         `json:"ended_at,omitempty"`
	EndReason      string         `json:"end_reason,omitempty"`
	Recording      *RecordingInfo `json:"recording,omitempty"`
	RecordingError string         `json:"recording_error,omitempty"`
}

type CallRecorder struct {
	callID string
	log    *slog.Logger

	startedAt time.Time
	agentFile *os.File
	peerFile  *os.File
	agentBuf  *bufio.Writer
	peerBuf   *bufio.Writer
	workDir   string

	mu                 sync.Mutex
	agentSamples       int64
	peerSamples        int64
	recordingStartedAt *time.Time
	recordingEndedAt   *time.Time
	finalized          bool
}

func newCallRecorder(callID string, logger *slog.Logger) (*CallRecorder, error) {
	baseDir := strings.TrimSpace(os.Getenv("WACALLS_RECORDINGS_DIR"))
	if baseDir == "" {
		baseDir = recordingTempDirectory
	}
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		logger.Warn("recording directory unavailable, falling back to system temp", "dir", baseDir, "err", err)
		baseDir = filepath.Join(os.TempDir(), "astracalls-recordings")
		if mkErr := os.MkdirAll(baseDir, 0o755); mkErr != nil {
			return nil, mkErr
		}
	}
	workDir, err := os.MkdirTemp(baseDir, "call-"+callID+"-")
	if err != nil {
		return nil, err
	}
	agentFile, err := os.Create(filepath.Join(workDir, "agent.s16le"))
	if err != nil {
		_ = os.RemoveAll(workDir)
		return nil, err
	}
	peerFile, err := os.Create(filepath.Join(workDir, "peer.s16le"))
	if err != nil {
		_ = agentFile.Close()
		_ = os.RemoveAll(workDir)
		return nil, err
	}
	started := time.Now()
	return &CallRecorder{
		callID:             callID,
		log:                logger.With("call_id", callID),
		startedAt:          started,
		agentFile:          agentFile,
		peerFile:           peerFile,
		agentBuf:           bufio.NewWriter(agentFile),
		peerBuf:            bufio.NewWriter(peerFile),
		workDir:            workDir,
		recordingStartedAt: &started,
	}, nil
}

func (r *CallRecorder) AddAgentFrame(pcm16 []float32) {
	r.addFrame(r.agentBuf, &r.agentSamples, pcm16)
}

func (r *CallRecorder) AddPeerFrame(pcm16 []float32) {
	r.addFrame(r.peerBuf, &r.peerSamples, pcm16)
}

func (r *CallRecorder) addFrame(buf *bufio.Writer, writtenSamples *int64, pcm16 []float32) {
	if len(pcm16) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finalized || r.recordingStartedAt == nil {
		return
	}

	targetSamples := int64(time.Since(*r.recordingStartedAt) * recordingSampleRate / time.Second)
	if targetSamples > recordingSampleRate*int64(recordingMaxDuration/time.Second) {
		r.finalized = true
		return
	}

	if err := padSilence(buf, writtenSamples, targetSamples); err != nil {
		r.log.Warn("recording silence padding failed", "err", err)
		return
	}
	if err := writePCM16(buf, pcm16); err != nil {
		r.log.Warn("recording frame write failed", "err", err)
		return
	}
	*writtenSamples += int64(len(pcm16))
}

func (r *CallRecorder) Finalize() (*RecordingInfo, string, error) {
	r.mu.Lock()
	if r.finalized {
		r.mu.Unlock()
		return nil, "", errors.New("recorder already finalized")
	}
	r.finalized = true
	ended := time.Now()
	r.recordingEndedAt = &ended
	duration := ended.Sub(*r.recordingStartedAt)
	agentSamples := r.agentSamples
	peerSamples := r.peerSamples
	r.mu.Unlock()

	if duration < recordingMinDuration || (agentSamples == 0 && peerSamples == 0) {
		r.cleanupTemps()
		return nil, "", nil
	}
	if duration > recordingMaxDuration {
		r.cleanupTemps()
		return nil, "", errors.New("recording exceeded maximum duration")
	}

	totalSamples := maxInt64(agentSamples, peerSamples)
	if err := padRecordingFiles(r.agentBuf, &r.agentSamples, r.peerBuf, &r.peerSamples, totalSamples); err != nil {
		r.cleanupTemps()
		return nil, "", err
	}
	if err := r.closeWriters(); err != nil {
		r.cleanupTemps()
		return nil, "", err
	}
	if size, err := tempRecordingSize(filepath.Join(r.workDir, "agent.s16le"), filepath.Join(r.workDir, "peer.s16le")); err == nil && size > recordingMaxTempBytes {
		r.cleanupTemps()
		return nil, "", fmt.Errorf("recording temp data exceeded %d bytes", recordingMaxTempBytes)
	}

	outputPath := filepath.Join(r.workDir, "recording.ogg")
	if err := transcodeRecording(filepath.Join(r.workDir, "agent.s16le"), filepath.Join(r.workDir, "peer.s16le"), outputPath); err != nil {
		r.cleanupTemps()
		return nil, "", err
	}
	info, err := buildRecordingInfo(outputPath, duration)
	if err != nil {
		r.cleanupTemps()
		return nil, "", err
	}
	return info, outputPath, nil
}

func (r *CallRecorder) cleanupTemps() {
	_ = os.RemoveAll(r.workDir)
}

func (r *CallRecorder) closeWriters() error {
	var firstErr error
	if r.agentBuf != nil {
		if err := r.agentBuf.Flush(); err != nil && firstErr == nil {
			firstErr = err
		}
		r.agentBuf = nil
	}
	if r.peerBuf != nil {
		if err := r.peerBuf.Flush(); err != nil && firstErr == nil {
			firstErr = err
		}
		r.peerBuf = nil
	}
	if r.agentFile != nil {
		if err := r.agentFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		r.agentFile = nil
	}
	if r.peerFile != nil {
		if err := r.peerFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		r.peerFile = nil
	}
	return firstErr
}

func tempRecordingSize(paths ...string) (int64, error) {
	var total int64
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}

func padRecordingFiles(agentBuf *bufio.Writer, agentSamples *int64, peerBuf *bufio.Writer, peerSamples *int64, totalSamples int64) error {
	if err := padSilence(agentBuf, agentSamples, totalSamples); err != nil {
		return err
	}
	if err := padSilence(peerBuf, peerSamples, totalSamples); err != nil {
		return err
	}
	return nil
}

func padSilence(buf *bufio.Writer, writtenSamples *int64, targetSamples int64) error {
	if targetSamples <= *writtenSamples {
		return nil
	}
	missing := targetSamples - *writtenSamples
	chunk := make([]byte, 4096)
	for missing > 0 {
		samplesThisChunk := int64(len(chunk) / 2)
		currentChunk := chunk
		if missing < samplesThisChunk {
			currentChunk = make([]byte, missing*2)
			samplesThisChunk = missing
		}
		if _, err := buf.Write(currentChunk); err != nil {
			return err
		}
		*writtenSamples += samplesThisChunk
		missing -= samplesThisChunk
	}
	return nil
}

func writePCM16(buf *bufio.Writer, pcm []float32) error {
	data := make([]byte, len(pcm)*2)
	for i, sample := range pcm {
		if sample > 1 {
			sample = 1
		}
		if sample < -1 {
			sample = -1
		}
		v := int16(sample * 32767)
		data[i*2] = byte(v)
		data[i*2+1] = byte(v >> 8)
	}
	_, err := buf.Write(data)
	return err
}

func transcodeRecording(agentPath, peerPath, outputPath string) error {
	cmd := exec.Command(
		"ffmpeg",
		"-y",
		"-f", "s16le", "-ar", fmt.Sprintf("%d", recordingSampleRate), "-ac", "1", "-i", agentPath,
		"-f", "s16le", "-ar", fmt.Sprintf("%d", recordingSampleRate), "-ac", "1", "-i", peerPath,
		"-filter_complex", "amix=inputs=2:weights=1 1:normalize=0",
		"-c:a", "libopus",
		"-b:a", recordingOpusBitrate,
		"-ac", "1",
		"-ar", "48000",
		outputPath,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func buildRecordingInfo(path string, duration time.Duration) (*RecordingInfo, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return nil, err
	}
	return &RecordingInfo{
		Format:      "ogg_opus",
		ContentType: "audio/ogg",
		Channels:    "mono",
		DurationMs:  duration.Milliseconds(),
		SizeBytes:   size,
		SHA256:      hex.EncodeToString(hash.Sum(nil)),
		FileName:    "whatsapp-call-recording.ogg",
	}, nil
}

func (s *Session) recordingEnabled() bool {
	config := s.getWebhook()
	return config.URL != "" && (config.matches("recording.*") || config.matches("recording.ready") || config.matches("recording.failed"))
}

func (s *Session) dispatchRecordingReady(record CallRecord, info *RecordingInfo, filePath string) {
	if info == nil || filePath == "" {
		return
	}
	config := s.getWebhook()
	if config.URL == "" || !config.matches("recording.ready") {
		return
	}
	phone := digitsOnly(record.Phone)
	if phone == "" {
		if jid, err := types.ParseJID(record.Peer); err == nil {
			phone = s.realPhone(jid)
		} else {
			phone = digitsOnly(record.Peer)
		}
	}
	data := RecordingWebhookData{
		CallID:      record.CallID,
		Direction:   record.Direction,
		Phone:       phone,
		Peer:        record.Peer,
		Status:      string(record.Status),
		Owner:       record.Owner,
		StartedAt:   record.StartedAt,
		ConnectedAt: record.ConnectedAt,
		EndedAt:     record.EndedAt,
		EndReason:   record.EndReason,
		Recording:   info,
	}
	body, err := json.Marshal(map[string]any{
		"session":   s.id,
		"event":     "recording.ready",
		"timestamp": time.Now().UnixMilli(),
		"data":      data,
	})
	if err != nil {
		_ = os.RemoveAll(filepath.Dir(filePath))
		return
	}
	go func() {
		defer func() {
			_ = os.RemoveAll(filepath.Dir(filePath))
		}()
		for attempt := 1; attempt <= 3; attempt++ {
			if s.sendWebhookFile(config, body, filePath, info) == nil {
				return
			}
			if attempt < 3 {
				time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
			}
		}
		s.log.Warn("recording webhook delivery failed", "event", "recording.ready", "attempts", 3)
	}()
}

func (s *Session) dispatchRecordingFailed(record CallRecord, err error) {
	config := s.getWebhook()
	if config.URL == "" || !config.matches("recording.failed") {
		return
	}
	phone := digitsOnly(record.Phone)
	if phone == "" {
		if jid, parseErr := types.ParseJID(record.Peer); parseErr == nil {
			phone = s.realPhone(jid)
		} else {
			phone = digitsOnly(record.Peer)
		}
	}
	s.dispatchWebhook("recording.failed", RecordingWebhookData{
		CallID:         record.CallID,
		Direction:      record.Direction,
		Phone:          phone,
		Peer:           record.Peer,
		Status:         string(record.Status),
		Owner:          record.Owner,
		StartedAt:      record.StartedAt,
		ConnectedAt:    record.ConnectedAt,
		EndedAt:        record.EndedAt,
		EndReason:      record.EndReason,
		RecordingError: err.Error(),
	})
}

func (s *Session) sendWebhookFile(config WebhookConfig, payload []byte, filePath string, info *RecordingInfo) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	fileSHA := info.SHA256
	timestamp := time.Now().UnixMilli()

	pr, pw := io.Pipe()
	writer := multipart.NewWriter(pw)

	go func() {
		defer func() {
			_ = writer.Close()
			_ = pw.Close()
		}()
		if err := writer.WriteField("payload", string(payload)); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		part, err := writer.CreateFormFile("file", info.FileName)
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		if _, err := io.Copy(part, file); err != nil {
			_ = pw.CloseWithError(err)
		}
	}()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, config.URL, pr)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-AstraCalls-Timestamp", fmt.Sprintf("%d", timestamp))
	req.Header.Set("X-AstraCalls-File-SHA256", fileSHA)
	if config.Secret != "" {
		mac := hmac.New(sha256.New, []byte(config.Secret))
		_, _ = mac.Write([]byte(fmt.Sprintf("%d.", timestamp)))
		_, _ = mac.Write(payload)
		_, _ = mac.Write([]byte("."))
		_, _ = mac.Write([]byte(fileSHA))
		req.Header.Set("X-AstraCalls-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}
	resp, err := webhookUploadClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
