package main

import (
	"errors"
	"sync"
	"testing"
)

func ownerPtr(s string) *string { return &s }

func TestOwnerActiveCall(t *testing.T) {
	b := NewBroker()
	b.upsertCall(CallRecord{SessionID: "s1", CallID: "c1", Owner: ownerPtr("op-A"), Status: StatusConnected})
	b.upsertCall(CallRecord{SessionID: "s1", CallID: "c2", Owner: ownerPtr("op-B"), Status: StatusRinging})

	if got := b.ownerActiveCall("op-A"); got != "c1" {
		t.Fatalf("op-A should own c1, got %q", got)
	}
	if got := b.ownerActiveCall("op-C"); got != "" {
		t.Fatalf("op-C owns nothing, got %q", got)
	}
	if got := b.ownerActiveCall(""); got != "" {
		t.Fatalf("empty owner must return empty, got %q", got)
	}

	b.endCall("c1", "done")
	if got := b.ownerActiveCall("op-A"); got != "" {
		t.Fatalf("op-A's call ended, expected empty, got %q", got)
	}
}

func TestReserveOutgoingCallIsIdempotentAndOwnerScoped(t *testing.T) {
	b := NewBroker()
	first := CallRecord{
		SessionID: "s1", CallID: "c1", Owner: ownerPtr("op-A"), Status: StatusStarting,
		IdempotencyKey: "request-1",
	}
	reserved, reused, _, err := b.reserveOutgoingCall(first, 8, 32)
	if err != nil || reused || reserved.CallID != "c1" {
		t.Fatalf("first reservation failed: record=%+v reused=%v err=%v", reserved, reused, err)
	}

	retry := first
	retry.CallID = "c2"
	reserved, reused, _, err = b.reserveOutgoingCall(retry, 8, 32)
	if err != nil || !reused || reserved.CallID != "c1" {
		t.Fatalf("retry must return c1: record=%+v reused=%v err=%v", reserved, reused, err)
	}

	retry.Owner = ownerPtr("op-B")
	_, _, conflictID, err := b.reserveOutgoingCall(retry, 8, 32)
	if !errors.Is(err, errIdempotencyForbidden) || conflictID != "c1" {
		t.Fatalf("another owner must not reuse the key: conflict=%q err=%v", conflictID, err)
	}
}

func TestReserveOutgoingCallSerializesConcurrentRequests(t *testing.T) {
	b := NewBroker()
	start := make(chan struct{})
	results := make(chan string, 2)
	var wg sync.WaitGroup
	for _, callID := range []string{"c1", "c2"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			<-start
			record, _, _, err := b.reserveOutgoingCall(CallRecord{
				SessionID: "s1", CallID: id, Owner: ownerPtr("op-A"), Status: StatusStarting,
				IdempotencyKey: "request-1",
			}, 8, 32)
			if err != nil {
				results <- "error"
				return
			}
			results <- record.CallID
		}(callID)
	}
	close(start)
	wg.Wait()
	close(results)

	var selected string
	for callID := range results {
		if callID == "error" {
			t.Fatal("concurrent reservation returned an error")
		}
		if selected == "" {
			selected = callID
		} else if callID != selected {
			t.Fatalf("concurrent retry created two calls: %q and %q", selected, callID)
		}
	}
	if b.activeCallCount() != 1 {
		t.Fatalf("expected one active call, got %d", b.activeCallCount())
	}
}

func TestReserveOutgoingCallEnforcesOperatorAndCapacity(t *testing.T) {
	b := NewBroker()
	_, _, _, err := b.reserveOutgoingCall(CallRecord{
		SessionID: "s1", CallID: "c1", Owner: ownerPtr("op-A"), Status: StatusStarting,
	}, 1, 2)
	if err != nil {
		t.Fatal(err)
	}

	_, _, conflictID, err := b.reserveOutgoingCall(CallRecord{
		SessionID: "s2", CallID: "c2", Owner: ownerPtr("op-A"), Status: StatusStarting,
	}, 1, 2)
	if !errors.Is(err, errOperatorBusy) || conflictID != "c1" {
		t.Fatalf("operator limit not enforced: conflict=%q err=%v", conflictID, err)
	}
	_, _, _, err = b.reserveOutgoingCall(CallRecord{
		SessionID: "s1", CallID: "c3", Owner: ownerPtr("op-B"), Status: StatusStarting,
	}, 1, 2)
	if !errors.Is(err, errSessionCapacity) {
		t.Fatalf("session limit not enforced: %v", err)
	}
}
