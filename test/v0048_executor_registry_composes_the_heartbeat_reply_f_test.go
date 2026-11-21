package test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Requirement: executor-registry composes the heartbeat reply from the claims task-manager holds.

type claimsReq struct {
	Body struct {
		TeamID     int64            `json:"team_id"`
		ExecutorID int64            `json:"executor_id"`
		Claims     []map[string]any `json:"claims"`
		LostSeq    int64            `json:"lost_seq"`
	} `json:"body"`
}

func lastClaimsReq(t *testing.T, h *harness) (claimsReq, map[string][]string) {
	t.Helper()
	reqs := h.broker.RequestsTo(subjExecutorClaims)
	if len(reqs) == 0 {
		t.Fatal("no executor-claims request")
	}
	var r claimsReq
	if err := json.Unmarshal(reqs[len(reqs)-1].Body, &r); err != nil {
		t.Fatal(err)
	}
	return r, reqs[len(reqs)-1].Header
}

func TestHeartbeatReply(t *testing.T) {
	t.Run("Reply lists the claims task-manager holds", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 7; i++ {
			h.create(3, "gpu", typeDocker)
		}
		h.claimsHeld = func(map[string]any) []map[string]any {
			return []map[string]any{{"task_id": 1010, "attempt": 1, "claim_seq": 1}}
		}
		b := defaultHB()
		b.Claims = []map[string]any{claim(1010, 1, 1, "running"), claim(1011, 2, 3, "pending")}
		r := h.heartbeat(7, b)
		if r.Body["connection_state"] != "connected" || r.Body["claims_known"] != true {
			t.Fatalf("reply %s", r.Raw)
		}
		cl := r.Body["claims"].([]any)
		if len(cl) != 1 || num(cl[0].(map[string]any)["task_id"]) != 1010 || len(cl[0].(map[string]any)) != 3 {
			t.Fatalf("claims %v", cl)
		}
		req, hd := lastClaimsReq(t, h)
		if req.Body.TeamID != 3 || req.Body.ExecutorID != 7 || len(req.Body.Claims) != 2 || req.Body.Claims[1]["state"] != "pending" || num(req.Body.Claims[1]["claim_seq"]) != 3 || hd["team"][0] != "3" {
			t.Fatalf("request %+v %v", req, hd)
		}
		for _, q := range h.broker.RequestsTo(subjExecutorClaims) {
			if strings.Contains(strings.ToLower(string(q.Body)), "token") {
				t.Fatal("executor-claims request carries a token")
			}
		}
		if strings.Contains(strings.ToLower(string(r.Raw)), "token") {
			t.Fatal("reply carries a token")
		}
	})

	t.Run("Request carries the latest executor_lost sequence", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 7; i++ {
			h.create(3, "gpu", typeDocker)
		}
		h.heartbeat(7, defaultHB())
		if req, _ := lastClaimsReq(t, h); req.Body.LostSeq != 0 {
			t.Fatalf("lost_seq before any loss = %d", req.Body.LostSeq)
		}
		h.store.SetNextPublisherSeq(899)
		h.advance(10 * time.Minute)
		h.sweep()
		lost := h.eventsFor("executor_lost", 7)
		if len(lost) != 1 {
			t.Fatal("no executor_lost")
		}
		if len(h.broker.Published) != 0 {
			t.Fatal("relay ran")
		}
		h.claimsHeld = func(map[string]any) []map[string]any { return nil }
		r := h.heartbeat(7, hb{Claims: []map[string]any{claim(1010, 1, 1, "running")}, MaxParallel: 2, Slots: 1, Interval: 15, Miss: 3, Wait: 600, IP: "192.0.2.10"})
		req, _ := lastClaimsReq(t, h)
		if req.Body.LostSeq != lost[0].Seq {
			t.Fatalf("lost_seq = %d, want %d", req.Body.LostSeq, lost[0].Seq)
		}
		if len(r.Body["claims"].([]any)) != 0 {
			t.Fatalf("reply %s", r.Raw)
		}
	})

	t.Run("New Executor before its first heartbeat", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		h.advance(time.Hour)
		h.sweep()
		if len(h.broker.RequestsTo(subjExecutorClaims)) != 0 || connState(t, h, 1) != "never_connected" {
			t.Fatal("claims request or state change before the first heartbeat")
		}
	})
}
