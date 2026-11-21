package test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Requirement: executor-registry records the IP an Executor reports.

func ipHB(ip any) hb {
	b := defaultHB()
	b.IP = ip
	return b
}

func TestReportedIP(t *testing.T) {
	t.Run("First valid IP is stored and published", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 7; i++ {
			h.create(3, "gpu", typeDocker)
		}
		h.now = time.Date(2026, 10, 15, 8, 58, 0, 0, time.UTC)
		h.heartbeat(7, ipHB("192.0.2.10"))
		e := h.get(3, 7)
		if e.Body["reported_ip"] != "192.0.2.10" || e.Body["ip_reported_at"] != "2026-10-15T08:58:00.000Z" {
			t.Fatalf("read %s", e.Raw)
		}
		ev := h.eventsFor("ip_changed", 7)
		if len(ev) != 1 || ev[0].Body["old_ip"] != nil || ev[0].Body["new_ip"] != "192.0.2.10" || ev[0].Body["occurred_at"] != "2026-10-15T08:58:00.000Z" || ev[0].Header["team"] != "3" {
			t.Fatalf("ip_changed %v", ev)
		}
		if _, ok := ev[0].Body["old_ip"]; !ok {
			t.Fatal("old_ip missing instead of null")
		}
		h.checkOutbox()
	})

	t.Run("Same IP publishes nothing", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		h.heartbeat(1, ipHB("192.0.2.10"))
		n := len(h.outbox())
		for i := 0; i < 5; i++ {
			h.advance(15 * time.Second)
			h.heartbeat(1, ipHB("192.0.2.10"))
			if got := h.get(3, 1).Body["ip_reported_at"]; got != h.clock().Format("2006-01-02T15:04:05.000Z") {
				t.Fatalf("ip_reported_at %v not updated", got)
			}
		}
		if len(h.outbox()) != n {
			t.Fatalf("same IP published %v", h.outbox()[n:])
		}
	})

	t.Run("Changed IP publishes old and new", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 8; i++ {
			h.create(3, "gpu", typeDocker)
		}
		h.heartbeat(8, ipHB("10.42.0.9"))
		n := len(h.outbox())
		h.advance(15 * time.Second)
		h.heartbeat(8, ipHB("10.42.0.17"))
		after := h.outbox()[n:]
		if len(after) != 1 || after[0].Subject != "executor-registry.event.ip_changed.pub" || after[0].Body["old_ip"] != "10.42.0.9" || after[0].Body["new_ip"] != "10.42.0.17" {
			t.Fatalf("published %v", after)
		}
		if h.get(3, 8).Body["reported_ip"] != "10.42.0.17" {
			t.Fatal("new IP not stored")
		}
	})

	t.Run("IPv6 address is accepted in canonical form", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		h.heartbeat(1, ipHB("2001:db8:4::21"))
		if h.get(3, 1).Body["reported_ip"] != "2001:db8:4::21" || len(h.events("ip_changed")) != 1 {
			t.Fatal("canonical IPv6 not stored")
		}
	})

	t.Run("Missing or invalid IP keeps liveness and the stored IP", func(t *testing.T) {
		for _, ip := range []any{omitIP, nil, "10.0.0.300", "2001:DB8::1", "2001:db8:0:0:0:0:0:1", "192.168.002.010", "192.0.2.10/24", "fe80::1%eth0", "0.0.0.0", "::", 7, "192.0.2.10:80"} {
			h := newHarness(t)
			for i := 0; i < 7; i++ {
				h.create(3, "gpu", typeDocker)
			}
			h.heartbeat(7, ipHB("192.0.2.10"))
			reportedAt := h.get(3, 7).Body["ip_reported_at"]
			h.advance(50 * time.Second)
			h.sweep()
			n := len(h.outbox())
			logBefore := len(h.logs.String())
			r := h.heartbeat(7, ipHB(ip))
			if r.Status != 200 || r.Body["connection_state"] != "connected" || r.Body["claims_known"] != true {
				t.Errorf("ip %v: reply %d %s", ip, r.Status, r.Raw)
				continue
			}
			var restored, ipEvent bool
			for _, m := range h.outbox()[n:] {
				restored = restored || m.Subject == "executor-registry.event.connection_restored.pub"
				ipEvent = ipEvent || m.Subject == "executor-registry.event.ip_changed.pub"
			}
			if !restored || ipEvent {
				t.Errorf("ip %v: restored=%v ip_changed=%v", ip, restored, ipEvent)
			}
			e := h.get(3, 7)
			if e.Body["reported_ip"] != "192.0.2.10" || e.Body["ip_reported_at"] != reportedAt {
				t.Errorf("ip %v: stored IP changed: %s", ip, e.Raw)
			}
			logs := h.logs.String()[logBefore:]
			var warnings int
			for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
				var rec map[string]any
				if json.Unmarshal([]byte(line), &rec) != nil || rec["msg"] != "heartbeat_ip_invalid" {
					continue
				}
				warnings++
				reason := "invalid"
				if _, skip := ip.(omit); skip || ip == nil {
					reason = "missing"
				}
				if rec["level"] != "WARN" || num(rec["executor_id"]) != 7 || rec["reason"] != reason {
					t.Errorf("ip %v: warning %v", ip, rec)
				}
			}
			if warnings != 1 {
				t.Errorf("ip %v: %d heartbeat_ip_invalid warnings", ip, warnings)
			}
			if s, ok := ip.(string); ok && strings.Contains(logs, s) {
				t.Errorf("ip %v: log carries the received value", ip)
			}
		}
	})

	t.Run("Reported IP grants nothing", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 6; i++ {
			h.create(4, "gpu", typeDocker)
		}
		h.create(3, "gpu", typeDocker) // 7
		h.heartbeat(6, ipHB("192.0.2.99"))
		h.heartbeat(7, ipHB("192.0.2.99"))
		if h.get(3, 7).Body["reported_ip"] != "192.0.2.99" || h.get(4, 6).Body["reported_ip"] != "192.0.2.99" {
			t.Fatal("IP not stored per Executor")
		}
		if g := h.get(4, 7); g.Status != 404 {
			t.Fatal("IP equality granted a cross-team read")
		}
		reqs := h.broker.RequestsTo(subjExecutorClaims)
		last := reqs[len(reqs)-1]
		if last.Header.Get("team") != "3" || !strings.Contains(string(last.Body), `"team_id":3`) || !strings.Contains(string(last.Body), `"executor_id":7`) {
			t.Fatalf("team not derived from executor_id: %v %s", last.Header, last.Body)
		}
		ev := h.eventsFor("ip_changed", 7)
		if len(ev) != 1 || ev[0].Header["team"] != "3" {
			t.Fatalf("ip_changed for 7: %v", ev)
		}
	})
}
