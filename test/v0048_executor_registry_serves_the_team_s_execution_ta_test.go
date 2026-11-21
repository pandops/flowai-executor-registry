package test

import (
	"fmt"
	"testing"
)

// Requirement: executor-registry serves the team's execution tags.

func TestExecutionTags(t *testing.T) {
	t.Run("Operator lists execution tags", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		h.create(3, "gpu", typeK8s)
		h.create(3, "cpu", typeDocker)
		h.create(4, "arm", typeDocker)
		r := h.ui("list-execution-tags", 3, nil, nil)
		if r.Status != 200 {
			t.Fatalf("list-execution-tags = %d %s", r.Status, r.Raw)
		}
		h.validate("ui.list-execution-tags.reply.schema.json", r.Raw)
		if fmt.Sprint(r.Body["items"]) != "[cpu gpu]" || r.Body["next_cursor"] != nil {
			t.Fatalf("tags = %s", r.Raw)
		}
		// Pagination: limit 1 then the cursor.
		p1 := h.ui("list-execution-tags", 3, map[string]any{"limit": 1}, nil)
		cur, _ := p1.Body["next_cursor"].(string)
		if fmt.Sprint(p1.Body["items"]) != "[cpu]" || cur == "" {
			t.Fatalf("page 1 = %s", p1.Raw)
		}
		p2 := h.ui("list-execution-tags", 3, map[string]any{"limit": 1, "cursor": cur}, nil)
		if fmt.Sprint(p2.Body["items"]) != "[gpu]" || p2.Body["next_cursor"] != nil {
			t.Fatalf("page 2 = %s", p2.Raw)
		}
		// Default limit 50, maximum 200.
		h2 := newHarness(t)
		for i := 0; i < 60; i++ {
			h2.create(3, fmt.Sprintf("t%02d", i), typeDocker)
		}
		d := h2.ui("list-execution-tags", 3, nil, nil)
		if n := len(d.Body["items"].([]any)); n != 50 || d.Body["next_cursor"] == nil {
			t.Fatalf("default page has %d items", n)
		}
		if m := h2.ui("list-execution-tags", 3, map[string]any{"limit": 200}, nil); len(m.Body["items"].([]any)) != 60 {
			t.Fatalf("limit 200 = %s", m.Raw)
		}
		// flow-registry's service.team-tags answers the same set.
		s := h.handle("executor-registry.service.team-tags.sub", map[string][]string{"team": {"3"}, "request_id": {"9"}}, envelope(map[string]any{"team_id": 3}, nil))
		h.validate("service.team-tags.reply.schema.json", s.Raw)
		if s.Status != 200 || fmt.Sprint(s.Body["tags"]) != "[cpu gpu]" {
			t.Fatalf("team-tags = %d %s", s.Status, s.Raw)
		}
		bad := h.handle("executor-registry.service.team-tags.sub", map[string][]string{"team": {"3"}, "request_id": {"9"}}, envelope(map[string]any{"team_id": 4}, nil))
		if bad.Status != 400 || bad.code() != "invalid_request" {
			t.Fatalf("team-tags with mismatching team = %d %s", bad.Status, bad.Raw)
		}
	})
}
