package test

import (
	"fmt"
	"testing"
)

// Requirement: service-platform service filters team-scoped reads before shaping.
// "Concurrent team streams" is not applicable to executor-registry (journal).

func TestTeamFilterFirst(t *testing.T) {
	t.Run("Teams share filter values", func(t *testing.T) {
		h := newHarness(t)
		for i := 0; i < 3; i++ {
			h.create(4, "gpu", typeDocker)
		}
		h.create(3, "gpu", typeDocker)
		h.create(4, "zeta", typeDocker)
		l := h.ui("list", 3, map[string]any{"team_id": 3, "limit": 1}, nil)
		if num(l.Body["total_count"]) != 1 || l.Body["next_cursor"] != nil || fmt.Sprint(listIDs(l)) != "[4]" {
			t.Fatalf("list %s", l.Raw)
		}
		tags := h.ui("list-execution-tags", 3, map[string]any{"limit": 1}, nil)
		if fmt.Sprint(tags.Body["items"]) != "[gpu]" || tags.Body["next_cursor"] != nil {
			t.Fatalf("tags %s", tags.Raw)
		}
	})
}
