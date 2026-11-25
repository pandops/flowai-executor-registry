package test

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/pandops/flowai-executor-registry/internal/cursor"
)

// Requirement: service-platform service binds opaque cursors to their read.

func cursorHarness(t *testing.T) (*harness, string, string) {
	t.Helper()
	h := newHarness(t)
	for i := 0; i < 5; i++ {
		h.create(3, "t"+string(rune('a'+i)), typeDocker)
		h.create(4, "t"+string(rune('a'+i)), typeDocker)
	}
	l := h.ui("list", 3, map[string]any{"team_id": 3, "limit": 2}, nil)
	g := h.ui("list-execution-tags", 3, map[string]any{"limit": 2}, nil)
	lc, _ := l.Body["next_cursor"].(string)
	gc, _ := g.Body["next_cursor"].(string)
	if lc == "" || gc == "" {
		t.Fatalf("no cursors: %s %s", l.Raw, g.Raw)
	}
	return h, lc, gc
}

func TestCursors(t *testing.T) {
	t.Run("Forward page advances", func(t *testing.T) {
		h, lc, _ := cursorHarness(t)
		var all []int64
		all = append(all, 1, 3)
		cur := lc
		for cur != "" {
			r := h.ui("list", 3, map[string]any{"team_id": 3, "limit": 2, "cursor": cur}, nil)
			if r.Status != 200 {
				t.Fatalf("page %d %s", r.Status, r.Raw)
			}
			all = append(all, listIDs(r)...)
			cur, _ = r.Body["next_cursor"].(string)
		}
		want := []int64{1, 3, 5, 7, 9}
		if len(all) != len(want) {
			t.Fatalf("pages %v", all)
		}
		for i := range want {
			if all[i] != want[i] {
				t.Fatalf("pages %v", all)
			}
		}
	})

	t.Run("Tampered cursor", func(t *testing.T) {
		h, lc, gc := cursorHarness(t)
		for _, c := range []string{lc, gc} {
			raw := []byte(c)
			raw[len(raw)/2] ^= 1
			for _, op := range []string{"list", "list-execution-tags"} {
				params := map[string]any{"team_id": 3, "cursor": string(raw)}
				if op == "list-execution-tags" {
					params = map[string]any{"cursor": string(raw)}
				}
				r := h.ui(op, 3, params, nil)
				if r.Status != 400 || r.code() != "invalid_pagination" {
					t.Errorf("%s tampered: %d %s", op, r.Status, r.Raw)
				}
			}
		}
		// Unknown key_id: a cursor signed under key 9 is refused.
		other := cursor.Keyring{Keys: []cursor.Key{{ID: 9, Secret: h.cursorKey}}}
		tok, err := cursor.Encode(other, cursor.Position{Operation: "list", TeamID: 3, After: "1"})
		if err != nil {
			t.Fatal(err)
		}
		if r := h.ui("list", 3, map[string]any{"team_id": 3, "cursor": tok}, nil); r.Status != 400 || r.code() != "invalid_pagination" {
			t.Errorf("unknown key_id: %d", r.Status)
		}
		// A non-integer key_id is refused before any query.
		bad := base64.RawURLEncoding.EncodeToString([]byte(`{"key_id":"1","op":"list","team":3,"after":"1"}`)) + "." + strings.Repeat("A", 43)
		if r := h.ui("list", 3, map[string]any{"team_id": 3, "cursor": bad}, nil); r.Status != 400 || r.code() != "invalid_pagination" {
			t.Errorf("string key_id: %d", r.Status)
		}
	})

	t.Run("Cursor reused for another operation or team", func(t *testing.T) {
		h, lc, gc := cursorHarness(t)
		for _, c := range []struct {
			op     string
			team   int64
			cursor string
		}{
			{"list-execution-tags", 3, lc},
			{"list", 3, gc},
			{"list", 4, lc},
			{"list-execution-tags", 4, gc},
		} {
			r := h.ui(c.op, c.team, map[string]any{"team_id": c.team, "cursor": c.cursor}, nil)
			if c.op == "list-execution-tags" {
				r = h.ui(c.op, c.team, map[string]any{"cursor": c.cursor}, nil)
			}
			if r.Status != 400 || r.code() != "invalid_pagination" {
				t.Errorf("%s team %d: %d %s", c.op, c.team, r.Status, r.Raw)
			}
		}
	})

	t.Run("Changed filter", func(t *testing.T) {
		// executor-registry reads have no filter parameters; an unknown query
		// parameter is rejected instead of silently changing the read.
		h, lc, _ := cursorHarness(t)
		r := h.ui("list", 3, map[string]any{"team_id": 3, "cursor": lc, "status": "busy"}, nil)
		if r.Status != 400 {
			t.Fatalf("unknown filter: %d %s", r.Status, r.Raw)
		}
	})

	t.Run("Limit out of range", func(t *testing.T) {
		h := newHarness(t)
		h.create(3, "gpu", typeDocker)
		for _, raw := range []string{`0`, `201`, `"abc"`, `1.5`, `-1`, `"50"`} {
			for _, op := range []string{"list", "list-execution-tags"} {
				data := []byte(`{"params":{"team_id":3,"limit":` + raw + `}}`)
				if op == "list-execution-tags" {
					data = []byte(`{"params":{"limit":` + raw + `}}`)
				}
				r := h.uiRaw(op, uiHeader(3, 9), data)
				if r.Status != 400 || r.code() != "invalid_pagination" {
					t.Errorf("%s limit %s: %d %s", op, raw, r.Status, r.Raw)
				}
			}
		}
	})
}
