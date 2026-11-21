package test

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// Requirement: service-platform service owns one PostgreSQL schema and role.
// The schema and grant behavior on a real database is in
// v0048_schema_integration_test.go.

func TestOwnSchemaConfig(t *testing.T) {
	t.Run("Access to a foreign schema is denied", func(t *testing.T) {
		// Unit half: production code and migrations name no other schema
		// and no public object; the real grant check is
		// v0048_schema_integration_test.go.
		other := regexp.MustCompile(`\b([a-z_]+_schema|public\.[a-z_]+)\b`)
		files := productionGoFiles(t)
		sqlFiles, _ := filepath.Glob("../internal/store/pgstore/migrations/*.sql")
		if len(sqlFiles) == 0 {
			t.Error("no migrations under internal/store/pgstore/migrations")
		}
		for _, p := range append(files, sqlFiles...) {
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range other.FindAllString(string(b), -1) {
				if m != "executor_registry_schema" {
					t.Errorf("%s names %s", p, m)
				}
			}
		}
	})
}
