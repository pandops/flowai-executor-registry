//go:build integration

package test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/pandops/flowai-executor-registry/internal/store"
)

// Spec: service-platform — "Team-owned parent keeps its team": the database
// refuses a write that changes a stored Executor's team_id, through the
// service's store, its role and an administrator alike.
func TestV0048TeamOwnedParentKeepsItsTeamInTheDatabase(t *testing.T) {
	ctx := context.Background()
	d := newTestDB(t, true)
	s := d.openStore(t)
	h := newHarness(t)
	h.svc = h.newService(s, h.broker)
	id, _ := h.create(3, "gpu", typeDocker)
	if r := h.reissue(4, id); r.Status != 404 {
		t.Fatalf("team-4 reissue of a team-3 Executor: %d", r.Status)
	}

	// The store's connection write either fails or leaves team_id alone;
	// the read-back below shows the team is unchanged.
	_ = s.InTx(ctx, func(tx store.Tx) error {
		c, err := tx.GetConnection(ctx, id)
		if err != nil {
			return err
		}
		c.TeamID = 4
		return tx.PutConnection(ctx, c)
	})
	role, err := sql.Open("pgx", pg.dsn(serviceRole, rolePassword, d.name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { role.Close() })
	for name, db := range map[string]*sql.DB{"service role": role, "administrator": d.admin} {
		for _, table := range []string{"executors", "connections"} {
			if _, err := db.Exec(`UPDATE `+serviceSchema+`.`+table+` SET team_id = 4 WHERE executor_id = $1`, id); err == nil {
				t.Errorf("%s changed team_id of %s row %d", name, table, id)
			}
		}
	}
	var team, connTeam int64
	if err := d.admin.QueryRow(`SELECT e.team_id, c.team_id FROM `+serviceSchema+`.executors e JOIN `+serviceSchema+`.connections c USING (executor_id) WHERE executor_id = $1`, id).
		Scan(&team, &connTeam); err != nil {
		t.Fatal(err)
	}
	if team != 3 || connTeam != 3 {
		t.Fatalf("Executor %d moved: executor team %d, connection team %d", id, team, connTeam)
	}
}
