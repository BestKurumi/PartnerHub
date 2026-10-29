//go:build integration

package integration_test

import (
	"context"
	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/model"
	"testing"
	"time"
)

func TestObservabilityRecoveryCountsDurableStateAndResetsZero(t *testing.T) {
	db := recoveryDB(t)
	ctx := context.Background()
	s := archiveSnapshot(t)
	s.ReceivedAt = time.Now().Add(-5 * time.Second)
	if err := hubmysql.NewArchiveStore(db).Save(ctx, []model.FirstRoundSnapshot{s}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE delivery_task SET status='RETRY_WAIT' WHERE delivery_id=?`, s.Attempts[0].Target.DeliveryID); err != nil {
		t.Fatal(err)
	}
	states, err := hubmysql.RecoveryObservations(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, v := range states {
		if v.Platform == "PLATFORM_A" && v.Status == "RETRY_WAIT" {
			found = true
			if v.Count != 1 || v.Age < 4500*time.Millisecond {
				t.Fatalf("wrong persistent recovery sample: %+v", v)
			}
		}
	}
	if !found {
		t.Fatal("missing state")
	}
	if _, err = db.ExecContext(ctx, `UPDATE delivery_task SET status='SUCCEEDED' WHERE delivery_id=?`, s.Attempts[0].Target.DeliveryID); err != nil {
		t.Fatal(err)
	}
	states, err = hubmysql.RecoveryObservations(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range states {
		if v.Platform == "PLATFORM_A" && v.Status == "RETRY_WAIT" && v.Count != 0 {
			t.Fatalf("stale state retained: %+v", v)
		}
	}
}
