//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"partner-hub/internal/delivery"
	hubmysql "partner-hub/internal/infra/mysql"
)

func TestRecoveryProbeEligibilityAndNoResponsibilityChange(t *testing.T) {
	db := recoveryDB(t)
	snapshot := dueRecovery(t, db)
	id := snapshot.Attempts[1].Target.DeliveryID
	store := hubmysql.NewRecoveryStore(db)
	for _, tc := range []struct {
		name, state, due string
		count            int
		want             bool
	}{
		{"retry-due", "RETRY_WAIT", "TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(3))", 1, true},
		{"pending-due", "PENDING", "TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(3))", 0, true},
		{"future", "RETRY_WAIT", "TIMESTAMPADD(HOUR,1,UTC_TIMESTAMP(3))", 1, false},
		{"success", "SUCCEEDED", "UTC_TIMESTAMP(3)", 1, false},
		{"attention", "NEEDS_ATTENTION", "UTC_TIMESTAMP(3)", 1, false},
		{"in-flight", "IN_FLIGHT", "UTC_TIMESTAMP(3)", 1, false},
		{"exhausted", "RETRY_WAIT", "UTC_TIMESTAMP(3)", delivery.MaxCycleAttempts, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// due is a fixed test expression, never user data.
			if _, err := db.Exec("UPDATE delivery_task SET status=?,cycle_attempt_count=?,next_attempt_at="+tc.due+" WHERE delivery_id=?", tc.state, tc.count, id); err != nil {
				t.Fatal(err)
			}
			got, err := store.HasDue(context.Background(), "PLATFORM_B")
			if err != nil || got != tc.want {
				t.Fatalf("due=%v want=%v err=%v", got, tc.want, err)
			}
			if got, err := store.HasDue(context.Background(), "PLATFORM_A"); err != nil || got {
				t.Fatalf("another platform's task became eligible: %v %v", got, err)
			}
			var attempts, cycle int
			var owner string
			if err := db.QueryRow("SELECT attempt_count,cycle_attempt_count,lease_owner FROM delivery_task WHERE delivery_id=?", id).Scan(&attempts, &cycle, &owner); err != nil || attempts != 1 || cycle != tc.count || owner != "" {
				t.Fatalf("probe changed durable responsibility: %d %d %q %v", attempts, cycle, owner, err)
			}
		})
	}
}

func TestRecoveryProbeDoesNotWaitForTaskRowLock(t *testing.T) {
	db := recoveryDB(t)
	snapshot := dueRecovery(t, db)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var id string
	if err = tx.QueryRow("SELECT delivery_id FROM delivery_task WHERE delivery_id=? FOR UPDATE", snapshot.Attempts[1].Target.DeliveryID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if due, err := hubmysql.NewRecoveryStore(db).HasDue(ctx, "PLATFORM_B"); err != nil || !due {
		t.Fatalf("read-only hint waited for task lock: %v %v", due, err)
	}
}

type competingProbe struct {
	*hubmysql.RecoveryStore
	claimed int
}

func (s *competingProbe) HasDue(ctx context.Context, platform string) (bool, error) {
	due, err := s.RecoveryStore.HasDue(ctx, platform)
	if err != nil || !due {
		return due, err
	}
	// Another instance claims after our read-only hint but before our claim.
	claims, err := s.RecoveryStore.ClaimDue(ctx, platform, "probe-competitor", 1)
	s.claimed += len(claims)
	return due, err
}

func TestRecoveryProbeStaleHintRefundsUnusedExecution(t *testing.T) {
	db := recoveryDB(t)
	dueRecovery(t, db)
	repo := &competingProbe{RecoveryStore: hubmysql.NewRecoveryStore(db)}
	cfg := delivery.BudgetConfig{Total: 2, First: 1, Recovery: 1, Rate: 0.001, Burst: 1}
	budget, err := delivery.NewPlatformBudget(map[string]delivery.BudgetConfig{"PLATFORM_B": cfg})
	if err != nil {
		t.Fatal(err)
	}
	n, err := delivery.NewWorker(repo, nil, "PLATFORM_B", "probe-loser", budget).RunOnce(context.Background())
	if err != nil || n != 0 || repo.claimed != 1 {
		t.Fatalf("stale hint stole task or execution: %d %d %v", n, repo.claimed, err)
	}
	permit, ok := budget.TryAcquire(context.Background(), "PLATFORM_B", delivery.FirstClass)
	if !ok {
		t.Fatal("empty claim after stale hint did not refund rate/slot")
	}
	permit.Release()
}
