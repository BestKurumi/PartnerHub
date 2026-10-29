//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"partner-hub/internal/delivery"
	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/model"
)

func TestRecoveryRetryPersistentScheduleAndExhaustion(t *testing.T) {
	db := recoveryDB(t)
	s := dueRecovery(t, db)
	id := s.Attempts[1].Target.DeliveryID
	ctx := context.Background()
	for attempt := 2; attempt <= 5; attempt++ {
		store := hubmysql.NewRecoveryStore(db)
		claims, err := store.ClaimDue(ctx, "PLATFORM_B", "retry-persistence", 1)
		if err != nil || len(claims) != 1 || claims[0].CycleAttempt != attempt {
			t.Fatalf("attempt %d claim %+v: %v", attempt, claims, err)
		}
		var before, after time.Time
		if err = db.QueryRow(`SELECT UTC_TIMESTAMP(3)`).Scan(&before); err != nil {
			t.Fatal(err)
		}
		// A transient unsent attempt still consumes its scheduling admission.
		outcome := model.RetryableFailure
		if attempt%2 == 1 {
			outcome = model.Unknown
		}
		if err = store.Complete(ctx, claims[0], model.SendResult{Outcome: outcome, Summary: "not sent"}); err != nil {
			t.Fatal(err)
		}
		if err = db.QueryRow(`SELECT UTC_TIMESTAMP(3)`).Scan(&after); err != nil {
			t.Fatal(err)
		}
		var due sql.NullTime
		var status, first, last string
		var attempts, cycle int
		if err = db.QueryRow(`SELECT status,first_outcome,last_outcome,next_attempt_at,attempt_count,cycle_attempt_count FROM delivery_task WHERE delivery_id=?`, id).Scan(&status, &first, &last, &due, &attempts, &cycle); err != nil {
			t.Fatal(err)
		}
		if first != "UNKNOWN" || last != string(outcome) || attempts != attempt || cycle != attempt {
			t.Fatalf("facts changed %s %s %d %d", first, last, attempts, cycle)
		}
		var histories, unsent int
		if err = db.QueryRow(`SELECT COUNT(*),SUM(send_started=false) FROM delivery_attempt WHERE delivery_id=?`, id).Scan(&histories, &unsent); err != nil {
			t.Fatal(err)
		}
		if histories != attempt || unsent != attempt-1 {
			t.Fatalf("unsent attempts not counted/history lost: %d %d", histories, unsent)
		}
		if attempt == 5 {
			if status != "NEEDS_ATTENTION" || due.Valid {
				t.Fatalf("exhausted attempt still scheduled %s %v", status, due)
			}
		} else {
			low := time.Second * time.Duration(1<<(attempt-2))
			high := low * 2
			if status != "RETRY_WAIT" || !due.Valid || due.Time.Before(before.Add(low-time.Millisecond)) || due.Time.After(after.Add(high+time.Millisecond)) {
				t.Fatalf("attempt %d persisted schedule %s %v outside DB window [%v,%v]", attempt, status, due, before.Add(low), after.Add(high))
			}
			// A new store has no in-memory timer; it must obey the persisted deadline.
			restarted := hubmysql.NewRecoveryStore(db)
			got, e := restarted.ClaimDue(ctx, "PLATFORM_B", "after-restart", 1)
			if e != nil || len(got) != 0 {
				t.Fatalf("restart bypassed persisted delay %+v %v", got, e)
			}
			var unchanged time.Time
			if err = db.QueryRow(`SELECT next_attempt_at FROM delivery_task WHERE delivery_id=?`, id).Scan(&unchanged); err != nil || !unchanged.Equal(due.Time) {
				t.Fatalf("restart changed persisted deadline %v %v", unchanged, err)
			}
			// Advance only the fixture's database deadline, without wall-clock sleeps.
			if _, err = db.Exec(`UPDATE delivery_task SET next_attempt_at=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(3)) WHERE delivery_id=?`, id); err != nil {
				t.Fatal(err)
			}
		}
	}
	got, err := hubmysql.NewRecoveryStore(db).ClaimDue(ctx, "PLATFORM_B", "after-exhaustion", 1)
	if err != nil || len(got) != 0 {
		t.Fatalf("exhaustion did not stop scheduling %+v %v", got, err)
	}
}

type failingPreparation struct{ sent int }

func (*failingPreparation) Prepare(model.TradeNotification, string) (model.PreparedRequest, error) {
	return model.PreparedRequest{}, errors.New("unavailable configuration")
}
func (a *failingPreparation) Send(context.Context, model.PreparedRequest) model.SendResult {
	a.sent++
	return model.SendResult{Outcome: model.Success, SendStarted: true}
}

func TestRecoveryRetryWorkerPreparationFailureStopsImmediately(t *testing.T) {
	db := recoveryDB(t)
	s := dueRecovery(t, db)
	id := s.Attempts[1].Target.DeliveryID
	ad := &failingPreparation{}
	worker := delivery.NewWorker(hubmysql.NewRecoveryStore(db), ad, "PLATFORM_B", "prepare-fails")
	{
		n, err := worker.RunOnce(context.Background())
		if err != nil || n != 1 {
			t.Fatalf("preparation attempt: %d %v", n, err)
		}
		var count int
		var state, outcome string
		var sent bool
		if err = db.QueryRow(`SELECT d.status,d.cycle_attempt_count,a.outcome,a.send_started FROM delivery_task d JOIN delivery_attempt a ON a.delivery_id=d.delivery_id AND a.attempt_no=d.attempt_count WHERE d.delivery_id=?`, id).Scan(&state, &count, &outcome, &sent); err != nil {
			t.Fatal(err)
		}
		if state != "NEEDS_ATTENTION" || count != 2 || outcome != "PERMANENT_FAILURE" || sent || ad.sent != 0 {
			t.Fatalf("Prepare failure must stop immediately and retain its attempt: %s %d %s %v sends=%d", state, count, outcome, sent, ad.sent)
		}
		var first string
		var due sql.NullTime
		var attempts int
		if err = db.QueryRow(`SELECT first_outcome,next_attempt_at,attempt_count FROM delivery_task WHERE delivery_id=?`, id).Scan(&first, &due, &attempts); err != nil || first != "UNKNOWN" || due.Valid || attempts != 2 {
			t.Fatalf("preparation failure rewrote first fact or scheduled another attempt: %s %v %d %v", first, due, attempts, err)
		}
		var histories int
		if err = db.QueryRow(`SELECT COUNT(*) FROM delivery_attempt WHERE delivery_id=?`, id).Scan(&histories); err != nil || histories != 2 {
			t.Fatalf("lost attempt history: %d %v", histories, err)
		}
		// Even an old due timestamp cannot make a terminal task eligible again.
		if _, err = db.Exec(`UPDATE delivery_task SET next_attempt_at=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(3)) WHERE delivery_id=?`, id); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := worker.RunOnce(context.Background()); err != nil || n != 0 || ad.sent != 0 {
		t.Fatalf("permanent preparation failure was retried %d %v sends=%d", n, err, ad.sent)
	}
}

func TestRecoveryRetryPermanentFailureStopsImmediately(t *testing.T) {
	db := recoveryDB(t)
	s := dueRecovery(t, db)
	store := hubmysql.NewRecoveryStore(db)
	ctx := context.Background()
	claims, err := store.ClaimDue(ctx, "PLATFORM_B", "permanent", 1)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim %+v %v", claims, err)
	}
	if err = store.Complete(ctx, claims[0], model.SendResult{Outcome: model.PermanentFailure, Summary: "invalid fixed UID"}); err != nil {
		t.Fatal(err)
	}
	var status string
	var due sql.NullTime
	if err = db.QueryRow(`SELECT status,next_attempt_at FROM delivery_task WHERE delivery_id=?`, s.Attempts[1].Target.DeliveryID).Scan(&status, &due); err != nil || status != "NEEDS_ATTENTION" || due.Valid {
		t.Fatalf("permanent failure scheduled %s %v %v", status, due, err)
	}
	got, err := store.ClaimDue(ctx, "PLATFORM_B", "again", 1)
	if err != nil || len(got) != 0 {
		t.Fatalf("permanent failure claimed %+v %v", got, err)
	}
}
