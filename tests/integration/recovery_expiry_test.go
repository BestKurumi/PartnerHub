//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"partner-hub/internal/adapter"
	"partner-hub/internal/delivery"
	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/model"
)

func expireRecovery(t *testing.T, db *sql.DB, id string) {
	t.Helper()
	if _, err := db.Exec(`UPDATE delivery_task SET lease_until=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(3)) WHERE delivery_id=?`, id); err != nil {
		t.Fatal(err)
	}
}

// Repeated exits before Prepare still consume the attempts recorded at claim.
func TestRecoveryExpiryUnsentAttemptsStopAtFive(t *testing.T) {
	db := recoveryDB(t)
	s := dueRecovery(t, db)
	store := hubmysql.NewRecoveryStore(db)
	id := s.Attempts[1].Target.DeliveryID
	for attempt := 2; attempt <= 5; attempt++ {
		claims, err := store.ClaimDue(context.Background(), "PLATFORM_B", "never-prepared", 1)
		if err != nil || len(claims) != 1 || claims[0].AttemptNo != attempt || claims[0].CycleAttempt != attempt {
			t.Fatalf("claim %d %+v %v", attempt, claims, err)
		}
		expireRecovery(t, db, id)
		if n, err := store.RecoverExpired(context.Background(), 1); err != nil || n != 1 {
			t.Fatalf("expire %d: %d %v", attempt, n, err)
		}
		var state, outcome string
		var due sql.NullTime
		var count int
		var sent bool
		var body []byte
		if err = db.QueryRow(`SELECT d.status,d.next_attempt_at,d.cycle_attempt_count,a.outcome,a.send_started,a.prepared_payload FROM delivery_task d JOIN delivery_attempt a ON d.delivery_id=a.delivery_id AND a.attempt_no=d.attempt_count WHERE d.delivery_id=?`, id).Scan(&state, &due, &count, &outcome, &sent, &body); err != nil {
			t.Fatal(err)
		}
		want := "RETRY_WAIT"
		if attempt == 5 {
			want = "NEEDS_ATTENTION"
		}
		if state != want || due.Valid != (attempt < 5) || count != attempt || outcome != "UNKNOWN" || sent || len(body) != 0 {
			t.Fatalf("attempt %d unbounded/lost facts: %s %v %d %s %v %s", attempt, state, due, count, outcome, sent, body)
		}
		if _, err = db.Exec(`UPDATE delivery_task SET next_attempt_at=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(3)) WHERE delivery_id=? AND status='RETRY_WAIT'`, id); err != nil {
			t.Fatal(err)
		}
	}
	claims, err := store.ClaimDue(context.Background(), "PLATFORM_B", "sixth", 1)
	if err != nil || len(claims) != 0 {
		t.Fatalf("sixth attempt admitted %+v %v", claims, err)
	}
	var count int
	if err = db.QueryRow(`SELECT COUNT(*) FROM delivery_attempt WHERE delivery_id=?`, id).Scan(&count); err != nil || count != 5 {
		t.Fatalf("history missing %d %v", count, err)
	}
}

func TestRecoveryExpiryEligibilityAndBatch(t *testing.T) {
	db := recoveryDB(t)
	store := hubmysql.NewRecoveryStore(db)
	var ids []string
	for range 3 {
		s := dueRecovery(t, db)
		ids = append(ids, s.Attempts[1].Target.DeliveryID)
	}
	claims, err := store.ClaimDue(context.Background(), "PLATFORM_B", "batch", 3)
	if err != nil || len(claims) != 3 {
		t.Fatalf("claim %+v %v", claims, err)
	}
	for _, id := range ids[:2] {
		expireRecovery(t, db, id)
	}
	for _, limit := range []int{0, -1, 101} {
		if n, err := store.RecoverExpired(context.Background(), limit); err == nil || n != 0 {
			t.Fatalf("invalid limit %d accepted: %d %v", limit, n, err)
		}
	}
	for i := range 2 {
		if n, err := store.RecoverExpired(context.Background(), 1); err != nil || n != 1 {
			t.Fatalf("bounded pass %d: %d %v", i, n, err)
		}
		var remaining int
		if err = db.QueryRow(`SELECT COUNT(*) FROM delivery_task WHERE status='IN_FLIGHT'`).Scan(&remaining); err != nil || remaining != 2-i {
			t.Fatalf("limit ignored pass=%d remaining=%d %v", i, remaining, err)
		}
	}
	if n, err := store.RecoverExpired(context.Background(), 100); err != nil || n != 0 {
		t.Fatalf("valid lease recovered: %d %v", n, err)
	}
}

// Once the platform has received the HTTP request, a lost process cannot retract
// it. Recovery retains UNKNOWN history and can legitimately deliver it again.
func TestRecoveryExpiryAfterExternalReceiptAllowsDuplicate(t *testing.T) {
	db := recoveryDB(t)
	s := abnormalSnapshot(t)
	s.Attempts[0].Result.Outcome = model.Unknown
	s.Attempts[1].Result.Outcome = model.Success
	if err := hubmysql.NewHandoffStore(db).Handoff(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	id := s.Attempts[0].Target.DeliveryID
	if _, err := db.Exec(`UPDATE delivery_task SET next_attempt_at=UTC_TIMESTAMP(3) WHERE delivery_id=?`, id); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var received [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(400)
			return
		}
		mu.Lock()
		received = append(received, body)
		mu.Unlock()
		io.WriteString(w, `{"code":0}`)
	}))
	defer server.Close()
	ad, err := adapter.NewPlatform1(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	store := hubmysql.NewRecoveryStore(db)
	claims, err := store.ClaimDue(context.Background(), "PLATFORM_A", "exiting", 1)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim %+v %v", claims, err)
	}
	c := claims[0]
	p, err := ad.Prepare(c.Notification, c.Target.PlatformUID)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.SavePrepared(context.Background(), c, p); err != nil {
		t.Fatal(err)
	}
	result := ad.Send(context.Background(), p)
	if result.Outcome != model.Success || !result.SendStarted {
		t.Fatalf("first external receipt failed %+v", result)
	}
	// Simulate process exit precisely after external receive and before Complete.
	expireRecovery(t, db, id)
	if n, err := store.RecoverExpired(context.Background(), 1); err != nil || n != 1 {
		t.Fatalf("expiry %d %v", n, err)
	}
	if err = store.Complete(context.Background(), c, result); !errors.Is(err, delivery.ErrLeaseLost) {
		t.Fatalf("old owner completion accepted %v", err)
	}
	if _, err = db.Exec(`UPDATE delivery_task SET next_attempt_at=UTC_TIMESTAMP(3) WHERE delivery_id=?`, id); err != nil {
		t.Fatal(err)
	}
	if n, err := delivery.NewWorker(hubmysql.NewRecoveryStore(db), ad, "PLATFORM_A", "replacement").RunOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("replacement send %d %v", n, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 2 || string(received[0]) != string(received[1]) {
		t.Fatalf("expected actual repeated receipt of the fixed DTO: %q", received)
	}
	var state, first, expiredOutcome, summary string
	var sent bool
	if err = db.QueryRow(`SELECT d.status,d.first_outcome,a.outcome,a.summary,a.send_started FROM delivery_task d JOIN delivery_attempt a ON d.delivery_id=a.delivery_id AND a.attempt_no=2 WHERE d.delivery_id=?`, id).Scan(&state, &first, &expiredOutcome, &summary, &sent); err != nil || state != "SUCCEEDED" || first != "UNKNOWN" || expiredOutcome != "UNKNOWN" || summary != "LEASE_EXPIRED" || sent {
		t.Fatalf("history claimed external knowledge: %s %s %s %s %v %v", state, first, expiredOutcome, summary, sent, err)
	}
}

// Removing the atomic transition, owner checks, or attempt finalization must fail
// this test against a real MySQL database, including two competing reclaimers.
func TestRecoveryExpiryConcurrentAndHistory(t *testing.T) {
	db := recoveryDB(t)
	s := dueRecovery(t, db)
	store := hubmysql.NewRecoveryStore(db)
	claims, err := store.ClaimDue(context.Background(), "PLATFORM_B", "expired-worker", 1)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim %+v %v", claims, err)
	}
	c := claims[0]
	p := s.Attempts[1].Request
	if err = store.SavePrepared(context.Background(), c, p); err != nil {
		t.Fatal(err)
	}
	// Preserve an already observed send_started fact; expiry cannot infer receipt.
	if _, err = db.Exec(`UPDATE delivery_attempt SET send_started=true WHERE delivery_id=? AND attempt_no=?`, c.Target.DeliveryID, c.AttemptNo); err != nil {
		t.Fatal(err)
	}
	expireRecovery(t, db, c.Target.DeliveryID)
	start := make(chan struct{})
	counts := make(chan int, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			n, err := store.RecoverExpired(context.Background(), 1)
			counts <- n
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(counts)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	total := 0
	for n := range counts {
		total += n
	}
	if total != 1 {
		t.Fatalf("expired task recovered %d times, want exactly one persisted transition", total)
	}
	var status, first, last, owner, outcome, summary, version string
	var payload []byte
	var until, due, finished sql.NullTime
	var count, cycle, attempts int
	var sent bool
	var now time.Time
	if err = db.QueryRow(`SELECT status,first_outcome,last_outcome,lease_owner,lease_until,next_attempt_at,attempt_count,cycle_attempt_count,UTC_TIMESTAMP(3) FROM delivery_task WHERE delivery_id=?`, c.Target.DeliveryID).Scan(&status, &first, &last, &owner, &until, &due, &count, &cycle, &now); err != nil {
		t.Fatal(err)
	}
	if status != "RETRY_WAIT" || first != "UNKNOWN" || last != "UNKNOWN" || owner != "" || until.Valid || !due.Valid || count != 2 || cycle != 2 || due.Time.After(now.Add(2*time.Second)) {
		t.Fatalf("wrong expiry transition status=%s first=%s last=%s owner=%s until=%v due=%v count=%d cycle=%d dbNow=%v", status, first, last, owner, until, due, count, cycle, now)
	}
	if err = db.QueryRow(`SELECT outcome,summary,finished_at,send_started,protocol_version,prepared_payload FROM delivery_attempt WHERE delivery_id=? AND attempt_no=?`, c.Target.DeliveryID, c.AttemptNo).Scan(&outcome, &summary, &finished, &sent, &version, &payload); err != nil {
		t.Fatal(err)
	}
	if outcome != "UNKNOWN" || summary != "LEASE_EXPIRED" || !finished.Valid || !sent || version != p.ProtocolVersion || string(payload) != string(p.Payload) {
		t.Fatalf("attempt facts lost %s %s %v %v %s %s", outcome, summary, finished, sent, version, payload)
	}
	if delay := due.Time.Sub(finished.Time); delay < time.Second || delay > 2*time.Second {
		t.Fatalf("expiry backoff not persisted from DB time: %v", delay)
	}
	if err = db.QueryRow(`SELECT COUNT(*) FROM delivery_attempt WHERE delivery_id=?`, c.Target.DeliveryID).Scan(&attempts); err != nil || attempts != 2 {
		t.Fatalf("history lost %d %v", attempts, err)
	}
	if err = db.QueryRow(`SELECT outcome FROM delivery_attempt WHERE delivery_id=? AND attempt_no=1`, c.Target.DeliveryID).Scan(&outcome); err != nil || outcome != "UNKNOWN" {
		t.Fatalf("first outcome overwritten %s %v", outcome, err)
	}
	if err = db.QueryRow(`SELECT status FROM delivery_task WHERE delivery_id=?`, s.Attempts[0].Target.DeliveryID).Scan(&status); err != nil || status != "SUCCEEDED" {
		t.Fatalf("successful platform changed %s %v", status, err)
	}
	if err = store.Complete(context.Background(), c, model.SendResult{Outcome: model.Success, SendStarted: true}); !errors.Is(err, delivery.ErrLeaseLost) {
		t.Fatalf("stale owner overwrote recovered state: %v", err)
	}
	// A newly constructed store observes the persisted delay; restart cannot send now.
	claims, err = hubmysql.NewRecoveryStore(db).ClaimDue(context.Background(), "PLATFORM_B", "restart", 1)
	if err != nil || len(claims) != 0 {
		t.Fatalf("persisted retry delay ignored %+v %v", claims, err)
	}
}

func TestRecoveryExpiryMismatchedAttemptRollsBack(t *testing.T) {
	db := recoveryDB(t)
	dueRecovery(t, db)
	store := hubmysql.NewRecoveryStore(db)
	claims, err := store.ClaimDue(context.Background(), "PLATFORM_B", "mismatched", 1)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim %+v %v", claims, err)
	}
	c := claims[0]
	expireRecovery(t, db, c.Target.DeliveryID)
	if _, err = db.Exec(`UPDATE delivery_attempt SET lease_version=lease_version+1 WHERE delivery_id=? AND attempt_no=?`, c.Target.DeliveryID, c.AttemptNo); err != nil {
		t.Fatal(err)
	}
	if n, err := store.RecoverExpired(context.Background(), 1); !errors.Is(err, delivery.ErrLeaseLost) || n != 0 {
		t.Fatalf("mismatched attempt accepted %d %v", n, err)
	}
	var taskState, attemptState string
	var finished sql.NullTime
	if err = db.QueryRow(`SELECT d.status,a.outcome,a.finished_at FROM delivery_task d JOIN delivery_attempt a ON d.delivery_id=a.delivery_id AND a.attempt_no=d.attempt_count WHERE d.delivery_id=?`, c.Target.DeliveryID).Scan(&taskState, &attemptState, &finished); err != nil || taskState != "IN_FLIGHT" || attemptState != "IN_FLIGHT" || finished.Valid {
		t.Fatalf("mismatched state partly changed %s %s %v %v", taskState, attemptState, finished, err)
	}
}
