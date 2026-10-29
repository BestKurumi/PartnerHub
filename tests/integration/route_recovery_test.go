//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	mysqldriver "github.com/go-sql-driver/mysql"
	"os"
	"partner-hub/internal/delivery"
	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/model"
	"partner-hub/internal/routing"
	"strings"
	"sync"
	"testing"
	"time"
)

func routeDB(t *testing.T) *sql.DB {
	t.Helper()
	db := recoveryDB(t)
	raw, err := os.ReadFile("../../deploy/mysql/migrations/006_route_recovery.sql")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for range 2 {
		for _, stmt := range strings.Split(string(raw), ";") {
			if strings.TrimSpace(stmt) != "" {
				if _, err = conn.ExecContext(context.Background(), stmt); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	manual, err := os.ReadFile("../../deploy/mysql/migrations/007_manual_operation.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range strings.Split(string(manual), ";") {
		if strings.TrimSpace(stmt) != "" {
			if _, err = conn.ExecContext(context.Background(), stmt); err != nil {
				t.Fatal(err)
			}
		}
	}
	return db
}
func dueRoute(t *testing.T, db *sql.DB) model.FirstRoundSnapshot {
	t.Helper()
	s := abnormalSnapshot(t)
	s.Route = model.RouteSnapshot{MsgID: s.Trade.MsgID, Status: "RETRY_WAIT", ResolvedAt: time.Now().UTC(), Targets: []model.RouteTarget{}}
	s.Attempts = nil
	s.RouteError = "binding unavailable"
	if err := hubmysql.NewHandoffStore(db).Handoff(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE trade_message SET route_next_attempt_at=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(3)) WHERE msg_id=?", s.Trade.MsgID); err != nil {
		t.Fatal(err)
	}
	return s
}
func TestRouteRecoveryClaim(t *testing.T) {
	db := routeDB(t)
	s := dueRoute(t, db)
	got, err := hubmysql.NewRouteRecoveryStore(db).ClaimRoutes(context.Background(), "route-worker", 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("due routing responsibility not claimed: %+v %v", got, err)
	}
	c := got[0]
	if c.MsgID != s.Trade.MsgID || c.ClientID != s.Trade.ClientID || c.AttemptNo != 2 || c.CycleAttempt != 2 || c.CycleNo != 1 || c.LeaseVersion != 1 || time.Until(c.LeaseDeadline) <= 0 || time.Until(c.LeaseDeadline) > 10*time.Second {
		t.Fatalf("incomplete route claim %+v", c)
	}
	var state, outcome, owner string
	var count int
	var finished sql.NullTime
	if err = db.QueryRow(`SELECT m.route_status,m.route_attempt_count,a.outcome,a.finished_at,a.lease_owner FROM trade_message m JOIN route_attempt a ON a.msg_id=m.msg_id AND a.attempt_no=m.route_attempt_count WHERE m.msg_id=?`, s.Trade.MsgID).Scan(&state, &count, &outcome, &finished, &owner); err != nil || state != "IN_FLIGHT" || count != 2 || outcome != "IN_FLIGHT" || finished.Valid || owner != c.Owner {
		t.Fatalf("non-atomic claim %s %d %s %v %s %v", state, count, outcome, finished, owner, err)
	}
}

func claimedRoute(t *testing.T, db *sql.DB) (model.FirstRoundSnapshot, routing.RouteRecoveryClaim) {
	t.Helper()
	s := dueRoute(t, db)
	c, err := hubmysql.NewRouteRecoveryStore(db).ClaimRoutes(context.Background(), "route-worker", 1)
	if err != nil || len(c) != 1 {
		t.Fatalf("claim: %+v %v", c, err)
	}
	return s, c[0]
}
func resolvedRoute(msg string) model.RouteSnapshot {
	return model.RouteSnapshot{MsgID: msg, Status: "ROUTED", ResolvedAt: time.Now().UTC(), Targets: []model.RouteTarget{{PlatformID: "PLATFORM_A", PlatformUID: "fixed-A", DeliveryID: model.NewID()}, {PlatformID: "PLATFORM_B", PlatformUID: "fixed-B", DeliveryID: model.NewID()}}}
}
func TestRouteRecoveryComplete(t *testing.T) {
	db := routeDB(t)
	s, c := claimedRoute(t, db)
	store := hubmysql.NewRouteRecoveryStore(db)
	route := resolvedRoute(c.MsgID)
	var before []byte
	if err := db.QueryRow("SELECT first_snapshot FROM trade_message WHERE msg_id=?", c.MsgID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteRoute(context.Background(), c, route, nil); err != nil {
		t.Fatal(err)
	}
	var status, first, routeError string
	var after, snap []byte
	if err := db.QueryRow("SELECT route_status,first_status,route_error,first_snapshot,route_snapshot FROM trade_message WHERE msg_id=?", c.MsgID).Scan(&status, &first, &routeError, &after, &snap); err != nil || status != "ROUTED" || first != "RECOVERY_PENDING" || routeError != s.RouteError || !bytes.Equal(before, after) {
		t.Fatalf("lost first facts or route %s %s %s %v", status, first, routeError, err)
	}
	var stored model.RouteSnapshot
	if err := json.Unmarshal(snap, &stored); err != nil || stored.MsgID != route.MsgID || len(stored.Targets) != 2 || stored.Targets[0] != route.Targets[0] || stored.Targets[1] != route.Targets[1] {
		t.Fatalf("incomplete fixed snapshot %+v %v", stored, err)
	}
	for _, target := range route.Targets {
		var uid, state, outcome string
		var total, cycle, attempts, payload int
		var due sql.NullTime
		if err := db.QueryRow(`SELECT platform_uid,status,first_outcome,attempt_count,cycle_attempt_count,OCTET_LENGTH(prepared_payload),next_attempt_at,(SELECT COUNT(*) FROM delivery_attempt WHERE delivery_id=?) FROM delivery_task WHERE delivery_id=?`, target.DeliveryID, target.DeliveryID).Scan(&uid, &state, &outcome, &total, &cycle, &payload, &due, &attempts); err != nil || uid != target.PlatformUID || state != "PENDING" || outcome != "NOT_ATTEMPTED" || total != 0 || cycle != 0 || payload != 0 || !due.Valid || attempts != 0 {
			t.Fatalf("route fabricated a send %s %s %s %d %d %d %v %d %v", uid, state, outcome, total, cycle, payload, due, attempts, err)
		}
	}
	if err := store.CompleteRoute(context.Background(), c, resolvedRoute(c.MsgID), nil); !errors.Is(err, delivery.ErrLeaseLost) {
		t.Fatalf("terminal route replaced: %v", err)
	}
	if got, err := store.ClaimRoutes(context.Background(), "other", 4); err != nil || len(got) != 0 {
		t.Fatalf("terminal route reclaimed %+v %v", got, err)
	}
}
func TestRouteRecoveryEmptyVersusError(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			db := routeDB(t)
			_, c := claimedRoute(t, db)
			store := hubmysql.NewRouteRecoveryStore(db)
			route := model.RouteSnapshot{MsgID: c.MsgID, Status: "NO_ROUTE", ResolvedAt: time.Now().UTC(), Targets: []model.RouteTarget{}}
			var lookupErr error
			if failed {
				lookupErr = errors.New("password=secret customer private query")
				route = model.RouteSnapshot{}
			}
			if err := store.CompleteRoute(context.Background(), c, route, lookupErr); err != nil {
				t.Fatal(err)
			}
			var status, outcome, summary string
			var due sql.NullTime
			var delay sql.NullInt64
			if err := db.QueryRow(`SELECT m.route_status,m.route_next_attempt_at,a.outcome,a.summary,TIMESTAMPDIFF(MICROSECOND,a.finished_at,m.route_next_attempt_at) FROM trade_message m JOIN route_attempt a ON a.msg_id=m.msg_id AND a.attempt_no=m.route_attempt_count WHERE m.msg_id=?`, c.MsgID).Scan(&status, &due, &outcome, &summary, &delay); err != nil {
				t.Fatal(err)
			}
			if failed {
				if status != "RETRY_WAIT" || outcome != "QUERY_FAILED" || !due.Valid || !delay.Valid || delay.Int64 < 999000 || delay.Int64 > 2000000 || strings.Contains(summary, "secret") || summary == "" {
					t.Fatalf("error became empty/leaked/no persistent delay: %s %s %s %v %v", status, outcome, summary, due, delay)
				}
			} else {
				if status != "NO_ROUTE" || outcome != "NO_ROUTE" || due.Valid {
					t.Fatalf("empty not terminal %s %s %v", status, outcome, due)
				}
				if got, err := store.ClaimRoutes(context.Background(), "again", 1); err != nil || len(got) != 0 {
					t.Fatalf("NO_ROUTE rerouted %+v %v", got, err)
				}
			}
			var n int
			if err := db.QueryRow("SELECT COUNT(*) FROM delivery_task WHERE msg_id=?", c.MsgID).Scan(&n); err != nil || n != 0 {
				t.Fatalf("invented tasks %d %v", n, err)
			}
		})
	}
}
func TestRouteRecoveryAtomicTargetFailure(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprint(conflict), func(t *testing.T) {
			db := routeDB(t)
			_, c := claimedRoute(t, db)
			route := resolvedRoute(c.MsgID)
			if conflict {
				other := abnormalSnapshot(t)
				route.Targets[1].DeliveryID = other.Attempts[1].Target.DeliveryID
				if err := hubmysql.NewHandoffStore(db).Handoff(context.Background(), other); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := db.Exec("CREATE TRIGGER route_target_fail BEFORE INSERT ON delivery_task FOR EACH ROW BEGIN IF NEW.platform_id='PLATFORM_B' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='route injection'; END IF; END"); err != nil {
					t.Fatal(err)
				}
			}
			if err := hubmysql.NewRouteRecoveryStore(db).CompleteRoute(context.Background(), c, route, nil); err == nil {
				t.Fatal("partial target failure accepted")
			}
			var n int
			var state, outcome string
			var finished sql.NullTime
			if err := db.QueryRow("SELECT COUNT(*) FROM delivery_task WHERE msg_id=?", c.MsgID).Scan(&n); err != nil || n != 0 {
				t.Fatalf("half targets persisted %d %v", n, err)
			}
			if err := db.QueryRow(`SELECT m.route_status,a.outcome,a.finished_at FROM trade_message m JOIN route_attempt a ON a.msg_id=m.msg_id AND a.attempt_no=m.route_attempt_count WHERE m.msg_id=?`, c.MsgID).Scan(&state, &outcome, &finished); err != nil || state != "IN_FLIGHT" || outcome != "IN_FLIGHT" || finished.Valid {
				t.Fatalf("partial completion %s %s %v %v", state, outcome, finished, err)
			}
		})
	}
}
func TestRouteRecoveryLeaseIdentity(t *testing.T) {
	for _, field := range []string{"owner", "version", "attempt", "cycle", "cycleAttempt", "client", "expired"} {
		t.Run(field, func(t *testing.T) {
			db := routeDB(t)
			_, c := claimedRoute(t, db)
			switch field {
			case "owner":
				c.Owner = "stale"
			case "version":
				c.LeaseVersion++
			case "attempt":
				c.AttemptNo++
			case "cycle":
				c.CycleNo++
			case "cycleAttempt":
				c.CycleAttempt++
			case "client":
				c.ClientID = "other"
			case "expired":
				if _, err := db.Exec("UPDATE trade_message SET route_lease_until=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(3)) WHERE msg_id=?", c.MsgID); err != nil {
					t.Fatal(err)
				}
			}
			if err := hubmysql.NewRouteRecoveryStore(db).CompleteRoute(context.Background(), c, resolvedRoute(c.MsgID), nil); !errors.Is(err, delivery.ErrLeaseLost) {
				t.Fatalf("invalid claim completed %s %v", field, err)
			}
		})
	}
}
func TestRouteRecoveryMalformedResults(t *testing.T) {
	db := routeDB(t)
	_, c := claimedRoute(t, db)
	store := hubmysql.NewRouteRecoveryStore(db)
	for _, kind := range []string{"msg", "empty", "status", "duplicate", "uid", "delivery", "extra-hyphens", "time", "no-route-targets"} {
		t.Run(kind, func(t *testing.T) {
			r := resolvedRoute(c.MsgID)
			switch kind {
			case "msg":
				r.MsgID = model.NewID()
			case "empty":
				r.Targets = nil
			case "status":
				r.Status = "RETRY_WAIT"
			case "duplicate":
				r.Targets[1].PlatformID = "PLATFORM_A"
			case "uid":
				r.Targets[1].PlatformUID = "bad\nuid"
			case "delivery":
				r.Targets[1].DeliveryID = "invalid"
			case "extra-hyphens":
				r.Targets[1].DeliveryID = "aa--aaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
			case "time":
				r.ResolvedAt = time.Time{}
			case "no-route-targets":
				r.Status = "NO_ROUTE"
			}
			if err := store.CompleteRoute(context.Background(), c, r, nil); err == nil {
				t.Fatalf("accepted malformed route %s", kind)
			}
		})
	}
}
func TestRouteRecoveryExhaustionAndExpiry(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(fmt.Sprint(expired), func(t *testing.T) {
			db := routeDB(t)
			s := dueRoute(t, db)
			store := hubmysql.NewRouteRecoveryStore(db)
			for attempt := 2; attempt <= 5; attempt++ {
				c, err := store.ClaimRoutes(context.Background(), "retry-route", 1)
				if err != nil || len(c) != 1 || c[0].AttemptNo != attempt {
					t.Fatalf("attempt %d: %+v %v", attempt, c, err)
				}
				if expired {
					if _, err = db.Exec("UPDATE trade_message SET route_lease_until=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(3)) WHERE msg_id=?", s.Trade.MsgID); err != nil {
						t.Fatal(err)
					}
					if n, e := store.RecoverExpiredRoutes(context.Background(), 1); e != nil || n != 1 {
						t.Fatalf("expired not recovered %d %v", n, e)
					}
					if err = store.CompleteRoute(context.Background(), c[0], resolvedRoute(c[0].MsgID), nil); !errors.Is(err, delivery.ErrLeaseLost) {
						t.Fatalf("expired owner completed %v", err)
					}
				} else {
					if err = store.CompleteRoute(context.Background(), c[0], model.RouteSnapshot{}, errors.New("lookup unavailable")); err != nil {
						t.Fatal(err)
					}
				}
				if attempt < 5 {
					if got, err := hubmysql.NewRouteRecoveryStore(db).ClaimRoutes(context.Background(), "restart", 1); err != nil || len(got) != 0 {
						t.Fatalf("restart lost delay %+v %v", got, err)
					}
					if _, err = db.Exec("UPDATE trade_message SET route_next_attempt_at=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(3)) WHERE msg_id=?", s.Trade.MsgID); err != nil {
						t.Fatal(err)
					}
				}
			}
			var state string
			var due sql.NullTime
			var count int
			if err := db.QueryRow("SELECT route_status,route_next_attempt_at,(SELECT COUNT(*) FROM route_attempt WHERE msg_id=?) FROM trade_message WHERE msg_id=?", s.Trade.MsgID, s.Trade.MsgID).Scan(&state, &due, &count); err != nil || state != "NEEDS_ATTENTION" || due.Valid || count != 5 {
				t.Fatalf("budget not bounded %s %v %d %v", state, due, count, err)
			}
			if got, err := store.ClaimRoutes(context.Background(), "sixth", 1); err != nil || len(got) != 0 {
				t.Fatalf("sixth claim %+v %v", got, err)
			}
			if expired {
				var n int
				if err := db.QueryRow("SELECT COUNT(*) FROM route_attempt WHERE msg_id=? AND outcome='UNKNOWN' AND summary='LEASE_EXPIRED' AND finished_at IS NOT NULL", s.Trade.MsgID).Scan(&n); err != nil || n != 4 {
					t.Fatalf("lost expiry history %d %v", n, err)
				}
			}
		})
	}
}

func TestRouteRecoveryClaimsCompeteWithoutLockingBacklog(t *testing.T) {
	db := routeDB(t)
	dueRoute(t, db)
	dueRoute(t, db)
	var name string
	if err := db.QueryRow("SELECT DATABASE()").Scan(&name); err != nil {
		t.Fatal(err)
	}
	cfg, err := mysqldriver.ParseDSN(os.Getenv("RECOVERY_TEST_ADMIN_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DBName = name
	connector, err := mysqldriver.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	held := sql.OpenDB(&claimHoldConnector{Connector: connector, entered: entered, release: release})
	defer held.Close()
	type answer struct {
		c   []routing.RouteRecoveryClaim
		err error
	}
	done := make(chan answer, 1)
	go func() {
		c, err := hubmysql.NewRouteRecoveryStore(held).ClaimRoutes(context.Background(), "held-route", 1)
		done <- answer{c, err}
	}()
	select {
	case <-entered:
	case a := <-done:
		t.Fatalf("did not reach claim commit %+v", a)
	case <-time.After(time.Second):
		t.Fatal("claim blocked")
	}
	second, err := hubmysql.NewRouteRecoveryStore(db).ClaimRoutes(context.Background(), "second-route", 1)
	if err != nil || len(second) != 1 {
		unblock()
		<-done
		t.Fatalf("overlocked routing backlog %+v %v", second, err)
	}
	unblock()
	first := <-done
	if first.err != nil || len(first.c) != 1 || first.c[0].MsgID == second[0].MsgID {
		t.Fatalf("overlapping route claims %+v %+v %v", first.c, second, first.err)
	}
}

type routeResolveFunc func(context.Context, model.TradeEvent) (model.RouteSnapshot, error)

func (f routeResolveFunc) ResolveTargets(ctx context.Context, e model.TradeEvent) (model.RouteSnapshot, error) {
	return f(ctx, e)
}
func TestRouteRecoveryLookupOutsideTransactionAndUIDStaysFixed(t *testing.T) {
	db := routeDB(t)
	s := dueRoute(t, db)
	store := hubmysql.NewRouteRecoveryStore(db)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	route := resolvedRoute(s.Trade.MsgID)
	resolver := routeResolveFunc(func(ctx context.Context, e model.TradeEvent) (model.RouteSnapshot, error) {
		close(entered)
		select {
		case <-release:
			return route, nil
		case <-ctx.Done():
			return model.RouteSnapshot{}, ctx.Err()
		}
	})
	done := make(chan error, 1)
	go func() {
		_, err := routing.NewRecoveryWorker(store, resolver, "live-route").RunOnce(context.Background())
		done <- err
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("worker never queried %v", err)
	case <-time.After(time.Second):
		t.Fatal("lookup stalled")
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	var id string
	err = tx.QueryRow("SELECT msg_id FROM trade_message WHERE msg_id=? FOR UPDATE NOWAIT", s.Trade.MsgID).Scan(&id)
	tx.Rollback()
	if err != nil {
		unblock()
		<-done
		t.Fatalf("lookup held message lock: %v", err)
	}
	unblock()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	// The binding authority can change after the first successful resolution.
	// Recovery must use the saved UID, never the current replacement binding.
	if _, err = db.Exec("INSERT INTO binding_current(client_id,platform_id,platform_uid,status,revision) VALUES(?,'PLATFORM_A','replacement-uid','BOUND',99)", s.Trade.ClientID); err != nil {
		t.Fatal(err)
	}
	got, err := hubmysql.NewRecoveryStore(db).ClaimDue(context.Background(), "PLATFORM_A", "delivery-after-route", 1)
	if err != nil || len(got) != 1 || got[0].Target != route.Targets[0] || got[0].AttemptNo != 1 || got[0].CycleAttempt != 1 || got[0].Notification.SecCode != s.Trade.SecCode || got[0].Notification.TraceID != s.Trade.TraceID || !got[0].Notification.ExecTime.Equal(s.Trade.ExecTime) {
		t.Fatalf("recovery did not use frozen UID/new send budget %+v %v", got, err)
	}
}
func TestRouteRecoveryLockWaitDoesNotExtendLease(t *testing.T) {
	db := routeDB(t)
	_, c := claimedRoute(t, db)
	if _, err := db.Exec("UPDATE trade_message SET route_lease_until=TIMESTAMPADD(MICROSECOND,150000,UTC_TIMESTAMP(3)) WHERE msg_id=?", c.MsgID); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec("UPDATE trade_message SET trace_id=trace_id WHERE msg_id=?", c.MsgID); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- hubmysql.NewRouteRecoveryStore(db).CompleteRoute(context.Background(), c, resolvedRoute(c.MsgID), nil)
	}()
	select {
	case err := <-done:
		t.Fatalf("completion did not wait for row: %v", err)
	case <-time.After(250 * time.Millisecond):
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = <-done; !errors.Is(err, delivery.ErrLeaseLost) {
		t.Fatalf("lease expired while waiting but finalized: %v", err)
	}
}
func TestRouteRecoveryExpiryCompetition(t *testing.T) {
	db := routeDB(t)
	_, c := claimedRoute(t, db)
	store := hubmysql.NewRouteRecoveryStore(db)
	if _, err := db.Exec("UPDATE trade_message SET route_lease_until=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(3)) WHERE msg_id=?", c.MsgID); err != nil {
		t.Fatal(err)
	}
	type answer struct {
		n   int
		err error
	}
	done := make(chan answer, 2)
	start := make(chan struct{})
	for range 2 {
		go func() { <-start; n, err := store.RecoverExpiredRoutes(context.Background(), 1); done <- answer{n, err} }()
	}
	close(start)
	a, b := <-done, <-done
	if a.err != nil || b.err != nil || a.n+b.n != 1 {
		t.Fatalf("expiry not once %+v %+v", a, b)
	}
	if _, err := db.Exec("UPDATE trade_message SET route_next_attempt_at=UTC_TIMESTAMP(3) WHERE msg_id=?", c.MsgID); err != nil {
		t.Fatal(err)
	}
	next, err := store.ClaimRoutes(context.Background(), "new-route-owner", 1)
	if err != nil || len(next) != 1 || next[0].LeaseVersion != c.LeaseVersion+1 {
		t.Fatalf("new owner %+v %v", next, err)
	}
	if err = store.CompleteRoute(context.Background(), c, resolvedRoute(c.MsgID), nil); !errors.Is(err, delivery.ErrLeaseLost) {
		t.Fatalf("stale owner overwrote current %v", err)
	}
	if err = store.CompleteRoute(context.Background(), next[0], resolvedRoute(c.MsgID), nil); err != nil {
		t.Fatal(err)
	}
}
func TestRouteRecoveryIndexesAndEligibility(t *testing.T) {
	db := routeDB(t)
	s := dueRoute(t, db)
	store := hubmysql.NewRouteRecoveryStore(db)
	if _, err := store.ClaimRoutes(context.Background(), "", 1); err == nil {
		t.Fatal("empty owner accepted")
	}
	for _, n := range []int{0, 5} {
		if _, err := store.ClaimRoutes(context.Background(), "valid", n); err == nil {
			t.Fatalf("claim limit %d accepted", n)
		}
	}
	for _, n := range []int{0, 101} {
		if _, err := store.RecoverExpiredRoutes(context.Background(), n); err == nil {
			t.Fatalf("expiry limit %d accepted", n)
		}
	}
	if _, err := db.Exec("UPDATE trade_message SET route_next_attempt_at=TIMESTAMPADD(HOUR,1,UTC_TIMESTAMP(3)) WHERE msg_id=?", s.Trade.MsgID); err != nil {
		t.Fatal(err)
	}
	if got, err := store.ClaimRoutes(context.Background(), "future", 1); err != nil || len(got) != 0 {
		t.Fatalf("future route claimed %+v %v", got, err)
	}
	for _, tc := range []struct{ key, sql string }{
		{"idx_routes_due", `EXPLAIN SELECT msg_id FROM trade_message FORCE INDEX(idx_routes_due) WHERE route_status='PENDING' AND route_next_attempt_at<=UTC_TIMESTAMP(3) AND route_cycle_attempt_count<5 ORDER BY route_next_attempt_at,msg_id LIMIT 4`},
		{"idx_routes_due", `EXPLAIN SELECT msg_id FROM trade_message FORCE INDEX(idx_routes_due) WHERE route_status='RETRY_WAIT' AND route_next_attempt_at<=UTC_TIMESTAMP(3) AND route_cycle_attempt_count<5 ORDER BY route_next_attempt_at,msg_id LIMIT 4`},
		{"idx_routes_expiry", `EXPLAIN SELECT msg_id FROM trade_message FORCE INDEX(idx_routes_expiry) WHERE route_status='IN_FLIGHT' AND route_lease_until<=UTC_TIMESTAMP(3) ORDER BY route_lease_until,msg_id LIMIT 4`},
	} {
		rows, err := db.Query(tc.sql)
		if err != nil {
			t.Fatal(err)
		}
		cols, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]sql.RawBytes, len(cols))
			ptr := make([]any, len(cols))
			for i := range values {
				ptr[i] = &values[i]
			}
			if err = rows.Scan(ptr...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			plan := map[string]string{}
			for i, col := range cols {
				plan[col] = string(values[i])
			}
			t.Logf("route EXPLAIN %+v", plan)
			if plan["key"] != tc.key || strings.Contains(plan["Extra"], "filesort") {
				t.Errorf("unbounded route scheduler access %+v", plan)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}
