//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"partner-hub/internal/adapter"
	"strings"
	"sync"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"partner-hub/internal/delivery"
	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/model"
)

// Each scan test owns a fresh schema. It never claims older demo or test tasks.
func recoveryDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("RECOVERY_TEST_ADMIN_DSN")
	if dsn == "" {
		t.Fatal("RECOVERY_TEST_ADMIN_DSN required for isolated, disposable recovery schemas")
	}
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Net != "tcp" || cfg.Addr != "127.0.0.1:13306" {
		t.Fatal("recovery tests require the explicit local isolated MySQL instance")
	}
	admin, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "it_recovery_" + strings.ReplaceAll(model.NewID(), "-", "")
	if _, err = admin.Exec("CREATE DATABASE " + name + " CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin"); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg.DBName = name
	cfg.ParseTime = true
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		if _, err := admin.Exec("DROP DATABASE " + name); err != nil {
			t.Errorf("cleanup own recovery schema: %v", err)
		}
		admin.Close()
	})
	for _, file := range []string{"001_bindings.sql", "002_delivery_records.sql", "004_handoff.sql", "005_recovery.sql"} {
		raw, err := os.ReadFile("../../deploy/mysql/migrations/" + file)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		runs := 1
		if file == "005_recovery.sql" {
			runs = 2
		}
		for range runs {
			for _, stmt := range strings.Split(string(raw), ";") {
				if strings.TrimSpace(stmt) != "" {
					if _, err = conn.ExecContext(context.Background(), stmt); err != nil {
						conn.Close()
						t.Fatal(file, err)
					}
				}
			}
		}
		conn.Close()
	}
	return db
}
func dueRecovery(t *testing.T, db *sql.DB) model.FirstRoundSnapshot {
	t.Helper()
	s := abnormalSnapshot(t)
	if err := hubmysql.NewHandoffStore(db).Handoff(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE delivery_task SET next_attempt_at=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(3)) WHERE delivery_id=?", s.Attempts[1].Target.DeliveryID); err != nil {
		t.Fatal(err)
	}
	return s
}
func TestRecoveryClaimAtomicCompetition(t *testing.T) {
	db := recoveryDB(t)
	s := dueRecovery(t, db)
	store := hubmysql.NewRecoveryStore(db)
	start := make(chan struct{})
	results := make(chan []delivery.RecoveryClaim, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, owner := range []string{"worker-a", "worker-b"} {
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			<-start
			c, e := store.ClaimDue(context.Background(), "PLATFORM_B", owner, 1)
			results <- c
			errs <- e
		}(owner)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var got []delivery.RecoveryClaim
	for claims := range results {
		got = append(got, claims...)
	}
	if len(got) != 1 || got[0].Target.DeliveryID != s.Attempts[1].Target.DeliveryID || got[0].Target.PlatformUID != "fixed-failed-uid" || got[0].AttemptNo != 2 || got[0].CycleAttempt != 2 || got[0].LeaseVersion != 1 || got[0].LeaseUntil.IsZero() || time.Until(got[0].LeaseDeadline) <= 0 || time.Until(got[0].LeaseDeadline) > 15*time.Second {
		t.Fatalf("non-exclusive/incomplete claim %+v", got)
	}
	c := got[0]
	var status, outcome, owner string
	var count, cycle int
	var finished sql.NullTime
	if err := db.QueryRow(`SELECT d.status,d.attempt_count,d.cycle_attempt_count,a.outcome,a.finished_at,a.lease_owner FROM delivery_task d JOIN delivery_attempt a ON a.delivery_id=d.delivery_id AND a.attempt_no=d.attempt_count WHERE d.delivery_id=?`, c.Target.DeliveryID).Scan(&status, &count, &cycle, &outcome, &finished, &owner); err != nil || status != "IN_FLIGHT" || count != 2 || cycle != 2 || outcome != "IN_FLIGHT" || finished.Valid || owner != c.Owner {
		t.Fatalf("claim not atomic %s %d %d %s %v %s %v", status, count, cycle, outcome, finished, owner, err)
	}
	if c.Notification.SecCode != s.Trade.SecCode || c.Notification.TraceID != s.Trade.TraceID || !c.Notification.ExecTime.Equal(s.Trade.ExecTime) {
		t.Fatalf("notification changed %+v", c.Notification)
	}
}
func TestRecoveryClaimEligibility(t *testing.T) {
	db := recoveryDB(t)
	s := dueRecovery(t, db)
	store := hubmysql.NewRecoveryStore(db)
	if _, err := db.Exec("UPDATE delivery_task SET next_attempt_at=TIMESTAMPADD(HOUR,1,UTC_TIMESTAMP(3)) WHERE delivery_id=?", s.Attempts[1].Target.DeliveryID); err != nil {
		t.Fatal(err)
	}
	for _, platform := range []string{"PLATFORM_A", "PLATFORM_B"} {
		c, err := store.ClaimDue(context.Background(), platform, "eligible", 4)
		if err != nil || len(c) != 0 {
			t.Fatalf("claimed success/future %+v %v", c, err)
		}
	}
	if _, err := db.Exec("UPDATE delivery_task SET status='PENDING',next_attempt_at=UTC_TIMESTAMP(3) WHERE delivery_id=?", s.Attempts[1].Target.DeliveryID); err != nil {
		t.Fatal(err)
	}
	c, err := store.ClaimDue(context.Background(), "PLATFORM_B", "eligible", 1)
	if err != nil || len(c) != 1 {
		t.Fatalf("pending not claimable %+v %v", c, err)
	}
}
func TestRecoveryClaimValidLeaseOnly(t *testing.T) {
	for _, invalid := range []string{"owner", "version", "expired"} {
		t.Run(invalid, func(t *testing.T) {
			db := recoveryDB(t)
			s := dueRecovery(t, db)
			store := hubmysql.NewRecoveryStore(db)
			claims, err := store.ClaimDue(context.Background(), "PLATFORM_B", "current-owner", 1)
			if err != nil || len(claims) != 1 {
				t.Fatalf("claim %+v %v", claims, err)
			}
			c := claims[0]
			p := s.Attempts[1].Request
			if err = store.SavePrepared(context.Background(), c, p); err != nil {
				t.Fatal(err)
			}
			stale := c
			switch invalid {
			case "owner":
				stale.Owner = "old"
			case "version":
				stale.LeaseVersion++
			case "expired":
				_, err = db.Exec("UPDATE delivery_task SET lease_until=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(3)) WHERE delivery_id=?", c.Target.DeliveryID)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err = store.SavePrepared(context.Background(), stale, p); !errors.Is(err, delivery.ErrLeaseLost) {
				t.Fatalf("stale prepare accepted %v", err)
			}
			result := model.SendResult{Outcome: model.Success, SendStarted: true, StartedAt: time.Now(), FinishedAt: time.Now()}
			if err = store.Complete(context.Background(), stale, result); !errors.Is(err, delivery.ErrLeaseLost) {
				t.Fatalf("stale completion accepted %v", err)
			}
			if invalid == "expired" {
				return
			}
			if err = store.Complete(context.Background(), c, result); err != nil {
				t.Fatal(err)
			}
			var status, first, version string
			var payload []byte
			if err = db.QueryRow("SELECT status,first_outcome,protocol_version,prepared_payload FROM delivery_task WHERE delivery_id=?", c.Target.DeliveryID).Scan(&status, &first, &version, &payload); err != nil || status != "SUCCEEDED" || first != "UNKNOWN" || version != p.ProtocolVersion || string(payload) != string(p.Payload) {
				t.Fatalf("current completion lost facts %s %s %s %s %v", status, first, version, payload, err)
			}
			if err = store.Complete(context.Background(), c, result); !errors.Is(err, delivery.ErrLeaseLost) {
				t.Fatalf("duplicate completion accepted %v", err)
			}
		})
	}
}

func TestRecoveryClaimLeaseExpiresWhileWaitingForLock(t *testing.T) {
	db := recoveryDB(t)
	dueRecovery(t, db)
	store := hubmysql.NewRecoveryStore(db)
	claims, err := store.ClaimDue(context.Background(), "PLATFORM_B", "lock-wait", 1)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim %+v %v", claims, err)
	}
	c := claims[0]
	if _, err = db.Exec("UPDATE delivery_task SET lease_until=TIMESTAMPADD(MICROSECOND,500000,UTC_TIMESTAMP(3)) WHERE delivery_id=?", c.Target.DeliveryID); err != nil {
		t.Fatal(err)
	}
	hold, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Rollback()
	var id string
	if err = hold.QueryRow("SELECT delivery_id FROM delivery_task WHERE delivery_id=? FOR UPDATE", c.Target.DeliveryID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- store.Complete(context.Background(), c, model.SendResult{Outcome: model.RetryableFailure, Summary: "waited"})
	}()
	time.Sleep(700 * time.Millisecond)
	if err = hold.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = <-done; !errors.Is(err, delivery.ErrLeaseLost) {
		t.Fatalf("expired lease accepted after lock wait: %v", err)
	}
}

func TestRecoveryClaimSendDoesNotHoldTransaction(t *testing.T) {
	db := recoveryDB(t)
	s := abnormalSnapshot(t)
	s.Attempts[0].Result.Outcome = model.Unknown
	s.Attempts[1].Result.Outcome = model.Success
	store := hubmysql.NewRecoveryStore(db)
	if err := hubmysql.NewHandoffStore(db).Handoff(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE delivery_task SET next_attempt_at=UTC_TIMESTAMP(3) WHERE delivery_id=?", s.Attempts[0].Target.DeliveryID); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		io.WriteString(w, `{"code":0}`)
	}))
	defer server.Close()
	defer unblock()
	ad, err := adapter.NewPlatform1(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	worker := delivery.NewWorker(store, ad, "PLATFORM_A", "blocked-http")
	done := make(chan error, 1)
	go func() {
		n, err := worker.RunOnce(context.Background())
		if n != 1 && err == nil {
			err = errors.New("no task executed")
		}
		done <- err
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("no send: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("send not started")
	}
	// A different connection can lock the task NOWAIT while the external server
	// is still blocked, proving no task transaction spans network I/O.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	var id string
	err = tx.QueryRow("SELECT delivery_id FROM delivery_task WHERE delivery_id=? FOR UPDATE NOWAIT", s.Attempts[0].Target.DeliveryID).Scan(&id)
	tx.Rollback()
	if err != nil {
		t.Fatalf("network send holds task row lock: %v", err)
	}
	unblock()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	var status, protocol string
	var sent bool
	if err = db.QueryRow(`SELECT d.status,a.protocol_version,a.send_started FROM delivery_task d JOIN delivery_attempt a ON a.delivery_id=d.delivery_id AND a.attempt_no=2 WHERE d.delivery_id=?`, id).Scan(&status, &protocol, &sent); err != nil || status != "SUCCEEDED" || protocol != "platform1.v1" || !sent {
		t.Fatalf("network result missing %s %s %v %v", status, protocol, sent, err)
	}
}

func TestRecoveryClaimIndexAccess(t *testing.T) {
	db := recoveryDB(t)
	dueRecovery(t, db)
	for _, tc := range []struct{ name, query string }{
		{"idx_tasks_due", `EXPLAIN SELECT delivery_id FROM delivery_task FORCE INDEX(idx_tasks_due) WHERE platform_id='PLATFORM_B' AND status='RETRY_WAIT' AND next_attempt_at<=UTC_TIMESTAMP(3) AND cycle_attempt_count<5 ORDER BY next_attempt_at,delivery_id LIMIT 4`},
		{"idx_tasks_due", `EXPLAIN SELECT delivery_id FROM delivery_task FORCE INDEX(idx_tasks_due) WHERE platform_id='PLATFORM_B' AND status='PENDING' AND next_attempt_at<=UTC_TIMESTAMP(3) AND cycle_attempt_count<5 ORDER BY next_attempt_at,delivery_id LIMIT 4`},
		{"idx_tasks_expiry", `EXPLAIN SELECT delivery_id FROM delivery_task WHERE status='IN_FLIGHT' AND lease_until<=UTC_TIMESTAMP(3) ORDER BY lease_until,delivery_id LIMIT 4`},
	} {
		rows, err := db.Query(tc.query)
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
			t.Logf("EXPLAIN %s: type=%s key=%s rows=%s extra=%s", tc.name, plan["type"], plan["key"], plan["rows"], plan["Extra"])
			if plan["key"] != tc.name || strings.Contains(plan["Extra"], "filesort") {
				t.Errorf("unexpected recovery access path %+v", plan)
			}
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
	}
}

// Hold the real ClaimDue transaction at Commit so another claimant observes
// exactly the rows locked by the production SELECT, rather than a copied SQL.
type claimHoldConnector struct {
	driver.Connector
	entered, release chan struct{}
}

func (c *claimHoldConnector) Connect(ctx context.Context) (driver.Conn, error) {
	base, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &claimHoldConn{handoffFaultConn: &handoffFaultConn{Conn: base, fault: &handoffCommitFault{}}, entered: c.entered, release: c.release}, nil
}

type claimHoldConn struct {
	*handoffFaultConn
	entered, release chan struct{}
}

func (c *claimHoldConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &claimHoldTx{Tx: tx, entered: c.entered, release: c.release}, nil
}

type claimHoldTx struct {
	driver.Tx
	entered, release chan struct{}
}

func (t *claimHoldTx) Commit() error { close(t.entered); <-t.release; return t.Tx.Commit() }
func TestRecoveryClaimSkipsOneLockedTaskWithoutLockingBacklog(t *testing.T) {
	db := recoveryDB(t)
	dueRecovery(t, db)
	dueRecovery(t, db)
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
	heldDB := sql.OpenDB(&claimHoldConnector{Connector: connector, entered: entered, release: release})
	t.Cleanup(func() { heldDB.Close() })
	type answer struct {
		claims []delivery.RecoveryClaim
		err    error
	}
	first := make(chan answer, 1)
	go func() {
		c, e := hubmysql.NewRecoveryStore(heldDB).ClaimDue(context.Background(), "PLATFORM_B", "held-claim", 1)
		first <- answer{c, e}
	}()
	select {
	case <-entered:
	case a := <-first:
		t.Fatalf("first claim did not reach commit: %+v", a)
	case <-time.After(time.Second):
		t.Fatal("first claim stalled")
	}
	second, err := hubmysql.NewRecoveryStore(db).ClaimDue(context.Background(), "PLATFORM_B", "skip-locked", 1)
	if err != nil || len(second) != 1 {
		t.Fatalf("small batch locked the remaining backlog: claims=%+v err=%v", second, err)
	}
	unblock()
	a := <-first
	if a.err != nil || len(a.claims) != 1 || a.claims[0].Target.DeliveryID == second[0].Target.DeliveryID {
		t.Fatalf("overlapping claims first=%+v second=%+v err=%v", a.claims, second, a.err)
	}
}
