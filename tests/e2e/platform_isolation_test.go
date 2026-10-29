//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	redisclient "github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"partner-hub/internal/adapter"
	"partner-hub/internal/binding"
	"partner-hub/internal/delivery"
	hubmysql "partner-hub/internal/infra/mysql"
	hubredis "partner-hub/internal/infra/redis"
	"partner-hub/internal/ingress"
	"partner-hub/internal/mock/platform1"
	"partner-hub/internal/mock/platform2"
	"partner-hub/internal/model"
	"partner-hub/internal/routing"
)

// All scans and mutations below target fresh schemas. In particular this test
// never starts an archive Flush or a worker against the running demo database.
func isolationDB(t *testing.T, migrations []string) *sql.DB {
	t.Helper()
	dsn := required(t, "RECOVERY_TEST_ADMIN_DSN")
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Net != "tcp" || cfg.Addr != "127.0.0.1:13306" {
		t.Fatal("explicit local isolated MySQL required")
	}
	admin, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "e2e_isolation_" + strings.ReplaceAll(model.NewID(), "-", "")
	if _, err = admin.Exec("CREATE DATABASE " + name + " CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin"); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg.DBName, cfg.ParseTime = name, true
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(12)
	t.Cleanup(func() {
		db.Close()
		if _, err := admin.Exec("DROP DATABASE " + name); err != nil {
			t.Errorf("cleanup own schema: %v", err)
		}
		admin.Close()
	})
	for _, file := range migrations {
		raw, err := os.ReadFile("../../deploy/mysql/" + file)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		// Migration PREPARE/user-variable statements must share one connection.
		for _, stmt := range strings.Split(string(raw), ";") {
			if strings.TrimSpace(stmt) == "" {
				continue
			}
			if _, err = conn.ExecContext(context.Background(), stmt); err != nil {
				conn.Close()
				t.Fatal(file, err)
			}
		}
		conn.Close()
	}
	return db
}

type isolationResponse struct {
	response model.TradeResponse
	code     int
	err      error
}

func isolationPost(ctx context.Context, url, clientID, id string) isolationResponse {
	fixture, err := os.ReadFile("../fixtures/trades/normal.json")
	if err != nil {
		return isolationResponse{err: err}
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(fixture, &fields); err != nil {
		return isolationResponse{err: err}
	}
	for key, value := range map[string]string{"msgId": id, "clientId": clientID, "traceId": "trace-" + id} {
		fields[key], _ = json.Marshal(value)
	}
	body, _ := json.Marshal(fields)
	req, err := http.NewRequestWithContext(ctx, "POST", url+"/api/v1/trades", bytes.NewReader(body))
	if err != nil {
		return isolationResponse{err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 8 * time.Second}).Do(req)
	if err != nil {
		return isolationResponse{err: err}
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 8193))
	if err != nil {
		return isolationResponse{err: err}
	}
	r := isolationResponse{code: response.StatusCode}
	r.err = json.Unmarshal(raw, &r.response)
	return r
}

// A missing shared budget would let recovery claim while a first HTTP send is
// in flight (or let first send during recovery). A missing platform boundary
// would prevent B's independent Kafka consumer from persisting these receipts.
func TestPlatformIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	hubDB := isolationDB(t, []string{"migrations/001_bindings.sql", "migrations/002_delivery_records.sql", "migrations/004_handoff.sql", "migrations/005_recovery.sql", "migrations/006_route_recovery.sql", "migrations/007_manual_operation.sql"})
	p1DB := isolationDB(t, []string{"mock-platform1/001_received.sql"})
	p2DB := isolationDB(t, []string{"mock-platform2/001_received.sql"})
	p1Handler, err := platform1.NewHandler(platform1.NewMySQLStore(p1DB), platform1.PersistDelay, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	p1HTTP := httptest.NewServer(p1Handler)
	defer p1HTTP.Close()
	a, err := adapter.NewPlatform1(p1HTTP.URL + "/notifications/trades")
	if err != nil {
		t.Fatal(err)
	}
	brokers := strings.Split(required(t, "KAFKA_TEST_BROKERS"), ",")
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer adminClient.Close()
	admin := kadm.NewClient(adminClient)
	topic := "e2e-isolation-" + model.NewID()
	if _, err = admin.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 3*time.Second)
		defer done()
		if _, err := admin.DeleteTopic(cleanup, topic); err != nil {
			t.Errorf("cleanup own Kafka topic: %v", err)
		}
	}()
	b, err := adapter.NewPlatform2(brokers, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	consumer, err := platform2.NewConsumer(platform2.ConsumerConfig{Brokers: brokers, Topic: topic, Group: "e2e-isolation-" + model.NewID()}, platform2.NewMySQLStore(p2DB))
	if err != nil {
		t.Fatal(err)
	}
	consume, stop := context.WithCancel(ctx)
	consumerDone := make(chan error, 1)
	go func() { consumerDone <- consumer.Run(consume) }()
	defer func() { stop(); consumer.Close(); <-consumerDone }()
	redisOptions := &redisclient.Options{Addr: required(t, "HUB_REDIS_ADDR"), DB: 2}
	cache := hubredis.NewBindings(redisOptions)
	defer cache.Close()
	archive, err := hubredis.NewArchiveWithConfig(redisOptions, hubmysql.NewArchiveStore(hubDB), hubredis.ArchiveConfig{Namespace: "e2e-isolation-" + model.NewID()})
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	clientID := "isolation-" + model.NewID()
	defer cache.Invalidate(context.Background(), clientID)
	uidA, uidB := "A-"+model.NewID(), "B-"+model.NewID()
	bindings := hubmysql.NewBindings(hubDB)
	for platform, uid := range map[string]string{"PLATFORM_A": uidA, "PLATFORM_B": uidB} {
		result, err := bindings.Apply(ctx, model.BindingEvent{SchemaVersion: 1, EventID: model.NewID(), EventType: "BIND", PlatformID: platform, ClientID: clientID, PlatformUID: uid, OccurredAt: time.Now().UnixMilli()})
		if err != nil || result.Outcome != "APPLIED" {
			t.Fatalf("binding=%+v err=%v", result, err)
		}
	}
	adapters := map[string]delivery.Adapter{"PLATFORM_A": a, "PLATFORM_B": b}
	newBudget := func(paused bool) *delivery.PlatformBudget {
		budget, err := delivery.NewPlatformBudget(map[string]delivery.BudgetConfig{
			"PLATFORM_A": {Total: 1, First: 1, Recovery: 1, Rate: 100, Burst: 100, Paused: paused},
			"PLATFORM_B": {Total: 2, First: 2, Recovery: 1, Rate: 100, Burst: 100},
		})
		if err != nil {
			t.Fatal(err)
		}
		return budget
	}
	newHub := func(budget *delivery.PlatformBudget) *httptest.Server {
		service := ingress.NewService(routing.NewRouter(binding.NewResolver(bindings, cache)), delivery.NewFirstDelivery(adapters, budget), archive, hubmysql.NewHandoffStore(hubDB))
		h := httptest.NewServer(ingress.NewHTTP(service, ingress.GateConfig{MaxInFlight: 8, Rate: 200, Burst: 40}))
		t.Cleanup(h.Close)
		return h
	}
	budget := newBudget(false)
	hub := newHub(budget)
	repo := hubmysql.NewRecoveryStore(hubDB)
	worker := delivery.NewWorker(repo, a, "PLATFORM_A", "e2e-isolation-worker", budget)
	checkAccepted := func(r isolationResponse) {
		t.Helper()
		if r.err != nil || r.code != 202 || r.response.Acceptance != "ACCEPTED" || r.response.RecordStatus != "PERSISTED" || r.response.Status != "RECOVERY_PENDING" || len(r.response.PlatformResults) != 2 {
			t.Fatalf("HTTP %d response=%+v err=%v", r.code, r.response, r.err)
		}
		for _, p := range r.response.PlatformResults {
			if p.PlatformID == "PLATFORM_B" && (p.FirstOutcome != model.Success || p.CurrentState != "SUCCEEDED") {
				t.Fatalf("B result=%+v", p)
			}
		}
	}
	receipts := func(db *sql.DB, id string) int {
		t.Helper()
		var n int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM received_notification WHERE trace_id=?", "trace-"+id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	checkB := func(id string) {
		t.Helper()
		eventually(t, ctx, func() bool { return receipts(p2DB, id) == 1 })
		var uid string
		var payload []byte
		if err := p2DB.QueryRowContext(ctx, "SELECT user_uid,payload FROM received_notification WHERE trace_id=?", "trace-"+id).Scan(&uid, &payload); err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if uid != uidB || json.Unmarshal(payload, &fields) != nil || len(fields) != 8 {
			t.Fatalf("B uid=%s payload=%s", uid, payload)
		}
		for _, key := range []string{"extUserId", "stockCode", "stockName", "operateType", "tradeDateTime", "marketNo", "bizType", "traceId"} {
			if _, ok := fields[key]; !ok {
				t.Fatalf("missing B field %s", key)
			}
		}
	}
	seed := model.NewID()
	checkAccepted(isolationPost(ctx, hub.URL, clientID, seed))
	checkB(seed)
	var deliveryID string
	if err := hubDB.QueryRowContext(ctx, "SELECT delivery_id FROM delivery_task WHERE msg_id=? AND platform_id='PLATFORM_A' AND status='RETRY_WAIT' AND first_outcome='UNKNOWN'", seed).Scan(&deliveryID); err != nil {
		t.Fatal(err)
	}
	if _, err := hubDB.ExecContext(ctx, "UPDATE delivery_task SET next_attempt_at=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(3)) WHERE delivery_id=?", deliveryID); err != nil {
		t.Fatal(err)
	}
	type taskFacts struct {
		count, cycle, version, attempts int
		status                          string
	}
	facts := func() taskFacts {
		t.Helper()
		var f taskFacts
		if err := hubDB.QueryRowContext(ctx, "SELECT attempt_count,cycle_attempt_count,lease_version,status,(SELECT COUNT(*) FROM delivery_attempt WHERE delivery_id=d.delivery_id) FROM delivery_task d WHERE delivery_id=?", deliveryID).Scan(&f.count, &f.cycle, &f.version, &f.status, &f.attempts); err != nil {
			t.Fatal(err)
		}
		return f
	}
	allAttemptFacts := func() [4]int {
		t.Helper()
		var f [4]int
		if err := hubDB.QueryRowContext(ctx, "SELECT SUM(attempt_count),SUM(cycle_attempt_count),SUM(lease_version),(SELECT COUNT(*) FROM delivery_attempt) FROM delivery_task").Scan(&f[0], &f[1], &f[2], &f[3]); err != nil {
			t.Fatal(err)
		}
		return f
	}
	initial := facts()
	if initial.count != 1 || initial.attempts != 1 {
		t.Fatalf("seed facts=%+v", initial)
	}
	blocked := model.NewID()
	firstDone := make(chan isolationResponse, 1)
	go func() { firstDone <- isolationPost(ctx, hub.URL, clientID, blocked) }()
	eventually(t, ctx, func() bool { return receipts(p1DB, blocked) == 1 })
	if n, err := worker.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("first owns only A slot: recovery claimed %d err=%v", n, err)
	}
	if got := facts(); got != initial {
		t.Fatalf("no quota changed durable attempt: before=%+v after=%+v", initial, got)
	}
	noFirstQuota := model.NewID()
	checkAccepted(isolationPost(ctx, hub.URL, clientID, noFirstQuota))
	checkAccepted(<-firstDone)
	if receipts(p1DB, noFirstQuota) != 0 {
		t.Fatal("A sent despite first quota occupied")
	}
	checkB(blocked)
	checkB(noFirstQuota)
	var unsent bool
	if err := hubDB.QueryRowContext(ctx, "SELECT a.send_started FROM delivery_task d JOIN delivery_attempt a USING(delivery_id) WHERE d.msg_id=? AND d.platform_id='PLATFORM_A' AND a.attempt_no=1", noFirstQuota).Scan(&unsent); err != nil || unsent {
		t.Fatalf("no quota was not explicit unsent: send_started=%v err=%v", unsent, err)
	}
	workerDone := make(chan error, 1)
	go func() {
		n, err := worker.RunOnce(ctx)
		if err == nil && n != 1 {
			err = fmt.Errorf("expected one recovery claim, got %d", n)
		}
		workerDone <- err
	}()
	eventually(t, ctx, func() bool { return receipts(p1DB, seed) == 2 })
	recoveryOccupied := facts()
	if recoveryOccupied.count != 2 || recoveryOccupied.status != "IN_FLIGHT" {
		t.Fatalf("recovery facts=%+v", recoveryOccupied)
	}
	noRecoveryQuota := model.NewID()
	checkAccepted(isolationPost(ctx, hub.URL, clientID, noRecoveryQuota))
	allBeforeNoQuota := allAttemptFacts()
	if n, err := worker.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("recovery owns only A slot: extra claim %d err=%v", n, err)
	}
	if got := facts(); got != recoveryOccupied {
		t.Fatalf("extra recovery changed facts before=%+v after=%+v", recoveryOccupied, got)
	}
	if got := allAttemptFacts(); got != allBeforeNoQuota {
		t.Fatalf("no quota claimed another task: before=%v after=%v", allBeforeNoQuota, got)
	}
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
	if receipts(p1DB, noRecoveryQuota) != 0 {
		t.Fatal("A first sent despite recovery using shared total slot")
	}
	checkB(noRecoveryQuota)
	if receipts(p2DB, seed) != 1 {
		t.Fatal("successful B was resent during A recovery")
	}
	pausedBudget := newBudget(true)
	pausedHub := newHub(pausedBudget)
	pausedID := model.NewID()
	checkAccepted(isolationPost(ctx, pausedHub.URL, clientID, pausedID))
	checkB(pausedID)
	beforePause := facts()
	allBeforePause := allAttemptFacts()
	var dueA int
	if err := hubDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM delivery_task WHERE platform_id='PLATFORM_A' AND status='RETRY_WAIT' AND next_attempt_at<=UTC_TIMESTAMP(3)").Scan(&dueA); err != nil || dueA == 0 {
		t.Fatalf("pause must face actual due responsibility: due=%d err=%v", dueA, err)
	}
	pausedWorker := delivery.NewWorker(repo, a, "PLATFORM_A", "e2e-paused-worker", pausedBudget)
	if n, err := pausedWorker.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("paused A claimed %d err=%v", n, err)
	}
	if got := facts(); got != beforePause {
		t.Fatalf("pause changed durable responsibility: before=%+v after=%+v", beforePause, got)
	}
	if got := allAttemptFacts(); got != allBeforePause {
		t.Fatalf("pause changed another task's attempts: before=%v after=%v", allBeforePause, got)
	}
	if receipts(p1DB, pausedID) != 0 {
		t.Fatal("paused A sent")
	}
	var wrongAUID int
	if err := p1DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM received_notification WHERE user_uid<>?", uidA).Scan(&wrongAUID); err != nil || wrongAUID != 0 {
		t.Fatalf("A fixed UID changed: invalid receipts=%d err=%v", wrongAUID, err)
	}
	var taskCount, pendingA, succeededB int
	if err := hubDB.QueryRowContext(ctx, "SELECT COUNT(*),SUM(platform_id='PLATFORM_A' AND status='RETRY_WAIT'),SUM(platform_id='PLATFORM_B' AND status='SUCCEEDED') FROM delivery_task").Scan(&taskCount, &pendingA, &succeededB); err != nil || taskCount != 10 || pendingA != 5 || succeededB != 5 {
		t.Fatalf("responsibility tasks=%d pendingA=%d successB=%d err=%v", taskCount, pendingA, succeededB, err)
	}
	t.Logf("A sustained actual timeout: seed=%s delivery=%s attempts=2 external receipts=2; B persisted 5 unique messages; first/recovery no-quota claims=0; paused A retains 5 responsibilities", seed, deliveryID)
}
