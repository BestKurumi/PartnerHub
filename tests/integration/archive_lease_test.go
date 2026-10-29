//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	mysqldriver "github.com/go-sql-driver/mysql"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"
	hubmysql "partner-hub/internal/infra/mysql"
	hubredis "partner-hub/internal/infra/redis"
	"partner-hub/internal/model"
)

// All keys and database records belong to this test; never scan old demo pending.
func leaseArchive(t *testing.T, client *redisclient.Client, namespace string, store hubredis.ArchiveStore) *hubredis.Archive {
	t.Helper()
	a, err := hubredis.NewArchiveWithConfig(client.Options(), store, hubredis.ArchiveConfig{Namespace: namespace, Owner: model.NewID()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })
	return a
}
func leaseNamespace(t *testing.T, client *redisclient.Client) string {
	t.Helper()
	namespace := "it-lease-" + model.NewID()
	t.Cleanup(func() {
		ctx := context.Background()
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, namespace+":*", 100).Result()
			if err != nil {
				t.Error(err)
				return
			}
			if len(keys) > 0 {
				if err = client.Del(ctx, keys...).Err(); err != nil {
					t.Error(err)
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	})
	return namespace
}
func TestArchiveLeaseNamespaceAndCapacity(t *testing.T) {
	client := bindingRedis(t)
	namespace := leaseNamespace(t, client)
	db := recoveryDB(t)
	a := leaseArchive(t, client, namespace, hubmysql.NewArchiveStore(db))
	ctx := context.Background()
	if err := a.Preflight(ctx); err != nil {
		t.Fatal(err)
	}
	s := archiveSnapshot(t)
	body, _ := json.Marshal(s)
	if err := a.Stage(ctx, s); err != nil {
		t.Fatal(err)
	}
	if client.Exists(ctx, namespace+":archive:message:{"+s.Trade.MsgID+"}").Val() != 1 {
		t.Fatal("isolated snapshot not saved")
	}
	before := time.Now()
	c, err := a.Capacity(ctx)
	if err != nil || c.Pending != 1 || c.Bytes != int64(len(body)) || c.MaxMemory <= 0 || c.UsedMemory <= 0 || c.SampledAt.Before(before) || c.SampledAt.After(time.Now()) {
		t.Fatalf("capacity observation: %+v %v", c, err)
	}
	if err = a.Stage(ctx, s); err != nil {
		t.Fatal(err)
	}
	c, err = a.Capacity(ctx)
	if err != nil || c.Pending != 1 || c.Bytes != int64(len(body)) {
		t.Fatalf("same Stage double-counted: %+v %v", c, err)
	}
	if err = a.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	c, err = a.Capacity(ctx)
	if err != nil || c.Pending != 0 || c.Bytes != 0 {
		t.Fatalf("ack did not release accounting: %+v %v", c, err)
	}
}

type leaseBlockingStore struct {
	inner   hubredis.ArchiveStore
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (s *leaseBlockingStore) Save(ctx context.Context, snapshots []model.FirstRoundSnapshot) error {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.resume:
		return s.inner.Save(ctx, snapshots)
	case <-ctx.Done():
		return ctx.Err()
	}
}
func TestArchiveLeaseConcurrentOwners(t *testing.T) {
	client := bindingRedis(t)
	namespace := leaseNamespace(t, client)
	db := recoveryDB(t)
	store := hubmysql.NewArchiveStore(db)
	blocked := &leaseBlockingStore{inner: store, entered: make(chan struct{}), resume: make(chan struct{})}
	a := leaseArchive(t, client, namespace, blocked)
	b := leaseArchive(t, client, namespace, store)
	ctx := context.Background()
	s := archiveSnapshot(t)
	if err := a.Stage(ctx, s); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Flush(ctx) }()
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("first owner did not claim")
	}
	defer func() {
		select {
		case <-blocked.resume:
		default:
			close(blocked.resume)
		}
	}()
	if err := b.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM trade_message WHERE msg_id=?", s.Trade.MsgID).Scan(&n); err != nil || n != 0 {
		t.Fatalf("second owner wrote live lease: %d %v", n, err)
	}
	close(blocked.resume)

	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestArchiveLeaseExpiredOwnerCannotAck(t *testing.T) {
	client := bindingRedis(t)
	namespace := leaseNamespace(t, client)
	db := recoveryDB(t)
	store := hubmysql.NewArchiveStore(db)
	first := &leaseBlockingStore{inner: store, entered: make(chan struct{}), resume: make(chan struct{})}
	second := &leaseBlockingStore{inner: store, entered: make(chan struct{}), resume: make(chan struct{})}
	a := leaseArchive(t, client, namespace, first)
	b := leaseArchive(t, client, namespace, second)
	ctx := context.Background()
	s := archiveSnapshot(t)
	if err := a.Stage(ctx, s); err != nil {
		t.Fatal(err)
	}
	da := make(chan error, 1)
	go func() { da <- a.Flush(ctx) }()
	<-first.entered
	lease := namespace + ":archive:lease:{" + s.Trade.MsgID + "}"
	old, err := client.HGetAll(ctx, lease).Result()
	if err != nil || old["token"] == "" || old["revision"] != s.ArchiveRevision {
		t.Fatalf("missing lease identity: %v %v", old, err)
	}
	// Advance only this test lease to Redis TIME - 1. No wall-clock sleeps or server clock changes.
	expire := redisclient.NewScript(`local t=redis.call('TIME'); local now=t[1]*1000+math.floor(t[2]/1000); redis.call('HSET',KEYS[1],'expires',now-1); redis.call('ZADD',KEYS[2],now-1,ARGV[1]); return now`)
	if err = expire.Run(ctx, client, []string{lease, namespace + ":archive:pending"}, s.Trade.MsgID).Err(); err != nil {
		t.Fatal(err)
	}
	dbch := make(chan error, 1)
	go func() { dbch <- b.Flush(ctx) }()
	<-second.entered
	current := client.HGetAll(ctx, lease).Val()
	if current["token"] == old["token"] || current["version"] == old["version"] {
		t.Fatalf("takeover reused fencing identity: %v -> %v", old, current)
	}
	close(first.resume)
	if err = <-da; err == nil {
		t.Fatal("old owner falsely acknowledged lease")
	}
	if client.Exists(ctx, namespace+":archive:message:{"+s.Trade.MsgID+"}").Val() != 1 {
		t.Fatal("old owner deleted new owner's snapshot")
	}
	close(second.resume)
	if err = <-dbch; err != nil {
		t.Fatal(err)
	}
	if client.Exists(ctx, namespace+":archive:message:{"+s.Trade.MsgID+"}").Val() != 0 {
		t.Fatal("new owner failed to acknowledge")
	}
}
func TestArchiveLeaseRevisionAndFailurePreserved(t *testing.T) {
	client := bindingRedis(t)
	namespace := leaseNamespace(t, client)
	db := recoveryDB(t)
	store := hubmysql.NewArchiveStore(db)
	ctx := context.Background()
	s := archiveSnapshot(t)
	failed := leaseArchive(t, client, namespace, failingArchiveStore{})
	good := leaseArchive(t, client, namespace, store)
	if err := failed.Stage(ctx, s); err != nil {
		t.Fatal(err)
	}
	if err := failed.Flush(ctx); err == nil {
		t.Fatal("DB failure falsely accepted")
	}
	if err := good.Flush(ctx); err != nil {
		t.Fatalf("released failed lease cannot replay: %v", err)
	}
	blocked := &leaseBlockingStore{inner: store, entered: make(chan struct{}), resume: make(chan struct{})}
	a := leaseArchive(t, client, namespace, blocked)
	if err := a.Stage(ctx, s); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Flush(ctx) }()
	<-blocked.entered
	changed := s
	changed.ArchiveRevision = model.NewID()
	body, _ := json.Marshal(changed)
	key := namespace + ":archive:message:{" + s.Trade.MsgID + "}"
	if err := client.Set(ctx, key, body, 0).Err(); err != nil {
		t.Fatal(err)
	}
	close(blocked.resume)
	if err := <-done; err == nil {
		t.Fatal("changed revision falsely acknowledged")
	}
	var actual model.FirstRoundSnapshot
	if err := json.Unmarshal([]byte(client.Get(ctx, key).Val()), &actual); err != nil || actual.ArchiveRevision != changed.ArchiveRevision {
		t.Fatalf("new revision lost: %v", err)
	}
}

func TestArchiveLeaseLegacyAccounting(t *testing.T) {
	client := bindingRedis(t)
	namespace := leaseNamespace(t, client)
	a := leaseArchive(t, client, namespace, failingArchiveStore{})
	ctx := context.Background()
	s := archiveSnapshot(t)
	body, _ := json.Marshal(s)
	key := namespace + ":archive:message:{" + s.Trade.MsgID + "}"
	if err := client.Set(ctx, key, body, 0).Err(); err != nil {
		t.Fatal(err)
	}
	client.ZAdd(ctx, namespace+":archive:pending", redisclient.Z{Score: 1, Member: s.Trade.MsgID})
	c, err := a.Capacity(ctx)
	if err != nil || c.Pending != 1 || c.Bytes != 256*1024 {
		t.Fatalf("legacy size underestimated: %+v %v", c, err)
	}
	if err = a.Stage(ctx, s); err != nil {
		t.Fatal(err)
	}
	c, err = a.Capacity(ctx)
	if err != nil || c.Bytes != int64(len(body)) {
		t.Fatalf("legacy confirmation not reconciled: %+v %v", c, err)
	}
}

func TestArchiveLeaseBatchLimitsAndEmpty(t *testing.T) {
	for _, large := range []bool{false, true} {
		t.Run(map[bool]string{false: "100-record-limit", true: "4MiB-limit"}[large], func(t *testing.T) {
			client := bindingRedis(t)
			namespace := leaseNamespace(t, client)
			db := recoveryDB(t)
			a := leaseArchive(t, client, namespace, hubmysql.NewArchiveStore(db))
			ctx := context.Background()
			count := 101
			if large {
				count = 60
			}
			var firstSize int
			for i := 0; i < count; i++ {
				s := archiveSnapshot(t)
				if large {
					s.Trade.RawPayload = append(s.Trade.RawPayload, []byte(strings.Repeat(" ", 64000-len(s.Trade.RawPayload)))...)
				}
				body, _ := json.Marshal(s)
				if i == 0 {
					firstSize = len(body)
				}
				if err := a.Stage(ctx, s); err != nil {
					t.Fatal(err)
				}
			}
			if err := a.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			var persisted int
			if err := db.QueryRow("SELECT COUNT(*) FROM trade_message").Scan(&persisted); err != nil {
				t.Fatal(err)
			}
			expected := 100
			if large {
				expected = (4 * 1024 * 1024) / firstSize
			}
			if persisted != expected {
				t.Fatalf("batch size: got=%d expected=%d snapshotBytes=%d", persisted, expected, firstSize)
			}
			c, err := a.Capacity(ctx)
			if err != nil || c.Pending != int64(count-persisted) {
				t.Fatalf("unclaimed remainder lost: %+v %v", c, err)
			}
			if err = a.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err = a.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err = db.QueryRow("SELECT COUNT(*) FROM trade_message").Scan(&persisted); err != nil || persisted != count {
				t.Fatalf("remainder not saved: %d %v", persisted, err)
			}
		})
	}
	// A fail-on-write store makes accidental empty transactions observable.
	client := bindingRedis(t)
	namespace := leaseNamespace(t, client)
	a := leaseArchive(t, client, namespace, failingArchiveStore{})
	if err := a.Flush(context.Background()); err != nil {
		t.Fatalf("empty flush called database: %v", err)
	}
}

func TestArchiveLeaseCommitAckLostReplayPreservesRecovery(t *testing.T) {
	client := bindingRedis(t)
	namespace := leaseNamespace(t, client)
	db := recoveryDB(t)
	ctx := context.Background()
	s := archiveSnapshot(t)
	var dbName string
	if err := db.QueryRow("SELECT DATABASE()").Scan(&dbName); err != nil {
		t.Fatal(err)
	}
	cfg, err := mysqldriver.ParseDSN(os.Getenv("RECOVERY_TEST_ADMIN_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DBName = dbName
	cfg.ParseTime = true
	connector, err := mysqldriver.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	fault := &handoffCommitFault{afterCommit: func() error {
		_, e := db.Exec("UPDATE delivery_task SET status='NEEDS_ATTENTION',attempt_count=5,cycle_no=3 WHERE delivery_id=?", s.Route.Targets[0].DeliveryID)
		return e
	}}
	faultDB := sql.OpenDB(&handoffFaultConnector{Connector: connector, fault: fault})
	t.Cleanup(func() { faultDB.Close() })
	a := leaseArchive(t, client, namespace, hubmysql.NewArchiveStore(faultDB))
	b := leaseArchive(t, client, namespace, hubmysql.NewArchiveStore(db))
	if err = a.Stage(ctx, s); err != nil {
		t.Fatal(err)
	}
	if err = a.Flush(ctx); err == nil {
		t.Fatal("lost commit ACK accepted")
	}
	if client.Exists(ctx, namespace+":archive:message:{"+s.Trade.MsgID+"}").Val() != 1 {
		t.Fatal("unconfirmed commit deleted snapshot")
	}
	if err = b.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	var status string
	var attempts, cycle int
	if err = db.QueryRow("SELECT status,attempt_count,cycle_no FROM delivery_task WHERE delivery_id=?", s.Route.Targets[0].DeliveryID).Scan(&status, &attempts, &cycle); err != nil || status != "NEEDS_ATTENTION" || attempts != 5 || cycle != 3 {
		t.Fatalf("replay overwrote recovery state: %s %d %d %v", status, attempts, cycle, err)
	}
	for _, table := range []string{"trade_message", "delivery_task", "delivery_attempt"} {
		var n int
		if err = db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil || n != 1 {
			t.Fatalf("replay duplicated %s: %d %v", table, n, err)
		}
	}
}

func TestArchiveLeaseExpiredWithoutTakeoverCannotAck(t *testing.T) {
	client := bindingRedis(t)
	namespace := leaseNamespace(t, client)
	db := recoveryDB(t)
	ctx := context.Background()
	s := archiveSnapshot(t)
	blocked := &leaseBlockingStore{inner: hubmysql.NewArchiveStore(db), entered: make(chan struct{}), resume: make(chan struct{})}
	a := leaseArchive(t, client, namespace, blocked)
	if err := a.Stage(ctx, s); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Flush(ctx) }()
	<-blocked.entered
	if err := client.HSet(ctx, namespace+":archive:lease:{"+s.Trade.MsgID+"}", "expires", 1).Err(); err != nil {
		t.Fatal(err)
	}
	close(blocked.resume)
	if err := <-done; err == nil {
		t.Fatal("expired lease accepted without takeover")
	}
	if client.Exists(ctx, namespace+":archive:message:{"+s.Trade.MsgID+"}").Val() != 1 {
		t.Fatal("expired owner deleted snapshot")
	}
}

func TestArchiveLeaseCanceledFlushDoesNotWaitForLocalOwner(t *testing.T) {
	client := bindingRedis(t)
	namespace := leaseNamespace(t, client)
	ctx := context.Background()
	s := archiveSnapshot(t)
	blocked := &leaseBlockingStore{inner: failingArchiveStore{}, entered: make(chan struct{}), resume: make(chan struct{})}
	a := leaseArchive(t, client, namespace, blocked)
	if err := a.Stage(ctx, s); err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { first <- a.Flush(ctx) }()
	<-blocked.entered
	short, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() { second <- a.Flush(short) }()
	select {
	case err := <-second:
		if err == nil {
			t.Error("canceled Flush accepted")
		}
		close(blocked.resume)
		<-first
	case <-time.After(200 * time.Millisecond):
		close(blocked.resume)
		<-first
		<-second
		t.Fatal("canceled Flush waited for another local Flush")
	}
}

func TestArchiveLeaseStageHardLimits(t *testing.T) {
	for _, mode := range []string{"count", "bytes"} {
		t.Run(mode, func(t *testing.T) {
			client := bindingRedis(t)
			namespace := leaseNamespace(t, client)
			cfg := hubredis.ArchiveConfig{Namespace: namespace, MaxPending: 1}
			allowed := 1
			if mode == "bytes" {
				cfg.MaxPending = 100
				cfg.MaxBytes = 256 * 1024
				allowed = 2
			}
			a, err := hubredis.NewArchiveWithConfig(client.Options(), failingArchiveStore{}, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { a.Close() })
			ctx := context.Background()
			for i := 0; i < allowed; i++ {
				s := archiveSnapshot(t)
				s.Trade.RawPayload = []byte(strings.Repeat("x", 65000))
				if err = a.Stage(ctx, s); err != nil {
					t.Fatal(err)
				}
			}
			before, err := a.Capacity(ctx)
			if err != nil {
				t.Fatal(err)
			}
			s := archiveSnapshot(t)
			s.Trade.RawPayload = []byte(strings.Repeat("x", 65000))
			if err = a.Stage(ctx, s); err == nil {
				t.Fatal("Stage exceeded hard capacity")
			}
			after, err := a.Capacity(ctx)
			if err != nil || before.Pending != after.Pending || before.Bytes != after.Bytes {
				t.Fatalf("rejected Stage changed accounting: %+v -> %+v %v", before, after, err)
			}
			if client.Exists(ctx, namespace+":archive:message:{"+s.Trade.MsgID+"}").Val() != 0 {
				t.Fatal("rejected Stage left orphan snapshot")
			}
		})
	}
}
