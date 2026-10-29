//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"partner-hub/internal/binding"
	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/model"
)

func bindingDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("HUB_TEST_DSN")
	if dsn == "" {
		t.Fatal("HUB_TEST_DSN required; run make test-deps")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	ddl, err := os.ReadFile("../../deploy/mysql/migrations/001_bindings.sql")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		for _, statement := range strings.Split(string(ddl), ";") {
			if strings.TrimSpace(statement) == "" {
				continue
			}
			if _, err = db.ExecContext(ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
	}
	return db
}
func bindingEvent(client string, id int, kind, platform, uid string) model.BindingEvent {
	return model.BindingEvent{SchemaVersion: 1, EventID: fmt.Sprintf("984bf276-30ce-4b3a-8536-%012d", id), EventType: kind, PlatformID: platform, ClientID: client, PlatformUID: uid, OccurredAt: int64(id), Position: model.BindingPosition{Topic: "customer.binding.events", Partition: 0, Offset: int64(id)}}
}
func cleanBindingClient(t *testing.T, db *sql.DB, client string) {
	t.Helper()
	for _, table := range []string{"binding_event", "binding_current"} {
		if _, err := db.Exec("DELETE FROM "+table+" WHERE client_id=?", client); err != nil {
			t.Fatal(err)
		}
	}
}

// Catches lost dual-platform targets, old-UID removal and replay reviving tombstones.
func TestBindingsTransitions(t *testing.T) {
	db := bindingDB(t)
	client := "it-binding-transitions"
	cleanBindingClient(t, db, client)
	repo := hubmysql.NewBindings(db)
	service := binding.NewService(repo, nil)
	ctx := context.Background()
	first := bindingEvent(client, 101, "BIND", "PLATFORM_A", "U1")
	for _, tt := range []struct {
		e        model.BindingEvent
		outcome  string
		revision uint64
	}{
		{first, "APPLIED", 1},
		{bindingEvent(client, 102, "BIND", "PLATFORM_A", "U1"), "NO_CHANGE", 1},
		{bindingEvent(client, 103, "BIND", "PLATFORM_A", "U2"), "CONFLICT", 1},
		{bindingEvent(client, 104, "BIND", "PLATFORM_B", "B1"), "APPLIED", 1},
		{bindingEvent(client, 105, "UNBIND", "PLATFORM_A", "old"), "UID_MISMATCH", 1},
	} {
		r, err := service.Apply(ctx, tt.e)
		if err != nil || r.Outcome != tt.outcome || r.Revision != tt.revision {
			t.Fatalf("apply %+v: %+v %v", tt.e, r, err)
		}
	}
	snapshot, err := repo.Load(ctx, client)
	if err != nil || len(snapshot.Targets) != 2 || snapshot.Targets[0].PlatformUID != "U1" || snapshot.Targets[1].PlatformUID != "B1" {
		t.Fatalf("complete snapshot: %+v %v", snapshot, err)
	}
	for _, e := range []model.BindingEvent{bindingEvent(client, 106, "UNBIND", "PLATFORM_A", "U1"), bindingEvent(client, 107, "BIND", "PLATFORM_A", "U2"), bindingEvent(client, 108, "UNBIND", "PLATFORM_A", "U1")} {
		if _, err = service.Apply(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	r, err := service.Apply(ctx, first)
	if err != nil || !r.Replayed || r.Outcome != "APPLIED" || r.Revision != 1 {
		t.Fatalf("replay result: %+v %v", r, err)
	}
	snapshot, err = repo.Load(ctx, client)
	if err != nil || len(snapshot.Targets) != 2 || snapshot.Targets[0].PlatformUID != "U2" {
		t.Fatalf("replay modified current UID: %+v %v", snapshot, err)
	}
	if _, err = service.Apply(ctx, bindingEvent(client, 109, "UNBIND", "PLATFORM_A", "U2")); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Apply(ctx, first); err != nil {
		t.Fatal(err)
	}
	snapshot, err = repo.Load(ctx, client)
	if err != nil || len(snapshot.Targets) != 1 || snapshot.Targets[0].PlatformID != "PLATFORM_B" {
		t.Fatalf("old bind revived: %+v %v", snapshot, err)
	}
	changed := first
	changed.PlatformUID = "tampered"
	changed.Position.Offset = 999101
	r, err = service.Apply(ctx, changed)
	if err != nil || r.Outcome != "EVENT_ID_CONFLICT" {
		t.Fatalf("content conflict: %+v %v", r, err)
	}
	var count int
	if err = db.QueryRow("SELECT COUNT(*) FROM ingress_anomaly WHERE source_topic=? AND source_partition=0 AND source_offset=999101", "customer.binding.events").Scan(&count); err != nil || count != 1 {
		t.Fatalf("conflict not persisted: %d %v", count, err)
	}
	if _, err = service.Apply(ctx, bindingEvent(client, 110, "UNBIND", "PLATFORM_A", "any")); err != nil {
		t.Fatal(err)
	}
	empty, err := repo.Load(ctx, "it-no-bindings")
	if err != nil || len(empty.Targets) != 0 || empty.Targets == nil {
		t.Fatalf("not explicit successful empty: %+v %v", empty, err)
	}
}

// Catches relationship commit surviving failure to persist event result.
func TestBindingsTransactionRollback(t *testing.T) {
	db := bindingDB(t)
	client := "it-binding-rollback"
	cleanBindingClient(t, db, client)
	_, _ = db.Exec("DROP TRIGGER IF EXISTS it_binding_fail")
	_, err := db.Exec("CREATE TRIGGER it_binding_fail BEFORE UPDATE ON binding_event FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='integration injected event result failure'")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec("DROP TRIGGER IF EXISTS it_binding_fail") })
	repo := hubmysql.NewBindings(db)
	_, err = repo.Apply(context.Background(), bindingEvent(client, 201, "BIND", "PLATFORM_A", "U1"))
	if err == nil {
		t.Fatal("injected DB failure hidden")
	}
	if !strings.Contains(err.Error(), "integration injected event result failure") {
		t.Fatalf("failed before reaching transaction injection: %v", err)
	}
	for _, table := range []string{"binding_event", "binding_current"} {
		var count int
		if err = db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE client_id=?", client).Scan(&count); err != nil || count != 0 {
			t.Fatalf("partial commit %s count=%d err=%v", table, count, err)
		}
	}
	if _, err = db.Exec("DROP TRIGGER it_binding_fail"); err != nil {
		t.Fatal(err)
	}
	if result, err := repo.Apply(context.Background(), bindingEvent(client, 201, "BIND", "PLATFORM_A", "U1")); err != nil || result.Outcome != "APPLIED" {
		t.Fatalf("rollback retained poison event identity: %+v %v", result, err)
	}
}

// Catches concurrent same-customer events overwriting the winning UID or replaying twice.
func TestBindingsConcurrent(t *testing.T) {
	db := bindingDB(t)
	client := "it-binding-concurrent"
	cleanBindingClient(t, db, client)
	repo := hubmysql.NewBindings(db)
	results := make(chan model.BindingResult, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			r, err := repo.Apply(ctx, bindingEvent(client, 301+i, "BIND", "PLATFORM_A", fmt.Sprintf("U%d", i)))
			results <- r
			errs <- err
		}(i)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	counts := map[string]int{}
	for r := range results {
		counts[r.Outcome]++
	}
	if counts["APPLIED"] != 1 || counts["CONFLICT"] != 1 {
		t.Fatalf("race outcomes: %v", counts)
	}
}
