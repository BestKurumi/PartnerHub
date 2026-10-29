//go:build integration

package integration_test

import (
	"context"
	"errors"
	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/ingress"
	"partner-hub/internal/model"
	"testing"
	"time"
)

// Real Redis observations include claimed entries while MySQL is stalled. New
// ingress stays closed until committed acknowledgments actually drain backlog.
func TestArchiveGateRealBacklogDrain(t *testing.T) {
	client := bindingRedis(t)
	namespace := leaseNamespace(t, client)
	db := recoveryDB(t)
	blocked := &leaseBlockingStore{inner: hubmysql.NewArchiveStore(db), entered: make(chan struct{}), resume: make(chan struct{})}
	archive := leaseArchive(t, client, namespace, blocked)
	ctx := context.Background()
	gateConfig := ingress.DefaultArchiveGateConfig()
	gateConfig.MaxPending = 10
	gateConfig.ReserveRequests = 1
	gate, err := ingress.NewArchiveGate(archive, archive, gateConfig)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if err = archive.Stage(ctx, archiveSnapshot(t)); err != nil {
			t.Fatal(err)
		}
	}
	if !errors.Is(gate.Preflight(ctx), ingress.ErrArchiveCapacity) {
		t.Fatal("real high backlog admitted input")
	}
	// High backlog must not prevent a restarted archiver from starting to drain.
	if err = archive.Preflight(ctx); err != nil {
		t.Fatalf("archive startup blocked drain: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- archive.Flush(ctx) }()
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("archive did not reach MySQL")
	}
	defer func() {
		select {
		case <-blocked.resume:
		default:
			close(blocked.resume)
		}
	}()
	observation, err := archive.Capacity(ctx)
	if err != nil || observation.Pending != 8 || observation.Bytes <= 0 {
		t.Fatalf("claimed data disappeared: %+v %v", observation, err)
	}
	if gate.Preflight(ctx) == nil {
		t.Fatal("claim alone falsely freed capacity")
	}
	close(blocked.resume)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if err = gate.Preflight(ctx); err != nil {
		t.Fatalf("confirmed drain did not reopen ingress: %v", err)
	}
	var rows int
	if err = db.QueryRow("SELECT COUNT(*) FROM trade_message").Scan(&rows); err != nil || rows != 8 {
		t.Fatalf("drain lost snapshots: %d %v", rows, err)
	}
	// Live observations also preserve hysteresis after new low/medium backlog.
	for i := 0; i < 6; i++ {
		s := archiveSnapshot(t)
		s.ArchiveRevision = model.NewID()
		if err = archive.Stage(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	if gate.Preflight(ctx) != nil {
		t.Fatal("low backlog rejected after drain")
	}
	if err = archive.Stage(ctx, archiveSnapshot(t)); err != nil {
		t.Fatal(err)
	}
	if gate.Preflight(ctx) == nil {
		t.Fatal("in-flight headroom not reserved")
	}
}
