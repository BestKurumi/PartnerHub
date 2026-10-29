//go:build integration

package integration_test

import (
	"context"
	"partner-hub/internal/model"
	"testing"
	"time"
)

func TestObservabilityArchiveLegacyCompactPageTail(t *testing.T) {
	client := bindingRedis(t)
	ns := leaseNamespace(t, client)
	a := leaseArchive(t, client, ns, failingArchiveStore{})
	ctx := context.Background()
	for i := 0; i < 101; i++ {
		s := archiveSnapshot(t)
		s.ReceivedAt = time.Now().Add(-time.Duration(i+1) * time.Second)
		if err := a.Stage(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.Del(ctx, ns+":archive:received").Err(); err != nil {
		t.Fatal(err)
	}
	var observed bool
	for i := 0; i < 4; i++ {
		c, age, err := a.Backlog(ctx)
		if err == nil {
			if c.Pending != 101 || age < 100*time.Second {
				t.Fatalf("incomplete oldest observation: %+v %v", c, age)
			}
			observed = true
			break
		}
	}
	if !observed {
		t.Fatal("legacy compact zset tail was permanently skipped by bounded repair")
	}
}

type observeArchiveStore34 struct{ entered, release chan struct{} }

func (s observeArchiveStore34) Save(ctx context.Context, _ []model.FirstRoundSnapshot) error {
	close(s.entered)
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func TestObservabilityArchiveAgeSurvivesClaimAndLegacyIndex(t *testing.T) {
	client := bindingRedis(t)
	ns := leaseNamespace(t, client)
	store := observeArchiveStore34{make(chan struct{}), make(chan struct{})}
	a := leaseArchive(t, client, ns, store)
	snapshot := archiveSnapshot(t)
	snapshot.ReceivedAt = time.Now().Add(-time.Hour)
	ctx := context.Background()
	if err := a.Stage(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	// Simulate a pre-3.4 snapshot with no immutable received-time index.
	if err := client.Del(ctx, ns+":archive:received").Err(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Flush(ctx) }()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("not claimed")
	}
	defer func() {
		select {
		case <-store.release:
		default:
			close(store.release)
		}
	}()
	// Docker Redis and the host have distinct wall clocks; allow subsecond skew.
	c, age, err := a.Backlog(ctx)
	if err != nil || c.Pending != 1 || age < time.Hour-500*time.Millisecond || age > time.Hour+5*time.Second {
		t.Fatalf("pending lease time used instead of original received time: count=%d age=%v err=%v", c.Pending, age, err)
	}
	close(store.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	c, age, err = a.Backlog(ctx)
	if err != nil || c.Pending != 0 || age != 0 {
		t.Fatalf("ack did not release age index: %+v %v %v", c, age, err)
	}
}
