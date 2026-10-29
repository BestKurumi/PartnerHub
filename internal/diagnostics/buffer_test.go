package diagnostics_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"partner-hub/internal/diagnostics"
)

// Removing overwrite accounting, boot identity, or filtering loses the visible
// retention boundary and can mistake a restarted instance for historical data.
func TestBufferRetainsNewestWithExplicitOverwriteAndRestartBoundary(t *testing.T) {
	b := diagnostics.NewBuffer("hub-1")
	for i := 0; i < 1005; i++ {
		b.Record("ingress_invalid", diagnostics.Fields{RequestID: fmt.Sprintf("request-%d", i)})
	}
	s := b.Snapshot(diagnostics.Query{Limit: 100})
	if len(s.Items) != 100 || s.Items[0].RequestID != "request-1004" || s.Items[99].RequestID != "request-905" {
		t.Fatalf("recent-first window=%+v", s.Items)
	}
	info := s.Instances[0]
	if info.Capacity != 1000 || info.Retained != 1000 || info.Dropped != 5 || info.OldestAt.IsZero() || info.BootID == "" {
		t.Fatalf("retention=%+v", info)
	}
	newer := diagnostics.NewBuffer("hub-1").Snapshot(diagnostics.Query{})
	if newer.Instances[0].BootID == info.BootID || newer.Instances[0].Retained != 0 || len(newer.Items) != 0 || newer.Retention.Persistent {
		t.Fatalf("restart boundary missing: %+v", newer)
	}
	filtered := b.Snapshot(diagnostics.Query{Level: "WARN", Q: "request-1004", Limit: 20})
	if len(filtered.Items) != 1 {
		t.Fatalf("filter=%+v", filtered)
	}
	if got := b.Snapshot(diagnostics.Query{Instance: "other"}); len(got.Items) != 0 {
		t.Fatal("cross-instance filter ignored")
	}
}

// Unknown event messages and arbitrary values must not turn the diagnostic
// buffer into a payload/error sink, including malformed correlation fields.
func TestBufferRejectsUnknownEventsAndBoundsWhitelistedFields(t *testing.T) {
	b := diagnostics.NewBuffer("hub-1")
	b.Record("secret-raw-error", diagnostics.Fields{MsgID: "secret-payload"})
	b.Record("ingress_unconfirmed", diagnostics.Fields{RequestID: strings.Repeat("a", 300), TraceID: strings.Repeat("界", 200), MsgID: "27c71118-a387-4f56-bd4b-99dcdb15c9d3", Status: "secret-password", Outcome: "raw-error", Platform: "https://secret"})
	s := b.Snapshot(diagnostics.Query{Limit: 100})
	if len(s.Items) != 1 {
		t.Fatalf("unknown kind retained: %+v", s.Items)
	}
	item := s.Items[0]
	if len(item.RequestID) > 128 || len([]rune(item.TraceID)) > 128 || item.Status != "" || item.Outcome != "" || item.Platform != "" {
		t.Fatalf("unbounded/unsafe fields: %+v", item)
	}
	encoded, _ := json.Marshal(s)
	for _, secret := range []string{"secret-raw-error", "secret-payload", "secret-password", "raw-error", "https://secret"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("secret retained: %s", secret)
		}
	}
	if item.ID == "" || item.BootID == "" || item.Time.IsZero() || item.Message == "" || item.Level != "ERROR" {
		t.Fatalf("incomplete event: %+v", item)
	}
}

// Concurrent ingress/log writers and readers must preserve unique identities
// and bounded storage; run under -race to catch unsynchronized snapshots.
func TestBufferConcurrentRecordAndRead(t *testing.T) {
	b := diagnostics.NewBuffer("hub-1")
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Go(func() {
			for i := 0; i < 500; i++ {
				b.Record("runtime_warning", diagnostics.Fields{})
				_ = b.Snapshot(diagnostics.Query{Limit: 20})
			}
		})
	}
	wg.Wait()
	s := b.Snapshot(diagnostics.Query{Limit: 100})
	if s.Instances[0].Retained != 1000 || s.Instances[0].Dropped != 3000 {
		t.Fatalf("count=%+v", s.Instances)
	}
	ids := map[string]bool{}
	for _, e := range s.Items {
		if ids[e.ID] {
			t.Fatal("duplicate ID")
		}
		ids[e.ID] = true
	}
}
