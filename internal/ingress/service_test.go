package ingress_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"partner-hub/internal/adapter"
	"partner-hub/internal/delivery"
	"partner-hub/internal/ingress"
	"partner-hub/internal/model"
	"partner-hub/internal/routing"
)

type resolvedBindings struct {
	snapshot model.BindingSnapshot
	err      error
}

func (r *resolvedBindings) Resolve(context.Context, string) (model.BindingSnapshot, error) {
	return r.snapshot, r.err
}

type stageArchive struct {
	snapshot    model.FirstRoundSnapshot
	entered     chan struct{}
	release     chan struct{}
	err         error
	calls       int
	unavailable bool
}

func (a *stageArchive) Preflight(context.Context) error {
	if a.unavailable {
		return errors.New("capacity unavailable")
	}
	return nil
}
func (a *stageArchive) Stage(ctx context.Context, s model.FirstRoundSnapshot) error {
	a.snapshot = s
	a.calls++
	if a.entered != nil {
		close(a.entered)
	}
	if a.release != nil {
		select {
		case <-a.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return a.err
}
func (a *stageArchive) Flush(context.Context) error { return nil }
func tradeFixture(t *testing.T) model.TradeEvent {
	t.Helper()
	b, err := os.ReadFile("../../tests/fixtures/trades/normal.json")
	if err != nil {
		t.Fatal(err)
	}
	trade, err := ingress.Decode(strings.NewReader(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	return trade
}
func setupFirst(t *testing.T, status int, a *stageArchive) (*ingress.Service, *resolvedBindings, *atomic.Int32) {
	t.Helper()
	calls := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"userUid":"fixed-uid"`) || strings.Contains(string(body), "execAmount") {
			t.Error("UID not fixed or sensitive input escaped")
		}
		w.WriteHeader(status)
		io.WriteString(w, `{"code":0,"receiveId":"receipt"}`)
	}))
	t.Cleanup(server.Close)
	ad, err := adapter.NewPlatform1(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	resolver := &resolvedBindings{snapshot: model.BindingSnapshot{Version: 1, ObservedAt: time.Now(), Targets: []model.BindingTarget{{PlatformID: "PLATFORM_A", PlatformUID: "fixed-uid"}}}}
	return ingress.NewService(routing.NewRouter(resolver), delivery.NewFirstDelivery(map[string]delivery.Adapter{"PLATFORM_A": ad}), a), resolver, calls
}

// A platform receipt alone cannot become COMPLETED while Stage is unconfirmed.
func TestStageBeforeCompleted(t *testing.T) {
	a := &stageArchive{entered: make(chan struct{}), release: make(chan struct{})}
	s, _, calls := setupFirst(t, 200, a)
	trade := tradeFixture(t)
	type result struct {
		response model.TradeResponse
		err      error
	}
	done := make(chan result, 1)
	go func() { r, e := s.Process(context.Background(), trade); done <- result{r, e} }()
	select {
	case <-a.entered:
	case r := <-done:
		t.Fatalf("returned before Stage: %+v %v", r.response, r.err)
	case <-time.After(time.Second):
		t.Fatal("Stage not reached")
	}
	select {
	case r := <-done:
		t.Fatalf("completed before Stage confirmation: %+v", r)
	default:
	}
	close(a.release)
	r := <-done
	if r.err != nil || r.response.Status != "SUCCEEDED" || r.response.Acceptance != "COMPLETED" || r.response.RecordStatus != "PENDING" || len(r.response.PlatformResults) != 1 || r.response.PlatformResults[0].DeliveryID == "" {
		t.Fatalf("response: %+v %v", r.response, r.err)
	}
	if calls.Load() != 1 || len(a.snapshot.Attempts) != 1 || a.snapshot.Attempts[0].Target.PlatformUID != "fixed-uid" || a.snapshot.Trade.ExecAmount.String() != "-12345678901234567890123456789012345678" || a.snapshot.ArchiveRevision == "" {
		t.Fatalf("incomplete immutable snapshot: %+v calls=%d", a.snapshot, calls.Load())
	}
}

// Failed platform or record persistence must not fake a recovery handoff.
func TestUnconfirmedNeverAccepted(t *testing.T) {
	for _, tt := range []struct {
		name        string
		status      int
		stageErr    bool
		routeErr    bool
		unavailable bool
		sends       int
	}{{"stage unavailable", 200, true, false, false, 1}, {"platform unknown", 503, false, false, false, 1}, {"route failure", 200, false, true, false, 0}, {"capacity unavailable", 200, false, false, true, 0}} {
		t.Run(tt.name, func(t *testing.T) {
			a := &stageArchive{unavailable: tt.unavailable}
			if tt.stageErr {
				a.err = errors.New("unconfirmed stage")
			}
			s, resolver, calls := setupFirst(t, tt.status, a)
			if tt.routeErr {
				resolver.err = errors.New("DB failed")
			}
			r, err := s.Process(context.Background(), tradeFixture(t))
			if !errors.Is(err, ingress.ErrUnconfirmed) || r.Acceptance != "UNCONFIRMED" || int(calls.Load()) != tt.sends {
				t.Fatalf("false confirmation or replay: %+v err=%v sends=%d", r, err, calls.Load())
			}
			if !tt.stageErr && a.calls != 0 {
				t.Fatal("failure entered normal archive")
			}
		})
	}
}

// Explicit empty succeeds only after Stage; a later binding does not resend it.
func TestFirstRoundNoRoute(t *testing.T) {
	a := &stageArchive{}
	s, resolver, calls := setupFirst(t, 200, a)
	resolver.snapshot.Targets = []model.BindingTarget{}
	r, err := s.Process(context.Background(), tradeFixture(t))
	resolver.snapshot.Targets = []model.BindingTarget{{PlatformID: "PLATFORM_A", PlatformUID: "later"}}
	if err != nil || r.Status != "NO_ROUTE" || r.Acceptance != "COMPLETED" || a.calls != 1 || calls.Load() != 0 || len(a.snapshot.Attempts) != 0 {
		t.Fatalf("NO_ROUTE: %+v %v sends=%d", r, err, calls.Load())
	}
}

// Cancelling the caller after processing starts must not discard known effects.
func TestClientDisconnectKeepsBoundedProcessing(t *testing.T) {
	a := &stageArchive{entered: make(chan struct{}), release: make(chan struct{})}
	s, _, calls := setupFirst(t, 200, a)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := s.Process(ctx, tradeFixture(t)); done <- err }()
	select {
	case <-a.entered:
	case err := <-done:
		t.Fatalf("did not reach stage: %v", err)
	case <-time.After(time.Second):
		t.Fatal("did not reach stage")
	}
	cancel()
	close(a.release)
	if err := <-done; err != nil || calls.Load() != 1 {
		t.Fatalf("disconnect lost processing: %v sends=%d", err, calls.Load())
	}
}
