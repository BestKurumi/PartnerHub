package ingress_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"partner-hub/internal/ingress"
	"partner-hub/internal/model"
	"partner-hub/internal/routing"
	"strings"
	"testing"
	"time"
)

type snapshotFirst struct {
	outcomes []model.SendOutcome
	calls    int
}

func (f *snapshotFirst) Execute(_ context.Context, t model.TradeEvent, r model.RouteSnapshot) model.FirstRoundSnapshot {
	f.calls++
	s := model.FirstRoundSnapshot{Trade: t, Route: r, ArchiveRevision: model.NewID()}
	for i, target := range r.Targets {
		s.Attempts = append(s.Attempts, model.FirstAttempt{Target: target, Request: model.PreparedRequest{PlatformID: target.PlatformID, ProtocolVersion: "test.v1", Payload: []byte(`{}`)}, Result: model.SendResult{Outcome: f.outcomes[i], SendStarted: true, StartedAt: time.Now(), FinishedAt: time.Now()}})
	}
	return s
}

type handoffRepo struct {
	snapshot         model.FirstRoundSnapshot
	err              error
	calls            int
	entered, release chan struct{}
}

func (h *handoffRepo) Handoff(ctx context.Context, s model.FirstRoundSnapshot) error {
	h.snapshot = s
	h.calls++
	if h.entered != nil {
		close(h.entered)
	}
	if h.release != nil {
		select {
		case <-h.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return h.err
}
func TestCompleteHandoffBeforeAcceptance(t *testing.T) {
	for _, tc := range []struct {
		outcome model.SendOutcome
		want    string
		http    int
	}{{model.Unknown, "RECOVERY_PENDING", 202}, {model.RetryableFailure, "RECOVERY_PENDING", 202}, {model.PermanentFailure, "NEEDS_ATTENTION", 200}} {
		t.Run(string(tc.outcome), func(t *testing.T) {
			a := &stageArchive{}
			f := &snapshotFirst{outcomes: []model.SendOutcome{model.Success, tc.outcome}}
			h := &handoffRepo{entered: make(chan struct{}), release: make(chan struct{})}
			resolver := &resolvedBindings{snapshot: model.BindingSnapshot{Version: 1, ObservedAt: time.Now(), Targets: []model.BindingTarget{{PlatformID: "PLATFORM_A", PlatformUID: "a"}, {PlatformID: "PLATFORM_B", PlatformUID: "b"}}}}
			svc := ingress.NewService(routing.NewRouter(resolver), f, a, h)
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				w := httptest.NewRecorder()
				raw := tradeFixture(t).RawPayload
				ingress.NewHTTP(svc, ingress.GateConfig{}).ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/trades", strings.NewReader(string(raw))))
				done <- w
			}()
			select {
			case <-h.entered:
			case w := <-done:
				t.Fatalf("returned without handoff HTTP%d %s", w.Code, w.Body.String())
			case <-time.After(time.Second):
				t.Fatal("handoff not entered")
			}
			select {
			case <-done:
				t.Fatal("returned before commit confirmation")
			default:
			}
			close(h.release)
			w := <-done
			if w.Code != tc.http || !strings.Contains(w.Body.String(), tc.want) || !strings.Contains(w.Body.String(), `"acceptance":"ACCEPTED"`) || !strings.Contains(w.Body.String(), `"recordStatus":"PERSISTED"`) {
				t.Fatalf("HTTP%d %s", w.Code, w.Body.String())
			}
			if a.calls != 0 || h.calls != 1 || len(h.snapshot.Attempts) != 2 || h.snapshot.Trade.MsgID == "" || h.snapshot.Attempts[0].Target.PlatformUID != "a" {
				t.Fatalf("mixed/partial responsibility: stage=%d handoff=%+v", a.calls, h.snapshot)
			}
		})
	}
}
func TestRouteFailureHandoffAndFailedCommit(t *testing.T) {
	for _, fail := range []bool{false, true} {
		a := &stageArchive{}
		f := &snapshotFirst{}
		h := &handoffRepo{}
		if fail {
			h.err = errors.New("commit unknown")
		}
		resolver := &resolvedBindings{err: errors.New("DB lookup failed")}
		svc := ingress.NewService(routing.NewRouter(resolver), f, a, h)
		r, err := svc.Process(context.Background(), tradeFixture(t))
		if fail {
			if !errors.Is(err, ingress.ErrUnconfirmed) || r.Acceptance != "UNCONFIRMED" {
				t.Fatalf("false commit confirmation %+v %v", r, err)
			}
		} else if err != nil || r.Acceptance != "ACCEPTED" || r.Status != "RECOVERY_PENDING" || r.RecordStatus != "PERSISTED" {
			t.Fatalf("route handoff %+v %v", r, err)
		}
		if h.calls != 1 || f.calls != 0 || a.calls != 0 || len(h.snapshot.Route.Targets) != 0 || h.snapshot.Route.Status != "RETRY_WAIT" || h.snapshot.RouteError == "" {
			t.Fatalf("route failure manufactured sends %+v first=%d stage=%d", h.snapshot, f.calls, a.calls)
		}
	}
}

func TestHandoffFailurePreservesObservedResults(t *testing.T) {
	a := &stageArchive{}
	f := &snapshotFirst{outcomes: []model.SendOutcome{model.Success, model.Unknown}}
	h := &handoffRepo{err: errors.New("cannot confirm commit")}
	resolver := &resolvedBindings{snapshot: model.BindingSnapshot{Version: 1, ObservedAt: time.Now(), Targets: []model.BindingTarget{{PlatformID: "PLATFORM_A", PlatformUID: "a"}, {PlatformID: "PLATFORM_B", PlatformUID: "b"}}}}
	svc := ingress.NewService(routing.NewRouter(resolver), f, a, h)
	w := httptest.NewRecorder()
	ingress.NewHTTP(svc, ingress.GateConfig{}).ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/trades", strings.NewReader(string(tradeFixture(t).RawPayload))))
	if w.Code != 503 || !strings.Contains(w.Body.String(), `"acceptance":"UNCONFIRMED"`) || !strings.Contains(w.Body.String(), `"firstOutcome":"SUCCESS"`) || !strings.Contains(w.Body.String(), `"firstOutcome":"UNKNOWN"`) || a.calls != 0 || f.calls != 1 || h.calls != 1 {
		t.Fatalf("false handoff or lost facts: HTTP%d %s stage=%d first=%d handoff=%d", w.Code, w.Body.String(), a.calls, f.calls, h.calls)
	}
}

func TestDisconnectDuringHandoffRetainsOriginalDeadline(t *testing.T) {
	a := &stageArchive{}
	f := &snapshotFirst{outcomes: []model.SendOutcome{model.RetryableFailure}}
	h := &handoffRepo{entered: make(chan struct{}), release: make(chan struct{})}
	resolver := &resolvedBindings{snapshot: model.BindingSnapshot{Version: 1, ObservedAt: time.Now(), Targets: []model.BindingTarget{{PlatformID: "PLATFORM_A", PlatformUID: "fixed"}}}}
	svc := ingress.NewService(routing.NewRouter(resolver), f, a, h)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := svc.Process(ctx, tradeFixture(t)); done <- err }()
	select {
	case <-h.entered:
	case <-time.After(time.Second):
		t.Fatal("handoff not started")
	}
	cancel()
	// Caller cancellation must not cancel persistence, but its original deadline
	// still bounds a stalled handoff. No fresh unbounded context is allowed.
	select {
	case err := <-done:
		t.Fatalf("disconnect discarded persistence: %v", err)
	default:
	}
	select {
	case err := <-done:
		if !errors.Is(err, ingress.ErrUnconfirmed) {
			t.Fatalf("stalled handoff falsely accepted: %v", err)
		}
	case <-time.After(700 * time.Millisecond):
		t.Fatal("handoff escaped original processing deadline")
	}
}

// Regression: an uncertain normal Stage is never converted into a second
// persistence path or another external send, even when Handoff is available.
func TestUncertainStageNeverSwitchesHandoff(t *testing.T) {
	a := &stageArchive{err: errors.New("stage confirmation lost")}
	f := &snapshotFirst{outcomes: []model.SendOutcome{model.Success}}
	h := &handoffRepo{}
	resolver := &resolvedBindings{snapshot: model.BindingSnapshot{Version: 1, ObservedAt: time.Now(), Targets: []model.BindingTarget{{PlatformID: "PLATFORM_A", PlatformUID: "a"}}}}
	service := ingress.NewService(routing.NewRouter(resolver), f, a, h)
	result, err := service.Process(context.Background(), tradeFixture(t))
	if !errors.Is(err, ingress.ErrUnconfirmed) || result.Acceptance != "UNCONFIRMED" || a.calls != 1 || f.calls != 1 || h.calls != 0 {
		t.Fatalf("uncertain stage replayed effects result=%+v err=%v stage=%d sends=%d handoff=%d", result, err, a.calls, f.calls, h.calls)
	}
}
