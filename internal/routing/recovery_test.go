package routing_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"partner-hub/internal/delivery"
	"partner-hub/internal/model"
	"partner-hub/internal/routing"
)

type routeRepo struct {
	claim    func(context.Context, string, int) ([]routing.RouteRecoveryClaim, error)
	complete func(context.Context, routing.RouteRecoveryClaim, model.RouteSnapshot, error) error
}

func (r *routeRepo) ClaimRoutes(ctx context.Context, owner string, limit int) ([]routing.RouteRecoveryClaim, error) {
	return r.claim(ctx, owner, limit)
}
func (r *routeRepo) CompleteRoute(ctx context.Context, c routing.RouteRecoveryClaim, s model.RouteSnapshot, err error) error {
	return r.complete(ctx, c, s, err)
}

type targetResolver func(context.Context, model.TradeEvent) (model.RouteSnapshot, error)

func (r targetResolver) ResolveTargets(ctx context.Context, t model.TradeEvent) (model.RouteSnapshot, error) {
	return r(ctx, t)
}

func routeClaim() routing.RouteRecoveryClaim {
	return routing.RouteRecoveryClaim{MsgID: model.NewID(), ClientID: "customer-0001", Owner: "route-worker", LeaseVersion: 7, LeaseUntil: time.Now().Add(10 * time.Second), LeaseDeadline: time.Now().Add(10 * time.Second), AttemptNo: 2, CycleNo: 1, CycleAttempt: 2}
}

func TestRouteRecoveryWorkerPersistsResolvedTargets(t *testing.T) {
	c := routeClaim()
	resolver := routing.NewRouter(&bindings{s: model.BindingSnapshot{Version: 1, ObservedAt: time.Now(), Targets: []model.BindingTarget{{PlatformID: "PLATFORM_A", PlatformUID: "00001"}, {PlatformID: "PLATFORM_B", PlatformUID: "00002"}}}})
	var saved model.RouteSnapshot
	repo := &routeRepo{
		claim: func(ctx context.Context, owner string, limit int) ([]routing.RouteRecoveryClaim, error) {
			if owner != c.Owner || limit != 1 {
				t.Errorf("claim should be one bounded item: %s %d", owner, limit)
			}
			return []routing.RouteRecoveryClaim{c}, nil
		},
		complete: func(ctx context.Context, claim routing.RouteRecoveryClaim, s model.RouteSnapshot, err error) error {
			if !reflect.DeepEqual(claim, c) || err != nil {
				t.Errorf("changed claim/error %+v %v", claim, err)
			}
			saved = s
			return nil
		},
	}
	n, err := routing.NewRecoveryWorker(repo, resolver, c.Owner).RunOnce(context.Background())
	if err != nil || n != 1 || saved.Status != "ROUTED" || saved.MsgID != c.MsgID || len(saved.Targets) != 2 || saved.Targets[0].PlatformUID != "00001" || saved.Targets[1].PlatformUID != "00002" || saved.Targets[0].DeliveryID == saved.Targets[1].DeliveryID {
		t.Fatalf("resolution not durably completed n=%d err=%v snapshot=%+v", n, err, saved)
	}
}

func TestRouteRecoveryWorkerEmptyDiffersFromLookupError(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "error"}[fail], func(t *testing.T) {
			c := routeClaim()
			completed := false
			repo := &routeRepo{claim: func(context.Context, string, int) ([]routing.RouteRecoveryClaim, error) {
				return []routing.RouteRecoveryClaim{c}, nil
			}, complete: func(ctx context.Context, claim routing.RouteRecoveryClaim, s model.RouteSnapshot, err error) error {
				completed = true
				if fail {
					if err == nil || s.Status != "" || len(s.Targets) != 0 {
						t.Errorf("lookup error became empty success: %+v %v", s, err)
					}
				} else if err != nil || s.Status != "NO_ROUTE" || s.MsgID != c.MsgID {
					t.Errorf("explicit empty result lost %+v %v", s, err)
				}
				return nil
			}}
			resolver := targetResolver(func(ctx context.Context, trade model.TradeEvent) (model.RouteSnapshot, error) {
				if trade.MsgID != c.MsgID || trade.ClientID != c.ClientID || trade.RawPayload != nil || trade.AccountID != "" {
					t.Errorf("resolver identity leaked/mutated: %+v", trade)
				}
				if fail {
					return model.RouteSnapshot{Status: "NO_ROUTE"}, errors.New("binding failure")
				}
				return model.RouteSnapshot{MsgID: c.MsgID, Status: "NO_ROUTE", ResolvedAt: time.Now(), Targets: []model.RouteTarget{}}, nil
			})
			n, err := routing.NewRecoveryWorker(repo, resolver, c.Owner).RunOnce(context.Background())
			if err != nil || n != 1 || !completed {
				t.Fatalf("claim not completed %d %v %v", n, err, completed)
			}
		})
	}
}

func TestRouteRecoveryWorkerDoesNotRebaseExpiredDeadline(t *testing.T) {
	c := routeClaim()
	c.LeaseDeadline = time.Now().Add(-time.Millisecond)
	c.LeaseUntil = time.Now().Add(time.Hour)
	repo := &routeRepo{claim: func(context.Context, string, int) ([]routing.RouteRecoveryClaim, error) {
		return []routing.RouteRecoveryClaim{c}, nil
	}, complete: func(context.Context, routing.RouteRecoveryClaim, model.RouteSnapshot, error) error {
		t.Error("expired claim completed")
		return nil
	}}
	resolver := targetResolver(func(context.Context, model.TradeEvent) (model.RouteSnapshot, error) {
		t.Error("expired claim queried bindings")
		return model.RouteSnapshot{}, nil
	})
	n, err := routing.NewRecoveryWorker(repo, resolver, c.Owner).RunOnce(context.Background())
	if n != 1 || !errors.Is(err, delivery.ErrLeaseLost) {
		t.Fatalf("expired deadline rebased %d %v", n, err)
	}
}

func TestRouteRecoveryWorkerBoundsLookupAndHonorsCancellation(t *testing.T) {
	for _, cancelDuring := range []bool{false, true} {
		t.Run(map[bool]string{false: "lease-deadline", true: "cancel"}[cancelDuring], func(t *testing.T) {
			c := routeClaim()
			c.LeaseDeadline = time.Now().Add(100 * time.Millisecond)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			repo := &routeRepo{claim: func(context.Context, string, int) ([]routing.RouteRecoveryClaim, error) {
				return []routing.RouteRecoveryClaim{c}, nil
			}, complete: func(context.Context, routing.RouteRecoveryClaim, model.RouteSnapshot, error) error {
				t.Error("cancelled lease/context wrote result")
				return nil
			}}
			resolver := targetResolver(func(query context.Context, _ model.TradeEvent) (model.RouteSnapshot, error) {
				deadline, ok := query.Deadline()
				if !ok || deadline.After(c.LeaseDeadline) || time.Until(deadline) > time.Second {
					t.Errorf("unbounded lookup %v %v", deadline, ok)
				}
				if cancelDuring {
					cancel()
				}
				<-query.Done()
				return model.RouteSnapshot{}, query.Err()
			})
			n, err := routing.NewRecoveryWorker(repo, resolver, c.Owner).RunOnce(ctx)
			if n != 1 || err == nil {
				t.Fatalf("cancelled lookup not stopped %d %v", n, err)
			}
		})
	}
}

func TestRouteRecoveryWorkerPersistenceFailureDoesNotRequery(t *testing.T) {
	c := routeClaim()
	calls := 0
	dbErr := errors.New("uncertain commit")
	repo := &routeRepo{claim: func(context.Context, string, int) ([]routing.RouteRecoveryClaim, error) {
		return []routing.RouteRecoveryClaim{c}, nil
	}, complete: func(context.Context, routing.RouteRecoveryClaim, model.RouteSnapshot, error) error { return dbErr }}
	resolver := targetResolver(func(context.Context, model.TradeEvent) (model.RouteSnapshot, error) {
		calls++
		return model.RouteSnapshot{MsgID: c.MsgID, Status: "NO_ROUTE"}, nil
	})
	n, err := routing.NewRecoveryWorker(repo, resolver, c.Owner).RunOnce(context.Background())
	if n != 1 || !errors.Is(err, dbErr) || calls != 1 {
		t.Fatalf("persistence uncertainty retried resolution %d %v calls=%d", n, err, calls)
	}
}

func TestRouteRecoveryWorkerNoClaimNoLookup(t *testing.T) {
	for _, claimErr := range []error{nil, errors.New("database unavailable")} {
		repo := &routeRepo{claim: func(context.Context, string, int) ([]routing.RouteRecoveryClaim, error) { return nil, claimErr }}
		resolver := targetResolver(func(context.Context, model.TradeEvent) (model.RouteSnapshot, error) {
			t.Error("queried without claim")
			return model.RouteSnapshot{}, nil
		})
		n, err := routing.NewRecoveryWorker(repo, resolver, "route-worker").RunOnce(context.Background())
		if n != 0 || !errors.Is(err, claimErr) {
			t.Fatalf("claim result %d %v", n, err)
		}
	}
}

func TestRouteRecoveryWorkerCancelledBeforeClaim(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	repo := &routeRepo{claim: func(context.Context, string, int) ([]routing.RouteRecoveryClaim, error) {
		t.Error("claimed after cancellation")
		return nil, nil
	}}
	n, err := routing.NewRecoveryWorker(repo, nil, "route-worker").RunOnce(ctx)
	if n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context ignored %d %v", n, err)
	}
}

func TestRouteRecoveryWorkerPreservesShorterCallerBudget(t *testing.T) {
	c := routeClaim()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	original, _ := ctx.Deadline()
	called := false
	repo := &routeRepo{
		claim: func(context.Context, string, int) ([]routing.RouteRecoveryClaim, error) {
			return []routing.RouteRecoveryClaim{c}, nil
		},
		complete: func(ctx context.Context, _ routing.RouteRecoveryClaim, _ model.RouteSnapshot, _ error) error {
			deadline, ok := ctx.Deadline()
			if !ok || deadline.After(original) {
				t.Errorf("writeback extended original budget: %v", deadline)
			}
			return nil
		},
	}
	resolver := targetResolver(func(query context.Context, _ model.TradeEvent) (model.RouteSnapshot, error) {
		called = true
		deadline, ok := query.Deadline()
		if !ok || deadline.After(original) {
			t.Errorf("lookup extended original budget: %v", deadline)
		}
		return model.RouteSnapshot{MsgID: c.MsgID, Status: "NO_ROUTE"}, nil
	})
	if n, err := routing.NewRecoveryWorker(repo, resolver, c.Owner).RunOnce(ctx); n != 1 || err != nil || !called {
		t.Fatalf("short-budget lookup %d %v called=%v", n, err, called)
	}
}

func TestRouteRecoveryWorkerDoesNotSpinAfterRepositoryFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	repo := &routeRepo{claim: func(context.Context, string, int) ([]routing.RouteRecoveryClaim, error) {
		calls++
		if calls == 2 {
			cancel()
		}
		return nil, errors.New("database unavailable")
	}}
	started := time.Now()
	err := routing.NewRecoveryWorker(repo, nil, "route-worker").Run(ctx)
	if !errors.Is(err, context.Canceled) || calls != 2 || time.Since(started) < 90*time.Millisecond {
		t.Fatalf("error scanning spins: calls=%d elapsed=%v err=%v", calls, time.Since(started), err)
	}
}

func TestRouteRecoveryWorkerDrainSavesKnownRouteAndStopsClaims(t *testing.T) {
	c := routeClaim()
	scan, stop := context.WithCancel(context.Background())
	defer stop()
	claims, saved := 0, false
	repo := &routeRepo{claim: func(context.Context, string, int) ([]routing.RouteRecoveryClaim, error) {
		claims++
		return []routing.RouteRecoveryClaim{c}, nil
	}, complete: func(write context.Context, _ routing.RouteRecoveryClaim, s model.RouteSnapshot, lookupErr error) error {
		if write.Err() != nil {
			return write.Err()
		}
		d, ok := write.Deadline()
		if !ok || time.Until(d) > 8*time.Second || d.After(c.LeaseDeadline) {
			t.Error("route completion lacks bounded deadline")
		}
		saved = lookupErr == nil && s.Status == "NO_ROUTE" && s.MsgID == c.MsgID
		return nil
	}}
	resolver := targetResolver(func(query context.Context, _ model.TradeEvent) (model.RouteSnapshot, error) {
		stop()
		if query.Err() != nil {
			t.Errorf("graceful stop cancelled known route: %v", query.Err())
		}
		return model.RouteSnapshot{MsgID: c.MsgID, Status: "NO_ROUTE"}, nil
	})
	if err := routing.NewRecoveryWorker(repo, resolver, c.Owner).Run(scan); !errors.Is(err, context.Canceled) {
		t.Fatalf("scan did not stop: %v", err)
	}
	if !saved || claims != 1 {
		t.Fatalf("drain lost resolved targets or claimed again: saved=%v claims=%d", saved, claims)
	}
}
