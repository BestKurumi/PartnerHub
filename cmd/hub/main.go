package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"partner-hub/internal/adapter"
	"partner-hub/internal/binding"
	"partner-hub/internal/config"
	"partner-hub/internal/delivery"
	"partner-hub/internal/demo"
	"partner-hub/internal/diagnostics"
	hubkafka "partner-hub/internal/infra/kafka"
	hubmysql "partner-hub/internal/infra/mysql"
	hubredis "partner-hub/internal/infra/redis"
	"partner-hub/internal/ingress"
	"partner-hub/internal/model"
	"partner-hub/internal/observability"
	"partner-hub/internal/ops"
	"partner-hub/internal/query"
	"partner-hub/internal/routing"
	hubruntime "partner-hub/internal/runtime"
)

func main() {
	slog.SetDefault(observability.NewLogger(os.Stdout))
	if err := run(); err != nil {
		slog.Error("hub stopped", "reason", err.Error())
		os.Exit(1)
	}
}
func run() error {
	metrics := observability.NewMetrics()
	c, err := config.Load()
	if err != nil {
		return err
	}
	diagnosticBuffer := diagnostics.NewBuffer(c.InstanceID)
	slog.SetDefault(slog.New(diagnostics.NewLogHandler(observability.NewLogger(os.Stdout).Handler(), diagnosticBuffer)))
	opsService, err := ops.New(ops.Config{PrometheusURL: c.PrometheusURL, GrafanaURL: c.GrafanaURL})
	if err != nil {
		return err
	}
	internalToken := c.OpsToken
	if internalToken == "" {
		internalToken = model.NewID()
	} // Standalone: local diagnostics, no remote peers.
	peers := make([]diagnostics.Peer, 0, len(c.DiagnosticPeers))
	for _, peer := range c.DiagnosticPeers {
		peers = append(peers, diagnostics.Peer{Instance: peer.Instance, URL: peer.URL})
	}
	diagnosticHandler, err := diagnostics.NewHandler(diagnosticBuffer, diagnostics.Config{Token: internalToken, Peers: peers})
	if err != nil {
		return err
	}
	ad, err := adapter.NewPlatform1(c.Platform1URL)
	if err != nil {
		return err
	}
	pools, err := hubmysql.NewPools(c.DSN)
	if err != nil {
		return err
	}
	closers := []func() error{pools.Close}
	closeClients := func() error {
		var failures []error
		for i := len(closers) - 1; i >= 0; i-- {
			failures = append(failures, closers[i]())
		}
		return errors.Join(failures...)
	}
	lifecycleOwnsClients := false
	defer func() {
		if lifecycleOwnsClients {
			return
		}
		// Startup failures also cannot hang indefinitely in a driver Close.
		done := make(chan struct{})
		go func() { defer close(done); _ = closeClients() }()
		startupClose, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		select {
		case <-done:
		case <-startupClose.Done():
		}
	}()
	startup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = hubruntime.CheckSchema(startup, pools.Query)
	cancel()
	if err != nil {
		return errors.New("hub database/schema unavailable; run one-shot migrations first")
	}
	resources, err := observability.NewResources(observability.ResourceConfig{DB: pools.Query, Redis: c.Redis, Brokers: c.Brokers}, metrics.Registry)
	if err != nil {
		return err
	}
	closers = append(closers, resources.Close)
	cache := hubredis.NewBindings(c.Redis)
	closers = append(closers, cache.Close)
	archive, err := hubredis.NewArchiveWithConfig(c.Redis, hubmysql.NewArchiveStore(pools.Archive), hubredis.ArchiveConfig{Owner: c.InstanceID + "/" + model.NewID(), MaxPending: c.ArchiveGate.MaxPending, MaxBytes: c.ArchiveGate.MaxBytes})
	if err != nil {
		return err
	}
	closers = append(closers, archive.Close)
	archive.ObserveFlush = metrics.ArchiveFlush
	archive.ObserveVisible = metrics.ArchiveVisible
	startup, cancel = context.WithTimeout(context.Background(), time.Second)
	err = archive.Preflight(startup)
	cancel()
	if err != nil {
		return errors.New("hub archive unavailable")
	}
	repo := hubmysql.NewBindings(pools.Bindings)
	consumer, err := hubkafka.NewBindingConsumer(hubkafka.BindingConfig{Brokers: c.Brokers, Group: "hub.binding.v1", Topic: "customer.binding.events"}, binding.NewService(repo, cache), repo)
	if err != nil {
		return err
	}
	closers = append(closers, func() error { consumer.Close(); return nil })
	platform2, err := adapter.NewPlatform2(c.Brokers, "platform2.trade.notifications")
	if err != nil {
		return err
	}
	closers = append(closers, func() error { platform2.Close(); return nil })
	budgets, err := delivery.NewPlatformBudget(c.Platforms)
	if err != nil {
		return err
	}
	adapters := map[string]delivery.Adapter{"PLATFORM_A": ad, "PLATFORM_B": platform2}
	resolver := binding.NewResolver(repo, cache)
	resolver.ObserveCache = metrics.BindingCache
	resolver.ObserveSource = metrics.BindingSource
	router := routing.NewRouter(resolver)
	recoveries := hubmysql.NewRecoveryStore(pools.Recovery)
	routes := hubmysql.NewRouteRecoveryStore(pools.Recovery)
	manager, err := hubruntime.NewRecoveryManager(hubruntime.RecoveryConfig{Deliveries: recoveries, Routes: routes, DeliveryExpiry: recoveries, RouteExpiry: routes, Resolver: router, Adapters: adapters, Budget: budgets, Platforms: c.Platforms, InstanceID: c.InstanceID, ObserveAttempt: func(claim delivery.RecoveryClaim, result model.SendResult, err error) {
		metrics.Recovery(claim.Target.PlatformID, result, err)
		if err == nil && result.Outcome == model.Success {
			metrics.RecoverySuccess(claim.Target.PlatformID, claim.ReceivedAt)
		}
		logRecoveryResult(claim, result, err)
	}, ObserveExpiry: func(kind string, n int, err error) {
		if err == nil {
			metrics.LeaseRecovered(kind, n)
		}
	}})
	if err != nil {
		return err
	}
	archiveGate, err := ingress.NewArchiveGate(archive, archive, c.ArchiveGate)
	if err != nil {
		return err
	}
	service := ingress.NewService(router, delivery.NewFirstDelivery(adapters, budgets), archiveGate, hubmysql.NewHandoffStore(pools.Handoff))
	service.ObserveFirst = func(snapshot model.FirstRoundSnapshot) {
		metrics.First(snapshot)
		for _, a := range snapshot.Attempts {
			slog.Info("first delivery result", "msgId", snapshot.Trade.MsgID, "traceId", snapshot.Trade.TraceID, "deliveryId", a.Target.DeliveryID, "platform", a.Target.PlatformID, "outcome", string(a.Result.Outcome))
		}
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.Handle("/api/v1/trades", ingress.NewHTTPWithObserver(service, c.Gate, diagnostics.ObserveIngress(diagnosticBuffer)))
	baseQuery := hubruntime.NewPlatformStatusHandler(c.Platforms, query.NewHTTP(query.NewService(pools.Query), delivery.NewRetryCommand(hubmysql.NewManualStore(pools.Recovery))))
	q := consoleBackend(baseQuery, opsService.Handler(), diagnosticHandler)
	mux.Handle(diagnostics.InternalPath, diagnosticHandler)
	mux.Handle("/api/v1/", demo.NewLocalCommandHandler(q, os.Getenv("DEMO_TOKEN")))
	mux.Handle("/hub/", demo.NewHubHandler(q, os.Getenv("DEMO_TOKEN")))
	health, err := hubruntime.NewHealthHandler(c.InstanceID, map[string]hubruntime.HealthCheck{
		"config": func(context.Context) error { return c.ArchiveGate.Validate() },
		"adapters": func(context.Context) error {
			if adapters["PLATFORM_A"] == nil || adapters["PLATFORM_B"] == nil {
				return errors.New("adapter missing")
			}
			return nil
		},
		"schema":          func(ctx context.Context) error { return hubruntime.CheckSchema(ctx, pools.Query) },
		"mysql":           pools.Query.PingContext,
		"redis":           archive.Preflight,
		"kafka":           func(ctx context.Context) error { return consumer.CheckReady(ctx) },
		"archiveCapacity": archiveGate.Preflight,
		"handoffCapacity": func(ctx context.Context) error {
			stats := pools.Handoff.Stats()
			if stats.InUse >= stats.MaxOpenConnections {
				return errors.New("handoff pool saturated")
			}
			return pools.Handoff.PingContext(ctx)
		},
	}, mux)
	if err != nil {
		return err
	}
	httpMetrics := metrics.HTTP(health)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/trades" {
			httpMetrics.ServeHTTP(w, r)
		} else {
			health.ServeHTTP(w, r)
		}
	})
	server := &http.Server{Addr: c.Listen, Handler: handler, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	stop, release := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer release()
	// Scan cancellation stops new claims; execution cancellation is reserved for
	// the shared shutdown deadline so known in-flight results can still commit.
	scans, cancelScans := context.WithCancel(context.Background())
	defer cancelScans()
	inFlight, cancelInFlight := context.WithCancel(context.Background())
	defer cancelInFlight()
	lifecycleOwnsClients = true
	opsDone := make(chan struct{})
	go func() { defer close(opsDone); opsService.Run(scans) }()
	resourcesDone := make(chan struct{})
	go func() { defer close(resourcesDone); resources.Run(scans, 5*time.Second) }()
	observationDone := make(chan struct{})
	go func() {
		defer close(observationDone)
		metrics.RunObservations(scans, 5*time.Second, []observability.Observation{
			{Kind: "archive", Sample: func(ctx context.Context) error {
				c, age, err := archive.Backlog(ctx)
				metrics.Redis(c.UsedMemory, c.MaxMemory, c.Durable)
				if err == nil {
					metrics.Backlog(c.Pending, c.Bytes, age)
				}
				return err
			}},
			{Kind: "recovery", Sample: func(ctx context.Context) error {
				for name, db := range map[string]*sql.DB{"bindings": pools.Bindings, "handoff": pools.Handoff, "archive": pools.Archive, "recovery": pools.Recovery, "query": pools.Query} {
					metrics.Pool(name, db.Stats())
				}
				states, err := hubmysql.RecoveryObservations(ctx, pools.Query)
				if err != nil {
					return err
				}
				for _, s := range states {
					metrics.RecoveryState(s.Platform, s.Status, s.Count, s.Age)
				}
				return nil
			}},
			{Kind: "binding_lag", Sample: func(ctx context.Context) error {
				lag, err := consumer.BindingLag(ctx)
				if err == nil {
					metrics.BindingLag(lag)
				}
				return err
			}},
		})
	}()
	recoveryDone := make(chan struct{})
	go func() { defer close(recoveryDone); _ = manager.RunDraining(scans, inFlight) }()
	bindingDone := make(chan error, 1)
	bindingRunDone := make(chan struct{})
	go func() { defer close(bindingRunDone); bindingDone <- consumer.Run(scans) }()
	archiveDone := make(chan struct{})
	go func() {
		defer close(archiveDone)
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		for {
			select {
			case <-scans.Done():
				return
			case <-timer.C:
				if scans.Err() != nil {
					return
				}
				flush, cancel := context.WithTimeout(inFlight, 3*time.Second)
				err := archive.Flush(flush)
				cancel()
				delay := 100 * time.Millisecond
				if err != nil {
					slog.Warn("archive flush unavailable; pending snapshots retained")
					delay = time.Second
				}
				timer.Reset(delay)
			}
		}
	}()
	workersDone := make(chan struct{})
	go func() {
		defer close(workersDone)
		<-recoveryDone
		<-bindingRunDone
		<-archiveDone
		<-observationDone
		<-opsDone
		<-resourcesDone
	}()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.ListenAndServe() }()
	slog.Info("hub listening", "address", c.Listen, "instance", c.InstanceID)
	var result error
	select {
	case <-stop.Done():
	case err := <-bindingDone:
		if scans.Err() == nil {
			result = errors.New("binding consumer stopped; inspect offsets and dependencies")
		}
		_ = err
	case err := <-serveDone:
		if !errors.Is(err, http.ErrServerClosed) {
			result = err
		}
	}
	// Every remaining phase consumes this one deadline; none receives a fresh
	// shutdown allowance. Container grace is longer than this 20-second budget.
	health.BeginDrain()
	cancelScans()
	drain, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stopForce := context.AfterFunc(drain, cancelInFlight)
	defer stopForce()
	err = health.Shutdown(drain, server, workersDone, func(ctx context.Context) error {
		// Flush has an independent 100ms lease-release cleanup; leave room for it
		// within the shared shutdown budget instead of extending that budget.
		deadline, _ := ctx.Deadline()
		deadline = deadline.Add(-100 * time.Millisecond)
		if !time.Now().Before(deadline) {
			return nil
		}
		flush, finish := context.WithDeadline(ctx, deadline)
		defer finish()
		if err := archive.Flush(flush); err != nil {
			slog.Warn("shutdown leaves pending snapshots for next archive worker")
		}
		return nil
	}, closeClients)
	if err != nil {
		result = errors.Join(result, err)
	}
	return result
}
