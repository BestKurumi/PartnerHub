package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"partner-hub/internal/mock/upstream"
	"syscall"
	"time"
)

func runUpstream(args []string) error {
	flags := flag.NewFlagSet("upstream", flag.ContinueOnError)
	listen := flags.String("listen", "127.0.0.1:28083", "local upstream address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected upstream arguments")
	}
	if err := validateListen(*listen); err != nil {
		return err
	}
	h, err := upstream.NewHandler(upstream.Config{HubURL: os.Getenv("HUB_TEST_URL"), Token: os.Getenv("DEMO_TOKEN")})
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/mock/upstream/", http.StripPrefix("/mock/upstream", h))
	mux.Handle("/", h)
	metrics := newMockMetrics(nil, nil, nil)
	server := &http.Server{Addr: *listen, Handler: withHealth(metrics.withHandler(mux)), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 40 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	log.Printf("upstream mock listening on %s", *listen)
	select {
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
		drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(drain)
	}
}
