package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"partner-hub/internal/mock/platform1"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err := run(os.Args[1:]); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) > 0 && args[0] == "upstream" {
		return runUpstream(args[1:])
	}
	if len(args) > 0 && args[0] == "platform2" {
		return runPlatform2(args[1:])
	}
	if len(args) == 0 || args[0] != "platform1" {
		return errors.New("usage: mock platform1 [-listen 127.0.0.1:18081] [-mode accept] [-migrate path]; set PLATFORM1_DSN")
	}
	flags := flag.NewFlagSet("platform1", flag.ContinueOnError)
	listen := flags.String("listen", "127.0.0.1:18081", "HTTP listen address")
	mode := flags.String("mode", "accept", "accept, reject-busy, persist-delay or persist-disconnect")
	delay := flags.Duration("delay", 3*time.Second, "delay after durable receipt")
	migration := flags.String("migrate", "", "apply platform1 migration file before serving")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected mock arguments")
	}
	if err := validateListen(*listen); err != nil {
		return err
	}
	config, err := mysql.ParseDSN(os.Getenv("PLATFORM1_DSN"))
	if err != nil || config.DBName != "mock_platform1" || config.User != "mock_p1" || config.MultiStatements {
		return errors.New("PLATFORM1_DSN must select the independent mock_platform1 schema and account; multiStatements disabled")
	}
	config.ParseTime = true
	config.Timeout = time.Second
	config.ReadTimeout = 2 * time.Second
	config.WriteTimeout = 2 * time.Second
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return errors.New("open platform1 database failed")
	}
	defer db.Close()
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(5 * time.Minute)
	h, err := platform1.NewHandler(platform1.NewMySQLStore(db), platform1.Mode(*mode), *delay)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = db.PingContext(ctx)
	if err == nil && *migration != "" {
		var ddl []byte
		ddl, err = os.ReadFile(*migration)
		if err == nil {
			_, err = db.ExecContext(ctx, string(ddl))
		}
	}
	if err == nil {
		err = checkSchema(ctx, db, "PLATFORM_A")
	}
	cancel()
	if err != nil {
		return errors.New("platform1 database startup or migration failed")
	}
	handler, closePortal, err := withPortal("PLATFORM_A", db, h)
	if err != nil {
		return err
	}
	defer closePortal()
	metrics := newMockMetrics(receiptSampler(db), nil, nil)
	server := &http.Server{Addr: *listen, Handler: withHealth(metrics.withHandler(handler)), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 35 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	shutdown, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go metrics.run(shutdown, 2*time.Second)
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	log.Printf("platform1 mock listening on %s, mode=%s", *listen, *mode)
	select {
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("platform1 HTTP server: %w", err)
		}
		return nil
	case <-shutdown.Done():
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(ctx)
	}
}
