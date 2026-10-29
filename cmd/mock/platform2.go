package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"github.com/go-sql-driver/mysql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"partner-hub/internal/mock/platform2"
	"strings"
	"syscall"
	"time"
)

func runPlatform2(args []string) error {
	flags := flag.NewFlagSet("platform2", flag.ContinueOnError)
	listen := flags.String("listen", "127.0.0.1:28082", "local control address")
	paused := flags.Bool("paused", false, "start with consumption paused")
	migration := flags.String("migrate", "", "independent platform2 migration path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected platform2 arguments")
	}
	if err := validateListen(*listen); err != nil {
		return err
	}
	token := os.Getenv("DEMO_TOKEN")
	if token == "" {
		return errors.New("DEMO_TOKEN required")
	}
	d, err := mysql.ParseDSN(os.Getenv("PLATFORM2_DSN"))
	if err != nil || d.DBName != "mock_platform2" || d.User != "mock_p2" || d.MultiStatements {
		return errors.New("PLATFORM2_DSN must use independent mock_platform2 schema/account")
	}
	d.ParseTime = true
	d.Timeout = time.Second
	d.ReadTimeout = 2 * time.Second
	d.WriteTimeout = 2 * time.Second
	db, err := sql.Open("mysql", d.FormatDSN())
	if err != nil {
		return errors.New("open platform2 database failed")
	}
	defer db.Close()
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(5 * time.Minute)
	startup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = db.PingContext(startup)
	if err == nil && *migration != "" {
		var ddl []byte
		ddl, err = os.ReadFile(*migration)
		if err == nil {
			for _, stmt := range strings.Split(string(ddl), ";") {
				if strings.TrimSpace(stmt) != "" {
					if _, err = db.ExecContext(startup, stmt); err != nil {
						break
					}
				}
			}
		}
	}
	if err == nil {
		err = checkSchema(startup, db, "PLATFORM_B")
	}
	cancel()
	if err != nil {
		return errors.New("platform2 startup/migration failed")
	}
	brokers := strings.Split(os.Getenv("MOCK_KAFKA_BROKERS"), ",")
	if len(brokers) == 1 && brokers[0] == "" {
		return errors.New("MOCK_KAFKA_BROKERS required")
	}
	consumer, err := platform2.NewConsumer(platform2.ConsumerConfig{Brokers: brokers, Group: "mock.platform2.receive.v1", Topic: "platform2.trade.notifications", Paused: *paused}, platform2.NewMySQLStore(db))
	if err != nil {
		return err
	}
	defer consumer.Close()
	handler, closePortal, err := withPortal("PLATFORM_B", db, platform2.NewControlHandler(consumer, token))
	if err != nil {
		return err
	}
	defer closePortal()
	metrics := newMockMetrics(receiptSampler(db), consumer.Lag, consumer.Paused)
	server := &http.Server{Addr: *listen, Handler: withHealth(metrics.withHandler(handler)), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	run, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go metrics.run(run, 2*time.Second)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(run) }()
	served := make(chan error, 1)
	go func() { served <- server.ListenAndServe() }()
	log.Printf("platform2 consumer/control listening on %s", *listen)
	var result error
	select {
	case <-ctx.Done():
	case err := <-done:
		result = err
	case err := <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			result = err
		}
	}
	drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = server.Shutdown(drain); err != nil {
		server.Close()
		if result == nil {
			result = err
		}
	}
	cancelRun()
	consumer.Close()
	return result
}
