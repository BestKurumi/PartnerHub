// Command load submits a bounded, unique-input cohort to the local isolated hub.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	driver "github.com/go-sql-driver/mysql"
	"net"
	"net/http"
	"os"
	"partner-hub/internal/model"
	"time"
)

type Report struct {
	PlannedOfferedQPS       float64           `json:"planned_offered_qps"`
	ActualOfferedQPS        float64           `json:"actual_offered_qps"`
	ScheduleAndDrainSeconds float64           `json:"schedule_and_drain_seconds"`
	Config                  Config            `json:"config"`
	Schedule                string            `json:"schedule"`
	StartedAt               time.Time         `json:"started_at"`
	FinishedAt              time.Time         `json:"finished_at"`
	InputWindowSeconds      float64           `json:"input_window_seconds"`
	ActualSubmittedQPS      float64           `json:"actual_submitted_qps"`
	DatabaseErrors          int               `json:"database_observation_errors"`
	DatabaseFinalAvailable  bool              `json:"database_final_available"`
	Error                   string            `json:"error,omitempty"`
	Boundaries              map[string]string `json:"boundaries"`
	Bindings                []BindingFixture  `json:"bindings"`
	Summary                 Summary           `json:"summary"`
	Inputs                  []Input           `json:"inputs"`
}

func localDB(dsn string) (*sql.DB, error) {
	c, e := driver.ParseDSN(dsn)
	if e != nil {
		return nil, errors.New("invalid HUB_TEST_DSN")
	}
	host, _, e := net.SplitHostPort(c.Addr)
	if e != nil || c.Net != "tcp" || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() || c.DBName != "partner_hub" || c.User != "hub" || c.MultiStatements {
		return nil, errors.New("HUB_TEST_DSN must target isolated local hub schema")
	}
	c.ParseTime = true
	c.Loc = time.UTC
	c.Timeout = time.Second
	c.ReadTimeout = 2 * time.Second
	c.WriteTimeout = 2 * time.Second
	db, e := sql.Open("mysql", c.FormatDSN())
	if e != nil {
		return nil, errors.New("cannot open local observation database")
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	return db, nil
}
func main() {
	if e := execute(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func execute() error {
	var config Config
	var output string
	flag.StringVar(&config.Scenario, "scenario", "normal", "normal|burst|high-failure|cold-bindings")
	flag.Float64Var(&config.QPS, "qps", 5, "nominal offered inputs/second")
	flag.DurationVar(&config.Duration, "duration", 10*time.Second, "nominal input duration")
	flag.IntVar(&config.Concurrency, "concurrency", 32, "maximum active HTTP requests; excess offered slots are recorded unsent")
	flag.DurationVar(&config.Settle, "settle", 30*time.Second, "maximum observation period after sending")
	flag.StringVar(&output, "output", "load-report.json", "new JSON manifest path (never overwrite)")
	flag.Parse()
	base := os.Getenv("HUB_TEST_URL")
	if e := validate(config, base); e != nil {
		return e
	}
	for _, key := range []string{"HUB_TEST_DSN", "HUB_REDIS_ADDR", "DEMO_TOKEN"} {
		if os.Getenv(key) == "" {
			return fmt.Errorf("required environment variable %s missing", key)
		}
	}
	host, _, e := net.SplitHostPort(os.Getenv("HUB_REDIS_ADDR"))
	if e != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return errors.New("HUB_REDIS_ADDR must be a loopback endpoint")
	}
	db, e := localDB(os.Getenv("HUB_TEST_DSN"))
	if e != nil {
		return e
	}
	defer db.Close()
	file, e := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return errors.New("cannot create new report path")
	}
	defer file.Close()
	rows := makeInputs(int(config.QPS*config.Duration.Seconds()), "load-"+model.NewID())
	if config.Scenario == "cold-bindings" {
		for i := range rows {
			rows[i].ClientID = "load-" + model.NewID()
		}
	}
	r := Report{Config: config, Inputs: rows, StartedAt: time.Now().UTC(), Schedule: "constant offered QPS; initial warm cache (30s TTL)", Boundaries: map[string]string{"PLATFORM_A": "ingress received_at -> first attempt finished_at, legal SUCCESS HTTP response and send_started", "PLATFORM_B": "ingress received_at -> first attempt finished_at, Broker confirmation SUCCESS and send_started; excludes consumer display", "timing": "visibility is first DB poll after input drain (250ms between passes, up to 2s per 100 IDs); Submit/observed use host clock, insert/ingress span Docker VM clocks and may be invalid if clocks differ; first finished/received share hub clock", "archive": "stored_at is DB insertion timestamp, not commit visibility; visibility_observed is first post-input DB polling observation upper bound", "cohort": "all planned inputs expected at both preconfirmed platforms; any failure/missing excludes all-cohort P99, success-only P99 separately labelled", "recovery": "received_at -> successful recovery attempt finished_at; reported separately", "http": "client submission -> response; excludes input generator scheduling and is not platform first delivery"}}
	if config.Scenario == "cold-bindings" {
		r.Schedule = "constant offered QPS; unique preconfirmed client per input, exact cache deletion before run"
	}
	interval := time.Duration(float64(time.Second) / config.QPS)
	if config.Scenario == "burst" {
		interval /= 5
		r.Schedule = "same planned input count offered at 5x nominal QPS in first fifth of nominal duration"
	}
	save := func() error {
		r.Summary = summarize(rows)
		if _, e := file.Seek(0, 0); e != nil {
			return e
		}
		if e := file.Truncate(0); e != nil {
			return e
		}
		enc := json.NewEncoder(file)
		enc.SetIndent("", "  ")
		if e := enc.Encode(r); e != nil {
			return e
		}
		return file.Sync()
	}
	if e := save(); e != nil {
		return errors.New("initial manifest write failed")
	}
	transport := &http.Transport{MaxConnsPerHost: config.Concurrency, MaxIdleConns: config.Concurrency, MaxIdleConnsPerHost: config.Concurrency, ResponseHeaderTimeout: 8 * time.Second}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 9 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	setup, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	r.Bindings, e = prepare(setup, client, base, os.Getenv("DEMO_TOKEN"), os.Getenv("HUB_REDIS_ADDR"), os.Getenv("HUB_REDIS_PASSWORD"), rows, config.Scenario == "cold-bindings")
	cancel()
	if e != nil {
		r.Error = e.Error()
		r.FinishedAt = time.Now().UTC()
		_ = save()
		return e
	}
	begin := time.Now()
	runSchedule(context.Background(), client, base, rows, interval, config.Concurrency)
	r.ScheduleAndDrainSeconds = time.Since(begin).Seconds()
	r.InputWindowSeconds = float64(len(rows)) * interval.Seconds()
	r.PlannedOfferedQPS = 1 / interval.Seconds()
	if len(rows) > 0 {
		var last time.Time
		for _, row := range rows {
			if row.OfferedAt != nil {
				last = *row.OfferedAt
			}
		}
		actualWindow := time.Since(begin).Seconds()
		if !last.IsZero() {
			actualWindow = last.Sub(begin).Seconds() + interval.Seconds()
		}
		if actualWindow > 0 {
			r.InputWindowSeconds = actualWindow
			r.ActualOfferedQPS = float64(len(rows)) / actualWindow
		}
	}
	r.DatabaseErrors, e = settle(context.Background(), db, rows, config.Settle)
	r.DatabaseFinalAvailable = e == nil
	r.FinishedAt = time.Now().UTC()
	r.Summary = summarize(rows)
	if r.InputWindowSeconds > 0 {
		r.ActualSubmittedQPS = float64(r.Summary.Submitted) / r.InputWindowSeconds
	}
	if e = save(); e != nil {
		return errors.New("final report write failed")
	}
	fmt.Printf("scenario=%s planned=%d submitted=%d local_rejected=%d unrecorded=%d report=%s\n", config.Scenario, r.Summary.Planned, r.Summary.Submitted, r.Summary.LocalRejected, r.Summary.Unrecorded, output)
	return nil
}
