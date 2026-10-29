package main

import (
	"errors"
	"math"
	"net"
	"net/url"
	"time"
)

type Config struct {
	Scenario    string        `json:"scenario"`
	QPS         float64       `json:"nominal_qps"`
	Duration    time.Duration `json:"nominal_duration_ns"`
	Concurrency int           `json:"concurrency"`
	Settle      time.Duration `json:"settle_ns"`
}

func validate(c Config, base string) error {
	u, e := url.Parse(base)
	if e != nil || u.Scheme != "http" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || net.ParseIP(u.Hostname()) == nil || !net.ParseIP(u.Hostname()).IsLoopback() {
		return errors.New("HUB_TEST_URL must be a local HTTP origin")
	}
	if c.Scenario != "normal" && c.Scenario != "burst" && c.Scenario != "high-failure" && c.Scenario != "cold-bindings" {
		return errors.New("unsupported scenario")
	}
	count := c.QPS * c.Duration.Seconds()
	if math.IsNaN(c.QPS) || math.IsInf(c.QPS, 0) || c.QPS <= 0 || c.QPS > 10000 || c.Duration < time.Millisecond || c.Duration > time.Hour || count < 1 || count > 100000 || c.Concurrency < 1 || c.Concurrency > 1000 || c.Settle < time.Millisecond || c.Settle > 10*time.Minute {
		return errors.New("bounded QPS/duration/concurrency/settle required (1..100000 inputs)")
	}
	if c.Scenario == "cold-bindings" && count > 2000 {
		return errors.New("cold bindings limited to 2000 run-owned clients")
	}
	return nil
}
