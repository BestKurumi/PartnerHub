package config

import (
	"errors"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"partner-hub/internal/delivery"
	"partner-hub/internal/ingress"
)

type Config struct {
	DSN, Listen, Platform1URL           string
	Redis                               *redis.Options
	Brokers                             []string
	Gate                                ingress.GateConfig
	Platforms                           map[string]delivery.BudgetConfig
	InstanceID                          string
	DBPools                             map[string]int
	ArchiveGate                         ingress.ArchiveGateConfig
	PrometheusURL, GrafanaURL, OpsToken string
	DiagnosticPeers                     []DiagnosticPeer
}

type DiagnosticPeer struct{ Instance, URL string }

func Load() (Config, error) {
	var c Config
	getenv, err := loadValues()
	if err != nil {
		return c, err
	}
	c.DBPools = map[string]int{"binding": 4, "handoff": 6, "archive": 3, "recovery": 7, "query": 4}
	d, err := mysql.ParseDSN(getenv("HUB_DSN"))
	if err != nil || d.DBName != "partner_hub" || d.User != "hub" || d.MultiStatements {
		return c, errors.New("HUB_DSN must select partner_hub with independent hub account, multiStatements disabled")
	}
	d.ParseTime = true
	d.Timeout = 1e9
	d.ReadTimeout = 2e9
	d.WriteTimeout = 2e9
	c.DSN = d.FormatDSN()
	addr := getenv("HUB_REDIS_ADDR")
	if addr == "" {
		return c, errors.New("HUB_REDIS_ADDR required")
	}
	c.Redis = &redis.Options{Addr: addr, Password: getenv("HUB_REDIS_PASSWORD")}
	if raw := getenv("HUB_REDIS_DB"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 0 || n > 15 {
			return c, errors.New("invalid HUB_REDIS_DB")
		}
		c.Redis.DB = n
	}
	for _, b := range strings.Split(getenv("HUB_KAFKA_BROKERS"), ",") {
		if strings.TrimSpace(b) != "" {
			c.Brokers = append(c.Brokers, strings.TrimSpace(b))
		}
	}
	if len(c.Brokers) == 0 {
		return c, errors.New("HUB_KAFKA_BROKERS required")
	}
	c.Platform1URL = getenv("HUB_PLATFORM1_URL")
	if c.Platform1URL == "" {
		return c, errors.New("HUB_PLATFORM1_URL required")
	}
	c.Listen = getenv("HUB_LISTEN")
	if c.Listen == "" {
		c.Listen = "127.0.0.1:18080"
	}
	c.Gate = ingress.GateConfig{MaxInFlight: 64, Rate: 200, Burst: 40}
	for _, entry := range []struct {
		name string
		dst  *int
	}{{"HUB_MAX_INFLIGHT", &c.Gate.MaxInFlight}, {"HUB_RATE_BURST", &c.Gate.Burst}} {
		if raw := getenv(entry.name); raw != "" {
			n, e := strconv.Atoi(raw)
			if e != nil || n < 1 || n > 10000 {
				return c, errors.New("invalid " + entry.name)
			}
			*entry.dst = n
		}
	}
	if raw := getenv("HUB_RATE_PER_SECOND"); raw != "" {
		n, e := strconv.ParseFloat(raw, 64)
		if e != nil || !(n > 0 && n <= 100000) {
			return c, errors.New("invalid HUB_RATE_PER_SECOND")
		}
		c.Gate.Rate = n
	}
	c.InstanceID = getenv("HUB_INSTANCE_ID")
	if c.InstanceID == "" {
		c.InstanceID = "hub-local"
	}
	if len(c.InstanceID) > 64 {
		return c, errors.New("invalid HUB_INSTANCE_ID")
	}
	for _, r := range c.InstanceID {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return c, errors.New("invalid HUB_INSTANCE_ID")
		}
	}
	c.Platforms = make(map[string]delivery.BudgetConfig, 2)
	for _, p := range []string{"A", "B"} {
		b := delivery.DefaultBudgetConfig()
		prefix := "HUB_PLATFORM_" + p + "_"
		for _, entry := range []struct {
			name string
			dst  *int
			max  int
		}{{"TOTAL_CONCURRENCY", &b.Total, 256}, {"FIRST_CONCURRENCY", &b.First, 256}, {"RECOVERY_CONCURRENCY", &b.Recovery, 64}, {"RATE_BURST", &b.Burst, 10000}} {
			if raw := getenv(prefix + entry.name); raw != "" {
				n, err := strconv.Atoi(raw)
				if err != nil || n < 1 || n > entry.max {
					return c, errors.New("invalid " + prefix + entry.name)
				}
				*entry.dst = n
			}
		}
		if b.First > b.Total || b.Recovery > b.Total {
			return c, errors.New("platform class concurrency exceeds total")
		}
		if raw := getenv(prefix + "RATE_PER_SECOND"); raw != "" {
			n, err := strconv.ParseFloat(raw, 64)
			if err != nil || math.IsNaN(n) || math.IsInf(n, 0) || n <= 0 || n > 100000 {
				return c, errors.New("invalid " + prefix + "RATE_PER_SECOND")
			}
			b.Rate = n
		}
		if raw := getenv(prefix + "PAUSED"); raw != "" {
			v, err := strconv.ParseBool(raw)
			if err != nil {
				return c, errors.New("invalid " + prefix + "PAUSED")
			}
			b.Paused = v
		}
		c.Platforms["PLATFORM_"+p] = b
	}
	c.ArchiveGate = ingress.DefaultArchiveGateConfig()
	maxInstances := int64(2)
	for _, entry := range []struct {
		name string
		dst  *int64
		max  int64
	}{
		{"HUB_ARCHIVE_MAX_PENDING", &c.ArchiveGate.MaxPending, 1000000000},
		{"HUB_ARCHIVE_MAX_BYTES", &c.ArchiveGate.MaxBytes, 1 << 40},
		{"HUB_MAX_INSTANCES", &maxInstances, 2},
	} {
		if raw := getenv(entry.name); raw != "" {
			n, e := strconv.ParseInt(raw, 10, 64)
			if e != nil || n < 1 || n > entry.max {
				return c, errors.New("invalid " + entry.name)
			}
			*entry.dst = n
		}
	}
	// Headroom includes all instances' admitted requests and the maximum arrivals
	// in the sample-age window. It is deliberately not a strict global quota.
	c.ArchiveGate.MaxSampleAge = 250 * time.Millisecond
	c.ArchiveGate.ReserveRequests = maxInstances * (int64(c.Gate.MaxInFlight) + int64(c.Gate.Burst) + int64(math.Ceil(c.Gate.Rate*c.ArchiveGate.MaxSampleAge.Seconds())))
	if err := c.ArchiveGate.Validate(); err != nil {
		return c, errors.New("archive budget cannot cover configured in-flight reserve")
	}
	c.PrometheusURL, c.GrafanaURL, c.OpsToken = getenv("HUB_PROMETHEUS_URL"), getenv("HUB_GRAFANA_URL"), getenv("HUB_OPS_TOKEN")
	for _, raw := range []string{c.PrometheusURL, c.GrafanaURL} {
		if raw != "" && !consoleURL(raw, false) {
			return c, errors.New("invalid operational URL")
		}
	}
	if len(c.OpsToken) > 256 || strings.ContainsFunc(c.OpsToken, unicode.IsControl) {
		return c, errors.New("invalid HUB_OPS_TOKEN")
	}
	seen := map[string]bool{}
	if raw := getenv("HUB_DIAGNOSTIC_PEERS"); raw != "" {
		for _, entry := range strings.Split(raw, ",") {
			id, address, ok := strings.Cut(strings.TrimSpace(entry), "=")
			if !ok || len(id) == 0 || len(id) > 64 || strings.ContainsFunc(id, func(r rune) bool {
				return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.')
			}) || seen[id] || !consoleURL(address, true) {
				return c, errors.New("invalid HUB_DIAGNOSTIC_PEERS")
			}
			seen[id] = true
			if len(seen) > 2 {
				return c, errors.New("at most two diagnostics instances supported")
			}
			if id != c.InstanceID {
				c.DiagnosticPeers = append(c.DiagnosticPeers, DiagnosticPeer{id, address})
			}
		}
		if c.OpsToken == "" {
			return c, errors.New("HUB_OPS_TOKEN required for diagnostics peers")
		}
	}
	return c, nil
}

func consoleURL(raw string, originOnly bool) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != "" && u.User == nil && u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" && u.Opaque == "" && (!originOnly || (u.Path == "" || u.Path == "/") && u.RawPath == "")
}
