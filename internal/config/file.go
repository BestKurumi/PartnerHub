package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
)

// loadValues never changes the process environment. A present empty env value
// deliberately masks its file value and preserves Load's empty/default rules.
func loadValues() (func(string) string, error) {
	values := map[string]string{}
	if path := os.Getenv("HUB_CONFIG_FILE"); path != "" {
		f, e := os.Open(path)
		if e != nil {
			return nil, errors.New("cannot read HUB_CONFIG_FILE")
		}
		defer f.Close()
		body, e := io.ReadAll(io.LimitReader(f, 65537))
		if e != nil || len(body) > 65536 {
			return nil, errors.New("HUB_CONFIG_FILE exceeds 64KiB or cannot be read")
		}
		values, e = decodeValues(body)
		if e != nil {
			return nil, e
		}
	}
	return func(k string) string {
		if v, ok := os.LookupEnv(k); ok {
			return v
		}
		return values[k]
	}, nil
}
func decodeValues(body []byte) (map[string]string, error) {
	allowed := map[string]bool{}
	for _, k := range []string{"HUB_PROMETHEUS_URL", "HUB_GRAFANA_URL", "HUB_OPS_TOKEN", "HUB_DIAGNOSTIC_PEERS"} {
		allowed[k] = true
	}
	for _, k := range []string{"HUB_DSN", "HUB_REDIS_ADDR", "HUB_REDIS_PASSWORD", "HUB_REDIS_DB", "HUB_KAFKA_BROKERS", "HUB_PLATFORM1_URL", "HUB_LISTEN", "HUB_MAX_INFLIGHT", "HUB_RATE_BURST", "HUB_RATE_PER_SECOND", "HUB_INSTANCE_ID", "HUB_ARCHIVE_MAX_PENDING", "HUB_ARCHIVE_MAX_BYTES", "HUB_MAX_INSTANCES"} {
		allowed[k] = true
	}
	for _, p := range []string{"A", "B"} {
		for _, k := range []string{"TOTAL_CONCURRENCY", "FIRST_CONCURRENCY", "RECOVERY_CONCURRENCY", "RATE_BURST", "RATE_PER_SECOND", "PAUSED"} {
			allowed["HUB_PLATFORM_"+p+"_"+k] = true
		}
	}
	d := json.NewDecoder(bytes.NewReader(body))
	tok, e := d.Token()
	if e != nil || tok != json.Delim('{') {
		return nil, errors.New("HUB_CONFIG_FILE must be an object")
	}
	values := map[string]string{}
	for d.More() {
		tok, e = d.Token()
		if e != nil {
			return nil, errors.New("invalid HUB_CONFIG_FILE")
		}
		k, ok := tok.(string)
		if !ok || !allowed[k] {
			return nil, errors.New("unknown HUB_CONFIG_FILE key")
		}
		if _, ok = values[k]; ok {
			return nil, errors.New("duplicate HUB_CONFIG_FILE key")
		}
		tok, e = d.Token()
		v, ok := tok.(string)
		if e != nil || !ok {
			return nil, errors.New("HUB_CONFIG_FILE values must be strings")
		}
		values[k] = v
	}
	if tok, e = d.Token(); e != nil || tok != json.Delim('}') {
		return nil, errors.New("invalid HUB_CONFIG_FILE")
	}
	if _, e = d.Token(); e != io.EOF {
		return nil, errors.New("trailing HUB_CONFIG_FILE data")
	}
	return values, nil
}
