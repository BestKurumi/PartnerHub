package main

import (
	"testing"
	"time"
)

// Catches a remote deployment target or unbounded queue/memory being accepted.
func TestValidateRejectsUnboundedAndRemoteConfig(t *testing.T) {
	good := Config{Scenario: "normal", QPS: 5, Duration: time.Second, Concurrency: 2, Settle: time.Second}
	for _, change := range []func(*Config){func(c *Config) { c.QPS = 0 }, func(c *Config) { c.Duration = 2 * time.Hour }, func(c *Config) { c.Concurrency = 1001 }, func(c *Config) { c.Scenario = "other" }, func(c *Config) { c.QPS = 1000000 }} {
		c := good
		change(&c)
		if validate(c, "http://127.0.0.1:1234") == nil {
			t.Fatalf("unbounded config accepted: %+v", c)
		}
	}
	for _, url := range []string{"http://example.com", "http://127.0.0.1:1234/path", "http://user:secret@127.0.0.1"} {
		if validate(good, url) == nil {
			t.Fatal("unsafe target accepted")
		}
	}
	if validate(good, "http://127.0.0.1:1234") != nil {
		t.Fatal("valid isolated configuration rejected")
	}
}
