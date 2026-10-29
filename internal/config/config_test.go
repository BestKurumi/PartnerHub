package config_test

import (
	"partner-hub/internal/config"
	"testing"
)

func configEnv(t *testing.T) {
	t.Helper()
	for k, v := range map[string]string{"HUB_DSN": "hub:local@tcp(127.0.0.1:13306)/partner_hub", "HUB_REDIS_ADDR": "127.0.0.1:16379", "HUB_KAFKA_BROKERS": "127.0.0.1:19092", "HUB_PLATFORM1_URL": "http://127.0.0.1:28081/notifications/trades", "HUB_INSTANCE_ID": "hub-test"} {
		t.Setenv(k, v)
	}
	for _, p := range []string{"A", "B"} {
		for _, name := range []string{"TOTAL_CONCURRENCY", "FIRST_CONCURRENCY", "RECOVERY_CONCURRENCY", "RATE_PER_SECOND", "RATE_BURST", "PAUSED"} {
			t.Setenv("HUB_PLATFORM_"+p+"_"+name, "")
		}
	}
}
func TestPlatformConfigDefaultsAndIndependentOverrides(t *testing.T) {
	configEnv(t)
	c, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.InstanceID != "hub-test" || len(c.Platforms) != 2 {
		t.Fatalf("platform configuration absent: %+v", c)
	}
	for p, b := range c.Platforms {
		if b.Total != 16 || b.First != 12 || b.Recovery != 4 || b.Rate != 100 || b.Burst != 4 || b.Paused {
			t.Fatalf("%s invalid defaults %+v", p, b)
		}
	}
	for k, v := range map[string]string{"HUB_PLATFORM_A_PAUSED": "true", "HUB_PLATFORM_A_TOTAL_CONCURRENCY": "8", "HUB_PLATFORM_A_FIRST_CONCURRENCY": "6", "HUB_PLATFORM_A_RECOVERY_CONCURRENCY": "2", "HUB_PLATFORM_A_RATE_PER_SECOND": "50", "HUB_PLATFORM_A_RATE_BURST": "2"} {
		t.Setenv(k, v)
	}
	c, err = config.Load()
	if err != nil {
		t.Fatal(err)
	}
	a, b := c.Platforms["PLATFORM_A"], c.Platforms["PLATFORM_B"]
	if !a.Paused || a.Total != 8 || a.First != 6 || a.Recovery != 2 || a.Rate != 50 || a.Burst != 2 || b.Total != 16 || b.Paused {
		t.Fatalf("platform overrides leaked: %+v", c.Platforms)
	}
}
func TestPlatformConfigRejectsUnsafeValues(t *testing.T) {
	for k, v := range map[string]string{"HUB_PLATFORM_A_TOTAL_CONCURRENCY": "0", "HUB_PLATFORM_A_FIRST_CONCURRENCY": "17", "HUB_PLATFORM_A_RECOVERY_CONCURRENCY": "257", "HUB_PLATFORM_A_RATE_PER_SECOND": "NaN", "HUB_PLATFORM_A_RATE_BURST": "0", "HUB_PLATFORM_A_PAUSED": "maybe", "HUB_INSTANCE_ID": "bad owner with spaces"} {
		t.Run(k, func(t *testing.T) {
			configEnv(t)
			t.Setenv(k, v)
			if _, err := config.Load(); err == nil {
				t.Fatalf("accepted %s=%s", k, v)
			}
		})
	}
}

func TestArchiveCapacityConfig(t *testing.T) {
	configEnv(t)
	for _, k := range []string{"HUB_ARCHIVE_MAX_PENDING", "HUB_ARCHIVE_MAX_BYTES", "HUB_MAX_INSTANCES"} {
		t.Setenv(k, "")
	}
	c, e := config.Load()
	if e != nil {
		t.Fatal(e)
	}
	if c.ArchiveGate.MaxPending != 10000 || c.ArchiveGate.MaxBytes != 128<<20 || c.ArchiveGate.ReserveRequests != 308 {
		t.Fatalf("capacity defaults absent: %+v", c.ArchiveGate)
	}
	t.Setenv("HUB_MAX_INFLIGHT", "32")
	t.Setenv("HUB_RATE_BURST", "10")
	t.Setenv("HUB_RATE_PER_SECOND", "100")
	t.Setenv("HUB_MAX_INSTANCES", "1")
	t.Setenv("HUB_ARCHIVE_MAX_PENDING", "20000")
	t.Setenv("HUB_ARCHIVE_MAX_BYTES", "268435456")
	c, e = config.Load()
	if e != nil || c.ArchiveGate.MaxPending != 20000 || c.ArchiveGate.MaxBytes != 256<<20 || c.ArchiveGate.ReserveRequests != 67 {
		t.Fatalf("capacity override %+v %v", c.ArchiveGate, e)
	}
}
func TestArchiveCapacityConfigRejectsUnsafe(t *testing.T) {
	for k, v := range map[string]string{"HUB_ARCHIVE_MAX_PENDING": "10", "HUB_ARCHIVE_MAX_BYTES": "1048576", "HUB_MAX_INSTANCES": "3"} {
		t.Run(k, func(t *testing.T) {
			configEnv(t)
			t.Setenv(k, v)
			if _, e := config.Load(); e == nil {
				t.Fatalf("accepted %s=%s", k, v)
			}
		})
	}
}
