package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"partner-hub/internal/config"
)

func TestConsoleConfigurationRejectsUnsafeValues(t *testing.T) {
	for key, value := range map[string]string{
		"HUB_PROMETHEUS_URL":   "http://user:password@prometheus:9090",
		"HUB_GRAFANA_URL":      "javascript:alert(1)",
		"HUB_OPS_TOKEN":        "bad\ntoken",
		"HUB_DIAGNOSTIC_PEERS": "hub-2=http://example.com/a?token=secret",
	} {
		t.Run(key, func(t *testing.T) {
			configEnv(t)
			t.Setenv("HUB_OPS_TOKEN", "local-ops-test-token")
			t.Setenv(key, value)
			if _, err := config.Load(); err == nil {
				t.Fatal("accepted unsafe console configuration")
			}
		})
	}
}

func TestConsoleConfigurationFileKeys(t *testing.T) {
	configEnv(t)
	path := filepath.Join(t.TempDir(), "hub.json")
	err := os.WriteFile(path, []byte(`{"HUB_PROMETHEUS_URL":"http://prometheus:9090","HUB_GRAFANA_URL":"http://127.0.0.1:3000/d/hub-overview","HUB_OPS_TOKEN":"local-ops-test-token","HUB_DIAGNOSTIC_PEERS":"hub-2=http://hub-2:8080"}`), 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HUB_CONFIG_FILE", path)
	if _, err = config.Load(); err != nil {
		t.Fatalf("console file configuration rejected: %v", err)
	}
}

func TestConsolePeerConfigurationAndOptionalMonitoring(t *testing.T) {
	configEnv(t)
	for _, key := range []string{"HUB_CONFIG_FILE", "HUB_PROMETHEUS_URL", "HUB_GRAFANA_URL", "HUB_OPS_TOKEN", "HUB_DIAGNOSTIC_PEERS"} {
		t.Setenv(key, "")
	}
	c, err := config.Load()
	if err != nil || c.PrometheusURL != "" || len(c.DiagnosticPeers) != 0 {
		t.Fatal("standalone without monitoring must remain supported", err)
	}
	t.Setenv("HUB_OPS_TOKEN", "local-ops-test-token")
	t.Setenv("HUB_DIAGNOSTIC_PEERS", "hub-test=http://hub-test:8080,hub-2=http://hub-2:8080")
	c, err = config.Load()
	if err != nil || len(c.DiagnosticPeers) != 1 || c.DiagnosticPeers[0].Instance != "hub-2" {
		t.Fatal("self peer not excluded", err)
	}
	t.Setenv("HUB_DIAGNOSTIC_PEERS", "hub-2=http://hub-2:8080,hub-2=http://hub-2:8081")
	if _, err = config.Load(); err == nil {
		t.Fatal("duplicate peers accepted")
	}
	t.Setenv("HUB_DIAGNOSTIC_PEERS", "hub-2=http://hub-2:8080")
	t.Setenv("HUB_OPS_TOKEN", "")
	if _, err = config.Load(); err == nil {
		t.Fatal("remote peers accepted without shared token")
	}
}
