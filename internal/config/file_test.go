package config_test

import (
	"os"
	"partner-hub/internal/config"
	"path/filepath"
	"strings"
	"testing"
)

func configFile(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "hub.json")
	if e := os.WriteFile(p, []byte(body), 0600); e != nil {
		t.Fatal(e)
	}
	return p
}
func unsetConfig(t *testing.T, k string) {
	t.Helper()
	old, ok := os.LookupEnv(k)
	_ = os.Unsetenv(k)
	t.Cleanup(func() {
		if ok {
			_ = os.Setenv(k, old)
		} else {
			_ = os.Unsetenv(k)
		}
	})
}
func TestConfigFileEnvironmentPrecedenceAndNoPollution(t *testing.T) {
	configEnv(t)
	unsetConfig(t, "HUB_INSTANCE_ID")
	t.Setenv("HUB_CONFIG_FILE", configFile(t, `{"HUB_INSTANCE_ID":"from-file","HUB_LISTEN":"127.0.0.1:19999"}`))
	unsetConfig(t, "HUB_LISTEN")
	c, e := config.Load()
	if e != nil {
		t.Fatal(e)
	}
	if c.InstanceID != "from-file" || c.Listen != "127.0.0.1:19999" {
		t.Fatalf("file settings absent: instance=%s listen=%s", c.InstanceID, c.Listen)
	}
	if _, ok := os.LookupEnv("HUB_INSTANCE_ID"); ok {
		t.Fatal("file polluted process environment")
	}
	t.Setenv("HUB_INSTANCE_ID", "from-env")
	t.Setenv("HUB_LISTEN", "")
	c, e = config.Load()
	if e != nil || c.InstanceID != "from-env" || c.Listen != "127.0.0.1:18080" {
		t.Fatalf("explicit env did not override file: %+v %v", c, e)
	}
}
func TestConfigFileRejectsMalformedInput(t *testing.T) {
	for _, body := range []string{`{"TYPO":"x"}`, `{"HUB_INSTANCE_ID":"a","HUB_INSTANCE_ID":"b"}`, `{"HUB_INSTANCE_ID":7}`, `{"HUB_INSTANCE_ID":null}`, `[]`, `{} {}`, `{"HUB_CONFIG_FILE":"nested"}`, `{"HUB_INSTANCE_ID":"` + strings.Repeat("x", 65536) + `"}`} {
		t.Run(body[:min(len(body), 40)], func(t *testing.T) {
			configEnv(t)
			t.Setenv("HUB_CONFIG_FILE", configFile(t, body))
			if _, e := config.Load(); e == nil {
				t.Fatal("accepted malformed/oversized configuration")
			}
		})
	}
}
