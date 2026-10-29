//go:build e2e

package e2e_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// This checks the rendered deployment, not an alternative test-only deployment.
// Runtime empty-volume/restart evidence belongs to TestStartup's live exercise.
func TestStartupDeploymentContract(t *testing.T) {
	cmd := exec.Command("docker", "compose", "--env-file", ".env.example", "config", "--format", "json")
	cmd.Dir = "../.."
	raw, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("render compose: %v: %s", err, raw)
	}
	var cfg struct {
		Services map[string]struct {
			Image       string
			Environment map[string]string
			Ports       []struct {
				HostIP string `json:"host_ip"`
			}
			DependsOn   map[string]struct{ Condition string } `json:"depends_on"`
			Healthcheck struct{ Test []string }
		}
	}
	if err = json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	for _, service := range []string{"hub-1", "hub-2", "mock-platform1", "mock-platform2", "mock-upstream", "migrate", "mysql", "redis", "kafka", "nginx", "prometheus", "grafana"} {
		if _, ok := cfg.Services[service]; !ok {
			t.Fatalf("missing service %s", service)
		}
	}
	if cfg.Services["hub-1"].Image != cfg.Services["hub-2"].Image {
		t.Fatal("hub replicas must use same image")
	}
	for _, name := range []string{"hub-1", "hub-2"} {
		s := cfg.Services[name]
		if s.Environment["HUB_INSTANCE_ID"] != name {
			t.Fatalf("wrong instance id for %s", name)
		}
		if len(s.Ports) != 0 {
			t.Fatalf("hub must be reached through nginx: %s", name)
		}
		if s.DependsOn["migrate"].Condition != "service_completed_successfully" {
			t.Fatalf("migration must precede %s", name)
		}
	}
	for name, s := range cfg.Services {
		for _, p := range s.Ports {
			if p.HostIP != "127.0.0.1" {
				t.Fatalf("non-loopback published port: %s %s", name, p.HostIP)
			}
		}
	}
	for _, name := range []string{"mock-platform1", "mock-platform2", "mock-upstream"} {
		s := cfg.Services[name]
		if s.Environment["DEMO_ALLOW_CONTAINER_LISTEN"] != "true" || s.Environment["DEMO_TRUSTED_CIDRS"] == "" {
			t.Fatalf("container demo trust not explicit: %s", name)
		}
	}
	nginx, err := os.ReadFile("../../deploy/nginx/nginx.conf")
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"server hub-1:8080", "server hub-2:8080", "proxy_next_upstream off;", "client_max_body_size 64k;", "proxy_read_timeout", "client_header_timeout", "limit_conn connections 128;"} {
		if !strings.Contains(string(nginx), text) {
			t.Fatalf("missing proxy boundary %q", text)
		}
	}
	dockerfile, err := os.ReadFile("../../Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"./cmd/migrate", "./cmd/healthprobe", "COPY web/ /app/web/", "COPY deploy/mysql/ /app/deploy/mysql/"} {
		if !strings.Contains(string(dockerfile), text) {
			t.Fatalf("runtime artifact missing %q", text)
		}
	}
}
