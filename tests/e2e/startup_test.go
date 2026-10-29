//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
)

const startupProject = "ph-startup-3456ec71"

func startupCompose(t *testing.T, args ...string) {
	t.Helper()
	if os.Getenv("COMPOSE_PROJECT_NAME") != startupProject || os.Getenv("STARTUP_E2E_AUTHORIZED") != startupProject {
		t.Fatal("isolated startup project authorization required")
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	expected := filepath.Join(root, ".local", "startup.env")
	if os.Getenv("STARTUP_ENV_FILE") != expected {
		t.Fatal("startup test only uses repository .local/startup.env")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	argv := append([]string{"compose", "--env-file", expected, "-p", startupProject}, args...)
	cmd := exec.CommandContext(ctx, "docker", argv...)
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		// Compose progress can include diagnostics, so report only operation names and
		// status; never a rendered environment or subprocess output containing DSNs.
		_ = out
		t.Fatalf("isolated compose %s failed: %v", args[0], err)
	}
}

func startupHealth(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	wantChecks := []string{"config", "adapters", "schema", "mysql", "redis", "kafka", "archiveCapacity", "handoffCapacity"}
	seen := map[string]bool{}
	for ctx.Err() == nil {
		all := true
		for _, path := range []string{"/readyz", "/readyz", "/livez", "/livez"} {
			req, err := http.NewRequestWithContext(ctx, "GET", required(t, "HUB_TEST_URL")+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
			if err != nil {
				all = false
				continue
			}
			var body struct {
				InstanceID, Status string
				Checks             map[string]string
			}
			err = json.NewDecoder(resp.Body).Decode(&body)
			resp.Body.Close()
			if err != nil || resp.StatusCode != 200 {
				all = false
				continue
			}
			if body.InstanceID != "hub-1" && body.InstanceID != "hub-2" {
				t.Fatalf("unknown instanceId %q", body.InstanceID)
			}
			if path == "/livez" {
				if body.Status != "LIVE" || len(body.Checks) != 0 {
					t.Fatal("liveness must remain separate from dependency readiness")
				}
			} else {
				if body.Status != "READY" || len(body.Checks) != len(wantChecks) {
					all = false
					continue
				}
				valid := true
				for _, name := range wantChecks {
					if body.Checks[name] != "READY" {
						valid = false
					}
				}
				if !valid {
					all = false
					continue
				}
			}
			seen[path+body.InstanceID] = true
		}
		if all && len(seen) == 4 {
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatal("both actual hub instances did not expose LIVE and eight READY checks within deadline")
}

func startupDB(t *testing.T, user, password, schema string) *sql.DB {
	t.Helper()
	c := driver.NewConfig()
	c.User = user
	c.Passwd = required(t, password)
	c.DBName = schema
	c.Net = "tcp"
	c.Addr = "127.0.0.1:" + required(t, "MYSQL_HOST_PORT")
	c.ParseTime = true
	c.Timeout = time.Second
	c.ReadTimeout = 2 * time.Second
	c.WriteTimeout = 2 * time.Second
	db, err := sql.Open("mysql", c.FormatDSN())
	if err != nil {
		t.Fatal("invalid isolated MySQL configuration")
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if db.PingContext(ctx) != nil {
		t.Fatalf("isolated %s account unavailable", user)
	}
	return db
}

func startupFingerprint(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatal("read isolated retention facts failed")
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var encoded []string
	for rows.Next() {
		values := make([]any, len(cols))
		dest := make([]any, len(cols))
		for i := range values {
			dest[i] = &values[i]
		}
		if rows.Scan(dest...) != nil {
			t.Fatal("scan retention facts failed")
		}
		data, err := json.Marshal(values)
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded, string(data))
	}
	if rows.Err() != nil {
		t.Fatal("read retention facts failed")
	}
	sort.Strings(encoded)
	digest := sha256.Sum256([]byte(strings.Join(encoded, "\n")))
	return fmt.Sprintf("%d:%s", len(encoded), hex.EncodeToString(digest[:]))
}

func startupVolumeNames(t *testing.T) map[string]string {
	t.Helper()
	names := map[string]string{}
	for _, service := range []string{"mysql", "redis", "kafka"} {
		cmd := exec.Command("docker", "compose", "--env-file", os.Getenv("STARTUP_ENV_FILE"), "-p", startupProject, "ps", "-q", service)
		cmd.Dir = "../.."
		raw, err := cmd.Output()
		if err != nil {
			t.Fatal("read isolated service id failed")
		}
		id := strings.TrimSpace(string(raw))
		if id == "" {
			t.Fatal("isolated dependency is not running")
		}
		raw, err = exec.Command("docker", "inspect", "--format", "{{json .Mounts}}", id).Output()
		if err != nil {
			t.Fatal("read isolated mount names failed")
		}
		var mounts []struct{ Type, Name, Destination string }
		if json.Unmarshal(raw, &mounts) != nil {
			t.Fatal("invalid mount description")
		}
		for _, m := range mounts {
			expectedPath := map[string]string{"mysql": "/var/lib/mysql", "redis": "/data", "kafka": "/var/lib/kafka/data"}[service]
			if m.Type == "volume" && m.Destination == expectedPath {
				if !strings.HasPrefix(m.Name, startupProject+"_") {
					t.Fatal("dependency volume belongs to another project")
				}
				names[service] = m.Name
			}
		}
		if names[service] == "" {
			t.Fatal("isolated dependency missing named volume")
		}
	}
	return names
}

func TestStartupProxyRecreation(t *testing.T) {
	startupCompose(t, "up", "-d", "--no-build", "--no-deps", "--force-recreate", "--wait", "--wait-timeout", "60", "mock-platform1", "mock-platform2")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		ready := true
		for _, path := range []string{"/mock/p1/", "/mock/p2/"} {
			req, err := http.NewRequestWithContext(ctx, "GET", required(t, "HUB_TEST_URL")+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
			if err != nil {
				ready = false
				continue
			}
			data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			if err != nil || resp.StatusCode != 200 || !bytes.Contains(bytes.ToLower(data), []byte("<!doctype html>")) {
				ready = false
			}
		}
		if ready {
			t.Log("both platform pages recovered through unchanged Nginx after Mock container recreation")
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatal("platform pages did not recover through unchanged Nginx within 20s after Mock recreation")
}

func TestStartup(t *testing.T) {
	if os.Getenv("STARTUP_E2E_AUTHORIZED") != startupProject || os.Getenv("COMPOSE_PROJECT_NAME") != startupProject {
		t.Fatal("run via scripts/check-startup.sh against the authorized isolated project")
	}
	origin := required(t, "HUB_TEST_URL")
	for _, name := range []string{"PLATFORM1_TEST_URL", "PLATFORM2_TEST_URL", "UPSTREAM_TEST_URL"} {
		if required(t, name) != origin {
			t.Fatal("all pages and APIs must use the single Nginx origin")
		}
	}
	startupHealth(t)
	beforeVolumes := startupVolumeNames(t)
	t.Run("single-origin-normal-flow", TestDemoNormalFlow)
	if t.Failed() {
		return
	}
	hub := startupDB(t, "hub", "MYSQL_PASSWORD", "partner_hub")
	a := startupDB(t, "mock_p1", "PLATFORM1_DB_PASSWORD", "mock_platform1")
	b := startupDB(t, "mock_p2", "PLATFORM2_DB_PASSWORD", "mock_platform2")
	for _, identity := range []struct {
		db  *sql.DB
		own string
	}{{hub, "partner_hub"}, {a, "mock_platform1"}, {b, "mock_platform2"}} {
		var current string
		if identity.db.QueryRow("SELECT DATABASE()").Scan(&current) != nil || current != identity.own {
			t.Fatal("wrong database identity")
		}
		for _, other := range []string{"partner_hub", "mock_platform1", "mock_platform2"} {
			if other == identity.own {
				continue
			}
			_, err := identity.db.Exec("SELECT version FROM " + other + ".hub_schema_migration LIMIT 1")
			var denied *driver.MySQLError
			if !errors.As(err, &denied) || (denied.Number != 1044 && denied.Number != 1142) {
				t.Fatal("identity could access another schema or isolation denial was not explicit")
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	code, body := demoRequest(t, ctx, "POST", origin+"/mock/p2/demo/consumer", map[string]bool{"paused": true})
	var control struct{ Paused bool }
	if code != 200 || json.Unmarshal(body, &control) != nil || !control.Paused {
		t.Fatal("platform B pause not confirmed")
	}
	startupHealth(t)
	code, body = demoRequest(t, ctx, "GET", origin+"/mock/p2/demo/consumer", nil)
	if code != 200 || json.Unmarshal(body, &control) != nil || !control.Paused {
		t.Fatal("platform B did not remain paused during health checks")
	}
	code, _ = demoRequest(t, ctx, "POST", origin+"/mock/p2/demo/consumer", map[string]bool{"paused": false})
	if code != 200 {
		t.Fatal("platform B resume failed")
	}
	type fact struct {
		name          string
		db            *sql.DB
		query, before string
	}
	facts := []fact{
		{name: "bindings", db: hub, query: "SELECT client_id,platform_id,platform_uid,status,revision FROM binding_current"},
		{name: "messages", db: hub, query: "SELECT msg_id,client_id,trace_id,first_status,route_status,route_snapshot FROM trade_message"},
		{name: "platform-A-receipts", db: a, query: "SELECT receive_id,user_uid,trace_id,payload,received_at FROM received_notification"},
		{name: "platform-B-receipts", db: b, query: "SELECT receive_id,source_topic,source_partition,source_offset,user_uid,trace_id,payload,received_at FROM received_notification"},
	}
	for _, v := range []struct {
		name  string
		db    *sql.DB
		count int
	}{{"hub-migrations", hub, 7}, {"platform-A-migrations", a, 1}, {"platform-B-migrations", b, 1}} {
		var n int
		if v.db.QueryRow("SELECT COUNT(*) FROM hub_schema_migration").Scan(&n) != nil || n != v.count {
			t.Fatalf("%s version count mismatch", v.name)
		}
		facts = append(facts, fact{name: v.name, db: v.db, query: "SELECT version,checksum,applied_at FROM hub_schema_migration"})
	}
	for i := range facts {
		facts[i].before = startupFingerprint(t, facts[i].db, facts[i].query)
		if strings.HasPrefix(facts[i].before, "0:") {
			t.Fatalf("retention evidence empty: %s", facts[i].name)
		}
	}
	startupCompose(t, "restart", "--timeout", "30", "mysql", "redis", "kafka", "hub-1", "hub-2", "mock-platform1", "mock-platform2", "mock-upstream")
	startupCompose(t, "up", "-d", "--no-build", "--wait", "--wait-timeout", "180", "mock-upstream", "mock-platform2")
	startupHealth(t)
	afterVolumes := startupVolumeNames(t)
	for service, name := range beforeVolumes {
		if afterVolumes[service] != name {
			t.Fatalf("dependency volume changed after restart: %s", service)
		}
	}
	// Force a second execution of the real one-shot migrator against retained data.
	startupCompose(t, "run", "--rm", "--no-deps", "migrate")
	for _, v := range facts {
		after := startupFingerprint(t, v.db, v.query)
		if after != v.before {
			t.Fatalf("retained data/version changed after restart and migration replay: %s", v.name)
		}
		t.Logf("retained %s rows/digest %s", v.name, after)
	}
	t.Run("mock-proxy-recreation", TestStartupProxyRecreation)
	if t.Failed() {
		return
	}
	t.Run("post-restart-single-origin-normal-flow", TestDemoNormalFlow)
	if t.Failed() {
		return
	}
	t.Log("two distinct live/ready hubs; one-origin normal flow; cross-schema access denied; B pause leaves liveness intact; same-volume application/dependency restart and one-shot migration replay preserve facts")
}
