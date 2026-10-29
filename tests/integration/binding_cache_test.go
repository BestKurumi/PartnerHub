//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"
	"partner-hub/internal/binding"
	hubmysql "partner-hub/internal/infra/mysql"
	hubredis "partner-hub/internal/infra/redis"
	"partner-hub/internal/model"
)

func bindingRedis(t *testing.T) *redisclient.Client {
	t.Helper()
	address := os.Getenv("REDIS_TEST_ADDR")
	if address == "" {
		t.Fatal("REDIS_TEST_ADDR required")
	}
	client := redisclient.NewClient(&redisclient.Options{Addr: address, DB: 1, DialTimeout: 100 * time.Millisecond, ReadTimeout: 100 * time.Millisecond, WriteTimeout: 100 * time.Millisecond, ContextTimeoutEnabled: true, MaxRetries: -1})
	t.Cleanup(func() { client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return client
}

// Catches resetting old fill TTL, treating corruption as no bindings and skipped invalidation.
func TestBindingsRedisCache(t *testing.T) {
	client := bindingRedis(t)
	cache := hubredis.NewBindings(client.Options())
	t.Cleanup(func() { cache.Close() })
	ctx := context.Background()
	key := "binding:v1:{it-cache}"
	client.Del(ctx, key)
	if _, err := cache.Get(ctx, "it-cache"); !errors.Is(err, binding.ErrCacheMiss) {
		t.Fatalf("missing cache: %v", err)
	}
	positive := model.BindingSnapshot{Version: 1, ObservedAt: time.Now().Add(-29 * time.Second), Targets: []model.BindingTarget{{PlatformID: "PLATFORM_A", PlatformUID: "A"}, {PlatformID: "PLATFORM_B", PlatformUID: "B"}}}
	if err := cache.Put(ctx, "it-cache", positive); err != nil {
		t.Fatal(err)
	}
	ttl := client.PTTL(ctx, key).Val()
	if ttl <= 0 || ttl > time.Second {
		t.Fatalf("old fill renewed TTL: %v", ttl)
	}
	got, err := cache.Get(ctx, "it-cache")
	if err != nil || len(got.Targets) != 2 {
		t.Fatalf("complete cache: %+v %v", got, err)
	}
	empty := model.BindingSnapshot{Version: 1, ObservedAt: time.Now().Add(-4 * time.Second), Targets: []model.BindingTarget{}}
	if err = cache.Put(ctx, "it-cache", empty); err != nil {
		t.Fatal(err)
	}
	if ttl = client.PTTL(ctx, key).Val(); ttl <= 0 || ttl > time.Second {
		t.Fatalf("empty TTL renewed: %v", ttl)
	}
	got, err = cache.Get(ctx, "it-cache")
	if err != nil || got.Targets == nil || len(got.Targets) != 0 {
		t.Fatalf("explicit empty: %+v %v", got, err)
	}
	for _, bad := range []string{`not json`, `{"version":1,"observedAt":"2026-01-01T00:00:00Z","targets":[]}`, `{"version":1,"targets":null}`, `{"version":99,"targets":[]}`} {
		client.Set(ctx, key, bad, time.Minute)
		if _, err = cache.Get(ctx, "it-cache"); !errors.Is(err, binding.ErrCacheMiss) {
			t.Fatalf("corrupt cache became result: %s %v", bad, err)
		}
	}
	validJSON, err := json.Marshal(model.BindingSnapshot{Version: 1, ObservedAt: time.Now(), Targets: []model.BindingTarget{{PlatformID: "PLATFORM_A", PlatformUID: "must-not-vanish"}}})
	if err != nil {
		t.Fatal(err)
	}
	duplicate := strings.TrimSuffix(string(validJSON), "}") + `,"targets":[]}`
	client.Set(ctx, key, duplicate, time.Minute)
	if _, err = cache.Get(ctx, "it-cache"); !errors.Is(err, binding.ErrCacheMiss) {
		t.Fatal("duplicate cache field became empty successful route")
	}
	for _, bad := range []string{
		strings.TrimSuffix(string(validJSON), "}") + `,"Targets":[]}`,
		strings.Replace(string(validJSON), `"platformUid"`, `"PlatformUid"`, 1),
		strings.Replace(string(validJSON), `"version"`, `"Version"`, 1),
	} {
		client.Set(ctx, key, bad, time.Minute)
		if _, err = cache.Get(ctx, "it-cache"); !errors.Is(err, binding.ErrCacheMiss) {
			t.Fatalf("case alias accepted as complete cache: %s", bad)
		}
	}
	expired := positive
	expired.ObservedAt = time.Now().Add(-31 * time.Second)
	client.Del(ctx, key)
	if err = cache.Put(ctx, "it-cache", expired); err != nil {
		t.Fatal(err)
	}
	if client.Exists(ctx, key).Val() != 0 {
		t.Fatal("expired late fill revived cache")
	}
	db := bindingDB(t)
	relation := "it-cache-binding"
	cleanBindingClient(t, db, relation)
	client.Del(ctx, "binding:v1:{"+relation+"}")
	repo := hubmysql.NewBindings(db)
	resolver := binding.NewResolver(repo, cache)
	service := binding.NewService(repo, cache)
	snapshot, err := resolver.Resolve(ctx, relation)
	if err != nil || len(snapshot.Targets) != 0 {
		t.Fatalf("initial empty: %+v %v", snapshot, err)
	}
	if _, err = service.Apply(ctx, bindingEvent(relation, 401, "BIND", "PLATFORM_A", "A401")); err != nil {
		t.Fatal(err)
	}
	snapshot, err = resolver.Resolve(ctx, relation)
	if err != nil || len(snapshot.Targets) != 1 || snapshot.Targets[0].PlatformUID != "A401" {
		t.Fatalf("cache not invalidated: %+v %v", snapshot, err)
	}
	client.Set(ctx, "binding:v1:{"+relation+"}", "corrupted", time.Minute)
	snapshot, err = resolver.Resolve(ctx, relation)
	if err != nil || len(snapshot.Targets) != 1 {
		t.Fatalf("corruption dropped relation: %+v %v", snapshot, err)
	}
}
