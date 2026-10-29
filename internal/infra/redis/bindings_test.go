package redis_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"
	hubredis "partner-hub/internal/infra/redis"
	"partner-hub/internal/model"
)

// A connected peer that never responds must not stall routing or invalidation
// for the Redis library's default seconds-long socket timeout.
func TestBindingsUnresponsivePeerBudget(t *testing.T) {
	for _, operation := range []string{"get", "put", "invalidate"} {
		t.Run(operation, func(t *testing.T) {
			options := &redisclient.Options{ReadTimeout: 500 * time.Millisecond, MaxRetries: -1,
				Dialer: func(ctx context.Context, network, address string) (net.Conn, error) {
					client, server := net.Pipe()
					go func() { defer server.Close(); io.Copy(io.Discard, server) }()
					return client, nil
				}}
			cache := hubredis.NewBindings(options)
			t.Cleanup(func() { cache.Close() })
			started := time.Now()
			var err error
			switch operation {
			case "get":
				_, err = cache.Get(context.Background(), "blocked")
			case "put":
				err = cache.Put(context.Background(), "blocked", model.BindingSnapshot{Version: 1, ObservedAt: time.Now(), Targets: []model.BindingTarget{}})
			case "invalidate":
				err = cache.Invalidate(context.Background(), "blocked")
			}
			if elapsed := time.Since(started); err == nil || elapsed > 250*time.Millisecond {
				t.Fatalf("unresponsive Redis exceeded 100ms budget (150ms scheduling tolerance): %v, err=%v", elapsed, err)
			}
		})
	}
}
