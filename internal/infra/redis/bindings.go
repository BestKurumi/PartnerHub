package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"partner-hub/internal/binding"
	"partner-hub/internal/model"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

type Bindings struct{ client *redisclient.Client }

// NewBindings owns its Redis client and enforces the cache's whole-operation
// budget, independent of go-redis's default context and retry settings.
func NewBindings(options *redisclient.Options) *Bindings {
	config := *options
	config.ContextTimeoutEnabled = true
	config.DialTimeout = 100 * time.Millisecond
	config.ReadTimeout = 100 * time.Millisecond
	config.WriteTimeout = 100 * time.Millisecond
	config.PoolTimeout = 100 * time.Millisecond
	config.MaxRetries = -1
	return &Bindings{client: redisclient.NewClient(&config)}
}

func (b *Bindings) Close() error { return b.client.Close() }
func (b *Bindings) Get(ctx context.Context, clientID string) (model.BindingSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	body, err := b.client.Get(ctx, bindingKey(clientID)).Bytes()
	if err != nil || len(body) > 8192 {
		return model.BindingSnapshot{}, binding.ErrCacheMiss
	}
	if !uniqueJSONKeys(body) {
		return model.BindingSnapshot{}, binding.ErrCacheMiss
	}
	var snapshot model.BindingSnapshot
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&snapshot) != nil || !binding.ValidSnapshot(snapshot, time.Now()) {
		return model.BindingSnapshot{}, binding.ErrCacheMiss
	}
	if _, err = d.Token(); err != io.EOF {
		return model.BindingSnapshot{}, binding.ErrCacheMiss
	}
	return snapshot, nil
}
func (b *Bindings) Put(ctx context.Context, clientID string, snapshot model.BindingSnapshot) error {
	now := time.Now()
	if !snapshot.ObservedAt.Add(binding.SnapshotTTL(snapshot)).After(now) {
		return nil
	}
	if !binding.ValidSnapshot(snapshot, now) {
		return binding.ErrCacheMiss
	}
	body, err := json.Marshal(snapshot)
	if err != nil {
		return binding.ErrCacheMiss
	}
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	return b.client.SetArgs(ctx, bindingKey(clientID), body, redisclient.SetArgs{ExpireAt: snapshot.ObservedAt.Add(binding.SnapshotTTL(snapshot))}).Err()
}
func (b *Bindings) Invalidate(ctx context.Context, clientID string) error {
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	return b.client.Del(ctx, bindingKey(clientID)).Err()
}
func bindingKey(id string) string { return "binding:v1:{" + id + "}" }

// Struct decoding accepts duplicate keys and case aliases. Require exact keys
// at both object levels so corruption cannot erase or change a route.
func uniqueJSONKeys(body []byte) bool {
	d := json.NewDecoder(bytes.NewReader(body))
	var value func(int) bool
	value = func(level int) bool {
		token, err := d.Token()
		if err != nil {
			return false
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return true
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return false
				}
				name, ok := key.(string)
				allowed := level == 0 && (name == "version" || name == "observedAt" || name == "targets") || level == 1 && (name == "platformId" || name == "platformUid")
				if !ok || seen[name] || !allowed {
					return false
				}
				seen[name] = true
				if !value(level + 1) {
					return false
				}
			}
			token, err = d.Token()
			return err == nil && token == json.Delim('}')
		case '[':
			for d.More() {
				if !value(level) {
					return false
				}
			}
			token, err = d.Token()
			return err == nil && token == json.Delim(']')
		default:
			return false
		}
	}
	if !value(0) {
		return false
	}
	_, err := d.Token()
	return err == io.EOF
}
