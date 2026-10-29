package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	redisclient "github.com/redis/go-redis/v9"
	"io"
	"net/http"
	bindingcache "partner-hub/internal/infra/redis"
	"partner-hub/internal/model"
	"time"
)

func demoJSON(ctx context.Context, c *http.Client, method, endpoint, token string, body any, result any) (int, error) {
	data, _ := json.Marshal(body)
	req, e := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(data))
	if e != nil {
		return 0, errors.New("invalid setup endpoint")
	}
	req.Header.Set("X-Demo-Token", token)
	req.Header.Set("Content-Type", "application/json")
	resp, e := c.Do(req)
	if e != nil {
		return 0, errors.New("setup request unavailable")
	}
	defer resp.Body.Close()
	e = json.NewDecoder(io.LimitReader(resp.Body, 8193)).Decode(result)
	return resp.StatusCode, e
}
func bind(ctx context.Context, c *http.Client, base, token, platform, client, uid string) error {
	endpoint := base + "/mock/" + platform + "/demo/binding-events"
	var publication struct{ EventID, Status string }
	code, e := demoJSON(ctx, c, "POST", endpoint, token, map[string]string{"eventType": "BIND", "clientId": client, "platformUid": uid}, &publication)
	if e != nil || code != 202 || publication.EventID == "" || publication.Status != "PUBLISHED" {
		return errors.New("binding publication failed")
	}
	for {
		var event struct{ Result string }
		code, e = demoJSON(ctx, c, "GET", endpoint+"/"+publication.EventID, token, nil, &event)
		if e != nil || (code != 200 && code != 404) {
			return errors.New("binding confirmation unavailable")
		}
		if event.Result == "APPLIED" {
			return nil
		}
		if event.Result != "PENDING" && event.Result != "" {
			return errors.New("binding not applied")
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.New("binding confirmation timeout")
		case <-timer.C:
		}
	}
}

type BindingFixture struct {
	ClientID string `json:"client_id"`
	UIDA     string `json:"uid_a"`
	UIDB     string `json:"uid_b"`
}

func prepare(ctx context.Context, c *http.Client, base, token, redisAddr, redisPassword string, rows []Input, cold bool) ([]BindingFixture, error) {
	cache := bindingcache.NewBindings(&redisclient.Options{Addr: redisAddr, Password: redisPassword})
	defer cache.Close()
	raw := redisclient.NewClient(&redisclient.Options{Addr: redisAddr, Password: redisPassword, PoolSize: 1, MaxRetries: -1, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, ContextTimeoutEnabled: true})
	defer raw.Close()
	fixtures := []BindingFixture{}
	for i := range rows {
		if i > 0 && !cold {
			continue
		}
		f := BindingFixture{rows[i].ClientID, "A-" + model.NewID(), "B-" + model.NewID()}
		for _, p := range []struct{ path, uid string }{{"p1", f.UIDA}, {"p2", f.UIDB}} {
			if e := bind(ctx, c, base, token, p.path, f.ClientID, p.uid); e != nil {
				return fixtures, e
			}
		}
		fixtures = append(fixtures, f)
		if cold {
			if e := raw.Del(ctx, "binding:v1:{"+f.ClientID+"}").Err(); e != nil {
				return fixtures, errors.New("run-owned cache deletion failed")
			}
		} else {
			// Only cache the two relationships just confirmed APPLIED by authority.
			snapshot := model.BindingSnapshot{Version: 1, ObservedAt: time.Now().UTC(), Targets: []model.BindingTarget{{PlatformID: "PLATFORM_A", PlatformUID: f.UIDA}, {PlatformID: "PLATFORM_B", PlatformUID: f.UIDB}}}
			if e := cache.Put(ctx, f.ClientID, snapshot); e != nil {
				return fixtures, errors.New("run-owned warm cache setup failed")
			}
		}
	}
	return fixtures, nil
}
