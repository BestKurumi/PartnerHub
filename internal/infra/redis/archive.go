package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	redisclient "github.com/redis/go-redis/v9"
	"partner-hub/internal/infra"
	"partner-hub/internal/model"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ArchiveStore interface {
	Save(context.Context, []model.FirstRoundSnapshot) error
}
type ArchiveConfig struct {
	Namespace  string
	Owner      string
	MaxPending int64
	MaxBytes   int64
}

func NewArchiveWithConfig(options *redisclient.Options, store ArchiveStore, config ArchiveConfig) (*Archive, error) {
	if options == nil || store == nil || strings.ContainsAny(config.Namespace, "*?[]{} \t\r\n") {
		return nil, errors.New("invalid archive configuration")
	}
	if config.MaxPending == 0 {
		config.MaxPending = 10000
	}
	if config.MaxBytes == 0 {
		config.MaxBytes = 128 * 1024 * 1024
	}
	if config.MaxPending < 1 || config.MaxBytes < 256*1024 || len(config.Namespace) > 128 || len(config.Owner) > 128 {
		return nil, errors.New("invalid archive limits")
	}
	if config.Owner == "" {
		config.Owner = model.NewID()
	}
	optionsCopy := *options
	optionsCopy.ContextTimeoutEnabled = true
	optionsCopy.DialTimeout = 100 * time.Millisecond
	optionsCopy.ReadTimeout = 100 * time.Millisecond
	optionsCopy.WriteTimeout = 100 * time.Millisecond
	optionsCopy.PoolTimeout = 100 * time.Millisecond
	optionsCopy.MaxRetries = -1
	prefix := "archive:"
	if config.Namespace != "" {
		prefix = config.Namespace + ":" + prefix
	}
	return &Archive{client: redisclient.NewClient(&optionsCopy), store: store, config: config, prefix: prefix, flush: make(chan struct{}, 1)}, nil
}

func (a *Archive) Capacity(ctx context.Context) (infra.ArchiveCapacity, error) {
	sample := infra.ArchiveCapacity{SampledAt: time.Now()}
	check, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	pipe := a.client.Pipeline()
	accounting := pipe.Eval(check, `return {redis.call('ZCARD',KEYS[1]),redis.call('HLEN',KEYS[2]),redis.call('GET',KEYS[3]) or '0'}`, []string{a.prefix + "pending", a.prefix + "sizes", a.prefix + "bytes"})
	memory := pipe.Info(check, "memory")
	persistence := pipe.Info(check, "persistence")
	policy := pipe.ConfigGet(check, "maxmemory-policy")
	_, err := pipe.Exec(check)
	if err != nil && err != redisclient.Nil {
		return sample, err
	}
	if accounting.Err() != nil || memory.Err() != nil || persistence.Err() != nil || policy.Err() != nil {
		return sample, errors.New("archive capacity unavailable")
	}
	mem := redisInfo(memory.Val())
	durable := redisInfo(persistence.Val())
	counts, err := accounting.Slice()
	if err != nil || len(counts) != 3 {
		return sample, errors.New("archive capacity accounting invalid")
	}
	pending, okPending := counts[0].(int64)
	sizes, okSizes := counts[1].(int64)
	rawBytes, okBytes := counts[2].(string)
	known, err := strconv.ParseInt(rawBytes, 10, 64)
	if !okPending || !okSizes || !okBytes || err != nil || known < 0 || sizes > pending {
		return sample, errors.New("archive capacity accounting invalid")
	}
	sample.Pending = pending
	sample.Bytes = known + (pending-sizes)*256*1024
	sample.UsedMemory, _ = strconv.ParseInt(mem["used_memory"], 10, 64)
	sample.MaxMemory, _ = strconv.ParseInt(mem["maxmemory"], 10, 64)
	if sample.UsedMemory <= 0 || sample.MaxMemory <= 0 || policy.Val()["maxmemory-policy"] != "noeviction" || durable["aof_enabled"] != "1" || durable["aof_last_write_status"] != "ok" {
		return sample, errors.New("archive Redis durability configuration unsafe")
	}
	sample.Durable = true
	return sample, nil
}

// Backlog repairs at most 100 legacy received-time entries per sample. Lease
// scheduling scores are never interpreted as original ingress timestamps.
func (a *Archive) Backlog(ctx context.Context) (infra.ArchiveCapacity, time.Duration, error) {
	a.observeMu.Lock()
	defer a.observeMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	c, err := a.Capacity(ctx)
	if err != nil {
		return c, 0, err
	}
	indexedCount, err := a.client.ZCard(ctx, a.prefix+"received").Result()
	if err != nil {
		return c, 0, err
	}
	if indexedCount != c.Pending {
		// Rank pages are strictly bounded, including compact zsets where SCAN
		// COUNT is only a hint and can return the entire encoding at once.
		ids, err := a.client.ZRange(ctx, a.prefix+"pending", int64(a.repairCursor), int64(a.repairCursor)+99).Result()
		if err != nil {
			return c, 0, err
		}
		a.repairCursor += uint64(len(ids))
		if a.repairCursor >= uint64(c.Pending) {
			a.repairCursor = 0
		}
		entries := make([]string, 0, 2*len(ids))
		for _, id := range ids {
			entries = append(entries, id, "")
		}
		pipe := a.client.Pipeline()
		bodies := make([]*redisclient.StringCmd, 0, len(entries)/2)
		for i := 0; i+1 < len(entries); i += 2 {
			bodies = append(bodies, pipe.Get(ctx, a.key(entries[i])))
		}
		if len(bodies) > 0 {
			if _, err = pipe.Exec(ctx); err != nil && err != redisclient.Nil {
				return c, 0, err
			}
		}
		repair := a.client.Pipeline()
		for i, b := range bodies {
			if b.Err() != nil {
				return c, 0, errors.New("pending archive observation missing")
			}
			var snapshot model.FirstRoundSnapshot
			if json.Unmarshal([]byte(b.Val()), &snapshot) != nil || snapshot.Trade.MsgID != entries[2*i] || snapshot.ReceivedAt.IsZero() {
				return c, 0, errors.New("pending archive observation invalid")
			}
			repair.Eval(ctx, `if redis.call('ZSCORE',KEYS[1],ARGV[1]) and redis.call('GET',KEYS[2])==ARGV[2] then return redis.call('ZADD',KEYS[3],'NX',ARGV[3],ARGV[1]) end return 0`, []string{a.prefix + "pending", a.key(entries[2*i]), a.prefix + "received"}, entries[2*i], b.Val(), snapshot.ReceivedAt.UnixMilli())
		}
		if len(bodies) > 0 {
			if _, err = repair.Exec(ctx); err != nil {
				return c, 0, err
			}
		}
	}
	values, err := a.client.Eval(ctx, `local t=redis.call('TIME');local now=t[1]*1000+math.floor(t[2]/1000);local oldest=redis.call('ZRANGE',KEYS[2],0,0,'WITHSCORES');return {redis.call('ZCARD',KEYS[1]),redis.call('ZCARD',KEYS[2]),oldest[2] or '0',tostring(now)}`, []string{a.prefix + "pending", a.prefix + "received"}).Slice()
	if err != nil || len(values) != 4 {
		return c, 0, errors.New("archive age observation unavailable")
	}
	pending, ok1 := values[0].(int64)
	indexed, ok2 := values[1].(int64)
	oldest, e1 := strconv.ParseFloat(fmt.Sprint(values[2]), 64)
	now, e2 := strconv.ParseFloat(fmt.Sprint(values[3]), 64)
	if !ok1 || !ok2 || e1 != nil || e2 != nil || pending != indexed || pending != c.Pending {
		return c, 0, errors.New("archive received-time index incomplete; bounded repair continues")
	}
	if pending == 0 {
		return c, 0, nil
	}
	return c, time.Duration(max(0, now-oldest)) * time.Millisecond, nil
}

func redisInfo(raw string) map[string]string {
	result := map[string]string{}
	for _, line := range strings.Split(raw, "\n") {
		parts := strings.SplitN(strings.TrimSpace(line), ":", 2)
		if len(parts) == 2 {
			result[parts[0]] = parts[1]
		}
	}
	return result
}

type Archive struct {
	client         *redisclient.Client
	store          ArchiveStore
	flush          chan struct{}
	config         ArchiveConfig
	prefix         string
	observeMu      sync.Mutex
	repairCursor   uint64
	ObserveFlush   func(string, int)
	ObserveVisible func(time.Time)
}

func NewArchive(options *redisclient.Options, store ArchiveStore) *Archive {
	archive, err := NewArchiveWithConfig(options, store, ArchiveConfig{})
	if err != nil {
		panic(err)
	}
	return archive
}
func (a *Archive) Close() error { return a.client.Close() }

// Startup checks durability without preventing a full backlog from being drained.
func (a *Archive) Preflight(ctx context.Context) error { _, err := a.Capacity(ctx); return err }
func (a *Archive) key(id string) string                { return a.prefix + "message:{" + id + "}" }

var stageScript = redisclient.NewScript(`
local old = redis.call('GET',KEYS[1])
if old and old ~= ARGV[2] then return redis.error_reply('snapshot conflict') end
local count=redis.call('ZCARD',KEYS[2])
local known=tonumber(redis.call('GET',KEYS[4]) or '0')
local accounted=redis.call('HLEN',KEYS[3])
local size=redis.call('HGET',KEYS[3],ARGV[1])
if not old then
 if count >= tonumber(ARGV[4]) or known+(count-accounted)*262144+string.len(ARGV[2]) > tonumber(ARGV[5]) then return redis.error_reply('archive full') end
 redis.call('SET',KEYS[1],ARGV[2])
end
if not size then redis.call('HSET',KEYS[3],ARGV[1],string.len(ARGV[2])); redis.call('INCRBY',KEYS[4],string.len(ARGV[2])) end
local t=redis.call('TIME'); local now=t[1]*1000+math.floor(t[2]/1000)
redis.call('ZADD',KEYS[2],'NX',now,ARGV[1])
redis.call('ZADD',KEYS[5],'NX',ARGV[3],ARGV[1])
return 1`)

func (a *Archive) Stage(ctx context.Context, snapshot model.FirstRoundSnapshot) error {
	if err := infra.ValidateNormalSnapshot(snapshot); err != nil {
		return err
	}
	body, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	if len(body) > 256*1024 {
		return errors.New("archive snapshot too large")
	}
	// Confirm the same immutable value after an uncertain result, never resend.
	var last error
	for i := 0; i < 2; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		stage, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		last = stageScript.Run(stage, a.client, []string{a.key(snapshot.Trade.MsgID), a.prefix + "pending", a.prefix + "sizes", a.prefix + "bytes", a.prefix + "received"}, snapshot.Trade.MsgID, body, snapshot.ReceivedAt.UnixMilli(), a.config.MaxPending, a.config.MaxBytes).Err()
		cancel()
		if last == nil {
			return nil
		}
	}
	return last
}

// Redis TIME is the authority for both claim and acknowledgment. Tokens are
// unique per claim even if a completed message is staged again later.
var claimScript = redisclient.NewScript(`
local t=redis.call('TIME');local now=t[1]*1000+math.floor(t[2]/1000)
local ids=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',now,'LIMIT',0,100)
local selected={};local total=0
for _,id in ipairs(ids) do
 local body=redis.call('GET',ARGV[1]..'message:{'..id..'}')
 if not body then return redis.error_reply('pending snapshot missing') end
 if string.len(body)>262144 then return redis.error_reply('oversize pending archive') end
 if total+string.len(body)>4194304 then break end
 local ok,snapshot=pcall(cjson.decode,body)
 if not ok or type(snapshot.archiveRevision)~='string' or snapshot.archiveRevision=='' then return redis.error_reply('invalid pending revision') end
 local lease=ARGV[1]..'lease:{'..id..'}'
 local expires=tonumber(redis.call('HGET',lease,'expires') or '0')
 if expires<=now then
  table.insert(selected,{id,body,snapshot.archiveRevision,lease});total=total+string.len(body)
 end
end
local result={}
for _,item in ipairs(selected) do
 local version=redis.call('HINCRBY',item[4],'version',1)
 redis.call('HSET',item[4],'owner',ARGV[2],'token',ARGV[3],'revision',item[3],'expires',now+15000)
 redis.call('ZADD',KEYS[1],now+15000,item[1])
 table.insert(result,item[1]);table.insert(result,item[2]);table.insert(result,item[3]);table.insert(result,tostring(version))
end
return result`)

const ackScriptSource = `
local t=redis.call('TIME');local now=t[1]*1000+math.floor(t[2]/1000)
if redis.call('HGET',KEYS[5],'owner')~=ARGV[3] or redis.call('HGET',KEYS[5],'token')~=ARGV[4] or redis.call('HGET',KEYS[5],'revision')~=ARGV[5] or redis.call('HGET',KEYS[5],'version')~=ARGV[6] or tonumber(redis.call('HGET',KEYS[5],'expires') or '0')<=now then return 0 end
if redis.call('GET',KEYS[1])~=ARGV[2] then return 0 end
redis.call('DEL',KEYS[1],KEYS[5])
redis.call('ZREM',KEYS[2],ARGV[1])
if KEYS[6] then redis.call('ZREM',KEYS[6],ARGV[1]) end
local size=redis.call('HGET',KEYS[3],ARGV[1]);if size then redis.call('DECRBY',KEYS[4],size);redis.call('HDEL',KEYS[3],ARGV[1]) end
return 1`

const releaseScriptSource = `
if redis.call('HGET',KEYS[2],'owner')~=ARGV[2] or redis.call('HGET',KEYS[2],'token')~=ARGV[3] or redis.call('HGET',KEYS[2],'revision')~=ARGV[4] or redis.call('HGET',KEYS[2],'version')~=ARGV[5] then return 0 end
local t=redis.call('TIME');local now=t[1]*1000+math.floor(t[2]/1000)
redis.call('HSET',KEYS[2],'expires',0,'owner','','token','')
redis.call('ZADD',KEYS[1],now,ARGV[1]);return 1`

type archiveClaim struct{ id, body, revision, version string }

func (a *Archive) Flush(ctx context.Context) (flushErr error) {
	count := 0
	flushOutcome := "error"
	defer func() {
		if a.ObserveFlush != nil {
			if flushErr == nil {
				if count == 0 {
					flushOutcome = "empty"
				} else {
					flushOutcome = "success"
				}
			}
			a.ObserveFlush(flushOutcome, count)
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case a.flush <- struct{}{}:
		defer func() { <-a.flush }()
	case <-ctx.Done():
		return ctx.Err()
	}
	claimCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	token := model.NewID()
	values, err := claimScript.Run(claimCtx, a.client, []string{a.prefix + "pending"}, a.prefix, a.config.Owner, token).StringSlice()
	cancel()
	if err != nil {
		return err
	}
	if len(values) == 0 {
		return nil
	}
	if len(values)%4 != 0 {
		return errors.New("invalid archive claim response")
	}
	count = len(values) / 4
	claims := make([]archiveClaim, 0, len(values)/4)
	for i := 0; i < len(values); i += 4 {
		claims = append(claims, archiveClaim{values[i], values[i+1], values[i+2], values[i+3]})
	}
	// A failed or uncertain commit retains the complete snapshot and can replay.
	// Cleanup gets one independent bounded budget even when the work was canceled.
	defer func() {
		release, cancel := context.WithTimeout(context.WithoutCancel(ctx), 100*time.Millisecond)
		defer cancel()
		pipe := a.client.Pipeline()
		for _, item := range claims {
			pipe.Eval(release, releaseScriptSource, []string{a.prefix + "pending", a.prefix + "lease:{" + item.id + "}"}, item.id, a.config.Owner, token, item.revision, item.version)
		}
		_, _ = pipe.Exec(release)
	}()
	snapshots := make([]model.FirstRoundSnapshot, 0, len(claims))
	for _, item := range claims {
		var snapshot model.FirstRoundSnapshot
		if err = json.Unmarshal([]byte(item.body), &snapshot); err != nil {
			return err
		}
		if snapshot.Trade.MsgID != item.id || snapshot.ArchiveRevision != item.revision {
			return errors.New("pending archive identity mismatch")
		}
		if err = infra.ValidateNormalSnapshot(snapshot); err != nil {
			return err
		}
		snapshots = append(snapshots, snapshot)
	}
	write, cancel := context.WithTimeout(ctx, 2*time.Second)
	err = a.store.Save(write, snapshots)
	cancel()
	if err != nil {
		return err
	}
	if a.ObserveVisible != nil {
		for _, s := range snapshots {
			a.ObserveVisible(s.ReceivedAt)
		}
	}
	ack, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	pipe := a.client.Pipeline()
	results := make([]*redisclient.Cmd, 0, len(claims))
	// EVAL in one bounded pipeline avoids one network round-trip per snapshot.
	for _, item := range claims {
		results = append(results, pipe.Eval(ack, ackScriptSource, []string{a.key(item.id), a.prefix + "pending", a.prefix + "sizes", a.prefix + "bytes", a.prefix + "lease:{" + item.id + "}", a.prefix + "received"}, item.id, item.body, a.config.Owner, token, item.revision, item.version))
	}
	if _, err = pipe.Exec(ack); err != nil {
		return err
	}
	for _, result := range results {
		n, e := result.Int()
		if e != nil {
			return e
		}
		if n != 1 {
			flushOutcome = "lease_lost"
			return errors.New("archive acknowledgment lease or revision lost")
		}
	}
	return nil
}
