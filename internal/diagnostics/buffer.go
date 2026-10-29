// Package diagnostics stores bounded, non-authoritative operational records.
// It never owns request payloads, delivery work, or persistent business history.
package diagnostics

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"partner-hub/internal/model"
)

const Capacity = 1000

type Fields struct {
	RequestID  string
	MsgID      string
	TraceID    string
	DeliveryID string
	Platform   string
	Outcome    string
	Status     string
}

type Event struct {
	ID         string    `json:"id"`
	Time       time.Time `json:"time"`
	Level      string    `json:"level"`
	Instance   string    `json:"instance"`
	BootID     string    `json:"bootId"`
	Kind       string    `json:"kind"`
	Message    string    `json:"message"`
	RequestID  string    `json:"requestId,omitempty"`
	MsgID      string    `json:"msgId,omitempty"`
	TraceID    string    `json:"traceId,omitempty"`
	DeliveryID string    `json:"deliveryId,omitempty"`
	Platform   string    `json:"platform,omitempty"`
	Outcome    string    `json:"outcome,omitempty"`
	Status     string    `json:"status,omitempty"`
}
type Instance struct {
	Instance string    `json:"instance"`
	Status   string    `json:"status"`
	BootID   string    `json:"bootId,omitempty"`
	Capacity int       `json:"capacity,omitempty"`
	Retained int       `json:"retained,omitempty"`
	Dropped  uint64    `json:"dropped,omitempty"`
	OldestAt time.Time `json:"oldestAt,omitempty"`
}
type Retention struct {
	CapacityPerInstance int  `json:"capacityPerInstance"`
	Persistent          bool `json:"persistent"`
}
type Snapshot struct {
	Items     []Event    `json:"items"`
	Instances []Instance `json:"instances"`
	Retention Retention  `json:"retention"`
}
type Query struct {
	Level, Instance, Q string
	Limit              int
}

type description struct{ level, message string }

var events = map[string]description{
	"ingress_invalid":                   {"WARN", "成交校验拒绝；未接管请求"},
	"ingress_body_too_large":            {"WARN", "请求体超过上限；未接管请求"},
	"ingress_rate_limited":              {"WARN", "入口速率限制；未接管请求"},
	"ingress_in_flight_limited":         {"WARN", "入口并发容量限制；未接管请求"},
	"ingress_unconfirmed":               {"ERROR", "成交接管结果未确认；请先查询权威记录，勿自动重交"},
	"ingress_first_abnormal":            {"WARN", "首轮结果需要恢复或人工检查"},
	"ingress_panic":                     {"ERROR", "成交入口运行异常；请查询权威记录"},
	"recovery_result":                   {"WARN", "恢复投递结果异常或未确认；请检查对应投递记录"},
	"runtime_warning":                   {"WARN", "运行警告；详查对应实例和业务记录"},
	"runtime_error":                     {"ERROR", "运行异常；详查对应实例和业务记录"},
	"binding_fetch_unavailable":         {"WARN", "绑定事件读取不可用"},
	"binding_commit_unconfirmed":        {"WARN", "绑定持久化或提交未确认；分区暂停"},
	"binding_cache_invalidation_failed": {"WARN", "绑定缓存失效通知失败；等待 TTL 收敛"},
	"platform2_fetch_unavailable":       {"WARN", "平台 B 消息读取不可用"},
	"platform2_commit_unconfirmed":      {"WARN", "平台 B 持久化或提交未确认；分区暂停"},
	"hub_stopped":                       {"ERROR", "中台进程停止；检查部署与依赖状态"},
}
var uuid = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// cleanText only handles designated correlation identifiers, never raw errors
// or messages. Control characters are rejected rather than hidden in the UI.
func cleanText(s string, limit int) string {
	if !utf8.ValidString(s) || strings.ContainsFunc(s, unicode.IsControl) {
		return ""
	}
	r := []rune(s)
	if len(r) > limit {
		r = r[:limit]
	}
	return string(r)
}
func allowed(value string, values ...string) string {
	for _, v := range values {
		if value == v {
			return value
		}
	}
	return ""
}
func cleanFields(f Fields) Fields {
	f.RequestID = cleanText(f.RequestID, 128)
	if !uuid.MatchString(f.MsgID) {
		f.MsgID = ""
	}
	f.TraceID = cleanText(f.TraceID, 128)
	f.DeliveryID = cleanText(f.DeliveryID, 128)
	f.Platform = allowed(f.Platform, "PLATFORM_A", "PLATFORM_B")
	f.Outcome = allowed(f.Outcome, "SUCCESS", "RETRYABLE_FAILURE", "PERMANENT_FAILURE", "UNKNOWN")
	f.Status = allowed(f.Status, "REJECTED", "UNCONFIRMED", "DEPENDENCY_UNAVAILABLE", "HANDOFF_UNCONFIRMED", "INTERNAL_ERROR", "RECOVERY_PENDING", "NEEDS_ATTENTION", "SUCCEEDED", "NO_ROUTE", "PENDING", "RETRY_WAIT", "IN_FLIGHT", "COMPLETED", "ACCEPTED", "PERSISTED")
	return f
}

type Buffer struct {
	mu                sync.RWMutex
	instance, bootID  string
	ring              [Capacity]Event
	next, retained    int
	sequence, dropped uint64
}

func NewBuffer(instance string) *Buffer {
	instance = cleanText(instance, 64)
	if instance == "" {
		instance = "unknown"
	}
	return &Buffer{instance: instance, bootID: model.NewID()}
}

// Record accepts only fixed event kinds and designated scalar identifiers.
// Identity and timestamps always come from the owning process.
func (b *Buffer) Record(kind string, f Fields) {
	d, ok := events[kind]
	if !ok || b == nil {
		return
	}
	b.record(kind, d.level, f)
}
func (b *Buffer) record(kind, level string, fields Fields) {
	d, ok := events[kind]
	if !ok || b == nil {
		return
	}
	f := cleanFields(fields)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sequence++
	b.ring[b.next] = Event{ID: fmt.Sprintf("%s:%d", b.bootID, b.sequence), Time: time.Now().UTC(), Level: level, Instance: b.instance, BootID: b.bootID, Kind: kind, Message: d.message, RequestID: f.RequestID, MsgID: f.MsgID, TraceID: f.TraceID, DeliveryID: f.DeliveryID, Platform: f.Platform, Outcome: f.Outcome, Status: f.Status}
	b.next = (b.next + 1) % Capacity
	if b.retained < Capacity {
		b.retained++
	} else {
		b.dropped++
	}
}
func normalizedLimit(limit int) int {
	if limit <= 0 {
		return 20
	}
	if limit > 100 {
		return 100
	}
	return limit
}
func matches(e Event, q Query) bool {
	if q.Level != "" && e.Level != q.Level || q.Instance != "" && e.Instance != q.Instance {
		return false
	}
	if q.Q == "" {
		return true
	}
	haystack := strings.Join([]string{e.ID, e.Message, e.Kind, e.RequestID, e.MsgID, e.TraceID, e.DeliveryID, e.Platform, e.Outcome, e.Status}, " ")
	return strings.Contains(strings.ToLower(haystack), strings.ToLower(q.Q))
}
func newSnapshot() Snapshot {
	return Snapshot{Items: []Event{}, Instances: []Instance{}, Retention: Retention{CapacityPerInstance: Capacity, Persistent: false}}
}
func (b *Buffer) Snapshot(q Query) Snapshot {
	s := newSnapshot()
	b.mu.RLock()
	defer b.mu.RUnlock()
	info := Instance{Instance: b.instance, Status: "ok", BootID: b.bootID, Capacity: Capacity, Retained: b.retained, Dropped: b.dropped}
	if b.retained > 0 {
		info.OldestAt = b.ring[(b.next-b.retained+Capacity)%Capacity].Time
	}
	s.Instances = append(s.Instances, info)
	for i := 0; i < b.retained && len(s.Items) < normalizedLimit(q.Limit); i++ {
		e := b.ring[(b.next-1-i+Capacity)%Capacity]
		if matches(e, q) {
			s.Items = append(s.Items, e)
		}
	}
	return s
}
func sortEvents(items []Event) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].Time.Equal(items[j].Time) {
			return items[i].ID > items[j].ID
		}
		return items[i].Time.After(items[j].Time)
	})
}
