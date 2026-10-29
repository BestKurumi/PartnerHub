// Package query exposes MySQL facts only. It never routes, sends, or reads Redis.
package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Service struct {
	db    *sql.DB
	slots chan struct{}
}

func NewService(db *sql.DB) *Service { return &Service{db: db, slots: make(chan struct{}, 4)} }

var errUnavailable = errors.New("query unavailable")

type reader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}
type filter struct {
	limit                                int
	platform, client, msg, delivery, uid string
	trace, status, currentState          string
	routeStatus                          string
	from, to                             time.Time
	cursor                               *position
}
type position struct {
	Time  time.Time `json:"time"`
	ID    string    `json:"id"`
	Extra string    `json:"extra,omitempty"`
	Scope string    `json:"scope"`
}

func rows(ctx context.Context, db reader, statement string, args ...any) ([]map[string]any, error) {
	r, err := db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	names, err := r.Columns()
	if err != nil {
		return nil, err
	}
	result := make([]map[string]any, 0)
	for r.Next() {
		values := make([]any, len(names))
		pointers := make([]any, len(names))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err = r.Scan(pointers...); err != nil {
			return nil, err
		}
		item := make(map[string]any, len(names))
		for i, name := range names {
			value := values[i]
			if b, ok := value.([]byte); ok {
				value = string(b)
			}
			if name == "routeSnapshot" {
				value = json.RawMessage(fmt.Sprint(value))
			}
			if tm, ok := value.(time.Time); ok {
				value = tm.UTC()
			}
			item[name] = value
		}
		result = append(result, item)
	}
	return result, r.Err()
}

func (s *Service) read(ctx context.Context, kind, id string, f filter) (any, error) {
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return nil, errUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if s.db == nil {
		return nil, errUnavailable
	}
	// A detail or page and its platform facts share one consistent database snapshot.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if kind == "message" {
		items, err := rows(ctx, tx, `SELECT msg_id AS msgId,client_id AS clientId,trace_id AS traceId,received_at AS receivedAt,stored_at AS storedAt,first_status AS firstStatus,route_status AS routeStatus,route_snapshot AS routeSnapshot FROM trade_message WHERE msg_id=?`, id)
		if err != nil {
			return nil, err
		}
		if len(items) == 0 {
			return nil, sql.ErrNoRows
		}
		if err = messageFacts(ctx, tx, items); err != nil {
			return nil, err
		}
		if err = manualFacts(ctx, tx, items[0]); err != nil {
			return nil, err
		}
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return items[0], nil
	}
	if kind == "event" {
		items, err := rows(ctx, tx, `SELECT event_id AS eventId,event_type AS eventType,platform_id AS platformId,client_id AS clientId,platform_uid AS platformUid,occurred_at AS occurredAt,result,revision,processed_at AS processedAt FROM binding_event WHERE event_id=?`, id)
		if err != nil {
			return nil, err
		}
		if len(items) == 0 {
			return nil, sql.ErrNoRows
		}
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return items[0], nil
	}
	var statement, tm, key, extra string
	var where []string
	var args []any
	add := func(condition string, v any) { where = append(where, condition); args = append(args, v) }
	switch kind {
	case "messages":
		statement = `SELECT m.msg_id AS msgId,m.client_id AS clientId,m.trace_id AS traceId,m.received_at AS receivedAt,m.stored_at AS storedAt,m.first_status AS firstStatus,m.route_status AS routeStatus FROM trade_message m`
		tm, key = "m.received_at", "m.msg_id"
		if f.platform != "" {
			add("EXISTS (SELECT 1 FROM delivery_task d WHERE d.msg_id=m.msg_id AND d.platform_id=?)", f.platform)
		}
		if f.client != "" {
			add("m.client_id=?", f.client)
		}
		if f.msg != "" {
			add("m.msg_id=?", f.msg)
		}
		if f.trace != "" {
			// Keep the index candidate lookup and enforce exact byte matching even
			// in an older database whose text collation may be case insensitive.
			add("m.trace_id=?", f.trace)
			add("BINARY m.trace_id=BINARY ?", f.trace)
		}
		if f.status != "" {
			// Match messageFacts' pending-before-attention aggregation in the
			// consistent snapshot, before cursor ordering and LIMIT are applied.
			add(`(CASE
			 WHEN m.route_status='NO_ROUTE' THEN 'NO_ROUTE'
			 WHEN m.route_status IN ('PENDING','RETRY_WAIT','IN_FLIGHT')
			  OR (m.route_status='ROUTED' AND NOT EXISTS (SELECT 1 FROM delivery_task ds WHERE ds.msg_id=m.msg_id))
			  OR EXISTS (SELECT 1 FROM delivery_task dp WHERE dp.msg_id=m.msg_id AND dp.status NOT IN ('SUCCEEDED','NEEDS_ATTENTION')) THEN 'RECOVERY_PENDING'
			 WHEN m.route_status='NEEDS_ATTENTION'
			  OR EXISTS (SELECT 1 FROM delivery_task da WHERE da.msg_id=m.msg_id AND da.status='NEEDS_ATTENTION') THEN 'NEEDS_ATTENTION'
			 ELSE 'SUCCEEDED' END)=?`, f.status)
		}
		if f.routeStatus != "" {
			add("m.route_status=?", f.routeStatus)
		}
	case "deliveries":
		statement = `SELECT d.delivery_id AS deliveryId,d.msg_id AS msgId,d.platform_id AS platformId,d.platform_uid AS platformUid,d.status AS currentState,d.first_outcome AS firstOutcome,d.last_outcome AS lastOutcome,d.cycle_no AS cycleNo,d.attempt_count AS attemptCount,m.client_id AS clientId,m.received_at AS receivedAt FROM delivery_task d JOIN trade_message m ON m.msg_id=d.msg_id`
		tm, key = "m.received_at", "d.delivery_id"
		if f.currentState != "" {
			add("d.status=?", f.currentState)
		}
		if f.platform != "" {
			add("d.platform_id=?", f.platform)
		}
		if f.client != "" {
			add("m.client_id=?", f.client)
		}
		if f.msg != "" {
			add("d.msg_id=?", f.msg)
		}
		if f.delivery != "" {
			add("d.delivery_id=?", f.delivery)
		}
		if f.uid != "" {
			add("d.platform_uid=?", f.uid)
		}
	case "attempts":
		statement = `SELECT a.delivery_id AS deliveryId,a.attempt_no AS attemptNo,a.cycle_no AS cycleNo,a.send_started AS sendStarted,a.protocol_version AS protocolVersion,a.started_at AS startedAt,a.finished_at AS finishedAt,a.outcome,a.summary,d.platform_id AS platformId,d.platform_uid AS platformUid,d.msg_id AS msgId FROM delivery_attempt a JOIN delivery_task d ON d.delivery_id=a.delivery_id`
		tm, key = "a.started_at", "a.attempt_no"
		add("a.delivery_id=?", id)
		if f.platform != "" {
			add("d.platform_id=?", f.platform)
		}
	case "bindings":
		statement = `SELECT client_id AS clientId,platform_id AS platformId,platform_uid AS platformUid,status,revision,updated_at AS updatedAt FROM binding_current`
		tm, key, extra = "updated_at", "client_id", "platform_id"
		if f.platform != "" {
			add("platform_id=?", f.platform)
		}
		if f.client != "" {
			add("client_id=?", f.client)
		}
		if f.uid != "" {
			add("platform_uid=?", f.uid)
		}
	case "anomalies":
		// Raw poison payloads can contain sensitive input and are deliberately not returned.
		statement = `SELECT a.anomaly_id AS anomalyId,a.event_id AS eventId,a.reason,a.source_topic AS sourceTopic,a.source_partition AS sourcePartition,a.source_offset AS sourceOffset,a.created_at AS createdAt FROM ingress_anomaly a`
		tm, key = "a.created_at", "a.anomaly_id"
		if f.platform != "" {
			add("EXISTS (SELECT 1 FROM binding_event e WHERE e.event_id=a.event_id AND e.platform_id=?)", f.platform)
		}
	default:
		return nil, errors.New("unknown query")
	}
	if !f.from.IsZero() {
		add(tm+">=?", f.from.UTC())
	}
	if !f.to.IsZero() {
		add(tm+"<=?", f.to.UTC())
	}
	if f.cursor != nil {
		p := f.cursor
		var value any = p.ID
		if kind == "attempts" {
			value, _ = strconv.ParseUint(p.ID, 10, 32)
		}
		if extra == "" {
			where = append(where, fmt.Sprintf("(%s < ? OR (%s = ? AND %s < ?))", tm, tm, key))
			args = append(args, p.Time, p.Time, value)
		} else {
			where = append(where, fmt.Sprintf("(%s < ? OR (%s = ? AND (%s < ? OR (%s = ? AND %s < ?))))", tm, tm, key, key, extra))
			args = append(args, p.Time, p.Time, value, value, p.Extra)
		}
	}
	if len(where) > 0 {
		statement += " WHERE " + strings.Join(where, " AND ")
	}
	statement += " ORDER BY " + tm + " DESC," + key + " DESC"
	if extra != "" {
		statement += "," + extra + " DESC"
	}
	statement += " LIMIT ?"
	args = append(args, f.limit+1)
	items, err := rows(ctx, tx, statement, args...)
	if err != nil {
		return nil, err
	}
	next := ""
	if len(items) > f.limit {
		items = items[:f.limit]
		last := items[len(items)-1]
		var tf, kf, ef string
		switch kind {
		case "messages":
			tf, kf = "receivedAt", "msgId"
		case "deliveries":
			tf, kf = "receivedAt", "deliveryId"
		case "attempts":
			tf, kf = "startedAt", "attemptNo"
		case "bindings":
			tf, kf, ef = "updatedAt", "clientId", "platformId"
		case "anomalies":
			tf, kf = "createdAt", "anomalyId"
		}
		p := position{Time: last[tf].(time.Time), ID: fmt.Sprint(last[kf]), Scope: scope(kind, id, f)}
		if ef != "" {
			p.Extra = fmt.Sprint(last[ef])
		}
		next = encodeCursor(p)
	}
	if kind == "messages" {
		if err = messageFacts(ctx, tx, items); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return map[string]any{"items": items, "nextCursor": next}, nil
}

// A message has at most one routing object and two fixed platform tasks.
// Each object reads at most 21 indexed rows; no unbounded audit scan is exposed.
func manualFacts(ctx context.Context, db reader, item map[string]any) error {
	all := make([]map[string]any, 0)
	truncated := false
	targets := [][2]string{{"ROUTING", item["msgId"].(string)}}
	for _, task := range item["platformResults"].([]map[string]any) {
		targets = append(targets, [2]string{"DELIVERY", task["deliveryId"].(string)})
	}
	for _, target := range targets {
		items, err := rows(ctx, db, `SELECT operation_id AS operationId,command_kind AS kind,target_id AS targetId,operator_name AS operator,reason,retry_cycle AS retryCycle,result_state AS state,created_at AS createdAt FROM manual_operation WHERE command_kind=? AND target_id=? ORDER BY created_at DESC,operation_id DESC LIMIT 21`, target[0], target[1])
		if err != nil {
			return err
		}
		if len(items) > 20 {
			items = items[:20]
			truncated = true
		}
		all = append(all, items...)
	}
	item["manualOperations"], item["manualOperationsTruncated"] = all, truncated
	return nil
}
func messageFacts(ctx context.Context, tx reader, items []map[string]any) error {
	if len(items) == 0 {
		return nil
	}
	params := make([]string, len(items))
	args := make([]any, len(items))
	byID := map[string]map[string]any{}
	for i, item := range items {
		params[i] = "?"
		args[i] = item["msgId"]
		byID[item["msgId"].(string)] = item
		item["recordStatus"] = "PERSISTED"
		item["platformResults"] = []map[string]any{}
	}
	tasks, err := rows(ctx, tx, `SELECT msg_id AS msgId,delivery_id AS deliveryId,platform_id AS platformId,platform_uid AS platformUid,first_outcome AS firstOutcome,status AS currentState FROM delivery_task WHERE msg_id IN (`+strings.Join(params, ",")+`) ORDER BY platform_id`, args...)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		item := byID[task["msgId"].(string)]
		item["platformResults"] = append(item["platformResults"].([]map[string]any), task)
	}
	for _, item := range items {
		status := "SUCCEEDED"
		route := item["routeStatus"]
		if route == "NO_ROUTE" {
			status = "NO_ROUTE"
		} else {
			attention, pending := route == "NEEDS_ATTENTION", route == "RETRY_WAIT" || route == "IN_FLIGHT" || route == "PENDING"
			facts := item["platformResults"].([]map[string]any)
			if len(facts) == 0 && route == "ROUTED" {
				pending = true
			}
			for _, t := range facts {
				switch t["currentState"] {
				case "SUCCEEDED":
				case "NEEDS_ATTENTION":
					attention = true
				default:
					pending = true
				}
			}
			if attention {
				status = "NEEDS_ATTENTION"
			}
			if pending {
				status = "RECOVERY_PENDING"
			}
		}
		item["status"] = status
	}
	return nil
}
