package mysql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"partner-hub/internal/binding"
	"partner-hub/internal/model"
	"time"
)

type Bindings struct{ db *sql.DB }

func NewBindings(db *sql.DB) *Bindings { return &Bindings{db: db} }
func (b *Bindings) Apply(ctx context.Context, e model.BindingEvent) (model.BindingResult, error) {
	if err := binding.ValidateEvent(e); err != nil {
		return model.BindingResult{}, err
	}
	tx, err := b.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return model.BindingResult{}, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO binding_event (event_id,event_type,platform_id,client_id,platform_uid,occurred_at,result,revision,source_topic,source_partition,source_offset) VALUES (?,?,?,?,?,?,'PROCESSING',0,?,?,?) ON DUPLICATE KEY UPDATE event_id=event_id`, e.EventID, e.EventType, e.PlatformID, e.ClientID, e.PlatformUID, e.OccurredAt, nullableTopic(e.Position.Topic), e.Position.Partition, e.Position.Offset)
	if err != nil {
		return model.BindingResult{}, err
	}
	var old model.BindingEvent
	var outcome string
	var revision uint64
	err = tx.QueryRowContext(ctx, `SELECT event_type,platform_id,client_id,platform_uid,occurred_at,result,revision FROM binding_event WHERE event_id=? FOR UPDATE`, e.EventID).Scan(&old.EventType, &old.PlatformID, &old.ClientID, &old.PlatformUID, &old.OccurredAt, &outcome, &revision)
	if err != nil {
		return model.BindingResult{}, err
	}
	result := model.BindingResult{EventID: e.EventID, Outcome: outcome, Revision: revision}
	if old.EventType != e.EventType || old.PlatformID != e.PlatformID || old.ClientID != e.ClientID || old.PlatformUID != e.PlatformUID || old.OccurredAt != e.OccurredAt {
		body, _ := json.Marshal(e)
		if err = saveBindingAnomaly(ctx, tx, model.BindingAnomaly{EventID: e.EventID, Position: e.Position, Reason: "EVENT_ID_CONFLICT", Payload: body}); err != nil {
			return model.BindingResult{}, err
		}
		result.Outcome = "EVENT_ID_CONFLICT"
		return result, tx.Commit()
	}
	if outcome != "PROCESSING" {
		result.Replayed = true
		return result, tx.Commit()
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO binding_current (client_id,platform_id,platform_uid,status,revision) VALUES (?,?,'','UNBOUND',0) ON DUPLICATE KEY UPDATE client_id=client_id`, e.ClientID, e.PlatformID)
	if err != nil {
		return model.BindingResult{}, err
	}
	var uid, status string
	if err = tx.QueryRowContext(ctx, `SELECT platform_uid,status,revision FROM binding_current WHERE client_id=? AND platform_id=? FOR UPDATE`, e.ClientID, e.PlatformID).Scan(&uid, &status, &revision); err != nil {
		return model.BindingResult{}, err
	}
	outcome = "NO_CHANGE"
	changed := false
	if e.EventType == "BIND" {
		if status == "UNBOUND" {
			uid = e.PlatformUID
			status = "BOUND"
			outcome = "APPLIED"
			changed = true
		} else if uid != e.PlatformUID {
			outcome = "CONFLICT"
		}
	} else if status == "BOUND" {
		if uid == e.PlatformUID {
			status = "UNBOUND"
			outcome = "APPLIED"
			changed = true
		} else {
			outcome = "UID_MISMATCH"
		}
	}
	if changed {
		revision++
		if _, err = tx.ExecContext(ctx, `UPDATE binding_current SET platform_uid=?,status=?,revision=? WHERE client_id=? AND platform_id=?`, uid, status, revision, e.ClientID, e.PlatformID); err != nil {
			return model.BindingResult{}, err
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE binding_event SET result=?,revision=? WHERE event_id=?`, outcome, revision, e.EventID); err != nil {
		return model.BindingResult{}, err
	}
	result.Outcome = outcome
	result.Revision = revision
	return result, tx.Commit()
}
func (b *Bindings) Load(ctx context.Context, clientID string) (model.BindingSnapshot, error) {
	snapshot := model.BindingSnapshot{Version: 1, ObservedAt: time.Now().UTC(), Targets: make([]model.BindingTarget, 0, 2)}
	rows, err := b.db.QueryContext(ctx, `SELECT platform_id,platform_uid FROM binding_current WHERE client_id=? AND status='BOUND' ORDER BY platform_id`, clientID)
	if err != nil {
		return model.BindingSnapshot{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var target model.BindingTarget
		if err = rows.Scan(&target.PlatformID, &target.PlatformUID); err != nil {
			return model.BindingSnapshot{}, err
		}
		snapshot.Targets = append(snapshot.Targets, target)
	}
	return snapshot, rows.Err()
}
func (b *Bindings) SaveAnomaly(ctx context.Context, a model.BindingAnomaly) error {
	return saveBindingAnomaly(ctx, b.db, a)
}

type bindingExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func saveBindingAnomaly(ctx context.Context, db bindingExecer, a model.BindingAnomaly) error {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	id[6] = (id[6] & 15) | 64
	id[8] = (id[8] & 63) | 128
	anomalyID := fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:])
	if a.Reason == "" {
		return errors.New("anomaly reason required")
	}
	_, err := db.ExecContext(ctx, `INSERT INTO ingress_anomaly (anomaly_id,event_id,reason,payload,source_topic,source_partition,source_offset) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE anomaly_id=anomaly_id`, anomalyID, nullableTopic(a.EventID), a.Reason, a.Payload, nullableTopic(a.Position.Topic), a.Position.Partition, a.Position.Offset)
	return err
}
func nullableTopic(value string) any {
	if value == "" {
		return nil
	}
	return value
}
