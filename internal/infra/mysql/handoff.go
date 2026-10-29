package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"partner-hub/internal/model"
	"time"
)

type HandoffStore struct{ db *sql.DB }

func NewHandoffStore(db *sql.DB) *HandoffStore { return &HandoffStore{db} }

// Handoff commits the entire first round. Replays confirm the immutable snapshot
// without resetting any task that a recovery worker may already have advanced.
func (s *HandoffStore) Handoff(ctx context.Context, snapshot model.FirstRoundSnapshot) error {
	if err := validateHandoff(snapshot); err != nil {
		return err
	}
	body, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	if len(body) > 256<<10 {
		return errors.New("handoff snapshot too large")
	}
	write, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(write, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var databaseNow time.Time
	if err = tx.QueryRowContext(write, `SELECT UTC_TIMESTAMP(3)`).Scan(&databaseNow); err != nil {
		return err
	}
	// First failure: 1s cap with 50%-100% jitter, based on primary DB time.
	nextAttempt := func() time.Time { return databaseNow.Add(time.Duration(500000+rand.IntN(500001)) * time.Microsecond) }
	route, err := json.Marshal(snapshot.Route)
	if err != nil {
		return err
	}
	status := "NEEDS_ATTENTION"
	routeCount := 0
	var routeDue any
	if snapshot.Route.Status == "RETRY_WAIT" {
		status = "RECOVERY_PENDING"
		routeCount = 1
		routeDue = nextAttempt()
	}
	for _, a := range snapshot.Attempts {
		if a.Result.Outcome == model.Unknown || a.Result.Outcome == model.RetryableFailure {
			status = "RECOVERY_PENDING"
		}
	}
	_, err = tx.ExecContext(write, `INSERT INTO trade_message(msg_id,client_id,trace_id,raw_payload,received_at,first_status,route_status,route_snapshot,archive_revision,first_snapshot,route_error,route_attempt_count,route_cycle_attempt_count,route_next_attempt_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE msg_id=msg_id`, snapshot.Trade.MsgID, snapshot.Trade.ClientID, snapshot.Trade.TraceID, snapshot.Trade.RawPayload, snapshot.ReceivedAt.UTC(), status, snapshot.Route.Status, route, snapshot.ArchiveRevision, body, snapshot.RouteError, routeCount, routeCount, routeDue)
	if err != nil {
		return err
	}
	var stored []byte
	var revision string
	if err = tx.QueryRowContext(write, `SELECT archive_revision,first_snapshot FROM trade_message WHERE msg_id=? FOR UPDATE`, snapshot.Trade.MsgID).Scan(&revision, &stored); err != nil {
		return err
	}
	if revision != snapshot.ArchiveRevision || !bytes.Equal(stored, body) {
		return errors.New("handoff immutable snapshot conflict")
	}
	for _, a := range snapshot.Attempts {
		state := "RETRY_WAIT"
		var due any = nextAttempt()
		if a.Result.Outcome == model.Success {
			state = "SUCCEEDED"
			due = nil
		} else if a.Result.Outcome == model.PermanentFailure {
			state = "NEEDS_ATTENTION"
			due = nil
		}
		payload := a.Request.Payload
		if payload == nil {
			payload = []byte{}
		}
		_, err = tx.ExecContext(write, `INSERT INTO delivery_task(delivery_id,msg_id,platform_id,platform_uid,status,first_outcome,last_outcome,protocol_version,prepared_payload,cycle_attempt_count,next_attempt_at) VALUES(?,?,?,?,?,?,?,?,?,1,?) ON DUPLICATE KEY UPDATE delivery_id=delivery_id`, a.Target.DeliveryID, snapshot.Trade.MsgID, a.Target.PlatformID, a.Target.PlatformUID, state, string(a.Result.Outcome), string(a.Result.Outcome), a.Request.ProtocolVersion, payload, due)
		if err != nil {
			return err
		}
		// A delivery identity collision must roll back the complete handoff;
		// an unrelated task cannot stand in for this snapshot's fixed target.
		var msgID, platform, uid, first string
		if err = tx.QueryRowContext(write, `SELECT msg_id,platform_id,platform_uid,first_outcome FROM delivery_task WHERE delivery_id=? FOR UPDATE`, a.Target.DeliveryID).Scan(&msgID, &platform, &uid, &first); err != nil {
			return err
		}
		if msgID != snapshot.Trade.MsgID || platform != a.Target.PlatformID || uid != a.Target.PlatformUID || first != string(a.Result.Outcome) {
			return errors.New("handoff delivery identity conflict")
		}
		_, err = tx.ExecContext(write, `INSERT INTO delivery_attempt(delivery_id,attempt_no,cycle_no,send_started,protocol_version,prepared_payload,started_at,finished_at,outcome,summary) VALUES(?,1,1,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE delivery_id=delivery_id`, a.Target.DeliveryID, a.Result.SendStarted, a.Request.ProtocolVersion, payload, a.Result.StartedAt.UTC(), a.Result.FinishedAt.UTC(), string(a.Result.Outcome), a.Result.Summary)
		if err != nil {
			return err
		}
	}
	if routeCount > 0 {
		_, err = tx.ExecContext(write, `INSERT INTO route_attempt(msg_id,attempt_no,cycle_no,started_at,finished_at,outcome,summary) VALUES(?,1,1,?,?,'QUERY_FAILED',?) ON DUPLICATE KEY UPDATE msg_id=msg_id`, snapshot.Trade.MsgID, snapshot.ReceivedAt.UTC(), snapshot.Route.ResolvedAt.UTC(), snapshot.RouteError)
		if err != nil {
			return err
		}
	}
	if err = tx.Commit(); err == nil {
		return nil
	}
	// Commit may have reached MySQL before its acknowledgement was lost. Confirm
	// this exact snapshot on the primary within the original processing budget.
	confirm, release := context.WithTimeout(ctx, time.Second)
	defer release()
	var actual []byte
	if e := s.db.QueryRowContext(confirm, `SELECT first_snapshot FROM trade_message WHERE msg_id=? AND archive_revision=?`, snapshot.Trade.MsgID, snapshot.ArchiveRevision).Scan(&actual); e == nil && bytes.Equal(actual, body) {
		return nil
	}
	return err
}
func validateHandoff(s model.FirstRoundSnapshot) error {
	invalid := errors.New("invalid abnormal first-round snapshot")
	if s.Trade.MsgID == "" || s.Trade.MsgID != s.Route.MsgID || s.ArchiveRevision == "" || s.ReceivedAt.IsZero() || s.Route.ResolvedAt.IsZero() || len(s.Trade.RawPayload) == 0 || len(s.Trade.RawPayload) > 65536 || len(s.Route.Targets) > 2 || len(s.Attempts) != len(s.Route.Targets) || len(s.RouteError) > 2048 {
		return invalid
	}
	if s.Route.Status == "RETRY_WAIT" {
		if len(s.Attempts) != 0 || s.RouteError == "" {
			return invalid
		}
		return nil
	}
	if s.Route.Status != "ROUTED" || len(s.Attempts) == 0 || s.RouteError != "" {
		return invalid
	}
	seen := map[string]bool{}
	failed := false
	for i, target := range s.Route.Targets {
		a := s.Attempts[i]
		if (target.PlatformID != "PLATFORM_A" && target.PlatformID != "PLATFORM_B") || seen[target.PlatformID] || target.PlatformUID == "" || target.DeliveryID == "" || a.Target != target || a.Result.StartedAt.IsZero() || a.Result.FinishedAt.Before(a.Result.StartedAt) || len(a.Request.Payload) > 8192 || len(a.Result.Summary) > 2048 {
			return invalid
		}
		seen[target.PlatformID] = true
		switch a.Result.Outcome {
		case model.Success:
			if !a.Result.SendStarted || len(a.Request.Payload) == 0 || a.Request.ProtocolVersion == "" || a.Request.PlatformID != target.PlatformID {
				return invalid
			}
		case model.Unknown, model.RetryableFailure, model.PermanentFailure:
			failed = true
		default:
			return invalid
		}
	}
	if !failed {
		return invalid
	}
	return nil
}
