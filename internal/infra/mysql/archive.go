package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"partner-hub/internal/infra"
	"partner-hub/internal/model"
	"time"
)

type ArchiveStore struct{ db *sql.DB }

func NewArchiveStore(db *sql.DB) *ArchiveStore { return &ArchiveStore{db: db} }
func (s *ArchiveStore) Save(ctx context.Context, snapshots []model.FirstRoundSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}
	if len(snapshots) > 100 {
		return errors.New("archive batch too large")
	}
	for _, snapshot := range snapshots {
		if err := infra.ValidateNormalSnapshot(snapshot); err != nil {
			return err
		}
	}
	write, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(write, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, snapshot := range snapshots {
		route, err := json.Marshal(snapshot.Route)
		if err != nil {
			return err
		}
		status := "SUCCEEDED"
		if snapshot.Route.Status == "NO_ROUTE" {
			status = "NO_ROUTE"
		}
		_, err = tx.ExecContext(write, `INSERT INTO trade_message(msg_id,client_id,trace_id,raw_payload,received_at,first_status,route_status,route_snapshot,archive_revision) VALUES(?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE msg_id=msg_id`, snapshot.Trade.MsgID, snapshot.Trade.ClientID, snapshot.Trade.TraceID, snapshot.Trade.RawPayload, snapshot.ReceivedAt.UTC(), status, snapshot.Route.Status, route, snapshot.ArchiveRevision)
		if err != nil {
			return err
		}
		var revision string
		if err = tx.QueryRowContext(write, `SELECT archive_revision FROM trade_message WHERE msg_id=? FOR UPDATE`, snapshot.Trade.MsgID).Scan(&revision); err != nil {
			return err
		}
		if revision != snapshot.ArchiveRevision {
			return errors.New("archive revision conflicts with stored message")
		}
		for _, a := range snapshot.Attempts {
			_, err = tx.ExecContext(write, `INSERT INTO delivery_task(delivery_id,msg_id,platform_id,platform_uid,status,first_outcome,last_outcome,protocol_version,prepared_payload) VALUES(?,?,?,?,'SUCCEEDED','SUCCESS','SUCCESS',?,?) ON DUPLICATE KEY UPDATE delivery_id=delivery_id`, a.Target.DeliveryID, snapshot.Trade.MsgID, a.Target.PlatformID, a.Target.PlatformUID, a.Request.ProtocolVersion, a.Request.Payload)
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(write, `INSERT INTO delivery_attempt(delivery_id,attempt_no,cycle_no,send_started,protocol_version,prepared_payload,started_at,finished_at,outcome,summary) VALUES(?,1,1,?,?,?,?,?,'SUCCESS',?) ON DUPLICATE KEY UPDATE delivery_id=delivery_id`, a.Target.DeliveryID, a.Result.SendStarted, a.Request.ProtocolVersion, a.Request.Payload, a.Result.StartedAt.UTC(), a.Result.FinishedAt.UTC(), a.Result.Summary)
			if err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}
