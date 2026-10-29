package mysql

import (
	"context"
	"database/sql"
	"errors"
	mysqldriver "github.com/go-sql-driver/mysql"
	"partner-hub/internal/delivery"
	"time"
)

type ManualStore struct{ db *sql.DB }

func NewManualStore(db *sql.DB) *ManualStore { return &ManualStore{db: db} }
func (s *ManualStore) ExecuteRetry(ctx context.Context, r delivery.RetryRequest) (delivery.RetryResult, error) {
	if err := delivery.ValidateRetryRequest(r); err != nil {
		return delivery.RetryResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return delivery.RetryResult{}, err
	}
	defer tx.Rollback()
	// Serialize one operation identity before touching its target. The placeholder
	// and target transition only become visible together on a successful commit.
	_, err = tx.ExecContext(ctx, `INSERT INTO manual_operation(operation_id,command_kind,target_id,operator_name,reason,retry_cycle,result_state) VALUES(?,?,?,?,?,0,'')`, r.OperationID, r.Kind, r.TargetID, r.Operator, r.Reason)
	if err != nil {
		var duplicate *mysqldriver.MySQLError
		if !errors.As(err, &duplicate) || duplicate.Number != 1062 {
			return delivery.RetryResult{}, err
		}
		var saved delivery.RetryRequest
		var result delivery.RetryResult
		err = tx.QueryRowContext(ctx, `SELECT operation_id,command_kind,target_id,operator_name,reason,retry_cycle,result_state FROM manual_operation WHERE operation_id=?`, r.OperationID).Scan(&saved.OperationID, &saved.Kind, &saved.TargetID, &saved.Operator, &saved.Reason, &result.RetryCycle, &result.State)
		if err != nil {
			return delivery.RetryResult{}, err
		}
		if saved != r {
			return delivery.RetryResult{}, delivery.ErrRetryConflict
		}
		if result.RetryCycle < 2 || result.State != "PENDING" {
			return delivery.RetryResult{}, errors.New("invalid saved retry result")
		}
		result.OperationID = saved.OperationID
		result.Kind = saved.Kind
		result.TargetID = saved.TargetID
		return result, nil
	}
	q := `SELECT status,cycle_no FROM delivery_task WHERE delivery_id=? FOR UPDATE`
	if r.Kind == delivery.RetryRouting {
		q = `SELECT route_status,route_cycle_no FROM trade_message WHERE msg_id=? FOR UPDATE`
	}
	var state string
	var cycle int
	err = tx.QueryRowContext(ctx, q, r.TargetID).Scan(&state, &cycle)
	if errors.Is(err, sql.ErrNoRows) {
		return delivery.RetryResult{}, delivery.ErrRetryNotFound
	}
	if err != nil {
		return delivery.RetryResult{}, err
	}
	if state != "NEEDS_ATTENTION" {
		return delivery.RetryResult{}, delivery.ErrRetryConflict
	}
	if cycle >= 4294967295 {
		return delivery.RetryResult{}, errors.New("retry cycle exhausted")
	}
	cycle++
	q = `UPDATE delivery_task SET status='PENDING',cycle_no=?,cycle_attempt_count=0,next_attempt_at=UTC_TIMESTAMP(3),lease_owner='',lease_until=NULL WHERE delivery_id=?`
	if r.Kind == delivery.RetryRouting {
		q = `UPDATE trade_message SET route_status='PENDING',route_cycle_no=?,route_cycle_attempt_count=0,route_next_attempt_at=UTC_TIMESTAMP(3),route_lease_owner='',route_lease_until=NULL WHERE msg_id=?`
	}
	if _, err = tx.ExecContext(ctx, q, cycle, r.TargetID); err != nil {
		return delivery.RetryResult{}, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE manual_operation SET retry_cycle=?,result_state='PENDING' WHERE operation_id=?`, cycle, r.OperationID); err != nil {
		return delivery.RetryResult{}, err
	}
	if err = tx.Commit(); err != nil {
		return delivery.RetryResult{}, err
	}
	return delivery.RetryResult{OperationID: r.OperationID, Kind: r.Kind, TargetID: r.TargetID, RetryCycle: cycle, State: "PENDING"}, nil
}
