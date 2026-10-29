package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"partner-hub/internal/delivery"
	"partner-hub/internal/model"
	"sort"
	"time"
)

type RecoveryStore struct{ db *sql.DB }

func NewRecoveryStore(db *sql.DB) *RecoveryStore { return &RecoveryStore{db: db} }
func validRecoveryOwner(owner string) bool {
	if len(owner) == 0 || len(owner) > 128 {
		return false
	}
	for _, r := range owner {
		if r < 33 || r > 126 {
			return false
		}
	}
	return true
}

// HasDue avoids reserving execution tokens while idle scans wait on the DB.
// This consistent read acquires no task locks and never changes responsibility.
// ClaimDue remains authoritative when another worker wins after this hint.
func (s *RecoveryStore) HasDue(ctx context.Context, platform string) (bool, error) {
	if platform != "PLATFORM_A" && platform != "PLATFORM_B" {
		return false, errors.New("invalid recovery platform")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for _, state := range []string{"PENDING", "RETRY_WAIT"} {
		var found int
		err := s.db.QueryRowContext(ctx, `SELECT 1 FROM delivery_task FORCE INDEX(idx_tasks_due) WHERE platform_id=? AND status=? AND next_attempt_at<=UTC_TIMESTAMP(3) AND cycle_attempt_count<? ORDER BY next_attempt_at,delivery_id LIMIT 1`, platform, state, delivery.MaxCycleAttempts).Scan(&found)
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
	}
	return false, nil
}

// ClaimDue acquires only a small batch. External work never runs in this transaction.
func (s *RecoveryStore) ClaimDue(ctx context.Context, platform, owner string, limit int) ([]delivery.RecoveryClaim, error) {
	if (platform != "PLATFORM_A" && platform != "PLATFORM_B") || !validRecoveryOwner(owner) || limit < 1 || limit > 4 {
		return nil, errors.New("invalid recovery claim parameters")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	type candidate struct {
		claim delivery.RecoveryClaim
		msgID string
		due   time.Time
	}
	selected := []candidate{}
	// Each equality range follows the complete index ordering. Lock at most
	// 2*limit candidates and merge the oldest across states without filesort.
	for _, state := range []string{"PENDING", "RETRY_WAIT"} {
		rows, err := tx.QueryContext(ctx, `SELECT delivery_id,msg_id,platform_uid,lease_version,attempt_count,cycle_no,cycle_attempt_count,next_attempt_at FROM delivery_task FORCE INDEX(idx_tasks_due) WHERE platform_id=? AND status=? AND next_attempt_at<=UTC_TIMESTAMP(3) AND cycle_attempt_count<? ORDER BY next_attempt_at,delivery_id LIMIT ? FOR UPDATE SKIP LOCKED`, platform, state, delivery.MaxCycleAttempts, limit)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var c candidate
			c.claim.Target.PlatformID = platform
			c.claim.Owner = owner
			if err = rows.Scan(&c.claim.Target.DeliveryID, &c.msgID, &c.claim.Target.PlatformUID, &c.claim.LeaseVersion, &c.claim.AttemptNo, &c.claim.CycleNo, &c.claim.CycleAttempt, &c.due); err != nil {
				rows.Close()
				return nil, err
			}
			selected = append(selected, c)
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	sort.Slice(selected, func(i, j int) bool {
		if selected[i].due.Equal(selected[j].due) {
			return selected[i].claim.Target.DeliveryID < selected[j].claim.Target.DeliveryID
		}
		return selected[i].due.Before(selected[j].due)
	})
	if len(selected) > limit {
		selected = selected[:limit]
	}
	leaseClockStart := time.Now()
	var now time.Time
	if len(selected) > 0 {
		if err = tx.QueryRowContext(ctx, `SELECT UTC_TIMESTAMP(3)`).Scan(&now); err != nil {
			return nil, err
		}
	}
	claims := make([]delivery.RecoveryClaim, 0, len(selected))
	for _, c := range selected {
		var body []byte
		if err = tx.QueryRowContext(ctx, `SELECT first_snapshot FROM trade_message WHERE msg_id=?`, c.msgID).Scan(&body); err != nil {
			return nil, err
		}
		var snapshot model.FirstRoundSnapshot
		if len(body) > 256<<10 || json.Unmarshal(body, &snapshot) != nil || snapshot.Trade.MsgID != c.msgID {
			return nil, errors.New("recovery source snapshot unavailable")
		}
		trade := snapshot.Trade
		c.claim.ReceivedAt = snapshot.ReceivedAt
		c.claim.Notification = model.TradeNotification{SecCode: trade.SecCode, SecName: trade.SecName, Side: trade.Side, Market: trade.Market, TradeType: trade.TradeType, TraceID: trade.TraceID, ExecTime: trade.ExecTime}
		c.claim.LeaseVersion++
		c.claim.AttemptNo++
		c.claim.CycleAttempt++
		c.claim.LeaseUntil = now.Add(15 * time.Second)
		_, err = tx.ExecContext(ctx, `UPDATE delivery_task SET status='IN_FLIGHT',lease_owner=?,lease_version=?,lease_until=?,attempt_count=?,cycle_attempt_count=?,next_attempt_at=NULL WHERE delivery_id=?`, owner, c.claim.LeaseVersion, c.claim.LeaseUntil, c.claim.AttemptNo, c.claim.CycleAttempt, c.claim.Target.DeliveryID)
		if err != nil {
			return nil, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO delivery_attempt(delivery_id,attempt_no,cycle_no,send_started,protocol_version,prepared_payload,started_at,finished_at,outcome,summary,lease_owner,lease_version) VALUES(?,?,?,false,'',?, ?,NULL,'IN_FLIGHT','',?,?)`, c.claim.Target.DeliveryID, c.claim.AttemptNo, c.claim.CycleNo, []byte{}, now, owner, c.claim.LeaseVersion)
		if err != nil {
			return nil, err
		}
		claims = append(claims, c.claim)
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	for i := range claims {
		claims[i].LeaseDeadline = leaseClockStart.Add(15*time.Second - time.Millisecond)
	}
	return claims, nil
}

// lockClaim uses database time, not the worker's local clock or mutable counters.
func lockClaim(ctx context.Context, tx *sql.Tx, c delivery.RecoveryClaim) (int, error) {
	var cycleAttempts int
	var until, now time.Time
	err := tx.QueryRowContext(ctx, `SELECT cycle_attempt_count,lease_until FROM delivery_task WHERE delivery_id=? AND platform_id=? AND platform_uid=? AND status='IN_FLIGHT' AND lease_owner=? AND lease_version=? AND lease_until>UTC_TIMESTAMP(3) AND attempt_count=? AND cycle_no=? FOR UPDATE`, c.Target.DeliveryID, c.Target.PlatformID, c.Target.PlatformUID, c.Owner, c.LeaseVersion, c.AttemptNo, c.CycleNo).Scan(&cycleAttempts, &until)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, delivery.ErrLeaseLost
	}
	if err != nil {
		return 0, err
	}
	// MySQL CURRENT_TIMESTAMP is fixed at statement start, before a row-lock
	// wait. Read it again after the lock is held so an expired owner cannot win.
	if err = tx.QueryRowContext(ctx, `SELECT UTC_TIMESTAMP(3)`).Scan(&now); err != nil {
		return 0, err
	}
	if !now.Before(until) {
		return 0, delivery.ErrLeaseLost
	}
	return cycleAttempts, nil
}
func (s *RecoveryStore) SavePrepared(ctx context.Context, c delivery.RecoveryClaim, p model.PreparedRequest) error {
	if p.PlatformID != c.Target.PlatformID || p.ProtocolVersion == "" || len(p.ProtocolVersion) > 64 || len(p.Payload) == 0 || len(p.Payload) > 8192 {
		return errors.New("invalid recovery prepared request")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = lockClaim(ctx, tx, c); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE delivery_attempt SET protocol_version=?,prepared_payload=? WHERE delivery_id=? AND attempt_no=? AND lease_owner=? AND lease_version=? AND finished_at IS NULL AND protocol_version=''`, p.ProtocolVersion, p.Payload, c.Target.DeliveryID, c.AttemptNo, c.Owner, c.LeaseVersion)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return delivery.ErrLeaseLost
	}
	r, err := tx.ExecContext(ctx, `UPDATE delivery_task SET protocol_version=?,prepared_payload=? WHERE delivery_id=? AND lease_until>UTC_TIMESTAMP(3)`, p.ProtocolVersion, p.Payload, c.Target.DeliveryID)
	if err != nil {
		return err
	}
	if n, err = r.RowsAffected(); err != nil {
		return err
	}
	if n != 1 {
		// The task's current DTO may equal the previous attempt's DTO. MySQL
		// then reports zero changed rows despite a matching, valid lease.
		if _, err = lockClaim(ctx, tx, c); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (s *RecoveryStore) Complete(ctx context.Context, c delivery.RecoveryClaim, result model.SendResult) error {
	switch result.Outcome {
	case model.Success, model.RetryableFailure, model.PermanentFailure, model.Unknown:
	default:
		return errors.New("invalid recovery result")
	}
	if len(result.Summary) > 2048 || result.Outcome == model.Success && !result.SendStarted {
		return errors.New("invalid recovery result metadata")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	attempts, err := lockClaim(ctx, tx, c)
	if err != nil {
		return err
	}
	r, err := tx.ExecContext(ctx, `UPDATE delivery_attempt SET outcome=?,summary=?,send_started=?,finished_at=UTC_TIMESTAMP(3) WHERE delivery_id=? AND attempt_no=? AND lease_owner=? AND lease_version=? AND finished_at IS NULL AND (?<>'SUCCESS' OR (OCTET_LENGTH(prepared_payload)>0 AND protocol_version<>''))`, string(result.Outcome), result.Summary, result.SendStarted, c.Target.DeliveryID, c.AttemptNo, c.Owner, c.LeaseVersion, string(result.Outcome))
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return delivery.ErrLeaseLost
	}
	state := "RETRY_WAIT"
	if result.Outcome == model.Success {
		state = "SUCCEEDED"
	} else if result.Outcome == model.PermanentFailure || attempts >= delivery.MaxCycleAttempts {
		state = "NEEDS_ATTENTION"
	}
	// Persist from database time; a process restart cannot reset this schedule.
	delay := delivery.RetryDelay(attempts)
	r, err = tx.ExecContext(ctx, `UPDATE delivery_task SET status=?,last_outcome=?,lease_owner='',lease_until=NULL,next_attempt_at=IF(?='RETRY_WAIT',TIMESTAMPADD(MICROSECOND,?,UTC_TIMESTAMP(3)),NULL) WHERE delivery_id=? AND lease_until>UTC_TIMESTAMP(3)`, state, string(result.Outcome), state, delay.Microseconds(), c.Target.DeliveryID)
	if err != nil {
		return err
	}
	if n, err = r.RowsAffected(); err != nil {
		return err
	}
	if n != 1 {
		return delivery.ErrLeaseLost
	}
	return tx.Commit()
}
