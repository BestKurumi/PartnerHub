package mysql

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"partner-hub/internal/delivery"
)

// RecoverExpired finalizes only the current, unfinished leased attempt. It never
// sends a notification or invents evidence of whether the platform received it.
func (s *RecoveryStore) RecoverExpired(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > 100 {
		return 0, errors.New("invalid expiry recovery limit")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT delivery_id,lease_owner,lease_version,attempt_count,cycle_no,cycle_attempt_count,lease_until FROM delivery_task FORCE INDEX(idx_tasks_expiry) WHERE status='IN_FLIGHT' AND lease_until<=UTC_TIMESTAMP(3) ORDER BY lease_until,delivery_id LIMIT ? FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, err
	}
	type expired struct {
		id, owner                     string
		version                       uint64
		attempt, cycle, cycleAttempts int
		until                         time.Time
	}
	var selected []expired
	for rows.Next() {
		var c expired
		if err = rows.Scan(&c.id, &c.owner, &c.version, &c.attempt, &c.cycle, &c.cycleAttempts, &c.until); err != nil {
			rows.Close()
			return 0, err
		}
		selected = append(selected, c)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	if len(selected) == 0 {
		return 0, nil
	}
	// Obtain time after the row locks: MySQL statement time is sampled before
	// any lock wait and must not arbitrate a transition performed afterwards.
	var now time.Time
	if err = tx.QueryRowContext(ctx, `SELECT UTC_TIMESTAMP(3)`).Scan(&now); err != nil {
		return 0, err
	}
	count := 0
	for _, c := range selected {
		if now.Before(c.until) {
			continue
		}
		r, err := tx.ExecContext(ctx, `UPDATE delivery_attempt SET outcome='UNKNOWN',summary='LEASE_EXPIRED',finished_at=? WHERE delivery_id=? AND attempt_no=? AND cycle_no=? AND lease_owner=? AND lease_version=? AND finished_at IS NULL AND outcome='IN_FLIGHT'`, now, c.id, c.attempt, c.cycle, c.owner, c.version)
		if err != nil {
			return 0, err
		}
		if n, err := r.RowsAffected(); err != nil {
			return 0, err
		} else if n != 1 {
			return 0, delivery.ErrLeaseLost
		}
		state := "RETRY_WAIT"
		var next any
		if c.cycleAttempts >= delivery.MaxCycleAttempts {
			state = "NEEDS_ATTENTION"
		} else {
			next = now.Add(delivery.RetryDelay(c.cycleAttempts))
		}
		r, err = tx.ExecContext(ctx, `UPDATE delivery_task SET status=?,last_outcome='UNKNOWN',lease_owner='',lease_until=NULL,next_attempt_at=? WHERE delivery_id=? AND status='IN_FLIGHT' AND lease_owner=? AND lease_version=? AND attempt_count=? AND cycle_no=? AND cycle_attempt_count=? AND lease_until<=?`, state, next, c.id, c.owner, c.version, c.attempt, c.cycle, c.cycleAttempts, now)
		if err != nil {
			return 0, err
		}
		if n, err := r.RowsAffected(); err != nil {
			return 0, err
		} else if n != 1 {
			return 0, delivery.ErrLeaseLost
		}
		count++
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}
