package mysql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"partner-hub/internal/delivery"
	"partner-hub/internal/model"
	"partner-hub/internal/routing"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

type RouteRecoveryStore struct{ db *sql.DB }

func NewRouteRecoveryStore(db *sql.DB) *RouteRecoveryStore { return &RouteRecoveryStore{db: db} }
func (s *RouteRecoveryStore) ClaimRoutes(ctx context.Context, owner string, limit int) ([]routing.RouteRecoveryClaim, error) {
	if !validRecoveryOwner(owner) || limit < 1 || limit > 4 {
		return nil, errors.New("invalid route claim parameters")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	type candidate struct {
		claim routing.RouteRecoveryClaim
		due   time.Time
	}
	var selected []candidate
	for _, state := range []string{"PENDING", "RETRY_WAIT"} {
		rows, err := tx.QueryContext(ctx, `SELECT msg_id,client_id,route_lease_version,route_attempt_count,route_cycle_no,route_cycle_attempt_count,route_next_attempt_at FROM trade_message FORCE INDEX(idx_routes_due) WHERE route_status=? AND route_next_attempt_at<=UTC_TIMESTAMP(3) AND route_cycle_attempt_count<? ORDER BY route_next_attempt_at,msg_id LIMIT ? FOR UPDATE SKIP LOCKED`, state, delivery.MaxCycleAttempts, limit)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var c candidate
			c.claim.Owner = owner
			if err = rows.Scan(&c.claim.MsgID, &c.claim.ClientID, &c.claim.LeaseVersion, &c.claim.AttemptNo, &c.claim.CycleNo, &c.claim.CycleAttempt, &c.due); err != nil {
				rows.Close()
				return nil, err
			}
			selected = append(selected, c)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	sort.Slice(selected, func(i, j int) bool {
		if selected[i].due.Equal(selected[j].due) {
			return selected[i].claim.MsgID < selected[j].claim.MsgID
		}
		return selected[i].due.Before(selected[j].due)
	})
	if len(selected) > limit {
		selected = selected[:limit]
	}
	// Sample the local monotonic clock before DB time; transaction and commit consume the original budget.
	leaseStart := time.Now()
	var now time.Time
	if len(selected) > 0 {
		if err = tx.QueryRowContext(ctx, `SELECT UTC_TIMESTAMP(3)`).Scan(&now); err != nil {
			return nil, err
		}
	}
	claims := make([]routing.RouteRecoveryClaim, 0, len(selected))
	for _, c := range selected {
		c.claim.LeaseVersion++
		c.claim.AttemptNo++
		c.claim.CycleAttempt++
		c.claim.LeaseUntil = now.Add(10 * time.Second)
		_, err = tx.ExecContext(ctx, `UPDATE trade_message SET route_status='IN_FLIGHT',route_lease_owner=?,route_lease_version=?,route_lease_until=?,route_attempt_count=?,route_cycle_attempt_count=?,route_next_attempt_at=NULL WHERE msg_id=?`, owner, c.claim.LeaseVersion, c.claim.LeaseUntil, c.claim.AttemptNo, c.claim.CycleAttempt, c.claim.MsgID)
		if err != nil {
			return nil, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO route_attempt(msg_id,attempt_no,cycle_no,started_at,finished_at,outcome,summary,lease_owner,lease_version) VALUES(?,?,?,?,NULL,'IN_FLIGHT','',?,?)`, c.claim.MsgID, c.claim.AttemptNo, c.claim.CycleNo, now, owner, c.claim.LeaseVersion)
		if err != nil {
			return nil, err
		}
		claims = append(claims, c.claim)
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	for i := range claims {
		claims[i].LeaseDeadline = leaseStart.Add(10*time.Second - time.Millisecond)
	}
	return claims, nil
}

// lockRouteClaim reads time again after acquiring the row lock. Statement time
// alone can precede a lock wait and would allow an expired owner to finalize.
func lockRouteClaim(ctx context.Context, tx *sql.Tx, c routing.RouteRecoveryClaim) (time.Time, error) {
	var until, now time.Time
	err := tx.QueryRowContext(ctx, `SELECT route_lease_until FROM trade_message WHERE msg_id=? AND client_id=? AND route_status='IN_FLIGHT' AND route_lease_owner=? AND route_lease_version=? AND route_attempt_count=? AND route_cycle_no=? AND route_cycle_attempt_count=? FOR UPDATE`, c.MsgID, c.ClientID, c.Owner, c.LeaseVersion, c.AttemptNo, c.CycleNo, c.CycleAttempt).Scan(&until)
	if errors.Is(err, sql.ErrNoRows) {
		return now, delivery.ErrLeaseLost
	}
	if err != nil {
		return now, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT UTC_TIMESTAMP(3)`).Scan(&now); err != nil {
		return now, err
	}
	if !now.Before(until) {
		return now, delivery.ErrLeaseLost
	}
	return now, nil
}
func validRouteResult(c routing.RouteRecoveryClaim, r model.RouteSnapshot) bool {
	if r.MsgID != c.MsgID || r.ResolvedAt.IsZero() || len(r.Targets) > 2 {
		return false
	}
	if r.Status == "NO_ROUTE" {
		return len(r.Targets) == 0
	}
	if r.Status != "ROUTED" || len(r.Targets) == 0 {
		return false
	}
	seenPlatforms := map[string]bool{}
	seenIDs := map[string]bool{}
	for _, t := range r.Targets {
		if (t.PlatformID != "PLATFORM_A" && t.PlatformID != "PLATFORM_B") || seenPlatforms[t.PlatformID] || seenIDs[t.DeliveryID] || !utf8.ValidString(t.PlatformUID) || strings.TrimSpace(t.PlatformUID) == "" || utf8.RuneCountInString(t.PlatformUID) > 128 {
			return false
		}
		for _, r := range t.PlatformUID {
			if unicode.IsControl(r) {
				return false
			}
		}
		id := t.DeliveryID
		if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
			return false
		}
		if decoded, err := hex.DecodeString(strings.ReplaceAll(id, "-", "")); err != nil || len(decoded) != 16 {
			return false
		}
		seenPlatforms[t.PlatformID] = true
		seenIDs[t.DeliveryID] = true
	}
	return true
}
func (s *RouteRecoveryStore) CompleteRoute(ctx context.Context, c routing.RouteRecoveryClaim, route model.RouteSnapshot, lookupErr error) error {
	if lookupErr == nil && !validRouteResult(c, route) {
		return errors.New("invalid route recovery result")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now, err := lockRouteClaim(ctx, tx, c)
	if err != nil {
		return err
	}
	state, outcome, summary := route.Status, route.Status, ""
	var next any
	if lookupErr != nil {
		state = "RETRY_WAIT"
		outcome = "QUERY_FAILED"
		summary = "BINDING_LOOKUP_UNAVAILABLE"
		if c.CycleAttempt >= delivery.MaxCycleAttempts {
			state = "NEEDS_ATTENTION"
		} else {
			next = now.Add(delivery.RetryDelay(c.CycleAttempt))
		}
	} else {
		// Plain INSERT deliberately rejects all identity collisions. A failed second
		// target rolls back the first target, snapshot and attempt together.
		for _, target := range route.Targets {
			_, err = tx.ExecContext(ctx, `INSERT INTO delivery_task(delivery_id,msg_id,platform_id,platform_uid,status,first_outcome,last_outcome,protocol_version,prepared_payload,cycle_no,attempt_count,cycle_attempt_count,next_attempt_at) VALUES(?,?,?,?,'PENDING','NOT_ATTEMPTED','NOT_ATTEMPTED','',?,1,0,0,?)`, target.DeliveryID, c.MsgID, target.PlatformID, target.PlatformUID, []byte{}, now)
			if err != nil {
				return err
			}
		}
	}
	r, err := tx.ExecContext(ctx, `UPDATE route_attempt SET finished_at=UTC_TIMESTAMP(3),outcome=?,summary=? WHERE msg_id=? AND attempt_no=? AND cycle_no=? AND lease_owner=? AND lease_version=? AND finished_at IS NULL AND outcome='IN_FLIGHT'`, outcome, summary, c.MsgID, c.AttemptNo, c.CycleNo, c.Owner, c.LeaseVersion)
	if err != nil {
		return err
	}
	if err = oneRouteRow(r); err != nil {
		return err
	}
	if lookupErr == nil {
		snapshot, err := json.Marshal(route)
		if err != nil {
			return err
		}
		r, err = tx.ExecContext(ctx, `UPDATE trade_message SET route_status=?,route_snapshot=?,route_lease_owner='',route_lease_until=NULL,route_next_attempt_at=NULL WHERE msg_id=? AND route_lease_until>UTC_TIMESTAMP(3)`, state, snapshot, c.MsgID)
		if err != nil {
			return err
		}
	} else {
		r, err = tx.ExecContext(ctx, `UPDATE trade_message SET route_status=?,route_lease_owner='',route_lease_until=NULL,route_next_attempt_at=? WHERE msg_id=? AND route_lease_until>UTC_TIMESTAMP(3)`, state, next, c.MsgID)
		if err != nil {
			return err
		}
	}
	if err = oneRouteRow(r); err != nil {
		return err
	}
	return tx.Commit()
}
func oneRouteRow(r sql.Result) error {
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return delivery.ErrLeaseLost
	}
	return nil
}

// RecoverExpiredRoutes records uncertainty without querying bindings or sending.
func (s *RouteRecoveryStore) RecoverExpiredRoutes(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > 100 {
		return 0, errors.New("invalid route expiry limit")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT msg_id,client_id,route_lease_owner,route_lease_version,route_attempt_count,route_cycle_no,route_cycle_attempt_count,route_lease_until FROM trade_message FORCE INDEX(idx_routes_expiry) WHERE route_status='IN_FLIGHT' AND route_lease_until<=UTC_TIMESTAMP(3) ORDER BY route_lease_until,msg_id LIMIT ? FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, err
	}
	var selected []routing.RouteRecoveryClaim
	for rows.Next() {
		var c routing.RouteRecoveryClaim
		if err = rows.Scan(&c.MsgID, &c.ClientID, &c.Owner, &c.LeaseVersion, &c.AttemptNo, &c.CycleNo, &c.CycleAttempt, &c.LeaseUntil); err != nil {
			rows.Close()
			return 0, err
		}
		selected = append(selected, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	if len(selected) == 0 {
		return 0, nil
	}
	var now time.Time
	if err = tx.QueryRowContext(ctx, `SELECT UTC_TIMESTAMP(3)`).Scan(&now); err != nil {
		return 0, err
	}
	count := 0
	for _, c := range selected {
		if now.Before(c.LeaseUntil) {
			continue
		}
		r, err := tx.ExecContext(ctx, `UPDATE route_attempt SET outcome='UNKNOWN',summary='LEASE_EXPIRED',finished_at=? WHERE msg_id=? AND attempt_no=? AND cycle_no=? AND lease_owner=? AND lease_version=? AND finished_at IS NULL AND outcome='IN_FLIGHT'`, now, c.MsgID, c.AttemptNo, c.CycleNo, c.Owner, c.LeaseVersion)
		if err != nil {
			return 0, err
		}
		if err = oneRouteRow(r); err != nil {
			return 0, err
		}
		state := "RETRY_WAIT"
		var next any
		if c.CycleAttempt >= delivery.MaxCycleAttempts {
			state = "NEEDS_ATTENTION"
		} else {
			next = now.Add(delivery.RetryDelay(c.CycleAttempt))
		}
		r, err = tx.ExecContext(ctx, `UPDATE trade_message SET route_status=?,route_lease_owner='',route_lease_until=NULL,route_next_attempt_at=? WHERE msg_id=? AND route_status='IN_FLIGHT' AND route_lease_owner=? AND route_lease_version=? AND route_attempt_count=? AND route_cycle_no=? AND route_cycle_attempt_count=? AND route_lease_until<=?`, state, next, c.MsgID, c.Owner, c.LeaseVersion, c.AttemptNo, c.CycleNo, c.CycleAttempt, now)
		if err != nil {
			return 0, err
		}
		if err = oneRouteRow(r); err != nil {
			return 0, err
		}
		count++
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}
