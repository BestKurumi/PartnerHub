package main

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

type Observation struct {
	FirstOutcome, FirstLeaseOwner string
	Received, Stored              time.Time
	Platform, Outcome             string
	Started                       bool
	Finished, Recovery            *time.Time
}

func applyObservation(r *Input, o Observation) {
	r.ReceivedAt = &o.Received
	r.StoredAt = &o.Stored
	if o.Platform != "" {
		first := o.FirstOutcome
		if first == "" {
			first = o.Outcome
		}
		started, finished := o.Started, o.Finished
		recovery := o.Recovery
		if o.FirstLeaseOwner != "" || first == "NOT_ATTEMPTED" {
			started = false
			finished = nil
			if o.Outcome == "SUCCESS" && o.Finished != nil && (recovery == nil || o.Finished.After(*recovery)) {
				recovery = o.Finished
			}
		}
		r.Targets[o.Platform] = Target{FirstOutcome: first, SendStarted: started, FirstFinished: finished, RecoveredAt: recovery}
	}
}

// At most 100 run-owned IDs, two expected platforms each. No unscoped table scan.
func collect(ctx context.Context, db *sql.DB, inputs []Input) error {
	for start := 0; start < len(inputs); start += 100 {
		end := min(start+100, len(inputs))
		index := map[string]*Input{}
		args := []any{}
		for i := start; i < end; i++ {
			if inputs[i].SubmittedAt != nil {
				index[inputs[i].MsgID] = &inputs[i]
				args = append(args, inputs[i].MsgID)
			}
		}
		if len(args) == 0 {
			continue
		}
		query := `SELECT m.msg_id,m.received_at,m.stored_at,t.platform_id,t.first_outcome,a.outcome,a.send_started,a.finished_at,a.lease_owner,
 (SELECT MAX(z.finished_at) FROM delivery_attempt z WHERE z.delivery_id=t.delivery_id AND (z.attempt_no>1 OR z.lease_owner<>'') AND z.outcome='SUCCESS')
 FROM trade_message m LEFT JOIN delivery_task t ON t.msg_id=m.msg_id
 LEFT JOIN delivery_attempt a ON a.delivery_id=t.delivery_id AND a.attempt_no=1
 WHERE m.msg_id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(args)), ",") + `) LIMIT 201`
		batch, cancel := context.WithTimeout(ctx, 2*time.Second)
		records, err := db.QueryContext(batch, query, args...)
		if err != nil {
			cancel()
			return err
		}
		count := 0
		for records.Next() {
			count++
			var id string
			var o Observation
			var platform, firstOutcome, outcome, owner sql.NullString
			var started sql.NullBool
			var finished, recovery sql.NullTime
			err = records.Scan(&id, &o.Received, &o.Stored, &platform, &firstOutcome, &outcome, &started, &finished, &owner, &recovery)
			if err != nil {
				break
			}
			o.Platform = platform.String
			o.FirstOutcome = firstOutcome.String
			o.FirstLeaseOwner = owner.String
			o.Outcome = outcome.String
			o.Started = started.Bool
			if finished.Valid {
				o.Finished = &finished.Time
			}
			if recovery.Valid {
				o.Recovery = &recovery.Time
			}
			if r := index[id]; r != nil {
				applyObservation(r, o)
				observeVisible(r, time.Now().UTC())
			}
		}
		if err == nil {
			err = records.Err()
		}
		records.Close()
		cancel()
		if err != nil {
			return err
		}
		if count > 200 {
			return sql.ErrNoRows
		}
	}
	return nil
}

func settle(ctx context.Context, db *sql.DB, inputs []Input, duration time.Duration) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	errors := 0
	var last error
	for {
		if ctx.Err() != nil {
			return errors, last
		}
		last = collect(ctx, db, inputs)
		if last != nil {
			errors++
		}
		complete := last == nil
		for _, r := range inputs {
			if r.Disposition != "accepted" && r.Disposition != "transport_error" && r.Disposition != "invalid_response" && r.Disposition != "http_unconfirmed" {
				continue
			}
			if r.StoredAt == nil || len(r.Targets) != 2 {
				complete = false
				continue
			}
			for _, p := range r.Targets {
				if p.FirstOutcome != "SUCCESS" && p.RecoveredAt == nil {
					complete = false
				}
			}
		}
		if complete {
			return errors, nil
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors, last
		case <-timer.C:
		}
	}
}

func observeVisible(r *Input, now time.Time) {
	if r.ObservedVisibleAt == nil {
		r.ObservedVisibleAt = &now
	}
}
