package mysql

import (
	"context"
	"database/sql"
	"time"
)

type RecoveryObservation struct {
	Platform, Status string
	Count            int64
	Age              time.Duration
}

// RecoveryObservations uses the existing bounded query pool and a strict
// deadline. Aggregate query failure leaves the last sample visibly unhealthy.
func RecoveryObservations(ctx context.Context, db *sql.DB) ([]RecoveryObservation, error) {
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT t.platform_id,t.status,COUNT(*),GREATEST(0,TIMESTAMPDIFF(MICROSECOND,MIN(m.received_at),UTC_TIMESTAMP(3))) FROM delivery_task t JOIN trade_message m ON m.msg_id=t.msg_id WHERE t.status IN ('PENDING','RETRY_WAIT','IN_FLIGHT','NEEDS_ATTENTION') GROUP BY t.platform_id,t.status`)
	if err != nil {
		return nil, err
	}
	result := []RecoveryObservation{}
	seen := map[string]bool{}
	for rows.Next() {
		var s RecoveryObservation
		var micros int64
		if err = rows.Scan(&s.Platform, &s.Status, &s.Count, &micros); err != nil {
			rows.Close()
			return nil, err
		}
		s.Age = time.Duration(micros) * time.Microsecond
		result = append(result, s)
		seen[s.Platform+"/"+s.Status] = true
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for _, p := range []string{"PLATFORM_A", "PLATFORM_B"} {
		for _, state := range []string{"PENDING", "RETRY_WAIT", "IN_FLIGHT", "NEEDS_ATTENTION"} {
			if !seen[p+"/"+state] {
				result = append(result, RecoveryObservation{Platform: p, Status: state})
			}
		}
	}
	rows, err = db.QueryContext(ctx, `SELECT route_status,COUNT(*),GREATEST(0,TIMESTAMPDIFF(MICROSECOND,MIN(received_at),UTC_TIMESTAMP(3))) FROM trade_message WHERE route_status IN ('PENDING','RETRY_WAIT','IN_FLIGHT','NEEDS_ATTENTION') GROUP BY route_status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	seen = map[string]bool{}
	for rows.Next() {
		s := RecoveryObservation{Platform: "ROUTE"}
		var micros int64
		if err = rows.Scan(&s.Status, &s.Count, &micros); err != nil {
			return nil, err
		}
		s.Age = time.Duration(micros) * time.Microsecond
		result = append(result, s)
		seen[s.Status] = true
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	for _, state := range []string{"PENDING", "RETRY_WAIT", "IN_FLIGHT", "NEEDS_ATTENTION"} {
		if !seen[state] {
			result = append(result, RecoveryObservation{Platform: "ROUTE", Status: state})
		}
	}
	return result, nil
}
