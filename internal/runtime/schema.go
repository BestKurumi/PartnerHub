package runtime

import (
	"context"
	"database/sql"
	"errors"
)

func CheckSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return errors.New("schema unavailable")
	}
	for _, q := range []string{
		"SELECT msg_id,archive_revision,first_snapshot,route_next_attempt_at,route_lease_owner,route_lease_version,route_lease_until FROM trade_message LIMIT 0",
		"SELECT lease_owner,lease_version,lease_until FROM delivery_task LIMIT 0",
		"SELECT operation_id FROM manual_operation LIMIT 0",
		"SELECT event_id FROM binding_event LIMIT 0",
		"SELECT delivery_id FROM delivery_attempt LIMIT 0",
		"SELECT version,checksum FROM hub_schema_migration LIMIT 0",
	} {
		rows, e := db.QueryContext(ctx, q)
		if e != nil {
			return errors.New("schema unavailable")
		}
		rows.Close()
	}
	var n int
	if e := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM hub_schema_migration").Scan(&n); e != nil || n != 8 {
		return errors.New("schema migrations incomplete")
	}
	return nil
}
