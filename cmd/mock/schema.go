package main

import (
	"context"
	"database/sql"
)

func checkSchema(ctx context.Context, db *sql.DB, platform string) error {
	queries := []string{"SELECT receive_id,user_uid,trace_id,payload,received_at FROM received_notification LIMIT 0"}
	if platform == "PLATFORM_B" {
		queries = append(queries,
			"SELECT source_topic,source_partition,source_offset FROM received_notification LIMIT 0",
			"SELECT source_topic,source_partition,source_offset,payload,reason,received_at FROM invalid_notification LIMIT 0")
	}
	for _, query := range queries {
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		if err = rows.Close(); err != nil {
			return err
		}
	}
	return nil
}
