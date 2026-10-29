package platform2

import (
	"context"
	"database/sql"
)

type Receipt struct {
	ReceiveID, Topic, UserUID, TraceID string
	Partition                          int32
	Offset                             int64
	Payload                            []byte
}
type InvalidRecord struct {
	Topic     string
	Partition int32
	Offset    int64
	Payload   []byte
}
type Store interface {
	Save(context.Context, Receipt) error
	SaveInvalid(context.Context, InvalidRecord) error
}
type MySQLStore struct{ db *sql.DB }

func NewMySQLStore(db *sql.DB) *MySQLStore { return &MySQLStore{db} }
func (s *MySQLStore) Save(ctx context.Context, r Receipt) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO received_notification(receive_id,source_topic,source_partition,source_offset,user_uid,trace_id,payload) VALUES(?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE receive_id=receive_id`, r.ReceiveID, r.Topic, r.Partition, r.Offset, r.UserUID, r.TraceID, r.Payload)
	return err
}
func (s *MySQLStore) SaveInvalid(ctx context.Context, r InvalidRecord) error {
	body := r.Payload
	if len(body) > 8192 {
		body = body[:8192]
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO invalid_notification(source_topic,source_partition,source_offset,payload,reason) VALUES(?,?,?,?,'INVALID_PLATFORM2_NOTIFICATION') ON DUPLICATE KEY UPDATE source_offset=source_offset`, r.Topic, r.Partition, r.Offset, body)
	return err
}
