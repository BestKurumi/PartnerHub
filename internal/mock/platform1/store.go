package platform1

import (
	"context"
	"database/sql"
)

type Receipt struct {
	ReceiveID, UserUID, TraceID string
	Payload                     []byte
}
type Store interface {
	Save(context.Context, Receipt) error
}
type MySQLStore struct{ db *sql.DB }

func NewMySQLStore(db *sql.DB) *MySQLStore { return &MySQLStore{db: db} }
func (s *MySQLStore) Save(ctx context.Context, r Receipt) error {
	// One autocommit insert is the independent receiver's durable boundary.
	// Deliberately no business unique key on traceId, UID or payload.
	_, err := s.db.ExecContext(ctx, "INSERT INTO received_notification (receive_id,user_uid,trace_id,payload) VALUES (?,?,?,?)", r.ReceiveID, r.UserUID, r.TraceID, r.Payload)
	return err
}
