CREATE TABLE IF NOT EXISTS received_notification (
    receive_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    user_uid VARCHAR(128) NOT NULL,
    trace_id VARCHAR(128) NOT NULL,
    payload MEDIUMBLOB NOT NULL,
    received_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_received_time (received_at, receive_id),
    INDEX idx_received_uid (user_uid, received_at, receive_id),
    INDEX idx_received_trace (trace_id, received_at, receive_id)
) ENGINE=InnoDB;
