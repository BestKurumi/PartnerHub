CREATE TABLE IF NOT EXISTS trade_message (
  msg_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
  client_id VARCHAR(128) NOT NULL,
  trace_id VARCHAR(128) NOT NULL,
  raw_payload MEDIUMBLOB NOT NULL,
  received_at DATETIME(3) NOT NULL,
  first_status VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  route_status VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  route_snapshot JSON NOT NULL,
  archive_revision CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  stored_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  INDEX idx_messages_time(received_at,msg_id),
  INDEX idx_messages_client_time(client_id,received_at,msg_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS delivery_task (
  delivery_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
  msg_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  platform_id VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  platform_uid VARCHAR(128) NOT NULL,
  status VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  first_outcome VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  last_outcome VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  protocol_version VARCHAR(64) NOT NULL,
  prepared_payload BLOB NOT NULL,
  cycle_no INT UNSIGNED NOT NULL DEFAULT 1,
  attempt_count INT UNSIGNED NOT NULL DEFAULT 1,
  UNIQUE KEY uq_message_platform(msg_id,platform_id),
  INDEX idx_tasks_message(msg_id,delivery_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS delivery_attempt (
  delivery_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  attempt_no INT UNSIGNED NOT NULL,
  cycle_no INT UNSIGNED NOT NULL,
  send_started BOOLEAN NOT NULL,
  protocol_version VARCHAR(64) NOT NULL,
  prepared_payload BLOB NOT NULL,
  started_at DATETIME(3) NOT NULL,
  finished_at DATETIME(3) NOT NULL,
  outcome VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  summary VARCHAR(2048) NOT NULL,
  PRIMARY KEY(delivery_id,attempt_no)
) ENGINE=InnoDB;
