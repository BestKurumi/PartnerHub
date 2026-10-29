CREATE TABLE IF NOT EXISTS received_notification (
 receive_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
 source_topic VARCHAR(249) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 source_partition INT NOT NULL,
 source_offset BIGINT NOT NULL,
 user_uid VARCHAR(128) NOT NULL,
 trace_id VARCHAR(128) NOT NULL,
 payload MEDIUMBLOB NOT NULL,
 received_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 UNIQUE KEY uq_source(source_topic,source_partition,source_offset),
 INDEX idx_received_uid(user_uid,received_at,receive_id),
 INDEX idx_received_trace(trace_id,received_at,receive_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS invalid_notification (
 source_topic VARCHAR(249) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 source_partition INT NOT NULL,
 source_offset BIGINT NOT NULL,
 payload MEDIUMBLOB NOT NULL,
 reason VARCHAR(64) NOT NULL,
 received_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 PRIMARY KEY(source_topic,source_partition,source_offset)
) ENGINE=InnoDB;
