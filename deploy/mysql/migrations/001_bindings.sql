CREATE TABLE IF NOT EXISTS binding_current (
    client_id VARCHAR(128) NOT NULL,
    platform_id VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    platform_uid VARCHAR(128) NOT NULL,
    status VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    revision BIGINT UNSIGNED NOT NULL,
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (client_id, platform_id),
    INDEX idx_binding_platform (platform_id, status, client_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS binding_event (
    event_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    event_type VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    platform_id VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    client_id VARCHAR(128) NOT NULL,
    platform_uid VARCHAR(128) NOT NULL,
    occurred_at BIGINT NOT NULL,
    result VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    revision BIGINT UNSIGNED NOT NULL,
    source_topic VARCHAR(255) NULL,
    source_partition INT NULL,
    source_offset BIGINT NULL,
    processed_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_binding_events_client (client_id, processed_at, event_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS ingress_anomaly (
    anomaly_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    event_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    reason VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    payload MEDIUMBLOB NOT NULL,
    source_topic VARCHAR(255) NULL,
    source_partition INT NULL,
    source_offset BIGINT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY uq_anomaly_source (source_topic,source_partition,source_offset),
    INDEX idx_anomaly_time (created_at,anomaly_id)
) ENGINE=InnoDB;
