CREATE TABLE IF NOT EXISTS manual_operation (
 operation_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
 command_kind VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 target_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 operator_name VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 reason VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
 retry_cycle INT UNSIGNED NOT NULL,
 result_state VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 INDEX idx_manual_target(command_kind,target_id,created_at,operation_id)
) ENGINE=InnoDB;
