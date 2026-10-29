-- Additive, rerunnable recovery responsibility fields. No data reset.
SET @handoff_ddl = IF(EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='trade_message' AND column_name='first_snapshot'), 'SELECT 1', 'ALTER TABLE trade_message ADD COLUMN first_snapshot MEDIUMBLOB NULL');
PREPARE handoff_stmt FROM @handoff_ddl;
EXECUTE handoff_stmt;
DEALLOCATE PREPARE handoff_stmt;
SET @handoff_ddl = IF(EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='trade_message' AND column_name='route_error'), 'SELECT 1', 'ALTER TABLE trade_message ADD COLUMN route_error VARCHAR(2048) NOT NULL DEFAULT ''''');
PREPARE handoff_stmt FROM @handoff_ddl;
EXECUTE handoff_stmt;
DEALLOCATE PREPARE handoff_stmt;
SET @handoff_ddl = IF(EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='trade_message' AND column_name='route_cycle_no'), 'SELECT 1', 'ALTER TABLE trade_message ADD COLUMN route_cycle_no INT UNSIGNED NOT NULL DEFAULT 1');
PREPARE handoff_stmt FROM @handoff_ddl;
EXECUTE handoff_stmt;
DEALLOCATE PREPARE handoff_stmt;
SET @handoff_ddl = IF(EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='trade_message' AND column_name='route_attempt_count'), 'SELECT 1', 'ALTER TABLE trade_message ADD COLUMN route_attempt_count INT UNSIGNED NOT NULL DEFAULT 0');
PREPARE handoff_stmt FROM @handoff_ddl;
EXECUTE handoff_stmt;
DEALLOCATE PREPARE handoff_stmt;
SET @handoff_ddl = IF(EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='trade_message' AND column_name='route_cycle_attempt_count'), 'SELECT 1', 'ALTER TABLE trade_message ADD COLUMN route_cycle_attempt_count INT UNSIGNED NOT NULL DEFAULT 0');
PREPARE handoff_stmt FROM @handoff_ddl;
EXECUTE handoff_stmt;
DEALLOCATE PREPARE handoff_stmt;
SET @handoff_ddl = IF(EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='trade_message' AND column_name='route_next_attempt_at'), 'SELECT 1', 'ALTER TABLE trade_message ADD COLUMN route_next_attempt_at DATETIME(3) NULL');
PREPARE handoff_stmt FROM @handoff_ddl;
EXECUTE handoff_stmt;
DEALLOCATE PREPARE handoff_stmt;
SET @handoff_ddl = IF(EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='delivery_task' AND column_name='cycle_attempt_count'), 'SELECT 1', 'ALTER TABLE delivery_task ADD COLUMN cycle_attempt_count INT UNSIGNED NOT NULL DEFAULT 1');
PREPARE handoff_stmt FROM @handoff_ddl;
EXECUTE handoff_stmt;
DEALLOCATE PREPARE handoff_stmt;
SET @handoff_ddl = IF(EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='delivery_task' AND column_name='next_attempt_at'), 'SELECT 1', 'ALTER TABLE delivery_task ADD COLUMN next_attempt_at DATETIME(3) NULL');
PREPARE handoff_stmt FROM @handoff_ddl;
EXECUTE handoff_stmt;
DEALLOCATE PREPARE handoff_stmt;
CREATE TABLE IF NOT EXISTS route_attempt (
 msg_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 attempt_no INT UNSIGNED NOT NULL,
 cycle_no INT UNSIGNED NOT NULL,
 started_at DATETIME(3) NOT NULL,
 finished_at DATETIME(3) NULL,
 outcome VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 summary VARCHAR(2048) NOT NULL,
 PRIMARY KEY(msg_id,attempt_no)
) ENGINE=InnoDB;
