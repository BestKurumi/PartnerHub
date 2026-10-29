-- Recovery leases and immutable attempt ownership. Additive migration.
SET @recovery_ddl = IF(EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='delivery_task' AND column_name='lease_owner'), 'SELECT 1', 'ALTER TABLE delivery_task ADD COLUMN lease_owner VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '''', ADD COLUMN lease_version BIGINT UNSIGNED NOT NULL DEFAULT 0, ADD COLUMN lease_until DATETIME(3) NULL');
PREPARE recovery_stmt FROM @recovery_ddl;
EXECUTE recovery_stmt;
DEALLOCATE PREPARE recovery_stmt;
SET @recovery_ddl = IF(EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='delivery_attempt' AND column_name='lease_owner'), 'SELECT 1', 'ALTER TABLE delivery_attempt ADD COLUMN lease_owner VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '''', ADD COLUMN lease_version BIGINT UNSIGNED NOT NULL DEFAULT 0');
PREPARE recovery_stmt FROM @recovery_ddl;
EXECUTE recovery_stmt;
DEALLOCATE PREPARE recovery_stmt;
ALTER TABLE delivery_attempt MODIFY COLUMN finished_at DATETIME(3) NULL;
SET @recovery_ddl = IF(EXISTS(SELECT 1 FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='delivery_task' AND index_name='idx_tasks_due'), 'SELECT 1', 'ALTER TABLE delivery_task ADD INDEX idx_tasks_due(platform_id,status,next_attempt_at,delivery_id)');
PREPARE recovery_stmt FROM @recovery_ddl;
EXECUTE recovery_stmt;
DEALLOCATE PREPARE recovery_stmt;
SET @recovery_ddl = IF(EXISTS(SELECT 1 FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='delivery_task' AND index_name='idx_tasks_expiry'), 'SELECT 1', 'ALTER TABLE delivery_task ADD INDEX idx_tasks_expiry(status,lease_until,delivery_id)');
PREPARE recovery_stmt FROM @recovery_ddl;
EXECUTE recovery_stmt;
DEALLOCATE PREPARE recovery_stmt;
