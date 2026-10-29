-- Additive route recovery leases and indexed scheduler ranges.
SET @route_ddl = IF(EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='trade_message' AND column_name='route_lease_owner'), 'SELECT 1', 'ALTER TABLE trade_message ADD COLUMN route_lease_owner VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '''', ADD COLUMN route_lease_version BIGINT UNSIGNED NOT NULL DEFAULT 0, ADD COLUMN route_lease_until DATETIME(3) NULL');
PREPARE route_stmt FROM @route_ddl;
EXECUTE route_stmt;
DEALLOCATE PREPARE route_stmt;
SET @route_ddl = IF(EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='route_attempt' AND column_name='lease_owner'), 'SELECT 1', 'ALTER TABLE route_attempt ADD COLUMN lease_owner VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '''', ADD COLUMN lease_version BIGINT UNSIGNED NOT NULL DEFAULT 0');
PREPARE route_stmt FROM @route_ddl;
EXECUTE route_stmt;
DEALLOCATE PREPARE route_stmt;
SET @route_ddl = IF(EXISTS(SELECT 1 FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='trade_message' AND index_name='idx_routes_due'), 'SELECT 1', 'ALTER TABLE trade_message ADD INDEX idx_routes_due(route_status,route_next_attempt_at,msg_id)');
PREPARE route_stmt FROM @route_ddl;
EXECUTE route_stmt;
DEALLOCATE PREPARE route_stmt;
SET @route_ddl = IF(EXISTS(SELECT 1 FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='trade_message' AND index_name='idx_routes_expiry'), 'SELECT 1', 'ALTER TABLE trade_message ADD INDEX idx_routes_expiry(route_status,route_lease_until,msg_id)');
PREPARE route_stmt FROM @route_ddl;
EXECUTE route_stmt;
DEALLOCATE PREPARE route_stmt;
