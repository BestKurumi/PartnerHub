-- Rerunnable additive migration: retain existing data and indexes.
SET @query_index_ddl = IF(EXISTS(SELECT 1 FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='binding_current' AND index_name='idx_binding_time'), 'SELECT 1', 'CREATE INDEX idx_binding_time ON binding_current(updated_at,client_id,platform_id)');
PREPARE query_index_stmt FROM @query_index_ddl;
EXECUTE query_index_stmt;
DEALLOCATE PREPARE query_index_stmt;
SET @query_index_ddl = IF(EXISTS(SELECT 1 FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='binding_current' AND index_name='idx_binding_platform_time'), 'SELECT 1', 'CREATE INDEX idx_binding_platform_time ON binding_current(platform_id,updated_at,client_id)');
PREPARE query_index_stmt FROM @query_index_ddl;
EXECUTE query_index_stmt;
DEALLOCATE PREPARE query_index_stmt;
SET @query_index_ddl = IF(EXISTS(SELECT 1 FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='delivery_task' AND index_name='idx_tasks_platform_message'), 'SELECT 1', 'CREATE INDEX idx_tasks_platform_message ON delivery_task(platform_id,msg_id,delivery_id)');
PREPARE query_index_stmt FROM @query_index_ddl;
EXECUTE query_index_stmt;
DEALLOCATE PREPARE query_index_stmt;
SET @query_index_ddl = IF(EXISTS(SELECT 1 FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='delivery_attempt' AND index_name='idx_attempt_time'), 'SELECT 1', 'CREATE INDEX idx_attempt_time ON delivery_attempt(delivery_id,started_at,attempt_no)');
PREPARE query_index_stmt FROM @query_index_ddl;
EXECUTE query_index_stmt;
DEALLOCATE PREPARE query_index_stmt;
