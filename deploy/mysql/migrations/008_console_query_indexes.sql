-- Additive, rerunnable; retain all messages and delivery history.
SET @console_index_ddl = IF(EXISTS(SELECT 1 FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='trade_message' AND index_name='idx_messages_trace_time'), 'SELECT 1', 'CREATE INDEX idx_messages_trace_time ON trade_message(trace_id,received_at,msg_id)');
PREPARE console_index_stmt FROM @console_index_ddl;
EXECUTE console_index_stmt;
DEALLOCATE PREPARE console_index_stmt;
