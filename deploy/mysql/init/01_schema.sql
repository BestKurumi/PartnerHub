-- 外部合作中台 · MySQL 8.4 本地初始化草案
-- 仅在 mysql-data 首次初始化时执行。正式字段、任务状态与索引在设计阶段完善。
-- PostgreSQL 旧草案保留在 deploy/postgres 中，不再挂载，也不迁移旧数据。

CREATE TABLE IF NOT EXISTS platform (
    platform_id VARCHAR(128) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    protocol_config JSON NOT NULL DEFAULT (JSON_OBJECT()),
    field_mapping JSON NOT NULL DEFAULT (JSON_OBJECT()),
    scope_rule JSON NOT NULL DEFAULT (JSON_OBJECT()),
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB COMMENT='平台配置；凭据通过环境变量注入';

CREATE TABLE IF NOT EXISTS customer_binding (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    internal_customer_id VARCHAR(128) NOT NULL,
    platform_id VARCHAR(128) NOT NULL,
    platform_uid VARCHAR(128) NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('BOUND', 'UNBOUND')),
    event_id VARCHAR(128) NOT NULL,
    event_time DATETIME(3) NOT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY uq_customer_binding_event_id (event_id),
    KEY idx_binding_customer (internal_customer_id, platform_id),
    KEY idx_binding_platform_uid (platform_id, platform_uid),
    KEY idx_binding_event_time (event_time),
    FOREIGN KEY (platform_id) REFERENCES platform (platform_id)
) ENGINE=InnoDB COMMENT='绑定事件日志，同一事件只保存一次';

-- 当前草案以写入序号表达消费到达顺序，不按业务事件时间裁决。
-- 同一客户在同一平台只取一个当前 UID；并发消费顺序在设计阶段落实。
CREATE OR REPLACE VIEW customer_binding_current AS
SELECT id, internal_customer_id, platform_id, platform_uid, status, event_time
FROM (
    SELECT id, internal_customer_id, platform_id, platform_uid, status, event_time,
           ROW_NUMBER() OVER (
               PARTITION BY internal_customer_id, platform_id ORDER BY id DESC
           ) AS row_num
    FROM customer_binding
) AS ranked
WHERE row_num = 1;

CREATE TABLE IF NOT EXISTS deal_message (
    message_id VARCHAR(128) PRIMARY KEY,
    internal_customer_id VARCHAR(128) NOT NULL,
    deal_time DATETIME(3) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    symbol_name VARCHAR(255),
    side VARCHAR(8) NOT NULL CHECK (side IN ('BUY', 'SELL')),
    price DECIMAL(20, 6),
    quantity DECIMAL(20, 6),
    amount DECIMAL(20, 6),
    trace_id VARCHAR(128),
    received_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    raw_payload JSON,
    KEY idx_deal_customer_time (internal_customer_id, deal_time DESC),
    KEY idx_deal_received_at (received_at DESC)
) ENGINE=InnoDB COMMENT='内部成交事件；数量、单价、金额仅保存在券商域内';

CREATE TABLE IF NOT EXISTS push_record (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    message_id VARCHAR(128) NOT NULL,
    platform_id VARCHAR(128) NOT NULL,
    platform_uid VARCHAR(128) NOT NULL COMMENT '路由时固化，重试不回查绑定关系',
    status VARCHAR(16) NOT NULL CHECK (status IN ('PENDING', 'RETRYING', 'SUCCESS', 'FAILED', 'DROPPED')),
    attempt INT NOT NULL DEFAULT 0,
    request_body JSON,
    response_status INT,
    response_body TEXT,
    error_message TEXT,
    trace_id VARCHAR(128),
    routed_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY uq_push_message_platform (message_id, platform_id),
    KEY idx_push_platform_status (platform_id, status),
    KEY idx_push_retry_scan (status, updated_at),
    FOREIGN KEY (message_id) REFERENCES deal_message (message_id),
    FOREIGN KEY (platform_id) REFERENCES platform (platform_id)
) ENGINE=InnoDB COMMENT='投递任务唯一，不保证外部平台恰好处理一次';

-- 仅占位协议类型；字段映射、鉴权及重试参数在设计阶段确定。
INSERT INTO platform (platform_id, name, protocol_config, scope_rule)
SELECT 'PLATFORM_A', '外部平台 1', JSON_OBJECT('protocol', 'HTTP_JSON'), JSON_OBJECT('type', 'BINDING_ONLY')
WHERE NOT EXISTS (SELECT 1 FROM platform WHERE platform_id = 'PLATFORM_A');

INSERT INTO platform (platform_id, name, protocol_config, scope_rule)
SELECT 'PLATFORM_B', '外部平台 2', JSON_OBJECT('protocol', 'KAFKA'), JSON_OBJECT('type', 'BINDING_ONLY')
WHERE NOT EXISTS (SELECT 1 FROM platform WHERE platform_id = 'PLATFORM_B');
