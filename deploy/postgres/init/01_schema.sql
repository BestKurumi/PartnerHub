-- ============================================================================
-- 外部合作中台 · MVP 初始化表结构
--
-- ⚠️ 本文件只在 PostgreSQL 数据卷【首次初始化】时执行一次。
--    修改后需 `make reset`（会删除数据卷）才会重新执行。
--
-- 对应需求文档「七、概念数据模型」。
-- ============================================================================

-- ---------------------------------------------------------------- 平台
-- 平台是一等维度：协议 / 字段映射 / 范围规则全部收敛在这里 + 适配器代码。
CREATE TABLE IF NOT EXISTS platform (
    platform_id     TEXT        PRIMARY KEY,
    name            TEXT        NOT NULL,
    enabled         BOOLEAN     NOT NULL DEFAULT TRUE,
    protocol_config JSONB       NOT NULL DEFAULT '{}'::jsonb,
    field_mapping   JSONB       NOT NULL DEFAULT '{}'::jsonb,
    scope_rule      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE  platform                 IS '外部合作平台配置，平台标识贯穿配置/消息/数据/缓存键/日志/指标';
COMMENT ON COLUMN platform.protocol_config IS '推送协议与鉴权配置。密钥不放这里，走环境变量（需求：配置中不硬编码密钥）';
COMMENT ON COLUMN platform.field_mapping   IS '内部标准成交事件 → 平台报文的字段映射规则';
COMMENT ON COLUMN platform.scope_rule      IS '客户范围筛选规则；MVP 只用「绑定关系 + 账户有效状态」，其余留钩子';

-- ---------------------------------------------------------------- 客户绑定（事件日志，append-only）
-- 存的是「事件」而非「当前状态」：因为存在乱序/迟到/重放，
-- 最终状态必须由事件时间裁决，不能按到达顺序覆盖（需求 2.3）。
-- 当前状态通过下方视图 customer_binding_current 推导。
CREATE TABLE IF NOT EXISTS customer_binding (
    id                   BIGSERIAL   PRIMARY KEY,
    internal_customer_id TEXT        NOT NULL,
    platform_id          TEXT        NOT NULL REFERENCES platform (platform_id),
    platform_uid         TEXT        NOT NULL,
    status               TEXT        NOT NULL CHECK (status IN ('BOUND', 'UNBOUND')),
    event_id             TEXT        NOT NULL,
    event_time           TIMESTAMPTZ NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE  customer_binding         IS '绑定/解绑事件日志；事件可重放以重建客户范围缓存';
COMMENT ON COLUMN customer_binding.event_id IS '上游事件唯一键，用于消费幂等（重复事件不产生重复记录）';

-- 幂等：同一事件重复到达只落一条
CREATE UNIQUE INDEX IF NOT EXISTS uq_customer_binding_event_id
    ON customer_binding (event_id);

-- ============================================================================
-- TODO(待确认 问题① —— 同平台多 UID)：
--   唯一约束取决于 mentor 结论，【暂不添加】。两种可能：
--     (a) 同一客户在同一平台只能绑 1 个账号：
--         CREATE UNIQUE INDEX uq_binding_customer_platform
--             ON customer_binding (internal_customer_id, platform_id)
--             WHERE status = 'BOUND';
--     (b) 允许同平台多账号（1:N）：
--         CREATE UNIQUE INDEX uq_binding_platform_uid
--             ON customer_binding (platform_id, platform_uid)
--             WHERE status = 'BOUND';
--   结论落定后再补，并把结论同步回需求文档第十一章 C 类开放问题。
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_binding_customer
    ON customer_binding (internal_customer_id, platform_id);
CREATE INDEX IF NOT EXISTS idx_binding_platform_uid
    ON customer_binding (platform_id, platform_uid);
CREATE INDEX IF NOT EXISTS idx_binding_event_time
    ON customer_binding (event_time);

-- 当前状态视图：每个 (平台, 平台UID) 取事件时间最新的一条。
-- 「先解绑后到绑定」这类乱序，靠 event_time 而非到达顺序裁决（需求 2.5）。
CREATE OR REPLACE VIEW customer_binding_current AS
SELECT DISTINCT ON (platform_id, platform_uid)
       id,
       internal_customer_id,
       platform_id,
       platform_uid,
       status,
       event_time
FROM customer_binding
ORDER BY platform_id, platform_uid, event_time DESC, id DESC;

-- ---------------------------------------------------------------- 成交消息
CREATE TABLE IF NOT EXISTS deal_message (
    message_id           TEXT        PRIMARY KEY,
    internal_customer_id TEXT        NOT NULL,
    deal_time            TIMESTAMPTZ NOT NULL,
    symbol               TEXT        NOT NULL,
    symbol_name          TEXT,
    side                 TEXT        NOT NULL CHECK (side IN ('BUY', 'SELL')),
    price                NUMERIC(20, 6),
    quantity             NUMERIC(20, 6),
    amount               NUMERIC(20, 6),
    trace_id             TEXT,
    received_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_payload          JSONB
);

COMMENT ON TABLE  deal_message          IS '接入即落库的内部标准成交事件';
COMMENT ON COLUMN deal_message.deal_time IS '业务时间戳。解绑时序裁决以它为准，不是 received_at';

CREATE INDEX IF NOT EXISTS idx_deal_customer_time
    ON deal_message (internal_customer_id, deal_time DESC);
CREATE INDEX IF NOT EXISTS idx_deal_received_at
    ON deal_message (received_at DESC);

-- ---------------------------------------------------------------- 推送记录
-- 一条成交消息 → N 条推送记录（1:N 扇出），页面展示与可观测的数据源。
CREATE TABLE IF NOT EXISTS push_record (
    id              BIGSERIAL   PRIMARY KEY,
    message_id      TEXT        NOT NULL REFERENCES deal_message (message_id),
    platform_id     TEXT        NOT NULL REFERENCES platform (platform_id),
    -- 路由那一刻固化的 (平台ID, 平台UID)。重试不再回查范围，
    -- 否则解绑后客户被移出范围，在途消息会漏推（需求 2.5 配套约定）。
    platform_uid    TEXT        NOT NULL,
    status          TEXT        NOT NULL CHECK (status IN ('PENDING', 'RETRYING', 'SUCCESS', 'FAILED', 'DROPPED')),
    attempt         INT         NOT NULL DEFAULT 0,
    request_body    JSONB,
    response_status INT,
    response_body   TEXT,
    error_message   TEXT,
    trace_id        TEXT,
    routed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE push_record IS '推送结果记录；(message_id, platform_id) 唯一，保证推送业务上恰好一次';

-- 幂等：以「消息ID × 平台」为去重键（需求 5.1 / 风险「重复推送」）
CREATE UNIQUE INDEX IF NOT EXISTS uq_push_message_platform
    ON push_record (message_id, platform_id);

-- 页面查询：按平台 + 状态筛选
CREATE INDEX IF NOT EXISTS idx_push_platform_status
    ON push_record (platform_id, status);

-- 补偿任务扫描：多副本安全靠 SELECT ... FOR UPDATE SKIP LOCKED
CREATE INDEX IF NOT EXISTS idx_push_retry_scan
    ON push_record (status, updated_at)
    WHERE status IN ('PENDING', 'RETRYING', 'FAILED');

-- ---------------------------------------------------------------- 演示用平台数据
-- ⚠️ 占位数据：真实协议/字段以 mentor 提供的平台文档为准，结论落定后替换。
INSERT INTO platform (platform_id, name, enabled, protocol_config, field_mapping, scope_rule)
VALUES
    ('PLATFORM_A', '合作平台 A（占位）', TRUE,
     '{"protocol": "HTTP_JSON", "auth": "HMAC_SHA256", "timeoutMs": 1500, "retry": {"maxAttempts": 3, "backoff": "exponential"}}'::jsonb,
     '{"dealId": "messageId", "uid": "platformUid", "ts": "dealTime"}'::jsonb,
     '{"type": "BINDING_AND_ACTIVE_ACCOUNT"}'::jsonb),
    ('PLATFORM_B', '合作平台 B（占位）', TRUE,
     '{"protocol": "HTTP_XML", "auth": "API_KEY", "timeoutMs": 2000, "retry": {"maxAttempts": 3, "backoff": "exponential"}}'::jsonb,
     '{"dealId": "messageId", "uid": "platformUid", "ts": "dealTime"}'::jsonb,
     '{"type": "BINDING_AND_ACTIVE_ACCOUNT"}'::jsonb)
ON CONFLICT (platform_id) DO NOTHING;
