# 外部合作中台 MVP 开发提案

## Why

已确认并归档的技术设计 v2.0 尚无对应运行应用。本 change 将其转化为可运行、可演示、可测试的本地 MVP；先打通正常业务闭环，再交付异常恢复、多实例、观测与实测。

## What Changes

- 实现 Go/Gin 模块化中台、上游 Mock、HTTP 平台 1 Mock 与 Kafka 平台 2 Mock，提供两平台实际接收消息中心及独立中台运维入口。
- 通过 Kafka 消费绑定/解绑事件，维护 MySQL 权威关系与 Redis 完整快照，按当前绑定固定 UID 路由；HTTP 成交请求直接首次投递。
- 正常成功/明确无路由经 Redis 暂存后返回、异步归档；失败/不确定/路由异常经 MySQL 同事务确认接管，有限重试、人工重试及租约恢复。
- 更新既有 SQL/Compose/Nginx/监控草案以匹配批准方案，增加最小可运行测试设施及真实中间件集成验收，逐切片执行 RED/GREEN/REFACTOR。
- 正常闭环优先：绑定 → 合法成交 → 双 Adapter → 双平台实际接收 → 客户页面 → 归档/msgId 查询。S1 不将后续故障演练及压测作为前置，不虚假承诺尚未完成的异常接管。

### 范围与非目标

完整 MVP 范围覆盖设计六模块、三个 Mock、两个 hub 实例、单节点 MySQL/Redis/Kafka、本地页面与监控。保留单个开发 change、内部按 S1～S4 验收：协议、任务模型、恢复和页面共享同一数据契约，阶段目标作为实现顺序，不拆出互相不完整的运行能力。

不做真实平台联调、真实用户身份/完整权限体系、业务成交防重/下游业务去重、成交入口 Kafka、可靠回调、中间件集群容灾或完整平台产品。外发可能重复；上游正常成交输入唯一，不将重复输入作为本版验收。

### 已确认来源与待实测项

- 唯一正式技术设计：[归档 design.md v2.0](../2026-10-22-partner-hub-technical-design/design.md)。确认摘要：[brainstorm-summary.md](../2026-10-22-partner-hub-technical-design/.comet/handoff/brainstorm-summary.md)。设计交付提交：e3dc204。
- 业务依据：[需求分析 v1.9](../../../../外部合作中台-MVP需求分析.md)；外部协议最高依据：[实习项目协议](../../../../实习项目文档-陈开鑫.md)；[技术选型 v1.5](../../../../外部合作中台-技术选型调研.md)。已确认结论直接复用，不重新访谈。
- 依赖具体版本、并发/批次/超时/容量初值须实施锁定及实测；真实生产方/鉴权未知不虚构为 Demo 必须事实。不存在阻碍业务规格编写的未定方案。

## Capabilities

### New Capabilities

- `trade-ingress`：成交字段校验、入口门控、直接首发编排与 HTTP 确认语义。
- `customer-bindings`：绑定事件一致性、平台范围、完整缓存与有界回源。
- `platform-notifications`：固定路由、双平台白名单报文、时间/类型映射及发送结果。
- `delivery-records`：正常暂存/归档、异常事务接管及 MySQL 权威状态查询。
- `delivery-recovery`：有限重试、平台隔离、租约恢复及幂等人工命令。
- `demo-experience`：唯一输入上游 Mock、双平台实际接收记录、绑定操作及页面展示。
- `runtime-observability`：可复现部署、多实例生命周期、有界容量、指标与条件化实测。

### Modified Capabilities

无。现有主 specs 为空；本次建立业务规格，不沿用设计 change 的 skip_specs:true 或已完成任务。

## Impact

新增 cmd/hub、cmd/mock、internal/{ingress,binding,routing,delivery,adapter,query,infra,config}、Go 依赖与行为测试；调整 deploy/mysql、Compose、Nginx、监控及演示脚本，淘汰与批准设计冲突的历史运行草案依赖。外部报文严格遵守协议，不新增外发 msgId 或去重 Header。

用户已要求沿用 partner-hub 当前目录；继续 current/main 串行工作。既有工具安装文件留在工作区，不因大量未跟踪文件而全仓库暂存；运行草案仅在对应实现任务中修改并验收。
