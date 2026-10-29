# 可观测性与消息耗时图

## 单消息耗时链路

中台“消息全链路”支持 msgId 或 Trace ID 精确查找。Trace ID 可能对应多条消息，每页20条、显式翻页并选择某条 msgId 后查看；不将同Trace ID的不同消息拼成一个Span树。查询和切换不重新发送成交。

消息摘要下方的“耗时链路”直接使用持久记录：接收／路由快照解析／数据库插入为事件点；每次平台尝试使用 startedAt 与 finishedAt 绘制执行区间，包含未开始外发的尝试，不等同于纯网络耗时。相同横轴保留平台并发关系；仅对同投递任务中序号相邻、时间有效且不重叠的已加载尝试显示间隔，间隔原因未记录，不能直接认定是排队或退避。数据库插入时间不是精确commit时间。

图与原始尝试表共用分页缓存，每平台20条/页、最多100条。未加载完、加载失败、范围上限均显式提示；“已记录时间跨度”和“最长已加载尝试”仅描述当前可见数据，不能替代完整端到端耗时。缺失／无效／倒序时间不画有效区间，0ms真实保留；点击条目查看平台、固定UID、轮次、时长、外发状态、结果与摘要。

本功能不依赖Prometheus单消息标签，不增加traceId指标标签、OTel或Tempo服务。校验、Redis确认、响应完成及外部平台内部处理未补采集，不生成推测Span；平台B的Broker确认与独立消费实收继续分开核查。跨实例时钟偏差可能影响时间先后，图不自动修正时钟。

## 控制台与采集边界

四个独立入口 `/hub/`、`/mock/upstream/`、`/mock/p1/`、`/mock/p2/` 共用控制台样式，各自维护导航和刷新状态。中台运行总览、全链路、资源和诊断使用真实业务查询和缓存观测；Grafana 链接通过固定的 `HUB_GRAFANA_URL` 配置，Compose 默认指向本机映射端口的 `hub-overview` 看板。

`HUB_PROMETHEUS_URL` 是服务端固定采集目标，原生单机未配置时指标明确不可用；浏览器不能提交 PromQL 或替换采集目标。运维读接口 `/api/v1/ops/*` 和 `/hub/demo/ops/*` 均要求本机同源及 `X-Demo-Token`。默认15秒后台刷新、保留样本时间与 stale/unavailable 状态；HTTP读取不实时访问中间件。

MySQL补采仅使用既有hub账户读取固定全局状态；Redis INFO 和两个应用Kafka topic元数据补充中间件状态，不增加数据库高权限。Go CPU/RSS、MySQL全局连接、Redis maxmemory、Kafka ISR 的含义各不相同，不能解释为宿主/容器资源限额。宿主磁盘、Kafka JVM等未安装exporter的指标显示未采集。

`HUB_DIAGNOSTIC_PEERS=hub-1=http://hub-1:8080,hub-2=http://hub-2:8080` 只允许最多两个固定实例，自身自动排除。内部 `/internal/ops/diagnostics` 使用独立共享 `HUB_OPS_TOKEN` 并限制私网来源；Nginx直接拒绝 `/internal/`。每实例最近1000条诊断，重启可清空，显示bootId/保留范围；不代替持久成交、尝试和人工操作历史。启动入口生成独立内部令牌并执行幂等迁移，不删除数据卷。

指标用于解释实际状态；性能结论需要额外的完整输入实验。Broker 确认、MySQL 可见、外部实际接收是三个独立终点。

## 采集与查看

沿用本地 Compose 环境及外置凭据。Prometheus 每 5 秒、最多 2 秒抓取 `hub-1:8080`、`hub-2:8080`、`mock-platform1:18081`、`mock-platform2:18082`、`mock-upstream:18083` 的 `/metrics`。两个 hub 分别列为 `instance=hub-1/hub-2`，不经 Nginx 负载均衡。应用和 Mock 每进程独立 Registry，包含 Go/process 指标。

通过 `.env` 配置的本机 Prometheus 端口打开 `/targets`，应看到两个 hub 和三个 Mock 全部 UP。Grafana 使用外置用户名与密码登录，查看「外部合作中台 · 首发、归档、恢复与外部消费」。不要在工单或报告中复制渲染后的 Compose 环境、DSN、密码或令牌。`/metrics` 为只读诊断端点，不提供控制能力；本地部署边界仍有效，不能直接当作生产公开接口。

```sh
docker compose --env-file .env exec -T prometheus promtool check config /etc/prometheus/prometheus.yml
```

修改 Prometheus 配置后重启其服务；Grafana 从已挂载目录加载看板。启动与验收入口见根 README 和[开发说明](development.md)。

## 统计口径

| 指标 | 解释与限制 |
| --- | --- |
| `hub_ingress_requests_total{status}` / `hub_ingress_in_flight` | 接入响应分布及当前在途；拒绝与不确定响应同样要看。HTTP 成功响应数量不是外部消费数量。 |
| `hub_ingress_duration_seconds{status}` | HTTP 首轮响应时长，包含正常暂存或异常持久接管。与各平台确认时长分开。 |
| `hub_route_results_total{status}` / `hub_first_targets` | 每消息固定路由状态及目标数（0～2）；NO_ROUTE 为正常零目标终态，后续绑定不补推。未结束的请求不在该观测中。 |
| `hub_first_delivery_total{platform,outcome,send_started}` | 固定路由目标的首次尝试事实；包含失败、UNKNOWN及未发送。B 的 SUCCESS 只表示 Broker 确认。 |
| `hub_first_delivery_duration_seconds{platform,outcome}` | 原接入至平台首次尝试结束，包含路由与平台额度等待；按结果拆开，不用成功样本掩盖失败。尚未结束的输入不在直方图中。 |
| `hub_archive_pending_count` / `hub_archive_pending_bytes` | 共享 Redis 待归档条数及序列化快照会计字节；不是进程队列长度或 Redis 总内存。 |
| `hub_archive_pending_oldest_age_seconds` | 原接入至今的最老待归档年龄，独立于领取租约的调度分数；领取/重领不使消息变年轻。 |
| `hub_archive_flush_total{outcome}` / `hub_archive_batch_size` | 归档执行结果及批量；空批次、重放均不能解释为新增业务量。 |
| `hub_archive_visible_duration_seconds` | 正常快照从接入至归档可见的延迟，与首发和外部实收分开；内部重放可能再次被观测，不可作唯一业务量。 |
| `hub_recovery_success_duration_seconds{platform}` | 原接入至确认恢复成功，包含重试等待；尚未成功与待人工需结合积压查看。 |
| `hub_db_pool_open_connections` / `hub_db_pool_in_use_connections` / `hub_db_pool_max_connections` | 各实例按固定用途 pool 分开的连接数及上限；累计等待使用 `hub_db_pool_wait_total` / `hub_db_pool_wait_seconds_total`，重启会重置。 |
| `hub_redis_used_memory_bytes` / `hub_redis_max_memory_bytes` / `hub_redis_persistence_healthy` | 共享 Redis 内存及 AOF/noeviction 配置健康，与 archive 采样时效共同判断。 |
| `hub_recovery_attempts_total{platform,outcome}` | 后台恢复尝试，与首发独立；不是客户新增成交数。 |
| `hub_lease_recovered_total{kind}` | 过期租约回收；过期时可能已有外部副作用，UNKNOWN不表示未发送或未接收。 |
| `hub_recovery_tasks{platform,status}` / `hub_recovery_oldest_age_seconds{platform,status}` | 共享 MySQL 恢复责任数量及最老年龄，包含待重试、在途和待人工；不把终态历史累加成积压。 |
| `hub_binding_cache_total{outcome}` / `hub_binding_source_total{outcome}` | 缓存结果及数据库回源结果；缓存缺键与明确无绑定不能混为一类。 |
| `hub_binding_consumer_lag` | 中台绑定事件消费组 lag；与 B 通知消费组不同。多个 hub 观测的是同一组，不能相加。 |
| `hub_observation_up{kind}` / `hub_observation_last_success_timestamp_seconds{kind}` | 后台采样健康与最后成功时间。失败时保留的旧值必须结合此项解读；不伪造零积压。 |
| `mock_received_rows` | A/B 各自 MySQL 中真正持久化的通知行数 gauge。B 同 topic/partition/offset 内部重放仍是一行；A 外部重复实收会新增行。显式重置数据库会使 gauge 下降。 |
| `mock_receipt_sample_success` / `mock_receipt_last_success_timestamp_seconds` | 实收行数采样是否成功及其时效。 |
| `mock_platform2_consumer_lag_records` | B 通知消费组的 Kafka lag，依据已提交位点；数据库已保存但位点未确认的窗口仍可能有 lag。 |
| `mock_platform2_consumer_paused` | B 消费者控制状态，1=暂停；不停止 hub 向 Broker 发送。 |
| `mock_platform2_lag_sample_success` / `mock_platform2_lag_last_success_timestamp_seconds` | B lag 后台采样健康与最后成功时间；暂停时继续采样。 |

应用内计数器随进程重启归零，用 `rate`/`increase` 观察区间变化，不能用当前 counter 反推重启前总量。MySQL/Redis 共享观测在各 hub 分别采样，聚合看板使用 `max`，避免两副本相加成为双倍积压；单实例健康与采样时间仍须分别检查。采样存在间隔与非原子观测窗口，不作为发送、恢复或入口配额的控制依据。

直方图 P99 仅描述已结束的对应样本。必须另列全输入 cohort、失败、未完成、机器、QPS、持续时间、消息大小、绑定缓存冷热和资源条件，才能讨论是否满足各平台首次确认 P99≤5秒；B 的消费展示不属于该终点。此看板自身不构成性能达标证明。

## 日志与标签边界

业务日志使用结构化 JSON，按允许字段关联 `msgId`、`deliveryId`、`traceId`。禁止记录原始成交全文、通知全文、价格/数量/金额、客户 UID、DSN、令牌或未经裁剪的外部错误正文。错误类别使用固定描述。ID 可以出现在日志中，不能成为指标标签；平台、结果、是否发送、固定工作类别与固定池名才适合做低基数维度。

Go/process 指标反映应用进程的 CPU、RSS、goroutine 等；它们不能代替 MySQL/Redis/Kafka 容器及主机磁盘监控。数据库磁盘余量需在部署主机单独观测，本地应用指标不声称覆盖磁盘/主机故障。中间件仍为单节点，不承诺主机或磁盘容灾。

本地可用 `docker compose --env-file .env exec -T mysql df -k /var/lib/mysql` 读取数据库卷所在文件系统余量；正式检查会记录这项真实采样。它是点时检查，不是持续的 Prometheus 磁盘指标。归档重放也未提供独立计数器，不能从重复的可见时延样本推算重放次数。

## 可重复验证的两种分离

1. 暂停 B 消费，发送一个新 msgId：中台 B 首发 SUCCESS 增加，B 实收不增加、lag 增加；恢复消费后，该 trace/UID 出现实际实收且 lag 消退。控制动作需演示令牌和现有本机/同源限制。
2. 用真实 MySQL 写阻塞放慢归档：待归档条数、字节及最老年龄增长，完整 Redis 快照保留、MySQL 暂不可见；解除阻塞后归档可见且积压归零。领取/租约时间不能替代原接入年龄。

自动验证读取实际 Prometheus 样本、Grafana 已加载的面板及每个面板的 PromQL，并核对独立存储事实。看板布局配置只做 JSON/加载检查和人工布局核对，不称为业务 TDD。


### 空闲窗口与采样失败

`/hub/demo/ops/metrics` 保留 `ok / stale / unavailable` 状态，新增可选 `reason`：

| reason | 含义 |
| --- | --- |
| `not_sampled` | 后台尚未成功采样（包括采集地址未配置） |
| `no_samples` | Prometheus 查询成功，但没有可用的有限数值；包括空向量、未达到速率最少抓取次数、无观测的直方图 NaN |
| `query_failed` | 最近一次查询失败；若有旧样本则保留并标为过期 |
| `expired` | 最近成功查询的缓存已超出时效 |

入口计数器在启动时创建固定 HTTP 状态的真实零基线；至少两次抓取后，空闲入口速率为 0。首轮 P99 依赖所选窗口内实际投递耗时，无观测时应显示“暂无有效耗时样本”，不能填 0。进程计数器重启重置，持久历史消息不回放到运行指标。资源双视图仅切换已有快照的展示，共享聚合指标不按实例复制或求和。


### 中台与本地联调监控边界

中台 ops 仅查询 hub 进程与自身 MySQL/Redis/Kafka 依赖；删除外部平台实收、消费 lag 和暂停状态。中台自身发起的投递结果、HTTP 响应、Broker 确认仍保留。Prometheus 可继续抓取 Mock 以进行联调，但这些内部指标仅在 `hub-local-qa` 看板中展示，主 `hub-overview` 看板不包含 Mock 指标。单条消息关联实收的显式联调查询保留，不能据此声明可监控生产外部系统。

业务图表通过 ops 快照的 `trends` 字段读取真实历史，后台固定查询入口速率、首轮速率、首轮P99，窗口15m/1h/6h，步长15/60/300秒；只读接口不直接查询Prometheus。响应上限1MiB、序列上限32、单序列121点、六worker与原查询超时限制。NaN/Inf保留为null断点，失败保留旧曲线并标过期，浏览器不能传入任意PromQL或时间范围。结果占比按所选窗口平均首轮速率计算，不是消息成功率或窗口累计条数。无流量不画比例，未采样不填零。火焰图需另行接入剖析数据，此项目未提供。
