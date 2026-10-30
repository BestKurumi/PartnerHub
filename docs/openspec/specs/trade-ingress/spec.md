# trade-ingress Specification

## Purpose
规定上游唯一成交输入进入合作中台时可观察的校验、拒绝及结果确认行为，让调用方能区分首轮完成、后台已接管和结果未确认，避免将异步可见记录或 HTTP 成功码误读为全部平台已接收。

## Requirements

### Requirement: 严格校验成交协议且保留精确值
中台 MUST 接收权威协议的20字段，执行正式设计 §4.1 的 Demo 必填、格式、长度与枚举校验；拒绝未知字段、重复键、null/空必填和未知枚举。标识 MUST 保留字符串前导零，精确数值 MUST 不经 float64，不新增正负值业务约束。

#### Scenario: 五类合法成交保留精度
- **WHEN** 上游分别提交 NORMAL/IPO/ETF/MARGIN/BLOCK 合法唯一 msgId，客户号含前导零，精确数字处于38位有效数字/12位小数范围
- **THEN** 中台保留身份及精确数字语义，按有效绑定执行，不因合法数值精度被改写

#### Scenario: 非法结构在外发前拒绝
- **WHEN** 请求存在重复 JSON 键、未知字段/枚举、非法 UUID、空必填或超出设计长度
- **THEN** 中台返回400 REJECTED，不调用任何平台，也不构造已接管成功响应

### Requirement: 入口过载在外发前有界拒绝
中台 MUST 限制请求体至64KiB，并实施有界在途、速率及容量门控；超体积返回413，入口额度耗尽返回429及 Retry-After，依赖/保存容量不可确认返回503。配置初值 MUST 按设计执行并可调整，不宣称全局严格配额。

#### Scenario: 体积和入口额度边界
- **WHEN** 请求体超64KiB或入口额度已满
- **THEN** 分别返回413或429，不进入无界等待队列，且没有平台外发副作用

### Requirement: 基于唯一输入直接首发
中台 MUST 信任上游每条 msgId 唯一，完成校验和路由后在请求预算内直接首次投递；首发前不建设成交判重缓存、历史成交查询、首发分布式锁或成交入口 Kafka。恢复协调和绑定事件一致性不属于该排除范围。

#### Scenario: 正常首发不等待成交数据库写入
- **WHEN** 有效绑定已可解析且各目标平台正常
- **THEN** 中台直接发起平台投递，发送前不以成交历史读写作资格判断，等待所有首轮结果再选择保存路径

### Requirement: 响应准确表达完成与接管边界
中台 MUST 按设计 §4.3 返回 msgId、status、acceptance、recordStatus、statusUrl 及已确定目标的 firstOutcome/currentState/deliveryId。只有正常结果暂存确认后返回200 COMPLETED；只有 MySQL 提交确认后返回202 RECOVERY_PENDING/ACCEPTED或200 NEEDS_ATTENTION/ACCEPTED；保存不明返回503 UNCONFIRMED。

#### Scenario: 全成功与明确无路由
- **WHEN** 所有目标成功或完整关系查询确认为空，且正常暂存确认
- **THEN** 分别返回200 SUCCEEDED或NO_ROUTE、COMPLETED和相应记录可见状态；不把尚未归档说成已持久可查

#### Scenario: 部分平台成功和可恢复失败
- **WHEN** A成功而B临时失败/UNKNOWN，全部结果与B恢复任务已确认提交
- **THEN** 返回202 RECOVERY_PENDING，A仍为成功，B保留其首轮结果，响应带各平台 deliveryId

#### Scenario: 只有永久失败或提交无法确认
- **WHEN** 只有永久失败且已保存，或结果保存/异常接管无法确认
- **THEN** 前者返回200 NEEDS_ATTENTION/ACCEPTED，后者503 UNCONFIRMED；两者均不得伪装全部投递成功
