# delivery-recovery Specification

## Purpose
规定已持久接管的异常路由和投递任务如何在平台故障、实例退出及人工操作下继续处理，确保恢复只针对原责任范围、尝试有明确终点，旧执行者不能覆盖新结果，也不以数据库协调承诺外部无重复副作用。

## Requirements

### Requirement: 仅恢复异常未完成任务且次数有限
后台 MUST 只领取到期PENDING/RETRY_WAIT异常任务，不领取SUCCEEDED或正常归档终态。默认每周期最多5次含首轮，未发送的调度尝试亦计次；永久错误直接转NEEDS_ATTENTION，临时/UNKNOWN按持久退避时间恢复，耗尽转人工。

#### Scenario: 部分成功不重发成功平台
- **WHEN** A成功且B重试到期
- **THEN** 只向B的原UID发起恢复，A的通知不会因B失败再次发送

#### Scenario: 连续失败和永久错误
- **WHEN** 同周期已达5次，或任一次明确为永久错误
- **THEN** 转NEEDS_ATTENTION并停止自动发送，完整历史和最后原因可查

### Requirement: 租约确保有效执行者更新任务
任务领取 MUST 原子登记owner/version/租约及尝试计次；网络发送不持有数据库事务。完成写回 MUST 验证有效owner/version/未过期租约；过期未结束尝试标UNKNOWN并按剩余额度恢复。旧执行者不得覆盖新状态。

#### Scenario: Worker退出及旧实例恢复
- **WHEN** 已领取任务的实例退出，租约过期后其他实例接管，旧实例又返回结果
- **THEN** 新实例继续有限恢复，旧结果不能覆盖新版本；未结束尝试记UNKNOWN，允许外发重复

### Requirement: 异常路由恢复原子固定全部目标
仅未完成异常路由 MUST 可自动或人工恢复；成功查询后全部UID/任务与路由终态同事务保存。任一目标写入失败 MUST 回滚全部；ROUTED/NO_ROUTE不得重新路由。路由尝试和投递次数分别计量。

#### Scenario: 路由查询恢复成功与目标写入失败
- **WHEN** 原查询故障已持久接管，恢复查询得到两个平台，但一个任务保存失败
- **THEN** 不产生半份目标或ROUTED终态；按路由恢复预算继续处理

### Requirement: 人工命令只重开符合资格的原任务
人工重试 MUST 只允许NEEDS_ATTENTION，包含operationId/operator/reason；操作记录、周期递增、周期次数归零与PENDING同事务。相同operationId同参数复用结果，不同参数409；禁止编辑原成交/UID或重开SUCCEEDED/IN_FLIGHT，保留累计历史。

#### Scenario: 重复按钮与自动领取竞争
- **WHEN** 同一人工命令重复提交，或与到期自动领取并发
- **THEN** 只开启一个合法新周期，不重复创建原任务；不符合状态返回409，不绕过同一任务的并发裁决

### Requirement: 平台资源隔离且首发恢复共享总预算
两个平台 MUST 有独立有界额度；同平台首发与恢复共享总并发/速率预算，取得资源后才执行/领取。暂停平台 MUST 停止其发送/领取并显示暂停配置状态，不删除任务或伪装成功；另一个平台仍可正常处理。

#### Scenario: 单平台超时或暂停
- **WHEN** A持续超时/无额度或配置暂停，B正常
- **THEN** B可继续交付；A按明确未发送/不确定事实保存或保留责任，不堆积无界本地队列
