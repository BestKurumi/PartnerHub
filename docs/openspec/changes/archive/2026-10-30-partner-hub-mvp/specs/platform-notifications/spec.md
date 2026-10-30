# 平台通知与适配规格

## Purpose

规定成交通知发送给不同合作平台时的客户身份、字段、枚举、时间精度和确认边界，使调用方与演示人员能够验证平台差异确由中台处理，同时不向外部泄露敏感成交数据或承诺外部恰好一次接收。

## ADDED Requirements

### Requirement: 按路由时点固化全部目标身份
中台 MUST 按路由时点完整有效绑定一次确定全部平台/UID并生成deliveryId；后续投递只使用固定UID，不因关系变化重路由已确定目标。明确空目标 MUST 不投递，后续绑定不补推旧成交。

#### Scenario: 在途解绑不改变原任务UID
- **WHEN** 原消息已固定A/U1目标，随后解绑或改绑
- **THEN** 原任务首发/恢复仍使用U1；只有新成交按新关系裁决

#### Scenario: 无绑定后再绑定
- **WHEN** 旧成交完整路由为空后客户新增有效绑定
- **THEN** 旧消息仍NO_ROUTE且不发送；新msgId成交才产生相应目标

### Requirement: 仅外发协议规定的八字段
平台1通知 MUST 仅含userUid/securityCode/securityName/direction/execTime/market/tradeType/traceId；平台2 MUST 仅含extUserId/stockCode/stockName/operateType/tradeDateTime/marketNo/bizType/traceId。数量、价格、金额及其他未规定字段 MUST 不出域，不额外发送msgId或去重Header。

#### Scenario: 完整上游报文不泄露额外字段
- **WHEN** 合法成交包含全部20字段并向两个平台投递
- **THEN** 实际收到的JSON键集合分别严格等于各8字段，日志及平台2记录key不泄露内部客户号/敏感成交全文

### Requirement: 类型和时间转换符合平台协议
NORMAL/IPO/ETF/MARGIN/BLOCK MUST 分别映射平台1 TRADE_NORMAL/TRADE_IPO/TRADE_ETF/TRADE_MARGIN/TRADE_BLOCK及平台2数字1/2/3/4/5。BUY/SELL、SH/SZ不变；Asia/Shanghai成交时间平台1截断至秒，平台2保留三位毫秒。

#### Scenario: 五种类型与毫秒边界
- **WHEN** 分别发送五类成交，原execTime含非零毫秒
- **THEN** 平台1显示对应编码和秒精度，平台2显示对应数字及原三位毫秒，日期/时区不因转换偏移

### Requirement: 各平台确认边界和错误分类可解释
平台1 MUST 在HTTP200且合法code=0时才判SUCCESS；处理前明确拒绝为临时失败，字段/配置错误为永久失败，已发送但结果不明为UNKNOWN。平台2 MUST 以Broker写入确认判SUCCESS，确认不明为UNKNOWN，不能将其说成外部已消费。分类按设计 §9 的Demo契约。

#### Scenario: 非法成功体与保存后超时
- **WHEN** 平台1返回HTTP200但业务体非法，或接收保存后响应超时
- **THEN** 记UNKNOWN并允许有限恢复，不宣称未发送或成功；关闭自动重定向并限制响应读取

#### Scenario: Broker成功但消费者暂停
- **WHEN** 平台2Broker已确认写入而Mock消费者暂停
- **THEN** 中台交付可成功，客户消息中心尚无新增；不得因此触发中台再次发送

### Requirement: 不以协议扩展实现外部业务去重
MVP MUST 保持原外发协议，不要求接收端新增业务去重，不把traceId用作去重键。超时/恢复可能重复送达 MUST 被如实记录及展示；客户端生产幂等仅减少单次传输重试重复。

#### Scenario: 外部收到但确认丢失
- **WHEN** 外部已保存通知但确认丢失，后续恢复再次交付
- **THEN** 允许显示多条实际接收记录，中台保留全部尝试，不宣称恰好一次
