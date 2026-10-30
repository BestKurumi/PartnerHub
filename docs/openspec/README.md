# OpenSpec 变更归档

归档的变更文档用于查阅需求背景、设计取舍和任务记录。当前运行和部署方式以根目录 [README](../../README.md) 为准；历史设计中的“尚未实现”“待实测”等表述描述归档当时的状态。

| Change | 归档情况 | 阅读入口 |
|---|---|---|
| `partner-hub-technical-design` | 2026-10-22 已归档；仅技术设计与文档核验 | [提案](changes/archive/2026-10-22-partner-hub-technical-design/proposal.md) · [完整设计与架构图](changes/archive/2026-10-22-partner-hub-technical-design/design.md) · [任务](changes/archive/2026-10-22-partner-hub-technical-design/tasks.md) |
| `partner-hub-mvp` | 2026-10-30 已归档；34 项任务完成，含四平台与观测、Trace 耗时图及验收修订 | [提案](changes/archive/2026-10-30-partner-hub-mvp/proposal.md) · [承接设计与增量](changes/archive/2026-10-30-partner-hub-mvp/design.md) · [任务](changes/archive/2026-10-30-partner-hub-mvp/tasks.md) · [验收摘要](../acceptance.md) |

MVP 归档已将 37 条需求合并为七项[正式业务规格](specs/)。归档任务中的“不归档、不推送”等文字记录各阶段当时的授权边界；用户于 2026-10-30 验收后明确授权本次归档并推送。已执行验证和仍未验证的范围见[验收摘要](../acceptance.md)。

归档正文与图示保留原文。历史文档引用的原始协议、需求分析、技术选型资料及 `.comet/` 内审阅记录未随公开仓库发布，对应历史链接在本仓库中不可访问。这些引用表示原设计的依据，不能作为当前运行说明；公开的行为边界见[架构说明](../architecture.md)，指标口径见[可观测说明](../observability.md)。

Git 保留已归档 change 的提案、设计、任务、已有的规格、`.openspec.yaml` 和配套图片。本次 MVP 额外保留 `.comet.yaml` 与 `.comet/delivery.json` 两份无凭据的归档/交付凭证，供工作流验证归档提交；其中引用的本地计划、报告和检查记录未公开发布。其他工作流状态、凭据、缓存及执行日志继续留在本地。
