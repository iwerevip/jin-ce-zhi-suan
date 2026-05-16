# DuckDB 增量同步串行写入设计

## 1. 背景

当前增量同步在 `write_mode=direct_db` 且 `direct_db_source=duckdb` 时，会将 `concurrency` 强制降级为 `1`。这样虽然规避了 DuckDB 单文件并发写入的锁冲突问题，但也导致整条同步链路完全串行，无法发挥单进程下多线程并发抓取和判重的能力。

当前实现的主要问题如下：

- 源数据抓取、判重、写入被耦合在同一股票任务内。
- DuckDB 直写路径为“每只股票直接开写连接并写入”，不适合多线程并发写。
- 一旦失败，任务只能逻辑上从头再跑，缺少明确恢复进度。
- 现有配置中 `history_sync.concurrency` 在 DuckDB 直写模式下无法实际发挥作用。

本设计目标是在不破坏现有同步接口和调用方式的前提下，实现“多线程抓取与判重 + 单写线程小批量刷 DuckDB + 按股票恢复进度”的最小侵入式改造。

## 2. 目标与非目标

### 2.1 目标

- 保留现有 `HistoryDiffSyncService` 入口与前端参数结构。
- 允许 DuckDB 增量同步在单进程内按股票多线程并发执行抓取和判重。
- 所有 DuckDB 写入统一收敛到单独写线程，避免多线程同时写单文件。
- 写线程采用“小批量优先”策略，平衡吞吐、延迟和错误定位。
- 支持“快速失败”，但保留按股票级别的恢复进度。
- 保持当前增量判重逻辑不变，避免重复写入和重复造轮子。

### 2.2 非目标

- 不实现跨进程分布式同步。
- 不实现按“股票-表-时间片”粒度恢复。
- 不实现内存队列落盘重放或 spool 文件机制。
- 不改造现有 API 协议，也不增加新的前端交互流程。
- 不修改回测引擎、策略基类和策略执行链路。

## 3. 方案选择

本次选择以下方案：

- 应用层单写线程 + 内存队列 + 按股票检查点。

未采用方案：

- 多生产者 + 本地磁盘缓冲：恢复更强，但复杂度明显上升，不适合当前 MVP。
- staging 表 + merge 线程：会增加更多 DuckDB 内部写操作和额外表管理，不符合最小侵入目标。

选择理由：

- 与现有 `history_sync_service.py` 的工作线程模型最兼容。
- 能最大化保留现有抓取和判重逻辑。
- 只把 DuckDB 最脆弱的“并发写入”阶段改为串行。
- 恢复模型简单清晰，后续扩展空间明确。

## 4. 总体架构

### 4.1 架构原则

- 并发放在“抓取 + 判重”阶段。
- 串行放在“DuckDB 写入”阶段。
- 恢复粒度固定为“按股票”。
- 只有确认完整落盘成功的股票才记录检查点。
- 保持现有同步参数、统计口径和报告结构尽量不变。

### 4.2 新增组件

本次设计新增以下内部组件，首版全部放在 `src/utils/history_sync_service.py` 内，避免拆散现有逻辑。

#### `DuckDbWriteTask`

表示单个待写任务，字段建议包含：

- `code`
- `table`
- `interval`
- `df`
- `source_rows`
- `existing_rows`
- `missing_rows`

说明：

- `df` 只承载“已经判重后的缺失数据”。
- `missing_rows` 允许为 `0`，用于统一成功确认路径。
- 不在此对象中重复存放配置上下文，避免内存膨胀。

#### `DuckDbSerialWriter`

职责如下：

- 持有唯一的 DuckDB 写连接。
- 运行独立写线程。
- 接收多个工作线程投递的 `DuckDbWriteTask`。
- 按“小批量优先”规则聚合任务。
- 对 DuckDB 串行调用写入接口。
- 回传每个任务的写入成功或失败结果。
- 暴露全局 `fatal_error`，供主流程快速失败。

#### `HistorySyncCheckpointStore`

职责如下：

- 生成任务签名。
- 加载检查点。
- 保存检查点。
- 更新 `completed_codes`。
- 标记失败信息和结束状态。

### 4.3 与现有模块关系

- `HistoryDiffSyncService` 仍然是同步总调度器。
- `DuckDbProvider` 仍然负责 DuckDB 的查询与落盘执行。
- `DuckDbSerialWriter` 作为 `HistoryDiffSyncService` 的内部协作对象，不单独暴露给外部。
- `HistorySyncCheckpointStore` 只服务于本轮历史同步任务，不影响其他模块。

## 5. 执行流程

### 5.1 启动阶段

1. 主线程解析配置与请求参数。
2. 基于关键参数计算 `task_signature`。
3. 加载检查点文件。
4. 从原始股票列表中剔除 `completed_codes`。
5. 初始化 `DuckDbSerialWriter`。
6. 初始化主流程统计对象。

### 5.2 工作线程阶段

每个工作线程按股票执行以下步骤：

1. 拉取源数据。
2. 按表构建 `source_frames`。
3. 读取 DuckDB 现有 key，计算缺失集。
4. 对每个表生成 `DuckDbWriteTask`。
5. 将任务投递到 `DuckDbSerialWriter`。
6. 等待该股票所有表的写入结果返回。
7. 若所有表都成功，则将该股票标记为完成，并立即写入检查点。
8. 若任一表失败，则当前股票失败，主流程触发快速失败。

### 5.3 写线程阶段

写线程持续从队列中读取任务，按以下规则执行：

1. 将待写任务按 `table + interval` 分桶。
2. 当满足任一刷盘条件时，将该桶内数据合并为批次。
3. 调用 DuckDB 写入接口执行串行落盘。
4. 为批次内所有任务写回成功结果。
5. 若批次写入失败，则记录 `fatal_error` 并停止后续消费。

### 5.4 收尾阶段

1. 主线程等待所有已提交股票任务结束。
2. 若写线程无错误，则执行最终 flush。
3. 记录本轮报告和检查点最终状态。
4. 关闭写线程与 DuckDB 连接。

## 6. 小批量写入策略

### 6.1 默认阈值

首版建议默认值如下：

- `duckdb_writer_batch_rows = 3000`
- `duckdb_writer_batch_codes = 8`
- `duckdb_writer_wait_ms = 800`
- `duckdb_writer_queue_maxsize = 256`

### 6.2 触发规则

任一条件满足即触发刷盘：

- 当前桶累计行数达到 `duckdb_writer_batch_rows`
- 当前桶涉及股票数达到 `duckdb_writer_batch_codes`
- 最早进入桶的任务等待时间达到 `duckdb_writer_wait_ms`

### 6.3 分桶原则

写线程仅按以下键分桶：

- `table`
- `interval`

原因如下：

- 同表合并最容易复用现有 `upsert_kline_data(interval=...)` 接口。
- 可以避免跨表拼接 DataFrame，降低错误风险。
- 失败时更容易定位是哪张表的哪一批数据有问题。

### 6.4 背压机制

写队列必须设置 `maxsize`。当队列满时：

- 工作线程阻塞等待。
- 不允许继续无限制堆积 DataFrame。
- 利用背压自然平衡抓取与写入速度，避免内存膨胀。

## 7. 检查点与恢复

### 7.1 文件位置

检查点文件放在：

- `reports/history_sync/checkpoint_<task_signature>.json`

### 7.2 任务签名

`task_signature` 由以下字段构成的稳定摘要生成：

- `provider_source`
- `write_mode`
- `direct_db_source`
- `start_time`
- `end_time`
- `tables`
- `codes` 的稳定摘要
- `session_only`

这样可以保证：

- 相同逻辑任务可恢复。
- 关键参数变化时不会误用旧检查点。

### 7.3 检查点内容

检查点文件结构如下：

```json
{
  "task_signature": "xxx",
  "created_at": "2026-05-16T12:00:00",
  "updated_at": "2026-05-16T12:10:00",
  "status": "running",
  "completed_codes": ["000001.SZ", "000002.SZ"],
  "failed_code": "",
  "error": "",
  "summary": {
    "codes_total": 1000,
    "codes_completed": 2
  }
}
```

### 7.4 提交规则

只有在一只股票满足以下条件时，才会写入 `completed_codes`：

- 该股票所有目标表都已生成写任务。
- 该股票所有写任务均被写线程确认成功。
- 主线程收到完整成功确认。

### 7.5 恢复规则

恢复时只做一件事：

- 过滤掉 `completed_codes` 中的股票。

对于未完成股票：

- 整只股票重跑。
- 不恢复未确认完成的内存写任务。
- 不恢复半只股票状态。

这样做的前提是继续复用当前增量判重能力：

- 已存在的时间 key 会被过滤。
- 未写入的数据仍然会被识别为缺失。

## 8. 快速失败规则

### 8.1 失败源

以下任一情况触发快速失败：

- DuckDB 写线程批次写入失败。
- 写线程连接异常且无法继续写入。
- 工作线程在抓取、构建或判重阶段抛出不可恢复异常。

### 8.2 失败行为

当发生写线程失败时：

- `DuckDbSerialWriter.fatal_error` 被设置。
- 写线程停止继续消费新任务。
- 主线程停止等待并抛出异常。
- 当前运行状态写入报告与检查点。

### 8.3 数据一致性约束

首版不承诺“股票级事务回滚”。

也就是说：

- 某股票部分表已经成功落盘、部分表失败是允许出现的。
- 但因为该股票不会进入 `completed_codes`，下次恢复会整只股票重跑。
- 重跑时依赖现有 DuckDB 判重逻辑避免重复数据污染。

该约束必须在日志和文档中明确说明，避免误解为“失败即完全回滚”。

## 9. 对现有代码的改动点

### 9.1 `src/utils/history_sync_service.py`

这是主改造文件，改动内容包括：

- 新增 `DuckDbWriteTask`
- 新增 `DuckDbSerialWriter`
- 新增 `HistorySyncCheckpointStore`
- 修改 `_resolve_effective_concurrency()`，移除 DuckDB 强制降级为 1 的逻辑
- 修改 `_process_code_sync()`，将 DuckDB 直写改为投递写任务
- 修改 `_run_sync_impl()`，接入：
  - checkpoint 加载
  - completed_codes 过滤
  - writer 生命周期管理
  - 按股票提交检查点
  - 快速失败收敛

### 9.2 `src/utils/duckdb_provider.py`

保持最小改动，建议新增以下能力：

- `upsert_kline_data_with_conn(conn, df, interval="1min", batch_size=2000)`

目的如下：

- 让写线程持有一个长连接持续写入。
- 避免每次批次写入都创建和关闭 DuckDB 连接。
- 降低连接开销和文件级锁初始化成本。

现有 `upsert_kline_data()` 保留不变，并可内部复用新方法，避免破坏其他调用者。

### 9.3 `config.json`

新增可选配置项：

```json
{
  "history_sync": {
    "resume_from_checkpoint": true,
    "duckdb_writer_enabled": true,
    "duckdb_writer_batch_rows": 3000,
    "duckdb_writer_batch_codes": 8,
    "duckdb_writer_wait_ms": 800,
    "duckdb_writer_queue_maxsize": 256
  }
}
```

兼容策略如下：

- 仅当 `write_mode=direct_db` 且 `direct_db_source=duckdb` 且 `duckdb_writer_enabled=true` 时启用新链路。
- 其他场景保持现有逻辑不变。

## 10. 统计与可观测性

### 10.1 日志建议

新增以下日志：

- writer 启动与关闭
- 队列长度与背压告警
- 每次 flush 的表、股票数、行数、耗时
- 股票完成检查点提交
- 快速失败原因

### 10.2 报告字段

保留现有报告结构，但补充以下统计更有价值：

- `checkpoint_task_signature`
- `checkpoint_completed_codes`
- `duckdb_writer_flush_count`
- `duckdb_writer_total_batches`
- `duckdb_writer_total_rows`
- `duckdb_writer_last_error`

### 10.3 统计口径

`written_rows` 必须以写线程确认成功的结果为准，不以“待写入行数”代替。

## 11. 测试策略

### 11.1 单元测试

优先补充以下测试：

- `task_signature` 稳定性测试
- `completed_codes` 恢复过滤测试
- 写线程批次聚合阈值测试
- 写线程快速失败传播测试
- 按股票完成提交检查点测试

### 11.2 集成验证

至少验证以下场景：

- DuckDB 模式下 `concurrency > 1` 时能并发抓取且串行写入
- 中途写入失败后自动保留已完成股票
- 相同任务签名下恢复能跳过完成股票
- 修改时间范围或股票池后不会误复用旧检查点

### 11.3 手工验证重点

- 观察日志中的写线程 flush 节奏是否合理
- 验证队列满时工作线程是否正确阻塞
- 验证最终 `summary` 与 DuckDB 实际写入条数是否一致

## 12. 风险与后续演进

### 12.1 已知风险

- 按股票恢复意味着失败股票会整只重跑。
- 队列参数过大可能导致内存占用偏高。
- 长连接模式下若 DuckDB 连接异常，恢复路径必须清晰。
- 部分表已落盘但整只股票未完成的场景，会依赖下次判重来收敛。

### 12.2 后续演进方向

- 增加“自动二分拆批重试”作为可选策略。
- 增加更细粒度的“按股票-表”恢复模式。
- 将写线程与 checkpoint store 抽离到独立文件，降低 `history_sync_service.py` 体积。
- 为前端增加 DuckDB writer 状态与恢复进度展示。

## 13. 实施边界

首版实施必须满足以下边界：

- 不修改回测引擎和策略体系。
- 不改变现有同步 API 的请求结构。
- 不影响 MySQL、PostgreSQL、API 写入模式。
- 只在 DuckDB 直写路径下引入专用优化逻辑。
- 所有新增代码必须有注释。

## 14. 结论

本设计通过“多线程抓取与判重 + 单写线程小批量刷 DuckDB + 按股票检查点恢复”的方式，将并发能力与 DuckDB 的单文件写入限制解耦，在保持现有系统稳定性和兼容性的前提下，显著提升 DuckDB 增量同步的吞吐与可恢复性。

该方案符合以下要求：

- 最小侵入式改造
- 单进程多线程并发利用
- 失败后可恢复
- 结果可复现
- 对现有系统兼容性高
