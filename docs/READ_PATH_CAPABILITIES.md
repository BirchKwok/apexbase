# 读取路径能力、限制与 fallback（单源）

本文件是读取路径“能力表 / 限制 / fallback 目标”的唯一来源（M1）。
新增读取 lane、修改 gate 或改变回退目标时，必须同步更新本表；lane 的 gate 处注释
引用本文件与对应小节。判定所用的可见状态由
`TableStorageBackend::overlay_state()` 单点探测，不要在调用方重新拼条件。

## 1. 可见状态分类（read visibility）

`apexbase/src/storage/backend.rs`：`OverlayState` / `overlay_state()` /
`has_pending_writes()` / `requires_merged_read()` / `visible_rows_exceed_base()`。

| 状态 | 判定 | 含义 |
| --- | --- | --- |
| InMemory | `storage.is_in_memory()` | 内存表；`columns` 缓冲区是权威视图，不存在持久化行组 |
| Clean | `overlay_state().is_clean_view()` | 持久化行组即完整可见视图，且基础未复制到内存缓冲区 |
| AppendedRows | `overlay_state().appended_rows` | `.delta` 文件存在完整追加行批次 |
| PendingCells | `overlay_state().pending_cells` | DeltaStore 存在单元更新/删除 |
| UnflushedRows | `overlay_state().unflushed_rows` | V4 追加行仍在内存缓冲区（`pending_v4_in_memory_rows() > 0`） |
| BaseInMemory | `overlay_state().base_in_memory` | 基础数据已加载进内存缓冲区（`has_v4_in_memory_data()`） |

派生判定：

- `has_pending_writes()` = PendingCells ∨ UnflushedRows（**不探测** delta 文件，
  保持点查守卫既有成本）；
- `requires_merged_read()` = AppendedRows ∨ PendingCells ∨ UnflushedRows；
- `visible_rows_exceed_base(base_rows)` = `has_delta()` ∨ 逻辑行数 > `base_rows`
  ∨ 活跃行数 > `base_rows`（覆盖“仅 DeltaStore”与“仅内存行”两种增量）。

## 2. lane × 支持 × 限制 × fallback

| lane | 入口 | 支持 | 限制（gate） | 回退目标 |
| --- | --- | --- | --- | --- |
| 内存表直读 | `storage.to_arrow_batch` / `to_arrow_batch_with_limit` | 内存表的投影/ LIMIT | `storage.is_in_memory()` | 通用执行器 |
| V4 mmap 直读 | `read_columns_to_arrow_physical` 的 `use_cache` | 全表读；已应用持久化删除向量，并合并 DeltaStore | `start_row == 0 && row_count.is_none() && !has_delta`（`has_delta` 取 `visible_rows_exceed_base`） | 合并读（§2.4） |
| V4 mmap LIMIT 读 | `to_arrow_batch_with_limit` | 前缀 LIMIT，RCIX O(1) 列定位 | `start_row == 0 && row_count.is_some() && (in_memory \|\| !has_delta)` | 合并读（§2.4） |
| 行组批次流（Clean） | `TableStorageBackend::scan_batches` → `RgBatchStream` | 单表、typed 谓词、投影；逐行组 morsel；持久化删除向量已应用 | 非 InMemory；无 UnflushedRows；`overlay_state().is_clean_view()`；投影列存在且谓词列类型受支持（§3）；V4 footer 存在 | 单批 `scan()` / 通用执行器 |
| 覆盖层批次流 | `scan_batches` → `OverlayBatchStream` | 同上，外加 AppendedRows / PendingCells；基础行组零拷贝直通，`_id` 命中批次才打补丁，追加行作为尾部批次 | 非 InMemory；无 UnflushedRows；`overlay_state().needs_merged_read()`；V4 | 单批 `scan()` / 通用执行器 |
| 单批合并读 | `read_columns_to_arrow` → `read_columns_to_arrow_physical` | 任意投影；物理基础行 + `.delta` 行 + DeltaStore；有持久化删除向量时先构造活跃视图再按活跃窗口 `slice` | 无（通用入口） | —（最终兜底为通用执行器） |
| 候选索引读 | `scan()` → `scan_candidate_indices` + `read_columns_by_indices_to_arrow` | 谓词命中率低的点查/范围查 | `overlay_state().is_clean_view()`；命中数 × 4 ≤ 行数 × 3 | 单批合并读 |
| 缓存字典读 | `read_columns_to_arrow_dict` | 低基数 string 列 GROUP BY，复用全局字典 | `!requires_merged_read()` | `read_columns_to_arrow` |
| 首值缓存 | `build_first_string_row_id_cache` | `col = 'x' LIMIT 1` 的常量时间点查 | `!has_pending_deltas() && delta_row_count() == 0` | 常规点查 |
| FTS/字符串列 mmap | `read_fts_string_columns_mmap` | FTS 字符串列直读 | `!visible_rows_exceed_base(base_rows) && !has_v4_in_memory_data()` | `None`（调用方走通用读） |
| 等值全匹配证明 | `string_eq_matches_all` | 证明 `col = 'x'` 保留全部行以跳过过滤 | `!requires_merged_read()` | `Ok(false)` |
| 列窗口读 | `read_columns_to_arrow_window` | CREATE INDEX 等流式消费者按活跃窗口读 | `!visible_rows_exceed_base(base_rows)` 时走 `to_arrow_batch_mmap_range` | `read_columns_to_arrow` |

队列/执行器层的 lane（详见第 13 节与 10.1）：

| lane | 入口 | 限制 | fallback |
| --- | --- | --- | --- |
| 分批聚合管线 | `try_batch_group_pipeline` | 单表；`BatchGroupAggregator::new` 形状门 + `can_use_incremental_aggregation` + 列可解析；`scan_batches`/`scan_batches_ranges` 可用 | 单批 `scan()` 聚合 |
| 单批 scan 聚合 | `try_scan_group_pipeline` → `backend.scan()` | 单表、WHERE 可转 typed 谓词、无 JOIN/DISTINCT/window | 通用执行器 |
| S3 流式投影 | `execute_streaming_select` | 单表投影 SELECT：无 DISTINCT/JOIN/GROUP/HAVING/ORDER/LIMIT/window；投影为 SELECT 顺序的普通列；谓词与投影列均受 typed 协议支持 | 物化执行（`execute`） |
| Flight 交付 | `flight::service` | 有界通道 `STREAM_CHANNEL_CAPACITY`；每块 `DELIVERY_CHUNK_ROWS` | 上一 lane 的 fallback 链 |

## 3. typed 批次协议的类型边界（单源）

- 谓词列类型：`TableStorageBackend::scan_predicate_column_supported`
  = `Bool / Int8 / Int16 / Int32 / Int64 / UInt8 / UInt16 / UInt32 / Float32 / Float64 / String`；
  **UInt64 明确排除**（mmap 谓词 lane 以 i64 解码，> i64::MAX 不安全）。
- 谓词值：`ScanValue::{Int, UInt, Float, String, Bool}` + `ScanPredicate`
  （`Compare / IsNull / Between / In / Like`）与 `And`/`Or` 树。
- 分组键类型由 `BatchKeyView` 门控；`Morsel::select` 无法解析列/类型时返回
  `BatchMorselOutcome::Unsupported`，调用方必须整体回退（已输出批次后不得回退，
  见 `execute_streaming_select`）。
- 覆盖层批次流的补丁需要 `_id`：调用方未投影时内部携带、补丁后剥离，因此对外
  schema 与 Clean 流一致；仅追加行的覆盖层不携带 `_id`。

## 4. 修改规则

1. 新增/放宽 lane 前先在本表登记它的可见状态前提与回退目标。
2. 可见状态判定只能经 `overlay_state()` 及其派生方法；不要在各 lane 重新拼
   `has_delta()/has_pending_deltas()/pending_v4_in_memory_rows()` 组合。
3. 每个 gate 至少一个“拒绝该状态并正确回退”的测试（存储层与 Python 层各一）。
4. 回退目标必须与主路径结果逐值一致；不一致先修主路径，不允许降低断言。
