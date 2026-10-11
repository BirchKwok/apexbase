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
| 行式点读（RCIX） | `retrieve_rcix` / `retrieve_rcix_projected`，经 `read_row_by_id_to_arrow` / `read_row_by_id_projected_to_arrow`、以及 Python 绑定层的 `try_pk_read` | 单行、按列定向解码（`_id` 由 id 推导，不读 ID 列）；行组目录按 id 二分（`find_row_group_for_id`） | 该 id 所在行组 `compress_flag == RG_COMPRESS_NONE`、`enc_ver == RG_ENCODING_VERSION` 且组内有 RCIX 目录 | 解码兜底（本表下一行） |
| 行式点读（解码兜底） | `read_row_by_id_values` / `read_row_by_id_values_projected`（`read_row_by_id_to_arrow` / `read_row_by_id_projected_to_arrow` 的第二段） | 压缩行组或旧编码；解压体按 `(offset, compressed_len)` 缓存。写路径在压缩**之前**就记录了每列偏移，所以压缩行组同样**带列目录**，且该目录寻址"解压后"的逻辑布局——解压体一到手，这一 lane 与未压缩 lane 走**同一份** `decode_point_column_value`（`read_write.rs`）：PLAIN/BITPACK/RLE/字典/Binary/向量按值定向解码，仅未知编码整列解码；带投影时只解码被请求的列 | V4；`read_row_by_id` 仍用于内存表与写缓冲镜像基表 | 通用读 / `read_row_by_id` |
| 行组批次流（Clean） | `TableStorageBackend::scan_batches` → `RgBatchStream` | 单表、typed 谓词、投影；逐行组 morsel；持久化删除向量已应用 | 非 InMemory；无 UnflushedRows；`overlay_state().is_clean_view()`；投影列存在且谓词列类型受支持（§3）；V4 footer 存在 | 单批 `scan()` / 通用执行器 |
| 覆盖层批次流 | `scan_batches` → `OverlayBatchStream` | 同上，外加 AppendedRows / PendingCells；基础行组零拷贝直通，`_id` 命中批次才打补丁，追加行作为尾部批次 | 非 InMemory；无 UnflushedRows；`overlay_state().needs_merged_read()`；V4 | 单批 `scan()` / 通用执行器 |
| 单批合并读 | `read_columns_to_arrow` → `read_columns_to_arrow_physical` | 任意投影；可见行空间按 `[持久基表][.delta 行][缓冲尾段]` 三段组合（`OnDemandStorage::visible_read_segments()`，`read_ids` 同源）；基表段仅在缓冲镜像基表（`v4_base_loaded && has_v4_in_memory_data()`）时切缓冲，纯尾段缓冲改走 RG mmap；有持久化删除向量时先构造活跃视图再按活跃窗口 `slice`，删除向量直接从行组读取（`persisted_deletion_bitmap()`） | 无（通用入口） | —（最终兜底为通用执行器） |
| 候选索引读 | `scan()` → `scan_candidate_indices` + `read_columns_by_indices_to_arrow` | 谓词命中率低的点查/范围查 | `overlay_state().is_clean_view()`；命中数 × 4 ≤ 行数 × 3 | 单批合并读 |
| 执行器索引快路 | `ApexExecutor::try_index_accelerated_read`（`index_accelerated_read`） | 规划器选中 `OltpIndexLookup` 的单列/复合等值与范围查；covering 时走 `try_index_only_scan`（完全不读表） | `!indexes_stale(path)`；`!backend.requires_merged_read()`（append delta / 缓冲追加行 / DeltaStore cells：索引只覆盖持久化行组，覆盖层行会静默丢失）；非 GROUP BY/JOIN/聚合；有可用索引 | 扫描 lane（记录 plan divergence）；索引失效/覆盖层状态由 `flag_indexes_stale_after_failure` 或 gate 兜住 |
| 缓存字典读 | `read_columns_to_arrow_dict` | 低基数 string 列 GROUP BY，复用全局字典 | `!requires_merged_read()` | `read_columns_to_arrow` |
| 首值缓存 | `build_first_string_row_id_cache` | `col = 'x' LIMIT 1` 的常量时间点查 | `!has_pending_deltas() && delta_row_count() == 0 && !(has_v4_in_memory_data() && !v4_base_loaded)`（纯尾段缓冲下缓冲列与组合后的 ID 会错位） | 常规点查 |
| FTS/字符串列 mmap | `read_fts_string_columns_mmap` | FTS 字符串列直读 | `!visible_rows_exceed_base(base_rows) && !has_v4_in_memory_data()`，且 V4 footer 可加载（内存表无持久化 base，必须回退） | `None`（调用方走通用读） |
| 等值全匹配证明 | `string_eq_matches_all` | 证明 `col = 'x'` 保留全部行以跳过过滤 | `!requires_merged_read()` | `Ok(false)` |
| 列窗口读 | `read_columns_to_arrow_window` | CREATE INDEX 等流式消费者按活跃窗口读 | `!visible_rows_exceed_base(base_rows)` 时走 `to_arrow_batch_mmap_range` | `read_columns_to_arrow` |

队列/执行器层的 lane（详见第 13 节与 10.1）：

| lane | 入口 | 限制 | fallback |
| --- | --- | --- | --- |
| 分批聚合管线 | `try_batch_group_pipeline` | 单表；`BatchGroupAggregator::new` 形状门 + `can_use_incremental_aggregation` + 列可解析；`scan_batches`/`scan_batches_ranges` 可用；并行折叠需 worker token 预算 ≥2，且 `APEX_PARALLEL_SCAN` 未显式关闭时由成本自动启用（预测串行 ≥2 ms 且并行历史更快） | 单批 `scan()` 聚合 |
| FTS 行选择下推 | `ApexExecutor::try_fts_row_selection`（路径 `fts_row_selection`） | WHERE 的顶层 `AND` 中含一个 `MATCH()`（已解析为 `FtsResolved` bitmap）时，只物化命中行：无删除向量时先把 bitmap 的 `_id` 解析成**物理行号**（优先用行组目录 `positions_for_ids`，否则回退扫 `_id` 列），再 `read_columns_by_indices_to_arrow(位置, required_columns)` —— 因此只读语句需要的列，**向量/blob 列也能完整取回**；有删除向量时退回按 `_id` 读的标量 reader。其余谓词、投影、聚合、排序、LIMIT 仍由共享尾部执行，因此结果与全扫逐值一致 | 单表（`joins.is_empty()`）；`overlay_state().is_clean_view()`（覆盖层行不在持久化索引快照内）；`0 < hits && hits * 3 <= active_row_count()`；`OR`/`NOT` 下的 `MATCH()` 不可拆分；位置版还需要 `active_row_count() == base_row_count()`（行组目录的稠密映射只在此成立），按 `_id` 的标量版还需要 schema 全为标量类型、无 `FixedList`/量化向量/复杂类型 | 单批合并读 / 通用执行器 |
| 距离 TopK 融合 | `ApexExecutor::try_fast_order_by_distance`（clean 视图走零拷贝 mmap：路径 `order_by_distance_topk_direct`；带选择性谓词时路径 `filtered_distance_topk`；否则 `order_by_distance_topk`） | `ORDER BY <distance>(col, [literal]) [ASC] LIMIT k`（含 `SELECT <expr> AS alias … ORDER BY alias` 写法）：clean 视图（无覆盖层、行组无删除向量、未压缩）时用 `topk_fixedlist_direct`/`topk_binary_direct` 直接在 mmap 上零拷贝打分，只 `read_columns_by_indices_to_arrow` 最近 k 行；有选择性谓词时先把谓词解析成候选行号（单列比较/`BETWEEN`/同列双区间、字符串等值），只对这些行打分后取堆（路径 `filtered_distance_topk`）；否则按 `required_columns()` 裁剪后跑无分配 SIMD 距离 + 有界堆并 `take`。三条路径都只读语句需要的列（`SELECT *` 除外），尾部仍执行 ORDER BY（k 行）、投影与 LIMIT。**含 `MATCH()` 的形态由上一行的 FTS 行选择先接管**（它同样按位置取行，随后由尾部排序） | 单表；无 GROUP BY / HAVING / DISTINCT / OFFSET>0 / 聚合 / 窗口（这些必须先作用于整个关系，或会因只喂 k 行而改变语义）；`limit` 存在且 > 0；升序；`order_by` 只有一项；距离函数名与 `expressions.rs` 的分发一致；第一个参数是列、第二个是 `[..]`/字符串字面量；度量不是"相似度值"型（`inner_product`/`cosine_similarity` 的有界堆返回"最相似"，与 `ASC` 相反）；向量列无 NULL（堆按平坦子数组打分，NULL 会被当 0）；维度匹配且是 FixedSizeList/Binary。`filtered_distance_topk` 额外要求：`WHERE` 是能被上述形态解析的合取（`OR`/`NOT`/多列混合不解析）、候选行数 `*4 <= active_row_count()`（否则候选太少，全扫更省）、`overlay_state().is_clean_view()` 且 `active_row_count() == base_row_count()`（候选按物理行号读取，删除向量会错位） | 通用执行器（`ORDER BY` 逐行求值 + 全排序） |
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
   `has_delta()/has_pending_deltas()/pending_v4_in_memory_rows()` 组合。**行空间组合**
   同理只经 `OnDemandStorage::visible_read_segments()`（`[持久基表][.delta][缓冲尾段]`
   三段，`buffer_base_rows` 区分"缓冲镜像基表"与"纯尾段缓冲"）——任何自行假设
   "写缓冲就是整表"的读者都会在 `spill_pending_v4_rows_to_delta()` 之后静默漏掉基表行。
3. 每个 gate 至少一个“拒绝该状态并正确回退”的测试（存储层与 Python 层各一）。
4. **点读的“行不存在”与“lane 不可用”必须分开**：`retrieve_rcix*` 对两种情况都返回
   `None`，因此只能由 `OnDemandStorage::point_read_lane_available(id)` 判定该 id 所在
   行组是否走 RCIX lane。把不可用当成“行不存在”会让 `_id = X` 在压缩表上退化到通用
   扫描（实测 23.6 ms / 行，见 `test/test_point_lookup_compressed_sql.py` 与 bench
   `wide` 阶段的 executor 形状）。新增点读调用方时必须走这个判定，不要自行假设。
5. 回退目标必须与主路径结果逐值一致；不一致先修主路径，不允许降低断言。
5b. **`None` 的两种含义必须经 `point_read_lane_available(id)` 区分**，绑定层的 `try_pk_read`
   （`python/bindings/sql.rs`）也不例外：它只查 lean RCIX 车道，曾把"车道不可用"当成
   "快路不适用"，于是压缩表上的 `_id = X` 整条语句掉进 SQL 执行器（每次都重新解析、规划，
   200k × 16 zstd 实测固定多 ~10 µs）。新增点读快路时必须接同一条判定，否则会静默失去
   快路（功能仍正确，只是慢），并且没有任何断言会失败。
6. **共享尾部的 WHERE 会再执行一次**：存储层快路把批次**交给共享尾部**时，尾部仍会重新求值
   WHERE，因此这种批次的列集必须包含 WHERE 列。两条 staged-read 辅助函数按用途分开：
   `read_matching_rows_for_tail` 用 `get_col_refs`（= `required_columns()`，含 WHERE），
   `read_matching_rows_adaptive`/`read_matching_rows_by_indices` 用 `post_filter_col_refs`
   （= required − WHERE）且**只有自行投影/排序/限流并直接返回的调用方**能用。写错会让谓词列
   缺失，过滤返回 `NotFound` 后共享尾部**重读整表并重试**（路径 `predicate_column_reread`，
   该重试保留为兜底）—— 实测 `SELECT doc FROM t WHERE k < 100 ORDER BY abs(doc) LIMIT 5`
   在 20 万行上是 3.785 ms，改走 `for_tail` 后 0.959 ms（3.9×）。目标形态：
   `ORDER BY <表达式>` + 谓词列未投影（含向量 rerank）。**同一个 lane 的不同调用点可以不同**：
   `try_fast_numeric_range_filter` 的两个调用点都是 early-return（自行投影后返回），内部仍走
   `read_matching_rows_adaptive`（谓词列保持裁剪）；`try_numeric_predicate_pushdown` 的批次回到
   尾部，故走 `for_tail`。新增 staged-read 调用方时先判断"结果是否回到共享尾部"，再选辅助函数
   —— 给 early-return 的调用点加谓词列会让它白读一列（实测 `String functions` 形状
   0.735 → 0.862 ms，+17%）。
7. **缓冲专用 reader 的前置条件**：`read_columns_filtered` / `read_columns_filtered_string` /
   `read_column_by_indices` 已在"纯尾段缓冲"（`!v4_base_loaded`）下改走
   `visible_read_segments()` 组合（`read_columns` + 共享匹配器 + 基表段删除向量）。
   `read_row_by_id` / `read_rows_by_ids` / `read_column_auto` / `read_column_scattered_auto`
   仍要求"写缓冲镜像基表"（或缓冲为空）；其当前调用方都有 gate（索引快路在覆盖层下让位、
   字符串 LIMIT 首值缓存拒绝纯尾段缓冲、late materialization 在 delta 态走 mmap 分支、
   Python 点查走独立的 `read_row_by_id_values` 组合实现）。新增调用方前必须确认该前提，
   否则先用 `visible_read_segments()` 组合或显式拒绝。
8. **候选行号的空间必须与读取 reader 一致**：`filtered_distance_topk` 与 `fts_row_selection`
   的**位置版**都用候选行号直接调 `read_columns_by_indices_to_arrow`，而该 reader 把 V4 mmap
   索引当**物理**行号（`read_rows_by_ids_to_arrow` 才按活动 `_id` 解释）。因此这两条 lane 都显式要求
   `overlay_state().is_clean_view()` 且 `active_row_count() == base_row_count()`：一旦存在
   覆盖层或删除向量，活动行号与物理行号会错位。`_id → 物理行号` 的解析优先走行组目录
   （`OnDemandStorage::positions_for_ids`，要求每个行组 `max_id - min_id + 1 == row_count`
   且 `deletion_count == 0`），目录无法证明稠密时回退扫 `_id` 列。新增"谓词 + 索引空间读取"的
   快路时必须先确认这两个条件，否则候选行号会读到错误的行（静默错答，而非报错）。
9. **渲染表达式的列名解析不得按最后一个 `.` 切分**：解析器会把 `ORDER BY <表达式>` 与
   `GROUP BY <表达式>` 的渲染文本当作列名（`cosine_distance(emb, [0.5, 0.25])`、
   `ABS(score - 1.5)`）。按最后一个 `.` 剥限定符会切在**浮点字面量**里，得到一个任何列都不叫的
   后缀，于是排序键为空（**静默保留扫描顺序**）或分组键取不到（**静默塌缩成单组**）——
   两类都已实测：`ORDER BY abs(score - 2.5) DESC` 曾原样返回升序，`GROUP BY ABS(score - 1.5)`
   曾在 200 个互不相同的取值上只返回 1 行。规则：
   - 先按**精确名**在批次里查（`ApexExecutor::resolve_order_col_name`）；
   - 需要剥离限定符时只用 `strip_column_qualifier`（`aggregation/pipeline.rs`）：仅当最后一个 `.`
     的前缀是纯标识符段（`u`、`db.t`）且后缀也是纯标识符才剥，`grouped.rs` 的
     `clean_name`/`clean_order_col` 都已委托它；
   - 新增任何"用列名字符串去批次里找列"的调用点，都必须走这两条，否则表达式名会被静默截断。
10. **编码列的两段式读取：tag 必须留在前面**：行组内一列的数据布局是 `[null bitmap][1 字节编码 tag][payload]`。`read_column_encoded(bytes, ty)` **按 `bytes[0]` 分派编码**，所以只跳过 null bitmap、把 `&body[col_off + null_bitmap_len + 1..]`（免 tag 的 payload）交给它的调用点会把第一个数据字节当编码、把后续字节当长度前缀 —— 实测 BITPACK 的 int 列读出 `count=216172782113783808` 并报 `Int64 column data truncated`（第 51 轮）。需要免 tag 解析时用该编码的专用解析器（如 `bitpack_agg_i64`），需要整列解码时传含 tag 的切片。**同理，用 `u64` 哈希做"分组身份"时必须给 NULL 与值加判别字节**：`AHasher` 下 `write_u8(0)` 与 `write_u64(0)`/`write_i64(0)` 哈希完全相同，第 51 轮前它让 NULL 组与 0/0.0 组静默合并（hash join 之后还有等值比较所以无感，GROUP BY 的哈希就是身份）。
