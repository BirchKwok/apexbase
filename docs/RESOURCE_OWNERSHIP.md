# 资源与状态归属清单（架构评审 R4）

本文档是 2026-09 架构评审 A4（缓存与会话生命周期分散）与 A5（按需存储
与查询内存上限未打通）的落地交付物：为每个进程内状态/缓存明确
**owner、key、数据来源、容量、失效时机、关闭时机、跨进程行为**。
评审原文 `ARCHITECTURE_REVIEW_2026_09.md` 已在 v1.34.0 收尾时移除；
本文件与 [Read-Path Capabilities](READ_PATH_CAPABILITIES.md) 是该轮
架构约束的现存权威来源。

规则（来自评审建议）：

- 每种状态只有一个权威 owner；失效与关闭只能由 owner 触发。
- 绑定层与 Python 层可以保留带代际（epoch）校验的低开销引用缓存，
  不持有 mmap 所有权。
- 不把全部缓存合并成一把全局锁；不删除仍被引用的 mmap 所有者。

## 1. 权威状态清单

### 1.1 查询执行器（`apexbase/src/query/executor/`）

| 状态 | owner | key | 数据来源 | 容量 | 失效时机 | 关闭时机 | 跨进程 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `STORAGE_CACHE` | 执行器 | `PathBuf`（表路径） | 打开的 `TableStorageBackend`（mmap 所有者之一） | 64 条，LRU | 写入后 `invalidate_storage_cache[_dir]`；epoch/mtime 变化时读路径自动重建 | 进程退出（mmap 随进程释放） | 无（进程本地）；跨进程写序列化靠 `TABLE_WRITE_LOCKS` 的 flock |
| `TABLE_WRITE_LOCKS` | 执行器 | `PathBuf`（表路径） | 每表 `Mutex` + 常驻 `.lock` 文件句柄 | 无上限（随表数增长） | 不失效（锁文件持久存在） | 进程退出 | fs2 `flock_exclusive`，跨进程互斥 |
| `SQL_PARSE_CACHE` | 执行器 | SQL 文本 | `Vec<SqlStatement>` 解析结果 | 1024 条；满后停止收录（S2 审计确认） | 不失效（SQL 文本不可变） | 进程退出 | 无 |
| `CTE_BATCH_CACHE` | 执行器 | CTE 临时文件路径 | CTE 子查询的 Arrow 批次 | 语句作用域（同时在飞的共享 CTE 数）；S2 起 RAII 保证成功/失败都移除 | 语句结束/失败时按路径移除 | 进程退出 | 无 |
| `INDEX_CACHE` | 执行器 | `base_dir/table_name` | `IndexManager`（磁盘索引目录） | 32 容量提示 | 写入后 `invalidate_index_cache[_dir]`；epoch 变化时读路径自动重载 | 进程退出 | 无 |
| `FTS_MANAGER_CACHE` / `FTS_BACKFILL_TASKS` | 执行器 | 表路径 / (表路径, 列) | FTS 索引管理器、后台回填任务 | 随表数增长（见 G1） | FTS 重建/失效入口 | 进程退出（回填线程为 detached） | 无 |
| `QUERY_ROOT_DIR` / `TEMP_DIR`（thread-local） | `Session`（façade） | 当前线程 | 调用方传入的 root/temp 目录 | — | `Session` drop 时 RAII 恢复 | 每查询 | 无（线程本地；R4 起调度器工作线程也会收到，见 §3） |
| `KEEP_DICT_PROJECTION`（thread-local） | 执行器 | 当前线程 | 布尔开关 | — | `with_keep_dict_projection` 结束 | 每查询 | 无 |
| `PATH_TRACE`（thread-local） | 执行器 | 当前线程 | EXPLAIN ANALYZE 记录的实际物理路径标签（首个胜出路由 + 可选细节） | 单个短字符串 | `begin_path_trace` 重置 / `finish_path_trace` 取出置 None | 每 EXPLAIN ANALYZE | 无（线程本地；默认关闭，非 EXPLAIN ANALYZE 查询零成本） |
| `QUERY_MEMORY_BUDGET`（thread-local，S1） | 执行器（`executor/memory.rs`） | 当前线程 | 顶层查询的聚合内存预算：`limit` + 共享 `AtomicUsize` 已用字节；并行 worker 通过捕获同一 `Arc` 共享计数 | 每顶层查询一个 `Arc`；`APEX_QUERY_MEMORY_MB` 可配置，`0`=不限，默认 1 GiB | `QueryMemoryBudgetGuard` RAII：查询结束（成功/取消/失败）恢复上一层上下文；失败的内核回退释放自己的预留 | 每查询 | 无（进程内计数，不跨进程） |
| `PLAN_DIVERGENCE`（thread-local，Cell） | 执行器 | 当前线程 | EXPLAIN ANALYZE 记录的规划/执行分歧说明（首个胜出，静态字符串） | 单个 `&'static str` | `begin_path_trace` 重置 / `finish_plan_divergence` 取出置 None | 每 EXPLAIN ANALYZE | 无（线程本地；默认关闭且未记录时零状态、零分配） |

### 1.2 查询规划与分类（`apexbase/src/query/`）

| 状态 | owner | key | 数据来源 | 容量 | 失效时机 | 关闭时机 | 跨进程 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `CLASSIFY_CACHE`（query_signature.rs） | 查询签名分类器 | SQL 文本 | `QuerySignature` | 512 条；满后整体清空（S2 审计确认） | 不失效 | 进程退出 | 无 |
| `STATS_CACHE`（planner.rs） | 查询规划器 | 表 key | 表统计 + 观察 epoch + 插入序号 | 1024 条 FIFO（S2 起；被逐出表在下次访问时从 sidecar 重读） | 写入后 `invalidate_table_stats` | 进程退出 | 无 |
| `PLAN_FEEDBACK`（planner.rs） | 查询规划器 | (表 key, 查询形状) | 计划反馈：行维度估计/实际行数滑动均值 + 按实际执行成本类（scan/index/parallel，R5.12 并行批量扫描独立成本类）的模型成本与实测时间滑动均值（R5.3 时间校准）；持久化于每表 sidecar `<table>.plan_feedback`（bincode + 版本；Q2 起版本 3，v2 及更早 sidecar 因缺少 OS/arch/并行度指纹而被忽略），每进程惰性加载一次，随表文件回收（R5.8） | 每表 256 个形状、进程内 256 张表（S2 起）；逐出观测样本最少者，sidecar 随内存快照一起收缩 | 仅 EXPLAIN ANALYZE 记录（记录时同步写 sidecar） | 进程退出（内存态）；sidecar 跨会话持久 | 有（sidecar 文件；他进程更新仅在本进程下次启动时可见） |
| `JIT_FILTER_CACHE`（jit.rs） | JIT 过滤器 | 谓词模式 | 编译后的过滤闭包 | 有界（内部 LRU） | 内部驱逐 | 进程退出 | 无 |
| `PARALLEL_SCAN_TOKENS`（executor/batch_group.rs） | 查询执行器（并行批量管道 worker 预算，R5.7/R5.11） | —（进程级计数） | 在飞并行扫描+折叠 worker token 池（`APEX_PARALLEL_SCAN` 显式诊断路径 + R5.12 成本自动启用路径，后两者共用同一预算） | `min(hardware_concurrency - 1, 8)`（R5.11 实测曲线/矩阵定案），惰性初始化（首次并行请求前零状态） | 每查询取 `min(请求, 可用)`，<2 退串行；RAII guard 查询结束归还 | 进程退出 | 无（仅计数，不保留查询数据） |

### 1.3 存储层（`apexbase/src/storage/`）

| 状态 | owner | key | 数据来源 | 容量 | 失效时机 | 关闭时机 | 跨进程 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `StorageEngine.cache`（读 backend，LRU） | `StorageEngine`（全局单例） | `PathBuf` | `TableStorageBackend` | `MAX_CACHE_ENTRIES`（64），LRU | `invalidate` / `invalidate_after_append` / `invalidate_dir`；epoch+mtime 读时校验 | 进程退出 | 无 |
| `StorageEngine.schema_cache` | 同上 | `PathBuf` | 列名集合、行数、epoch | 128 容量提示 | 同上 | 进程退出 | 无 |
| `StorageEngine.insert_cache` | 同上 | `PathBuf` | 插入模式 backend（热写） | 64，LRU | `invalidate` / `invalidate_dir`（append 后保留热写 backend） | 进程退出 | 无 |
| `StorageEngine.memory_tables` | 同上 | `PathBuf` | 内存表 backend（权威，非缓存） | 无上限（见 G1） | `drop_memory_table` / `drop_memory_database` | 内存库 drop；进程退出 | 无 |
| `TABLE_EPOCHS` / `GLOBAL_EPOCH`（epoch.rs） | 存储 epoch 模块 | `PathBuf` | 逻辑写入发布计数 | 随表数增长 | 逻辑写入提交时发布 | 进程退出 | 无（每进程独立计数；跨进程可见性靠文件 mtime+flock） |
| `GLOBAL_DICT_CACHE` + 字节/时钟计数器（backend.rs） | 存储 backend | `(PathBuf, 列)` | 全局字典（低基数列） | 字节上限 + 时钟驱逐 | `invalidate_global_dict_cache`（写入后） | 进程退出 | 无 |
| `GLOBAL_COLUMN_NULL_CACHE` | 存储 backend | `(PathBuf, 列)` | NULL 判定 + mtime + epoch | `GLOBAL_DICT_CACHE_MAX_ENTRIES * 2` 条，满后不再收录新键（S2 审计确认） | mtime/epoch 变化时读路径失效 | 进程退出 | 无 |
| `GLOBAL_DICT_HIGH_CARD_CACHE` | 存储 backend | `(PathBuf, 列)` | 高基数负缓存 | `GLOBAL_DICT_CACHE_MAX_ENTRIES * 2` 条，满后不再收录新键（S2 审计确认） | mtime/epoch 变化时读路径失效 | 进程退出 | 无 |
| `DELTA_*_CACHE`（on_demand/storage_core.rs：字符串索引、数值范围、行数、批次） | on-demand 存储 | `PathBuf` / `(PathBuf, 列)` | delta 文件内容 | 字节/条目上限（内部） | delta 落盘/合并后失效 | 进程退出 | 无 |
| `CATALOGS`（table_catalog.rs） | 表目录模块 | `PathBuf`（base dir） | 映射的 catalog 文件 | 随库数增长 | 目录变更入口 | 进程退出 | 无 |
| 每 backend 页缓存（on_demand 内部） | `TableStorageBackend` 实例 | backend 内部 | mmap/页读 | 内部有界 | backend 自身 `invalidate_page_cache` / footer 失效 | backend drop（mmap 释放） | 无 |

### 1.4 绑定层（`apexbase/src/python/bindings/`）

| 状态 | owner | key | 数据来源 | 容量 | 失效时机 | 关闭时机 | 跨进程 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `ApexStorage.entries`（wrapper.rs） | `ApexStorage` 实例 | 表名 | 带 epoch 校验的 backend 引用缓存 | 随表数增长 | epoch 变化时读路径重建 | 实例 drop / `close()`（清空 `cached_backends`） | 无（A4 允许的代际引用缓存） |
| `cached_backends`（read.rs，实例字段） | `ApexStorage` 实例 | cache key | 同上 | 随表数增长 | 同上 | 实例 `close()` 显式清空 | 无 |
| `update_by_id_numeric_cache` / `update_by_id_cell_cache` / `replace_exact_row_cache` / `flush_prewarm_tables` | `ApexStorage` 实例 | 表名/复合 key | 热写辅助结构 | 随表数增长 | 失效入口按表清理 | 实例 drop | 无 |

### 1.5 Python 层（`apexbase/python/apexbase/client.py`）

| 状态 | owner | key | 数据来源 | 容量 | 失效时机 | 关闭时机 | 跨进程 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `_query_result_cache` | `ApexClient` 实例 | `(database, table, sql, show_flag)`（代际 token 存在 value 中） | 查询结果（缓存值含数据代际 token） | `min(_cache_size, 64)`；>4096 行或 >8 MiB 的结果不缓存；事务内禁用 | 本地写入后按 token/路由失效（见 `test_cache_invalidation_contract.py`） | 实例 drop | 无 |
| `_query_result_cacheability` | 同上 | SQL | 可缓存性判定 | 256（超限整体清空） | 同上 | 实例 drop | 无 |
| `_simple_sql_cache` | 同上 | SQL | 简单 SQL 路由 | 256 条；满后整体清空（S2 审计确认） | 本地写入后清空相关项 | 实例 drop | 无 |
| 模块级 `_auto_scheduler_*` | 模块 | — | 自动调度器开关 | — | `_disable_auto_scheduler` | 进程退出 | 无 |

### 1.6 调度器（`apexbase/src/query/scheduler.rs`）

| 状态 | owner | key | 数据来源 | 容量 | 失效时机 | 关闭时机 | 跨进程 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `SCHEDULER`（thread-local） | 初始化它的线程 | — | `ThreadPoolExecutor`（工作线程池） | 线程数由初始化参数决定 | 不失效 | `Drop`（shutdown 并 join 全部工作线程） | 无。**已知约束**：thread-local 意味着只有初始化线程能提交任务；进程级共享调度器是 R5 候选，不在本轮改动 |
| 任务队列 `task_queue` | `ThreadPoolExecutor` | — | `VecDeque<QueryTask>` | **本轮起有界**（默认 1024，可配置，见 §3） | 任务出队即释放 | 池 drop | 无 |
| `QueryTask.cancel` | 提交方（经 `ScheduledQuery` 句柄） | — | `Arc<AtomicBool>` 取消标记 | — | `cancel()` 置位 | 任务完成 | 无（进程内跨线程） |

### 1.7 Flight 服务（`apexbase/src/flight/`、`bin/flight_main.rs`）

每个请求在 `spawn_blocking` 工作线程上构建一次性 `Session`
（root_dir = 库目录），不持有跨请求状态；TLS 上下文天然按
工作线程隔离。

S3 起 `do_get` 对流式形状直接交付执行器按行组产生的批次：
阻塞生产者通过容量 2 的 `tokio::sync::mpsc` 通道喂给 gRPC 流，
服务端内存由“一个行组 + 通道”封顶；慢消费者形成背压，断连会
drop 接收端，生产者在下个批次边界停止。非流式形状（聚合、Join、
排序、表达式、delta 视图）回落到物化路径并按 65536 行切片交付。
`get_flight_info` / `get_schema` 不再执行完整查询：流式形状从首个
行组批次取 schema，其余形状执行一次并把 IPC schema 按 SQL 缓存
（256 条上限），`total_records` 报 `-1` 表示未计数。

## 2. 所有权结论与缺口

- **mmap 所有权**：`StorageEngine.cache` 与执行器 `STORAGE_CACHE`
  是两套并行的读 backend 缓存（双缓存）。两者都只做 LRU +
  epoch/mtime 失效，语义一致、互不引用。合并为单一权威缓存会
  触碰全部查询热路径，本轮**不合并**，记录为 G2，待 R5 规划
  阶段按"每种缓存单独迁移"的原则处理。
- **写序列化**：唯一 owner 是执行器 `TABLE_WRITE_LOCKS`
  （进程内 Mutex + 跨进程 flock）。
- **可见性发布**：唯一 owner 是 `storage/epoch`
  （逻辑写入 → epoch 发布 → 缓存读路径校验）。
- **索引/FTS**：执行器缓存为 owner，磁盘目录为数据源。
- **Python 结果缓存**：owner 是 client 实例；跨客户端失效依赖
  数据代际 token（已有契约测试覆盖）。
- 绑定层的 `entries` / `cached_backends` 符合 A4 允许的
  "带代际校验的低开销引用缓存"，保留。

### 缺口（G*）

- **G1 无上限缓存 → S2 已逐项关闭**（审计与证据见 §6）。其中
  `SQL_PARSE_CACHE`、`CLASSIFY_CACHE`、`GLOBAL_COLUMN_NULL_CACHE`、
  `GLOBAL_DICT_HIGH_CARD_CACHE` 与 Python `_simple_sql_cache` 在 S2 审计时
  已实际有界（本清单早期结论过时）；S2 修复了 `CTE_BATCH_CACHE` 失败路径
  泄漏，并为 `STATS_CACHE`、`PLAN_FEEDBACK` / `FEEDBACK_LOADED` 增加容量
  上限与逐出。
- **G2 双 backend 缓存**：见上文结论。
- **G3 调度器 thread-local**：见 §1.6 约束。

## 3. 本轮（R4.1–R4.3）代码变更

1. **调度器会话上下文传播**（A4/A5）：`QueryTask` 携带
   `root_dir` / `temp_dir`；工作线程执行前安装、结束后 RAII
   恢复，与 `Session` 的 TLS 语义一致。此前工作线程丢失这两项
   上下文（`QueryTask` 只带 SQL 与表路径）。
2. **任务队列准入控制**（A5"服务端限制排队"）：队列默认上限
   1024，`init_query_scheduler(num_threads, max_queue)` 可配置；
   超限提交立即拒绝（不阻塞），调用方收到明确错误。
3. **查询取消**（A5"取消传播到执行器"）：`ScheduledQuery` 句柄
   暴露 `cancel()`；工作线程将取消标记装入线程本地
   `QUERY_CANCEL`；分批聚合流水线（R3 的
   `try_batch_group_pipeline`）在每个批次边界检查一次（一次
   原子读 + 一个分支，位于批次级而非行级），命中即返回
   `Interrupted("query cancelled")`。单批次路径与融合内核内部
   不做行级检查（成本/收益不支持；A5 的串行分批前提已满足）。

## 4. 暂缓项（R4 余项 → 后续阶段）

- **查询内存预算**：批次扫描内存已由 R3 限定为一个行组；
  聚合器状态（高基数 GROUP BY）与全局准入预算需要独立设计
  （A5 顺序：先明确内存所有权——本文档——再谈预算）。
- **Flight 分批结果桥接（S3 已实现）**：`do_get` 按行组流式交付并带
  背压/断连取消；`get_flight_info` / `get_schema` 不再完整执行，schema
  与流式批次一致（Rust 测试与 `bench_flight.py` 验证）。
- **G1/G2/G3**：按"每种缓存和每个入口单独迁移"原则，
  在后续阶段逐项处理，每项独立提交与验收。

## 5. S1 查询内存预算（2026-09-17）

§4 的"查询内存预算"已按 A5 顺序（先明确所有权，再谈预算）落地第一版：

- **预算口径**：按字节计量，针对**查询自身持有的聚合状态**（分组 map、每行索引向量、
  字典直索引数组、每组 distinct 集合），不含扫描批次（R3 已限定为一个行组）与最终
  结果物化（完整结果 API 允许 O(输出)）。
- **配置**：`APEX_QUERY_MEMORY_MB`（正整数 MiB，`0`=不限），每个顶层查询安装时读取，
  可逐查询切换；默认 1 GiB。未配置上限的嵌入式点查询只支付一次线程本地写入。
- **超预算**：返回 `io::ErrorKind::OutOfMemory`，错误文本包含已用/上限字节，Python 侧
  为 `RuntimeError`。失败的内核若回退到其他算子，会先释放自己的预留，避免重复计数。
- **已覆盖入口**：分批管道（串行流 + 并行 partial 及合并）、`execute_group_by_with_indices`
  行索引回退、单键 streaming（含 COUNT DISTINCT）、`execute_group_by_incremental`
  通用键路径（含 rayon 分区局部状态与合并）、`VectorizedHashAgg` 单键哈希、字典直索引
  路径（`execute_group_by_string_dict` / dict case count / vectorized dict count）。
- **仍不在本预算内**：无 WHERE 的整型键查询若命中存储层 numeric dict cache（u16 组 ID，
  上限 65536 组）或存储原生 `execute_group_agg`（结果本身 O(组数)），其内存属于全局缓存
  容量（G1）与输出物化，按 S2/G1 单独处理，不用查询预算重复计量。


## 6. S2 缓存容量审计与 G1 关闭（2026-09-17）

§2 的 G1 逐项复核如下（`capacity` 列为当前源码事实，非计划值）。审计原则：
每种缓存独立处理、保留 epoch 引用缓存、不引入新的全局大锁。

| 缓存 | 审计结论 | 处理 |
| --- | --- | --- |
| `SQL_PARSE_CACHE` | 已有 1024 条上限（满后停止收录，不再写入） | 无需改动；本清单旧结论更正 |
| `CLASSIFY_CACHE` | 已有 512 条上限（满后整体清空） | 无需改动；旧结论更正 |
| `GLOBAL_COLUMN_NULL_CACHE` | 已有 `MAX_ENTRIES*2` 上限（满后不收录新键） | 无需改动；旧结论更正 |
| `GLOBAL_DICT_HIGH_CARD_CACHE` | 已有 `MAX_ENTRIES*2` 上限（满后不收录新键） | 无需改动；旧结论更正 |
| Python `_simple_sql_cache` | 已有 256 条上限（满后整体清空） | 无需改动；旧结论更正 |
| `CTE_BATCH_CACHE` | 条目按“每次执行唯一”的临时路径键控；主语句失败时原实现跳过移除，条目永不再被查询并长期占用 Arrow 批次 | **S2 修复**：改为 RAII guard，成功/失败/取消都移除 |
| `STATS_CACHE` | 真无上限：仅按表 key 增长，长进程访问大量表时持续累积 | **S2 新增**：1024 条 FIFO 逐出（读路径仍只取读锁，不触碰逐出元数据；逐出后按 sidecar 重读） |
| `PLAN_FEEDBACK` / `FEEDBACK_LOADED` | 真无上限：每个 EXPLAIN ANALYZE 形状与每张加载过的表各占一条 | **S2 新增**：每表 256 形状、进程内 256 表，逐出观测样本最少者；`FEEDBACK_LOADED` 上限 1024，逐出只允许后续从 sidecar 重新加载（内存条目优先合并，不会覆盖新记录） |

未改动的 G2（双读 backend 缓存合并）与 G3（调度器进程级共享）仍按“每项独立评审”
保留；二者都涉及跨入口状态迁移，不以本轮缓存容量工作夹带。既有生命周期覆盖
（`test_lifecycle_management.py`、`test_cache_invalidation_contract.py` 的 close/reopen、
跨客户端、持有结果视图、外部进程改写）在 S2 验收中复跑，新增容量边界行为测试。
