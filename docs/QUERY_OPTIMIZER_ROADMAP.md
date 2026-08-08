# ApexBase 查询优化器演进路线

## 1. 决策结论

ApexBase 值得继续建设轻量查询优化器，但不以实现通用数据库优化器为目标。当前优先完善单表访问路径、统计生命周期和计划可观测性；Join 算法、运行时反馈和计划缓存必须由真实 workload 触发，不能仅为了功能完整而实现。

所有开发必须遵守仓库根目录的 `AGENTS.md`。其中性能基线、release 构建、完整 pytest、完整 cargo test 和修改后 benchmark 均为合入前置条件。

## 2. 设计边界

- 正确性优先：候选计划必须可由执行器实现，且不能丢失残余谓词、Join 条件、类型或 NULL 语义。
- 热路径低侵入：`QuerySignature` 已识别的点查、COUNT、LIMIT、字典聚合和文件读取不进入完整 CBO。
- 复用现有能力：复用 `IndexManager`、Arrow SIMD、Zone Map、mmap、字典缓存、JIT 和现有 Join 执行器。
- 成本可校准：成本常量必须由 micro-benchmark 支撑，不接受仅凭经验增加常量。
- 规划有预算：优化时间、候选数、Memo 状态数和缓存容量都必须有上限。
- 可解释：`EXPLAIN` 展示实际可执行候选、统计质量、选择理由和 fallback 原因。
- 可停止：达不到本路线图收益门槛的阶段不继续扩展。

## 3. 当前能力与已知缺口

### 3.1 已有能力

- `QuerySignature` 对常见查询提供专用 fast path。
- 单表规划器比较顺序扫描、单列索引及完整复合等值索引。
- `ANALYZE` 收集 row count、NDV、NULL、数值范围和数值直方图，并持久化 sidecar。
- 索引执行后重新应用完整 `WHERE`，保留残余谓词。
- 小规模安全 INNER 等值 Join 可进行受限顺序调整。
- `EXPLAIN` 展示候选成本；`EXPLAIN ANALYZE` 可记录整条查询的基数反馈。

### 3.2 必须先解决的缺口

- 规划器候选提取与索引执行器能力必须使用同一语义，尤其是 OR、表达式及复合索引列顺序。
- 复合 key 的 NUL 拼接编码没有类型边界和格式版本，属于正确性及磁盘兼容问题。
- sidecar 需要覆盖 base、delta 和 deltastore 的数据版本，不能只依赖单个文件 mtime。
- 固定成本常量尚未按命中率、投影宽度、缓存状态和存储后端校准。
- Join 规划目前主要改变顺序，并没有形成可执行的完整物理算子树。
- 反馈使用 AST Debug 文本，且只有整条查询的输出基数，不能安全驱动复杂物理决策。

## 4. 阶段计划

### P0：正确性、基线和候选一致性

目标：确保 EXPLAIN 中出现的访问路径能够执行，并建立后续优化所需的可靠测量基础。

- [x] 复合等值索引候选按索引定义顺序匹配，不受 WHERE 谓词顺序或额外残余等值条件影响。
- [x] mmap/V4 表允许生成二级索引候选；最终仍由成本模型和实际命中率决定是否物化。
- [x] OR Union 未实现前，不生成单分支索引候选。
- [x] 等值/范围候选只接受执行器能转换的字面量形式。
- [x] sidecar 新鲜度同时考虑 base、delta、deltastore 的大小和修改时间。
- [x] 为统计 sidecar 增加显式 schema/data generation；DML、DDL、ANALYZE、压缩和文件替换统一推进 generation。
- [x] 将复合 key 改为带版本、类型标签和长度边界的 typed tuple；提供旧索引检测及重建策略。
- [x] 建立 planner overhead micro-benchmark：无 WHERE、单表 WHERE、两表 Join、三表 Join、EXPLAIN。
- [x] 建立索引物化 micro-benchmark：命中率、逐行/批量、投影宽度、覆盖索引、冷热缓存、mmap/delta。
- [x] 分别记录规划耗时和执行耗时。
- [x] Join Memo 增加最大关系数、最大状态数和规划时间预算；超限明确 fallback。

验收门槛：

- 新增边界必须有 Python 端到端测试和对应 Rust 单元测试；
- 普通 fast path 不新增统计读取、锁或候选构造；
- pytest 不超过 9 秒，完整 cargo test 无明显变慢；
- 规定 benchmark 的核心 workload 和总分均无可重复回退；
- EXPLAIN 不展示执行器无法执行的候选。

### P1：单表高收益访问路径

触发条件：P0 benchmark 能稳定复现扫描与索引的 crossover，且至少有一个真实 workload 受当前访问路径限制。

- [x] 增加 MCV，优先修正倾斜字符串/类别列的等值估算。
- [x] 支持复合索引前缀等值查询。
- [x] 支持 BTree 复合索引前缀范围扫描。
- [x] 支持多个 AND 索引结果的低分配交集。
- [x] 支持 OR 分支索引 Union、去重和完整残余谓词。
- [x] 将覆盖列、索引顺序、投影宽度、LIMIT early-stop 和物化方式纳入成本。
- [x] 将 Zone Map 剪枝行组数作为扫描成本输入。
- [x] 根据 micro-benchmark 选择逐行或批量 row-id 物化阈值。

验收门槛：目标 workload 的 P50/P95 至少改善 15%，规划开销不超过查询执行时间的 5%，非目标核心 workload P95 回退不超过 2%，全量 benchmark 不回退。未达到门槛则回滚或保持实验开关关闭。

### P2：Join 可观测性与物理计划

触发条件：至少两个代表性 4–8 表 workload 证明 Join 中间结果或算法选择是主要瓶颈。

- [ ] 先生成只读 `JoinPlan`，记录每步 available relations、estimated rows、cost 和选择原因。
- [ ] `EXPLAIN` 展示执行器实际使用的 Join 顺序和算法，而不是仅展示原 SQL 顺序。
- [ ] 修正 base relation 本地过滤、双侧 NDV 和中间结果基数传播。
- [ ] 仅在右侧估算结果很小且存在合法索引时生成 Index Nested Loop 候选。
- [ ] Merge Join 仅在输入已有可利用顺序、无需额外全排序时生成。
- [ ] 小 Join 图使用有界 Memo；大图使用有确定上限的贪心回退。

验收门槛：代表性 Join workload P95 至少改善 20%，规划时间 P99 不超过约定预算，简单查询 P95 回退不超过 2%。没有 workload 证据时不实现新 Join 算法。

### P3：算子反馈与计划缓存

触发条件：P1/P2 已有稳定物理计划节点标识，并观察到统计误估导致的重复错误选择。

- [ ] `EXPLAIN ANALYZE` 记录每个物理算子的 estimated rows、actual rows 和耗时。
- [ ] 反馈 key 使用参数化规范化签名、schema generation、index generation 和 statistics generation。
- [ ] 反馈缓存具有容量上限、TTL、衰减平均和异常值裁剪。
- [ ] 普通查询反馈只做有界采样，可完全关闭。
- [ ] 最后建立轻量计划缓存，并统一处理 DDL、DML、索引和统计失效。

验收门槛：缓存命中能抵消查找与失效成本；常驻内存有硬上限；反馈关闭时热路径与当前一致；反馈开启时错误计划率有可测下降。否则不启用默认采样或计划缓存。

### P4：外部优化器重新评估

只有在 ApexBase 明确需要复杂子查询、大范围 SQL 标准覆盖、多种 Join 算法和统一物理算子树时才评估 DataFusion 等组件。必须先做独立 adapter prototype，对比编译时间、二进制大小、内存、计划质量和完整 benchmark；未达到收益门槛不引入依赖。

## 5. 暂缓项

- 列相关性统计：先用 MCV、直方图和算子误差报告确认实际误估，再决定是否实现扩展统计。
- 普通查询自动反馈：缺少算子级指标前，整条查询输出基数不足以安全修正访问成本。
- 完整计划缓存：版本体系和物理计划标识稳定前容易产生错误失效。
- 无条件 Merge Join：若需要额外排序，通常不适合当前执行器和本地存储定位。
- 直接替换现有执行器：会破坏 fast path，并扩大性能与正确性风险。

## 6. 每批开发流程

每一批只实现一个可独立回滚的行为集合，并严格执行：

```text
修改前：python benchmarks/bench_vs_sqlite_duckdb.py
开发：  最小实现 + Python 测试 + Rust 测试
构建：  maturin develop --release
验证：  pytest
验证：  cargo test
复测：  python benchmarks/bench_vs_sqlite_duckdb.py
```

结果必须记录日期、commit/worktree 状态、平台、数据规模、关键分组总耗时、单项最大回退和测试总耗时。benchmark 波动时至少复测可疑分组或完整 workload，不能用一次较快结果掩盖回退。

## 7. 当前批次记录

- 基线日期：2026-07-11。
- 环境：macOS arm64，10 cores，32 GB，Python 3.12.2，ApexBase 1.21.0。
- 数据：Tabular 1,000,000 行；Vector 1,000,000 × 128。
- 修改前结果：OLAP 45/45、OLTP 27/27、Tabular 72/72、Vector 1/1。
- 关键 ApexBase workload：Filtering 573.02ms；Point & Limited Reads 8.73ms；Vector batch TopK 46.58ms。
- 本批范围：mmap 索引候选、复合候选列顺序、OR 候选合法性、统计 sidecar 数据文件新鲜度。
- release 构建：`maturin develop --release` 成功。
- Python 验证：1408 passed，稳定复测 9.00s；达到 9 秒上限但余量不足，后续批次不得再增加固定测试/规划开销。
- Rust 验证：376 passed；文档测试 6 passed。
- 修改后 benchmark：OLAP 45/45、OLTP 27/27、Tabular 72/72、Vector 1/1。
- 关键 ApexBase workload：Filtering 574.52ms；Point & Limited Reads 8.64ms；Vector batch TopK 49.38ms。
- 波动说明：首次修改后 benchmark 的 `COUNT WHERE category` 出现 4.58ms 异常值，完整复测恢复为 0.508ms；Vector 两次修改后分别为 51.15ms 和 49.38ms，代码未修改向量路径，后续应为 benchmark 增加多次运行和波动阈值。

### 2026-07-13 P0/P1 批次

- 工作区：未提交修改；macOS arm64，10 cores，32 GB，Python 3.12.2，ApexBase 1.21.0。
- 数据：Tabular 1,000,000 行；Vector 1,000,000 × 128。
- 实现范围：统计 generation、typed composite key、规划/物化 micro-benchmark、规划/执行耗时、Join Memo 预算、MCV、复合前缀/范围、AND Intersection、OR Union、覆盖/顺序/LIMIT/物化成本、Zone Map 成本。
- release 构建：`maturin develop --release` 成功。
- Python 验证：1410 passed in 8.74s。
- Rust 验证：380 passed；文档测试 6 passed；完整命令实耗 17.60s。
- 修改后 benchmark 稳定复测：OLAP 45/45、OLTP 27/27、Tabular 72/72、Vector 1/1。
- 关键 ApexBase workload：Filtering 575.97ms；Point & Limited Reads 8.12ms；Vector batch TopK 46.09ms。相对路线图正式基线分别为 +0.5%、-7.0%、-1.1%，无可重复核心回退。
- 优化器 micro-benchmark（5,000 行，5 次）：规划中位数 0.04–0.18ms；目标执行场景中位数 0.03–0.23ms。
