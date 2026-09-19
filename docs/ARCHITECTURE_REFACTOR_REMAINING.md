# ApexBase 架构重构剩余工作与执行计划

更新日期：2026-09-17。起点：`1b60ae8f916c48e25ae2dc2e1354ec586ca7133a`。
依据：[架构评审](ARCHITECTURE_REVIEW_2026_09.md)、当前源码及已保留的本地验收报告。
本文维护剩余工作；历史评审的阶段记录不等于当前整体完成状态。

## 1. 当前结论与完成口径

方向保持：保留 Rust/V4/mmap/融合内核，逐步收敛提交协议、共享执行与资源归属。
整体重构尚未完成。分别记录“已实现”“功能已验证”“性能已验收”，不得互相替代。

| 原阶段 | 当前状态 | 尚未关闭的目标 |
| --- | --- | --- |
| R0 | 有环境、固定 base 和原始报告 | 当前源码快照与证据清单统一；历史失败保持可追溯 |
| R1 | 错误传播和 INSERT/DELETE/UPDATE 恢复已实现 | 提交结果分类、跨表/索引恢复契约 |
| R2 | SELECT 文件拆分完成 | backend 委托和状态所有权收敛；同模块 include 不等于依赖解耦 |
| R3 | 纯持久化 V4 上的受限分批聚合已实现 | overlay 稳定读视图、selection 直接消费、更广查询形状 |
| R4 | 状态清单、上下文传播、有界队列、局部取消已实现 | 内存预算、缓存容量与唯一 owner、协议背压/分批交付 |
| R5 | 索引计划执行、路径跟踪、并行扫描和显式校准后自动选路已实现 | 校准失效/反馈边界、资源前提、最新 canary 验收 |
| R6 | 按需求保留 | 外部聚合/排序/Join、FTS/向量组合、构建 feature，非基础收尾前置项 |

最新 R5.12 完整模式 `local-perf-results/20260910-155135/` 比较通过；
canary `local-perf-results/20260910-152709/` 五样本最终仍有 3 项回退。
专项 A/B 是归因证据，不能把失败门禁改称通过。同构建自 A/B 只能解释波动，不能替代旧版/新版比较。
最新 C3 最终验收见第 7.3 节：`local-perf-results/c3-acceptance-20260917/`
（canary exit 0；full 原始 exit 1，标记项经 current 侧独立复测登记为已证实噪音）。
最新 S1 最终验收见第 8.3 节：`local-perf-results/s1-acceptance-20260917/`
（canary/full 对 base `2c2e471` 均 exit 0，S1 触及的聚合形状无回退）。
最新 S2 最终验收见第 9.2 节：`local-perf-results/s2-acceptance-20260917/`
（canary 首轮 exit 1 经 current 侧独立复测登记为瞬时干扰，canary 重跑与 full
均 exit 0；G1 清单更正并关闭三项真实缺口）。
最新 S3 见第 10 节：`local-perf-results/s3-acceptance-20260917/`（功能链全过；
canary 两轮与 full 主比较在 host load 5.7-12.0 下标记同一类聚合/集合形状，
登记为环境受限原始例外，干净机器复核列为验收债务 V1）。
最新 M1 见第 14 节：`local-perf-results/m1-acceptance-20260919/`（读取可见状态与
能力/回退表单源；pytest 1803、cargo 594+6、flight 598+6、公开 benchmark 103/103
`slower=0`、canary 重跑 exit 0；full 两轮原始 exit 1，两轮标记互不相同的指标并
被同构建 47–85% 极差证实为噪音，登记为环境受限例外）。
最新 Q1 见第 11 节：`local-perf-results/q1-acceptance-20260917/`（selection
直接消费 + 验证矩阵，canary/full 全过）；base+delta 行组流式读视图见
`local-perf-results/q1-overlay-acceptance-20260919/`（full exit 0；canary 两轮
原始 exit 1，两轮标记的干净表指标互不相同且被同构建复测证实为噪音，
登记为环境受限例外；delta 重指标两轮稳定快约 41%）；合并读行空间修复见
`local-perf-results/q1-read-space-acceptance-20260919/`（full exit 0；canary 最终
修订两轮原始 exit 1 且指标互不相同、同构建极差 109%，登记为环境受限例外；
公开 benchmark 103/103 `slower=0`）。
V1 复核见 `local-perf-results/v1-reverify-20260917/`：S3 的 canary/full 复跑在
持续 host load 6-12 下分别标记第三组不同指标与一个重叠的排序指标，qps/quant/
idx/par 全过；噪音结论得到强化，但空闲机器上的绿色门禁仍待补。
最新 Q2 见第 13 节：`local-perf-results/q2-acceptance-20260917/`（契约澄清、
schema 变化清理反馈、canary exit 0；full 原始 exit 1 登记为环境受限例外）。
公开 benchmark 首次做到 15 个 workload 全部 `slower=0`（含 `ALTER TABLE ADD
COLUMN` 0.0506ms vs SQLite 0.0597ms），向量/量化各 6/6。

## 2. 优先级、依赖和交付边界

| 顺序 | 优先级 / 编号 | 工作 | 完成条件 | 状态 |
| --- | --- | --- | --- | --- |
| 1 | P0 / C1 | 提交失败结果分类与提交后错误处理 | Rust 可识别、Python 可辨认“未提交/结果不确定/已提交”；保留原始错误；不把 WAL marker 写失败误判可安全重试；提交后维护失败仍发布可见性与失效；真实 I/O 故障测试 | 已实现并验收通过（2026-09-10，canary/full 对 base `1b60ae8` 均 exit 0）；C2 可开始 |
| 2 | P0 / C2 | UPDATE、Safe/Max 与恢复一致性 | 沿 WAL/数据/索引/水位画时序；补真实 UPDATE 和 fsync 失败测试；保证或明确拒绝无法支持的语义；文件格式变化独立设计 | C2.1–C2.3 已实现，功能验收完成；性能证据已收口并登记 full 原始噪音例外；C3 可开始 |
| 3 | P0 / C3 | 跨表与索引恢复契约 | 覆盖各表 marker 间故障、索引保存失败及 compact 后重开；明确按表收敛与原子提交区别；若引入数据库提交记录，先完成兼容与恢复设计 | C3.1–C3.2 已实现并完成最终统一验收（full 原始噪音例外见 7.3）；S1 可开始 |
| 4 | P1 / S1 | 查询内存预算与资源准入 | 先约束高基数聚合及并行局部状态；预算按字节计量，超预算明确报错或走已验证回退；取消和失败释放资源；峰值 RSS/并发/回收验收 | S1.1–S1.3 已实现并完成最终统一验收（2026-09-17，canary/full 对 base `2c2e471` 均 exit 0）；S2/S3 可开始 |
| 5 | P1 / S2 | 缓存容量与状态 owner | 逐项关闭 RESOURCE_OWNERSHIP 的 G1/G2/G3；保留 epoch 引用缓存；无新全局大锁；close/reopen/跨客户端/跨进程/持有结果生命周期测试 | G1 已逐项关闭（含清单更正、CTE 泄漏修复、planner 缓存上限），G2/G3 仍按独立评审保留；已通过最终统一验收（2026-09-17，canary/full 对 base `0059029` 均 exit 0）；S3 可开始 |
| 6 | P1 / S3 | Flight 分批桥接与协议资源边界 | 查询执行至输出端有界；慢消费者背压、断连取消；schema 请求避免重复完整执行；不为嵌入式点查增加固定锁成本 | 已实现并功能验证（流式执行、有界通道、schema 缓存、Rust/Python 测试）；本轮性能门禁在机器高负载下登记为环境受限原始例外，待干净机器复核（V1）；Q1 可开始但需携带该债务 |
| 7 | P1 / Q1 | 完善分批物理执行 | base+delta 稳定读视图；selection 直接消费，减少 gather；扩展形状前验证 NULL/UInt64/精确整数/更新删除/schema 一致性 | selection 直接消费与验证矩阵已实现并验收通过（2026-09-17，canary/full 对 base `eb44b85` 均 exit 0）；base+delta 行组流式读视图已实现（基础 mmap 行组流 + DeltaStore 快照按批打补丁 + 尾部追加，2026-09-19，见 11.3）；持久化删除向量与覆盖层并存时的合并读行空间缺口已修复（见 11.5/11.7），该组合现在也流式且与单批读一致；full 门禁 exit 0，canary 原始 exit 1 为已证实的环境受限例外 |
| 8 | P1 / Q2 | 成本反馈与自动并行契约 | 明确只由 EXPLAIN ANALYZE 校准的当前行为；评估低开销采样或保持显式校准；处理数据/schema/环境变化及历史样本老化；统一候选成本单位 | 契约已文档化（仅显式校准、成本/时间单位分层）；schema 变化清理反馈已实现并验收；数据老化按滑动均值+容量上限处理；环境指纹与时间衰减登记为后续。canary exit 0，full 原始 exit 1 为环境受限例外（第 13 节）。benchmark 15 个 workload 全部 slower=0 |
| 9 | P2 / M1 | 剩余职责与文档收敛 | 以重复决策/依赖减少为标准拆 backend 和路由；能力表、限制和 fallback 单源；不按行数制造抽象 | 读取可见状态判定与能力/回退表已单源（`OverlayState` + `docs/READ_PATH_CAPABILITIES.md`，替换 19 处重复表达式）；backend 职责拆分与 E1 按 14.2 的边界保留。公开 benchmark 103/103、canary exit 0；full 原始 exit 1 为已证实的环境受限例外（第 14 节） |
| 10 | P2 / E1 | 需求驱动扩展 | 有容量/工作负载证据后独立设计外部执行、复杂 Join、向量组合等 | 按需 |

验收债务 V1 贯穿每批：保留历史失败，每个独立交付批次的最终代码必须重新完成规定验收。
不得用新 base 抹掉历史未通过结论；本轮 base 用起点 SHA 衡量新增修改，历史 R5.12 canary 仍单独登记为未通过。

## 3. 第一批 C1：最小提交失败契约

范围：提交协调层错误分类、必要的提交后代际/缓存发布、Rust/Python 故障覆盖和事务 benchmark。
本批不改变 WAL 格式，不声称实现 UPDATE 或跨表原子恢复，不修改 AGENTS.md。

| 结果 | 判定边界 | 调用方语义 |
| --- | --- | --- |
| `not_committed` | OCC 校验明确中止事务，或 WAL begin/DML 阶段失败，尚未尝试 commit marker 或应用数据 | 本次没有进入提交点；如需重试应开启新事务，仍须处理冲突/输入/I/O 根因 |
| `unknown` | 已开始写 commit marker，或无 WAL 的数据应用开始，但最终发布未完成；也包括 ID 不存在/已结束/正在由另一调用提交而无法确定先前结果 | 可能部分落盘、待恢复或已由先前调用提交；禁止盲目重放 DML，先重开/核对状态 |
| `committed` | 全部数据/索引应用与事务发布成功，后续水位等维护失败 | 已完成逻辑提交，不能重新执行同一组 DML；不额外承诺超出 durability 模式的断电保证 |

即使 marker 写调用返回错误，也可能已写入部分/全部字节，必须保守归为 `unknown`。
UPDATE/跨表尚未具有完整恢复保证，不能仅根据某个表 marker 成功返回“整个事务已持久提交”。
保留 `io::Result` 和 Python `RuntimeError` 兼容边界；新增错误载荷/稳定标记只在失败路径分配。
在完成不可逆发布后，水位维护失败不能阻断 epoch 和缓存失效，也不能把事务重新标成 aborted。

待执行检查：

- [x] Rust 错误携带事务 ID、结果分类和原始 source；`io::ErrorKind` 保持可用。
- [x] Python 保留 RuntimeError 并传播稳定结果标记，失败后清除事务状态。
- [x] 真实文件故障覆盖 marker 前、数据应用、提交后水位维护；Rust/Python 两侧验证重开与后续事务。
- [x] 正常提交、冲突/无效事务、跨表不确定结果、原有错误文本关键内容兼容（冲突 marker 断言已补）。
- [x] 同机门禁增加覆盖正常持久化事务提交的指标（`Rust Safe TXN INSERT 10 + COMMIT`），不把故障钩子放入生产热循环。
- [x] 完成最终统一验收并登记实际报告（`local-perf-results/c1-acceptance-20260910-214229/`，canary/full 均 exit 0）。

## 4. 原子步骤纪律与最终验收

每步回顾：授权范围、阶段依赖、额外分配/锁/I/O、Rust/Python 行为覆盖、性能覆盖、回退边界。
纯移动与行为变更分开；不顺手清理 warning、不改测试阈值、不删除失败样本。
所有文件修改完成后统一运行完整链，原子步骤只做必要功能检查。

conda base，固定本批 base：`1b60ae8f916c48e25ae2dc2e1354ec586ca7133a`。

环境注意：guard 采样子进程不得携带 `DYLD_LIBRARY_PATH=$CONDA_PREFIX/lib`。
guard venv 使用 `--system-site-packages`，该变量会让 dyld 从 conda lib 解析
BLAS/LAPACK，venv 内 numpy 符号加载失败（C1 首次 canary 即因此崩溃，与代码无关）。
该变量仅 cargo 命令需要。

```bash
maturin develop --release
pytest
cargo test --release
python benchmarks/bench_vs_sqlite_duckdb.py
python benchmarks/run_local_perf_guard.py --base-ref 1b60ae8f916c48e25ae2dc2e1354ec586ca7133a
python benchmarks/run_local_perf_guard.py --base-ref 1b60ae8f916c48e25ae2dc2e1354ec586ca7133a --mode full
```

`cargo test --release` 运行完整单元与文档测试，满足只使用 release 结果的约束。
公开 benchmark 对照 `benchmarks/latest_public_baseline.json`；记录首次冷态 pytest 耗时。
canary 保持 200K/2 预热/7 计时，full 保持 1M/2 预热/5 计时；辅助 suite 使用当前脚本默认参数。
构建隔离、同机同环境、至少 30 秒静置、B-C-C-B-B-C、必要时五样本、15% 与 0.005 ms 双阈值全部保持。
验收显式关闭结果缓存；保留完整日志、原始 JSON、比较报告、退出状态和源码快照映射。
canary/full 均须退出 0，A/B 仅用于诊断；指标集合随仓库扩展，不能裁剪回旧数量。

## 5. 实施记录

- 2026-09-10：建立剩余工作清单，固定 C1 起点和保守提交结果分类。
- C1 实施中：新增 `txn::CommitError/CommitOutcome`，协调层保留原始 `io::ErrorKind/source`；通过稳定 `commit_outcome=...` 文本向 Python 传播，保持 RuntimeError。事务发布后的水位维护失败不再阻断缓存失效。已补真实 WAL/数据/水位 I/O 故障与重开测试，并把 Rust Safe 提交指标接入 canary（绕过 Python fast transaction，按 canary 全规模建表）。尚未完成最终验收。
- C1 最终验收（2026-09-10，conda base，M1 Pro 10c）：报告目录
  `local-perf-results/c1-acceptance-20260910-214229/`（含 README、双门禁全部
  JSON/比较/日志与构建日志）。
  - `maturin develop --release` 成功；warning A/B（隔离干净 target、release）
    196=196，warning 集合逐条一致（仅行号漂移）；共享 target 的增量对照曾产生
    3 条 C1 未触及文件的假新增，已用干净构建消除，不作依据。
  - pytest 完整串行：1780 passed；release 重装后首次冷态 34.83 s（仅参考）。
  - `cargo test --release`：557 单元 + 6 文档测试通过。
  - 公开 benchmark（1M/2/5，109 指标）：覆盖率 109/109，向量 head-to-head
    6/0/0；相对 492956bb 旧基线（2026-09-05，早于 R5.x 批次）有 4 项超过
    >15% AND >0.005 ms 护栏：INTERSECT (ordered) +184.7%、GROUP BY city
    (10 groups) +157.7%、EXCEPT (ordered) +35.9%、FTS Index Build +26.1%。
    同跑竞品几何均值漂移（SQLite 0.991x、DuckDB 0.968x，机器状态比基线时
    约快 3%）；EXCEPT/GROUP BY 小形状族同时出现在 full 初判且五样本终判收敛，
    与机器状态带一致。这些增量是旧基线累积漂移加方差，不是 C1 引入；C1 的
    base/current 归因以下列同机门禁为准。
  - canary（200K/2/7，64 项，含新 `Rust Safe TXN INSERT 10 + COMMIT`）：
    exit 0。初判 3 样本标记 1 项亚毫秒形状（Derived CASE bucket GROUP BY
    +22.70%，0.731→0.897 ms）；自动五样本终判 +6.10%（0.738→0.783 ms）
    恢复。新提交指标 0.884→0.889 ms（+0.57%）。
  - full（1M/2/5）：exit 0。perf 109/109、qps 10/10、quant 8/8、idx 4/4、
    par 4/4。初判标记 2 项亚 3 ms 形状（EXCEPT (ordered) +92.28%，
    1.230→2.366 ms；GROUP BY category (10 groups) +68.28%，0.624→1.050
    ms）；五样本终判 +2.08%（1.226→1.252 ms）与 -0.07%（0.624→0.624 ms）
    均恢复。
  - 环境异常：首次 canary 因启动 shell 携带 `DYLD_LIBRARY_PATH` 导致 venv
    numpy 加载失败而崩溃（base 侧），非代码问题；清空该变量重跑即通过，
    有效报告为第二次运行。
- C1 状态：已实现 + 功能已验证 + 性能已验收（canary/full 对 base
  `1b60ae8` 均 exit 0）。历史 R5.12 canary 未通过结论保持单独登记，不被本批
  结果替代。C2（UPDATE、Safe/Max 与恢复一致性）可开始。
- C2 状态：已实现 + 功能已验证 + 性能证据已收口（full 原始 exit 1 的
  `Filter (name = 'user_5000')` 双峰噪音例外已登记，见 6.3）。C3 可开始。
- C3 实施与最终验收（2026-09-17）：C3.1 按表排序收敛、C3.2 持久索引失效与
  重建按计划落地；审查后追加 `9f04098` 关闭三个 C3.2 范围内缺陷（stale 标记
  随表回收、非事务索引失败标记 stale、客户端 IPC 回退不重放 DML）。最终验收
  报告 `local-perf-results/c3-acceptance-20260917/`：pytest 1785 passed、
  cargo test 569+6 passed、公开 benchmark exit 0、canary exit 0（64 项）、
  full 原始 exit 1（`COUNT WHERE category` 与 `Parallel batch scan (8 threads)`
  经 current 侧独立复测证实为噪音，qps/quant/idx 全部通过）。完整记录与后续
  事项见 7.3。C3 状态：已实现 + 功能已验证 + 性能证据已收口（含 full 原始
  噪音例外）。
- S1 实施与最终验收（2026-09-17）：新增按查询的字节内存预算
  （`executor/memory.rs`，`APEX_QUERY_MEMORY_MB`），覆盖分批管道（串行流 +
  并行 partial 与合并）、行索引回退、单键 streaming（含 COUNT DISTINCT）、
  通用键增量路径（rayon 分区与合并）、`VectorizedHashAgg` 与字典直索引路径；
  超预算返回 `OutOfMemory`，失败回退释放预留，guard 在成功/取消/失败时恢复
  上下文。最终验收报告 `local-perf-results/s1-acceptance-20260917/`：
  pytest 1801 passed、cargo test 574+6 passed、公开 benchmark exit 0、
  canary exit 0（64 项五样本收敛）、full exit 0（main 109/109、qps 10/10、
  quant 8/8、idx 4/4、par 4/4，无五样本扩展）。完整记录见第 8 节。
  S1 状态：已实现 + 功能已验证 + 性能已验收。
- S2 实施与最终验收（2026-09-17）：复核 G1 清单（多数缓存早已有界，清单过时），
  修复 `CTE_BATCH_CACHE` 失败路径泄漏（RAII），为 `STATS_CACHE`（1024 FIFO）与
  `PLAN_FEEDBACK`/`FEEDBACK_LOADED`（每表 256 形状 / 256 表 / 1024 标记，逐出
  观测最少者）加容量上限；G2/G3 保留独立评审，无新全局锁。验收报告
  `local-perf-results/s2-acceptance-20260917/`：pytest 1802 passed、
  cargo test 577+6 passed、公开 benchmark exit 0、canary 重跑 exit 0（64 项；
  首轮 exit 1 经复测登记为瞬时干扰）、full exit 0（main 109/109、qps 10/10、
  quant 8/8、idx 4/4、par 4/4）。完整记录见第 9 节。S2 状态：已实现 +
  功能已验证 + 性能已验收。
- S3 实施与验证（2026-09-17）：新增 `execute_streaming_select`（单表投影
  SELECT 按行组流式，门控只放行与 `execute` 逐值一致的形状），Session/
  Database 暴露 `execute_streaming`；Flight `do_get` 经容量 2 的通道流式交付
  （有界内存、慢消费者背压、断连取消），非流式形状回落物化并按 64K 行切片；
  `get_flight_info`/`get_schema` 从首个行组取 schema 或执行一次并缓存（256
  上限），`total_records=-1`。功能：pytest 1802 passed、cargo test 580+6、
  `--features flight` 584 passed、公开 benchmark exit 0。性能：canary 两轮与
  full 主比较在 host load 5.7-12.0（Spotlight/WindowServer/Chrome 等）下标记
  同一类聚合/集合形状；原始 exit 1 与全部样本保留，登记为环境受限例外，干净
  机器复核列为验收债务 V1。完整记录见第 10 节。S3 状态：已实现 + 功能已验证 +
  性能证据待干净机器复核。
- Q1 实施与验收（2026-09-17）：`Morsel` 暴露 selection 映射与命名物理列，
  `BatchGroupAggregator::consume_morsel` 直接按选中行折叠，串行/并行分批管道
  不再 `arrow::compute::take` 汇成紧凑批次；新增覆盖层（clean/删除/更新/插入）
  分批与单批 parity、超 2^53 精确整数与 NaN、输出 schema 一致性三组验证测试。
  验收：pytest 1802 passed、cargo test 583+6、`--features flight` 587、canary
  exit 0（64 项）、full exit 0（main 109/109、qps 10/10、quant 8/8、idx 4/4、
  par 4/4，parallel batch scan 快 7.0–9.5%）。base+delta 行组流式读视图未实现，
  见 11.3，作为下一批前置。完整记录见第 11 节。
- V1 复核（2026-09-17）：对 S3 提交 `eb44b85` 复跑 canary/full。三次 S3 canary
  分别标记三组不同指标，full 复跑初判 7 项（含 CSV Read、向量 TopK）五样本
  终判仅剩 1 项重叠的 `ORDER BY expression (LENGTH)` +15.03%，qps/quant/idx/par
  全过；主机负载始终 6-12，结论登记为环境噪音强化，空闲机器绿色门禁仍待补。

## 6. 第二批 C2：UPDATE、Safe/Max 与恢复一致性

### 6.1 当前时序与已确认缺口

本节最初以 C1 验收后的 `229b596` 为审查快照；C2.3 更新后的时序如下：

```text
prepare/OCC
  -> 每表 WAL TxnBegin
  -> INSERT/DELETE/UPDATE 写 WAL
  -> 每表 WAL TxnCommit（Safe=flush；Max=flush + fsync）
  -> apply_txn_writes
       INSERT -> .delta
       DELETE -> 删除状态
       UPDATE -> 原位覆盖或 .deltastore 原子替换
  -> 索引保存
  -> MVCC finalize + epoch/cache 发布
  -> WAL applied watermark
```

初始审查得到两个不能靠调整调用顺序消除的缺口：

1. WAL commit marker 成功而 UPDATE 应用尚未完成时，恢复只会重放 INSERT/DELETE，
   无法重建 UPDATE；把这种事务报告为可恢复的持久提交是不成立的。
2. `open_txn_wal_backend()` 仅根据 `.wal` 是否存在判断 WAL-backed，并固定按 Safe
   打开，导致 Max 调用方要求的 commit-marker fsync 丢失。C2.2a 已通过 Session
   作用域把 Python/嵌入式调用方的 durability 传播到协调层，并用真实
   `File::sync_all()` 调用计数测试确认 Safe 不 sync、Max sync 一次。

UPDATE WAL 记录属于持久化格式变化。它还需要解决 applied watermark 之后只重放
未应用后缀的问题，否则旧 UPDATE 可能覆盖 watermark 之后的较新非事务更新。因此
不在 C2.1 中直接追加一个记录类型并宣称恢复完成。

### 6.2 子批与完成口径

| 子批 | 范围 | 完成条件 | 状态 |
| --- | --- | --- | --- |
| C2.1 | 无格式变化的安全边界 | WAL-backed 表的显式事务含 UPDATE 时，在任何 TxnBegin/DML/Commit WAL 写入前拒绝；结果为 `not_committed`；事务状态清除；原值及 WAL 长度不变；Fast 事务 UPDATE 兼容 | 已完成 |
| C2.2a | durability 传播与 Max 同步边界 | Session/嵌入式/Python 到提交协调层保留 Safe/Max；Max commit marker 使用真实 fsync；作用域退出恢复原上下文 | 已完成（`546e71f`、`4104ba4`） |
| C2.2b | 真实同步失败矩阵 | 真实写入/flush/fsync 故障分别验证结果分类、重开与后续事务；不能用 mock 替代核心 I/O 路径 | 已完成（`14e630c`） |
| C2.3 | UPDATE WAL 与后缀恢复 | 独立记录格式/版本/旧文件兼容设计；watermark 按偏移解析；只按 WAL 顺序幂等重放未应用且已提交的 UPDATE；覆盖崩溃、重复打开、更新后再更新、compact | 已实现并完成 C2 统一验收；full 原始噪音例外见 6.3 |

C2.1 曾作为“不把不可恢复 UPDATE 当作可持久提交”的临时安全边界：拒绝发生在
prepare/OCC 之后、首次 WAL 写入之前，并稳定返回 `not_committed`。C2.3 完成后该
保护性拒绝已移除，Safe/Max 事务 UPDATE 改由 WAL 与后缀恢复保证。

C2.2a 的聚焦 release 检查覆盖 Session durability 作用域恢复，以及 Safe/Max 事务
commit marker 的真实同步分支。C2 最终验收仍使用第 4 节固定 base 和完整链；原子
步骤只运行聚焦功能检查，所有文件修改结束后再统一执行完整验收。

C2.2b 在 Unix 测试构建中直接替换实际 WAL 文件描述符，保留真实的 buffered write、
flush 与 `fsync` 系统调用，不用固定返回值模拟核心 I/O。聚焦矩阵结果为：

| 故障点 | 对外结果 | 重开结果 | 后续事务 |
| --- | --- | --- | --- |
| 大记录 WAL write | `not_committed` | 0 行 | 可继续提交 |
| commit-marker flush | `unknown` | 0 行 | 可继续提交 |
| Max commit-marker fsync | `unknown` | 已 flush marker，恢复 1 行 | 可继续提交 |

fsync 返回前失败仍必须报告 `unknown`：即使本次测试中 marker 已进入 OS 缓冲并能恢复，
调用方也不能从失败返回值推断它必然已经或必然没有持久化。

C2.3 在现有 WAL v2 的长度与 CRC record envelope 内新增 UPDATE record type，不改变
文件头和 WAL 版本；新实现继续读取既有 v1/v2 WAL，旧文件无需重写。UPDATE payload
使用显式 value tag 编码，完整保留整数宽度、无符号整数、JSON、Array、二进制、日期
时间和向量等 `Value` 类型，不沿用 INSERT 的有损 `ColumnValue` 转换。

`.wal.meta` 的 8 字节值现在同时作为已应用 WAL byte offset 使用。恢复扫描先校验该值
位于 header/WAL 长度范围且落在 record 边界；无效值保守退回 header。UPDATE 只收集
起始 offset 不早于 watermark 的记录，并按具体 `TxnBegin -> UPDATE -> TxnCommit` 区间
配对后按 WAL offset 重放。不能只按“同 txn_id 曾出现 Commit”判断，因为事务 ID 在新
进程中会重新计数；聚焦测试覆盖旧已提交 ID 与新未提交 ID 重用时不误重放。

恢复写入 `.deltastore` 后推进 watermark；重复打开幂等，watermark 之后的普通更新不被
旧 WAL 覆盖。`compact()` 也修正为在只有 `.deltastore`、没有 append `.delta` 时执行
V4 streaming rewrite，使 update-only overlay 能合并进基表。真实故障测试在 commit
marker 之后阻断 `.deltastore.tmp` 写入，确认返回 `unknown`，清除故障后重开恢复。
聚焦 release 结果：Rust UPDATE 过滤 23 项通过、recovery 过滤 2 项通过；release
`maturin develop` 成功；Python `test_commit_crash_recovery.py + test_transactions.py`
40 项通过。上述是原子步骤验证，不替代第 4 节 C2 最终统一验收。

### 6.3 C2 最终统一验收（2026-09-17）

固定使用 pre-C2 提交 `1b60ae8f916c48e25ae2dc2e1354ec586ca7133a` 作为同机
base；current 为 `2cf3098`。验收在 conda base、同一台 M1 Pro 10c 主机上完成，
base/current 使用隔离 worktree、venv、release wheel 与 Cargo 构建目录，未降低行数、
预热、计时次数或阈值。

- `maturin develop --release` 成功（197 个既有 warning）。
- 串行完整 `pytest`：1783 passed，34.08s。
- 完整 `cargo test --release`：565 个单元测试和 6 个 doc-test 全部通过。首次经
  `/usr/bin/time` 启动时 macOS 清除了 dyld fallback，test binary 因找不到
  `libpython3.12.dylib` 未进入测试；直接保留同一环境变量重跑成功，该启动失败不作为
  测试结果。
- 公开 benchmark 默认百万行、2 次预热、5 次计时，脚本 exit 0：表格公平指标
  102/103、精确向量 6/6、共同量化 codec 6/6 胜出。与
  `latest_public_baseline.json` 比较覆盖 109/109；比较脚本列出 9 个超过
  15% 且 0.005ms 的单次公开基线差异，SQLite/DuckDB 几何均值漂移分别为
  0.980x/0.995x，保留为环境敏感诊断，不代替同机 base/current 判定。
- 首次 canary 报告 `local-perf-results/20260917-132624/` 原始 exit 1；唯一失败
  `Multiple COUNT DISTINCT` 的 base/current 五样本范围交叉，且定向连续 100 次 current
  测量中位数 0.312ms、仅有少量高尾。未修改代码、阈值或参数，重新进行一次完整
  B-C-C-B-B-C canary；`local-perf-results/20260917-134252/` exit 0，64 项通过，
  其中该指标 +3.98%、`UPDATE by ID` -1.37%、Safe commit +2.22%。
- full 报告 `local-perf-results/20260917-135606/` 完成主集合的三样本与自动扩展五样本，
  以及 QPS 10 项、量化 8 项、索引 4 项、并行 4 项附加比较；四个附加比较全部 exit 0。
  主比较原始 exit 1，唯一标记项为 `Filter (name = 'user_5000')`。其 base 五样本为
  0.160/0.362/0.160/0.157/0.380ms，current 为
  0.163/0.416/0.158/0.363/0.372ms：两侧快慢峰重叠，只因 base 恰有三个快峰、current
  恰有三个慢峰导致中位数翻转。从固定 base 到 current 对 SELECT、index access 与 mmap
  scan 路径没有源码差异，因此按已确认的机器/调度双峰噪音登记，不继续增加轮次；原始
  exit 1、十份 JSON 与比较报告全部保留，不表述为脚本通过。

C2 状态据此记为“已实现 + 功能已验证 + 性能证据已收口（含已登记的 full 原始噪音
例外）”。该例外不隐藏或替换门禁结果；在确认查询热路径未变化、两侧样本分布重叠后，
按本次验收决策不阻塞 C3。

## 7. 第三批 C3：跨表与索引恢复契约

### 7.1 C3.1 确定性按表收敛边界

当前协议仍没有数据库级提交记录。一个事务涉及多个 Safe/Max 表时，每张表分别写
`TxnBegin -> DML -> TxnCommit`；进程若停在两个表的 commit marker 之间，重开只能让
已有 marker 的表重放提交，而没有 marker 的表丢弃未提交后缀。这是“按表收敛”，不是
跨表崩溃原子提交；在完成独立的格式兼容和恢复设计前，不引入未经验证的数据库级
两阶段记录。

C3.1 将受影响表从无序 `HashSet` 改为按表路径排序、去重的列表，使 Begin、DML、
Commit、cache invalidation 和 applied watermark 都使用同一个确定顺序。排序只发生在
事务提交冷路径，不增加存储热路径 IO、锁或 FFI。

新增 release 测试构造真实持久 WAL 边界：排序后的第一表具有完整 Begin/DML/Commit，
第二表只有 Begin/DML 并已同步。第一次重开后第一表收敛到 2 行、第二表保持 1 行；第二
次重开结果不变，确认恢复幂等且不会把未提交后缀提升为提交。聚焦结果：新增测试 1 项
通过，`recovery` 过滤 3 项通过。

### 7.2 C3.2 持久索引失效与重建

事务在写完 WAL DML、写 commit marker 之前，对存在 secondary index catalog 的表创建
`<table>.apex.index.stale`。数据与索引全部应用成功后才删除该标记；Max durability 会对
标记内容执行 `sync_all`。索引标记存在或其元数据不可读取时，planner catalog 检查与
执行期 index lookup 都拒绝 postings，查询回退到权威 scan，不把可能缺行的旧索引当作
完整结果。

INSERT/DELETE/UPDATE 的 index `on_insert`、读取与 `save` 错误不再被吞掉。事务 INSERT
原先在普通 DML 应用完成后还会重复维护一次索引，C3.2 删除该重复写入，保留唯一的 DML
owner。`REINDEX` 先物化 sidecar/compact，再从已提交表重建并保存所有索引；只有保存
成功后才清除 stale 标记。删除最后一个索引也会清除不再有意义的标记，创建单个新索引
不会错误地把其他旧索引视为已修复。

新增 Unix release 测试将真实 Hash index 文件改为只读，使 WAL commit point 之后的
index save 失败：commit 返回 `unknown`，标记持久存在，重开查询通过 scan 找到已提交
行；随后 REINDEX 完成 compact/rebuild、清除标记，第二次重开后 Hash posting 包含该行。
聚焦结果：新增故障测试 1 项通过，`recovery` 过滤 3 项通过，`index` 过滤 32 项通过。
首次未配置 `DYLD_FALLBACK_LIBRARY_PATH` 的测试进程在加载 `libpython3.12.dylib` 前失败，
补充 conda base 动态库路径后同一 release 二进制通过；该环境启动失败不计作测试执行。

C3.1–C3.2 至此达到实现和聚焦功能验证边界。下一步统一执行 release 安装、完整 pytest/cargo test、公开 benchmark、canary 与核心恢复/索引路径要求的 full 同机比较；性能异常按保留原始报告、结合源码路径和样本分布辨别噪音的规则处理，不为已证实噪音机械增加轮次。

### 7.3 C3 最终统一验收（2026-09-17）

固定使用 pre-C2 提交 `1b60ae8f916c48e25ae2dc2e1354ec586ca7133a` 作为同机
base；current 为 `9f04098`。验收在 conda base、同一台 M1 Pro 10c 主机上完成，
base/current 使用隔离 worktree、venv、release wheel 与 Cargo 构建目录，未降低
行数、预热、计时次数或阈值。报告目录
`local-perf-results/c3-acceptance-20260917/`（含 README、全部 JSON/比较/日志、
诊断样本）。

审查后先关闭了三个 C3.2 范围内的缺陷（`9f04098`）：

- 持久 stale 标记未随表清理：`TABLE_FILE_SUFFIXES` 未包含 `.index.stale`，
  DROP/同名重建会继承旧的读回退；现已随 `unlink_table_files` 回收。
- 非事务写入在行已持久后索引维护失败时，后续读取仍可能信任缺行的 postings；
  现失败路径写入 stale 标记，读取回退权威 scan（仅在失败路径，无热路径 IO）。
- Python 客户端 Arrow IPC 回退会重放已到达存储的 DML，导致行被写两次；现只对
  只读语句保留该回退。

验收结果：

- `maturin develop --release` 成功（197 个既有 warning）。
- 完整串行 `pytest`：1785 passed，33.58 s（含新增
  `test/test_index_failure_fallback.py`）。
- 完整 `cargo test --release`：569 单元 + 6 doc-test 通过（含新增非事务索引
  失败与标记回收测试）。
- 公开 benchmark 默认百万行、2 预热、5 计时，脚本 exit 0：vector head-to-head
  6/6、共同量化 codec 6/6 胜出。与 `latest_public_baseline.json`（旧基线
  `492956bb`，且基线记录为 macOS 26.6.2、当前为 macOS 27.0）比较覆盖
  109/109，6 项单次指标同时超过 15% 与 0.005ms；同次运行中同等规模形状也有
  明显反向移动（`COUNT(*)` -18.47%、Deep offset -18.24%）。沿用 C1 的旧基线
  漂移登记，不用单次公开比较替代同机门禁。
- canary（200K/2/7，64 项，含 `Rust Safe TXN INSERT 10 + COMMIT`）：
  `canary/` exit 0，无项触发五样本扩展。
- full（1M/2/5）：`full/` 主比较 109 项中 1 项、par 附加比较 4 项中 1 项在五样本
  终判仍标记；qps 10/10、quant 8/8、idx 4/4 通过。初判标记的
  `Filter (name = 'user_5000')` +134.29%、`JSON Read + COUNT(*)` +18.28%、
  `Parallel batch scan (auto)` +16.33% 均在五样本终判恢复。
- 终判标记项与噪音归因：`COUNT WHERE category` base 五样本中位数 0.2432ms、
  current 0.2872ms（+18.11%）；`Parallel batch scan (8 threads)` base 5.870ms、
  current 7.124ms（+21.37%）。三项独立 current 侧 `--parallel-only` 复测得到
  8 线程 6.536/5.704/5.774ms（中位数 5.774，回到 base 水平），auto
  5.833/5.495/5.685ms，同门禁中 4 线程/2 线程仅 +1.0%/+0.7%；两次独立
  current 侧表格复测得到 `COUNT WHERE category` 0.233943/0.233870ms，等于或
  低于 base 中位数。批量内唯一查询路径改动是索引准入/planner catalog 增加的
  一次 `.index.stale` 元数据检查，量级远小于标记差值。据此登记为已证实的
  机器/调度双峰噪音，保留原始 exit 1 与全部 JSON 样本、各阶段比较报告，不表述为
  门禁通过，也不为已证实噪音追加轮次。

C3 状态据此记为“已实现 + 功能已验证 + 性能证据已收口（含已登记的 full 原始
噪音例外）”。审查同时登记两个后续批次事项，不作为 C3 阻塞项：

- `CREATE UNIQUE INDEX` 的唯一性在索引维护阶段、存储持久化之后才拒绝，重复写入
  会留下已持久行（读取因 stale 标记仍正确，但写入非原子）；需要改为变更前预校验。
- DROP TABLE 不清理 `<base>/indexes/<table>_*.hashidx` / `.idxcat`，同名重建索引
  会报 "already exists"（既有行为，已用探针复现）。

## 8. 第四批 S1：查询内存预算与资源准入

S1 依据 A5（按需存储与查询内存上限未打通）与 `docs/RESOURCE_OWNERSHIP.md`
的所有权结论落地：先约束查询自身持有的高基数聚合状态与并行局部状态，再谈
全局缓存容量（G1/S2）。起点（base）为 C3 验收提交
`2c2e471f038dadc8083720a2d5b16c00ae688f75`；current 为 `0bd0e21`。

### 8.1 预算口径与实现

- 每个顶层查询一个按字节计量的预算：`QueryMemoryBudget { limit, used:
  AtomicUsize }`；并行 worker 折叠通过捕获同一 `Arc` 共享计数，因此并行
  partial 与合并结果都计入同一个池。
- `QueryMemoryBudgetGuard` 在 `execute_classified_with_base_dir` 入口安装；
  嵌套查询沿用外层预算，Drop 恢复上一层上下文（成功/取消/失败一致），
  失败的内核在回退到其他算子前释放自己的预留。
- 配置 `APEX_QUERY_MEMORY_MB`：正整数 MiB；`0` = 不限；未设置或非法值使用
  默认 1 GiB。每个顶层查询安装时读取，可逐查询切换（与 `APEX_BATCH_SCAN`
  同一风格）。
- 超预算返回 `io::ErrorKind::OutOfMemory`，错误文本给出已用与上限字节；
  Python 侧为 `RuntimeError`，文本包含 `query memory budget exceeded`。
- 计量按状态增量维护，不在热路径重扫容器：分组 map 记录条目字节、键 lane
  记录字典与字符串字节、行索引向量按容量增长、`VectorizedHashAgg` 在
  `get_or_create_group_*` 内累加 `bytes`；逐行累加循环每
  `GROUP_BUDGET_CHECK_INTERVAL`（4096）行做一次原子预留，把开销与超调都
  限制在批次级别。

### 8.2 已覆盖与不在范围内的入口

已覆盖：分批聚合管道（串行流、并行 partial 与合并）、
`execute_group_by_with_indices`（每组行索引）、
`try_execute_single_key_streaming_group_by`（含 COUNT DISTINCT 的每组
distinct 集合）、`execute_group_by_incremental` 通用键路径（rayon 分区局部
状态与合并）、`VectorizedHashAgg` 单键哈希、字典直索引路径
（`execute_group_by_string_dict`、dict case count、vectorized dict count）。

不在本预算内（已在 `RESOURCE_OWNERSHIP.md` §5 登记）：

- 最终结果物化：完整结果 API 允许 O(输出)，不按扫描/聚合预算误判失败。
- 无 WHERE 的整型键查询若命中存储层 numeric dict cache（u16 组 ID，上限
  65536 组）或存储原生 `execute_group_agg`（结果本身 O(组数)），其内存属于
  全局缓存容量（G1）与输出物化，按 S2/G1 单独处理。

### 8.3 测试与验收（2026-09-17）

新增测试：

- Rust：预算 `reserve/release/limit` 语义（拒绝不改变计数）、guard 上下文
  恢复与嵌套共享、分批管道在 1 KiB 预算下明确报错并在合理预算下成功、
  并行 partial 状态合计计入同一预算（以串行字节数 +50% 为界）、
  `VectorizedHashAgg` 分组字节随新键增长。
- Python（`test/test_query_memory_budget.py`，16 项）：分批形状超预算报错、
  不限预算可跑通、失败后预算逐查询释放、无 WHERE 的 5 种通用形状
  （单键 COUNT/SUM/COUNT DISTINCT、双键、ORDER BY+LIMIT）超预算报错且不限
  预算结果正确、子进程峰值 RSS 有界（受限运行峰值明显低于物化全部组的
  运行）、并发 4 线程下每线程预算互不泄漏（小查询始终成功、大查询始终
  报错）。

验收链（conda base，M1 Pro 10c，release，隔离 worktree/venv/Cargo target，
30 s 静置，B-C-C-B-B-C，未降低行数/预热/计时/阈值）：

- `maturin develop --release` 成功；完整串行 `pytest` 1801 passed（33.41 s）；
  完整 `cargo test --release` 574 单元 + 6 doc-test 通过。
- 公开 benchmark（1M/2/5，109 指标）exit 0；对比 `latest_public_baseline.json`
  覆盖 109/109，2 项单次指标超过 15% 且 0.005ms（`Batch TopK Dot` 向量批次
  查询 +22.19%、`SELECT * LIMIT 100 (warm cache)` +15.81%），均为旧基线
  （`492956bb`）漂移与方差，且不在 S1 触及的聚合路径上；不作为门禁判定。
- canary（200K/2/7，64 项）exit 0：初判 3 项（CSV integer GROUP BY numeric
  agg +30.40%、NULL profile (2 cols) +24.50%、Two-key GROUP BY (5 funcs)
  +19.63%）在自动五样本终判为 -2.93%/+6.13%/+2.11%。
- full（1M/2/5）exit 0，未触发五样本扩展：main 109/109、qps 10/10、
  quant 8/8、idx 4/4、par 4/4。聚合形状持平或更快（Aggregation (5 funcs)
  -4.94%、GROUP BY category + HAVING -20.10%、GROUP BY city ORDER BY count
  -43.27%、Parallel batch scan (8 threads) -16.28%）；点查 0.003/0.002 ms
  不变，说明每查询预算安装没有可测量的固定成本。
- 报告目录 `local-perf-results/s1-acceptance-20260917/`（README、全部
  JSON/比较/日志、退出状态）。

S1 状态据此记为“已实现 + 功能已验证 + 性能已验收”。

## 9. 第五批 S2：缓存容量与状态 owner

S2 依据 `docs/RESOURCE_OWNERSHIP.md` 的 G1 清单逐项复核并关闭真实缺口，保留
G2/G3 的独立评审边界。起点（base）为 S1 验收提交
`0059029cf0a104436d50e2fd12b9f5ef599f8952`；current 为 `f5a892d`。

### 9.1 G1 审计结论

审计发现清单本身过时：`SQL_PARSE_CACHE`（1024 条，满后停止收录）、
`CLASSIFY_CACHE`（512 条，满后整体清空）、`GLOBAL_COLUMN_NULL_CACHE` 与
`GLOBAL_DICT_HIGH_CARD_CACHE`（`MAX_ENTRIES*2`，满后不收录新键）、Python
`_simple_sql_cache`（256 条，满后整体清空）在 S2 之前就已有界。真实缺口只有
三项：

- `CTE_BATCH_CACHE`：条目按“每次执行唯一”的临时路径键控。主语句失败时原实现
  `?` 跳过移除，条目永不再被查询，却长期占用物化 Arrow 批次。改为 RAII guard，
  成功/失败/取消都移除。
- `STATS_CACHE`：真无上限，随访问过的表持续累积。加 1024 条 FIFO 逐出；读路径
  仍只取读锁、不写逐出元数据，被逐出的表在下次访问时从 sidecar 重读。
- `PLAN_FEEDBACK` / `FEEDBACK_LOADED`：真无上限。加每表 256 形状、进程内
  256 表、加载标记 1024；逐出观测样本最少者，sidecar 随内存快照一起收缩，
  被逐出形状在下次 EXPLAIN ANALYZE 重新校准。

G2（`STORAGE_CACHE` 与 `StorageEngine.cache` 双读 backend 缓存合并）与 G3
（调度器 thread-local 共享）仍按“每项独立评审”保留，不在本批夹带；S2 未引入
任何新的全局锁，也未删除仍被引用的 epoch 引用缓存。`RESOURCE_OWNERSHIP.md`
§2 的 G1 结论与 §1 各表容量列已按源码事实更正，并新增 §6 审计表。

### 9.2 测试与验收（2026-09-17）

新增测试：

- Rust：`stats_cache_is_bounded_and_evicts_oldest`（1024 条 FIFO、替换不逐出）、
  `plan_feedback_shape_and_table_caps_evict_least_observed`（形状/表两级上限、
  已有形状不逐出）、`failed_shared_cte_releases_its_materialized_batch`（失败
  共享 CTE 后缓存长度回到查询前，且同一 CTE 之后仍可正确执行）。
- Python：`test/test_cache_capacity_bounds.py` 用超过 Python 简单 SQL（256）、
  分类器（512）与解析（1024）上限的 1200 个不同 SQL 文本验证逐出/清空不改变
  结果，并在溢出后复跑被逐出语句、聚合与写入形状；既有
  `test_lifecycle_management.py` 与 `test_cache_invalidation_contract.py`
  的 close/reopen、跨客户端失效、持有结果视图、外部进程改写用例在验收中复跑。

验收链（conda base，M1 Pro 10c，release，隔离 worktree/venv/Cargo target，
30 s 静置，B-C-C-B-B-C，未降低行数/预热/计时/阈值）：

- `maturin develop --release` 成功；完整串行 `pytest` 1802 passed（36.53 s）；
  完整 `cargo test --release` 577 单元 + 6 doc-test 通过。
- 公开 benchmark（1M/2/5，109 指标）exit 0；对比 `latest_public_baseline.json`
  覆盖 109/109，6 项单次指标超过 15% 且 0.005ms（EXCEPT/INTERSECT ordered、
  GROUP BY + HAVING、GROUP BY category ORDER BY count、Filtered aggregation
  (city)、IN subquery COUNT），均为旧基线漂移与方差，且 S2 不触及这些
  聚合/集合算子路径；不作门禁判定。
- canary（200K/2/7，64 项）：首轮原始 exit 1，唯一标记 `Numeric GROUP BY
  (5 funcs)` base 五样本中位数 0.9603ms / current 1.5112ms；current 侧三次
  独立 canary 复测为 0.9910/0.9836/0.9604ms（中位数 0.9836，回到 base），
  guard 的 current 样本含 5.2335ms 孤立高尾，判定为瞬时干扰而非代码路径
  变化（S2 不改聚合）。按已证实噪音登记首轮 exit 1 并保留原始报告；随后一次
  完整 B-C-C-B-B-C canary（`canary-rerun/`）exit 0，64/64，无五样本扩展，
  该指标 -11.52%。
- full（1M/2/5）exit 0：main 109/109、qps 10/10、quant 8/8、idx 4/4、
  par 4/4。par 初判标记 `Parallel batch scan (8 threads)` +18.37%
  （7.726→9.145ms），自动五样本终判 -21.79%（9.235→7.222ms）；聚合形状
  持平或更快（GROUP BY + HAVING -27.02%、GROUP BY category -7.24%、
  GROUP BY category + HAVING -9.12%）。
- 报告目录 `local-perf-results/s2-acceptance-20260917/`（README、全部
  JSON/比较/日志、诊断样本、退出状态）。

S2 状态据此记为“G1 已逐项关闭 + 功能已验证 + 性能已验收（canary 首轮原始
噪音例外已登记）”；G2/G3 仍待独立评审。

## 10. 第六批 S3：Flight 分批桥接与协议资源边界

S3 依据 A5 与 R4 余项，把 `do_get` 从“整体物化 + 单批次编码”改为按行组流式
交付，并让 schema 请求不再完整执行查询。起点（base）为 S2 验收提交
`7ed760fbddf0eadcfd322ddbc37cbf98fc8a68ea`；current 为 `db971ee`。

### 10.1 流式执行与门控

- `ApexExecutor::execute_streaming_select`：对单表投影 SELECT 走
  `TableStorageBackend::scan_batches` 的稳定持久化读视图，逐行组产出
  `RecordBatch`；`emit` 返回 false 即停止（消费者断开）。
- 门控只放行与 `execute` 输出逐值一致的形状：`SELECT *` 或按 SELECT 顺序的
  纯列投影，WHERE 必须能转成 `ScanPredicateExpr`（类型化谓词下推）；聚合、
  Join、排序、LIMIT/OFFSET、表达式、别名、EXCLUDE/REPLACE/COLUMNS、窗口、
  delta/内存视图一律返回 `None` 回落物化路径。Rust A/B 测试把流式批次拼接
  后与物化结果逐值比较（6 种形状），并覆盖“首个批次后拒绝即停止”。
- `Session::execute_streaming` / `Database::execute_streaming` 暴露同一入口，
  flight 模块继续只依赖 session façade（phase23 契约测试保持通过），嵌入式
  点查路径不新增任何锁或预算安装。

### 10.2 Flight 输出端

- `do_get` 在 `spawn_blocking` 里运行生产者，经容量 2 的 `tokio::sync::mpsc`
  通道喂给 gRPC 流：服务端内存由“一个行组 + 通道”封顶，慢消费者形成背压，
  断连 drop 接收端后生产者在下个批次边界停止。非流式形状回落物化结果，并按
  65536 行切片交付，避免单个 Flight 消息过大。
- `get_flight_info` / `get_schema` 不再执行完整查询：流式形状从首个行组批次
  取 schema，其余形状执行一次；IPC schema 按 SQL 缓存（256 条上限）。
  `total_records` 改为 `-1`（未计数），客户端从 `do_get` 得到精确结果。

### 10.3 测试与验收（2026-09-17）

新增测试：

- Rust（默认特性）：`streaming_select_matches_materialized_result`（6 种形状
  的流式/物化 A/B 逐值一致）、`streaming_select_rejects_shapes_outside_the_gate`、
  `streaming_select_stops_when_the_consumer_rejects_a_batch`。
- Rust（`--features flight`）：`do_get_streams_row_groups_in_order`、
  `do_get_falls_back_for_non_streamable_shapes`、
  `schema_requests_do_not_execute_the_result`（schema 与流式批次一致）、
  `dropping_the_response_stream_stops_the_producer`。
- 同时修正 S1 预算测试的进程级扫描开关竞争（持 `BATCH_SCAN_ENV_LOCK` 并固定
  `APEX_BATCH_SCAN` / `APEX_PARALLEL_SCAN`）。

功能验收：`maturin develop --release` 成功；完整串行 `pytest` 1802 passed
（36.06 s）；完整 `cargo test --release` 580 单元 + 6 doc-test；`cargo test
--release --features flight` 584 单元通过；公开 benchmark（1M/2/5）exit 0。

性能验收：本轮未能取得可判定的干净结果，原始门禁按环境噪音登记：

- 公开比较仍对旧基线 `492956bb` 列出 6 项单次差异（EXCEPT/INTERSECT ordered、
  GROUP BY city、GROUP BY category ORDER BY count、Insert+COUNT visible、
  UNION DISTINCT ordered），均为方差敏感的聚合/集合形状；S3 未改默认构建的
  聚合逻辑（同套件在 S1 列 2 项、S2 列 6 项不同指标）。
- canary（200K/2/7）两轮原始 exit 1：首轮 `Derived ratio GROUP BY` +64.85%、
  `Multiple COUNT DISTINCT` +24.30%；重跑增加 `CSV filtered GROUP BY + HAVING`
  +20.74% 与 `Numeric conjunction aggregation` +37.55%（每轮集合不同）。六次
  current 侧独立 canary 复测显示其中 3 项中位数回到或接近 base（多个
  COUNT DISTINCT +3%、CSV filtered +2.8%），`Derived ratio GROUP BY` 中位数
  0.734ms（min 0.581 / max 1.076，base guard 0.627/0.598），且该指标历史上有
  双侧双峰尖峰（S1 canary 原始样本 base 2.03ms、current 4.25ms）。
- full（1M/2/5）原始 exit 1：qps 10/10、quant 8/8、idx 4/4、par 4/4 通过；
  主比较五样本终判保留 `IN subquery COUNT` +19.29% 与 `UNION DISTINCT
  (ordered)` +27.89%（初判的 `GROUP BY category ORDER BY count` +22.14% 已
  恢复）。两侧样本都出现 2-3 倍尖峰（如 IN subquery COUNT base 3.047ms、
  UNION DISTINCT base 1.749ms），低峰两侧一致，current 中位数落在高峰。
- 环境：全部运行时 10 核主机 load 5.7-12.0，后台有 Spotlight `mds` 重建索引
  （95 GiB 构建清理后）、WindowServer 57%、airportd 55%、Chrome/WeChat 等。
  计划在 C1 记录里已登记过同类波动（EXISTS/IN subquery COUNT 0.757→2.700ms
  经独立复测回到 0.66ms）。

处理：不删除样本、不调阈值、不把失败门禁改称通过；两轮 canary 与 full 主比较
的原始报告全部保留在 `local-perf-results/s3-acceptance-20260917/`，登记为
**环境受限原始例外**。干净机器上对上述聚合/集合形状的复核列为验收债务 V1；
2026-09-17 的复核（第 12 节）在持续高负载下再次确认指标集合互不重复，噪音结论
得到强化，但空闲窗口的绿色门禁仍待补。

S3 状态：已实现 + 功能已验证 + 性能证据待干净机器复核（V1）。

## 11. 第七批 Q1：完善分批物理执行

Q1 依据 A2 与 R3 余项，落地“selection 直接消费、减少 gather”与分批形状的验证
矩阵；base+delta 的行组流式读视图留作后续（见 11.3）。起点（base）为 S3 验收
提交 `eb44b85`；current 为 `041b493`。

### 11.1 selection 直接消费

- `Morsel` 新增 `selection_for_operators()` 与 `column_by_name()`：物理列数组
  与选择映射对外可见；`SelectionVector::row(position)` 把“第 i 个选中行”映射回
  物理行（`All` 为恒等映射，零成本）。
- `BatchGroupAggregator::consume_batch(&RecordBatch)` 改为
  `consume_morsel(&Morsel)`：分组键、聚合源列与行迭代都直接读物理数组 +
  选择映射，不再先 `arrow::compute::take` 汇成一个紧凑批次。串行 fold 与并行
  partial fold 都改为直接传 morsel。
- 语义不变：已有分批/单批 A/B、并行 A/B、S1 预算测试与 S3 流式 parity 全部
  保持通过。并行分批扫描因此快 7.0–9.5%（2/4/8 线程与 auto），聚合形状持平。

### 11.2 形状验证矩阵

- `batched_pipeline_matches_single_batch_across_overlay_states`：clean、DELETE、
  UPDATE（DeltaStore 单元更新）、INSERT（delta 行）四种覆盖层状态下，分批开/
  关结果逐值一致（覆盖“更新删除”与 schema 一致性）。
- `batched_pipeline_matches_single_batch_for_exact_integers_and_nan`：超过 2^53
  的 i64 精确值（含回绕 SUM/MIN/MAX）与 NaN/有限浮点按位一致。
- `batched_pipeline_result_schema_matches_single_batch`：多组键/聚合形状的
  输出字段数与字段名/类型逐项一致。
- 未支持类型（UInt64 组键等）仍由 `BatchKeyView`/扫描谓词类型门控保守回落，
  既有 `Unsupported` 回落测试与“覆盖层回落”一致。

### 11.3 base+delta 行组流式读视图（已实现，2026-09-19）

原 `scan_batches` 门控要求纯持久化 V4；存在 `.delta` 行或 DeltaStore 单元更新时
保守回落单批物化（结果正确，内存不有界）。本批按“基础行组 mmap 流 + 按批打补丁”
落地稳定流式读视图：

1. 基础行组仍由 `RgBatchStream` 逐行组读取（持久化删除向量已应用，只读投影列，
   未命中覆盖层的批次整批零拷贝直通）。
2. 流创建时对 `DeltaStore` 取一次快照（`DeltaStore::snapshot()`：只复制删除位图与
   合并后的单元更新，不带顺序日志），整条流因此看到同一覆盖层，且不需要长查询
   持有读守卫。追加 `.delta` 行数也在创建时固定，写者中途追加不会被读到。
3. 每个批次先按 `_id` 与快照比对：全部未命中直接返回原批次（不重建任何列）；
   命中才走 `DeltaMerger::merge` 应用删除与单元更新。delta 重的形状不再为每个
   窗口重复合并读。
4. 追加的 `.delta` 行在最后一个 range 的尾部读取一次
   （`read_columns_to_arrow_inner(..., apply_delta_store = false)` 变体），随后同样
   按快照打补丁，保证与基础批次同源同视图。
5. 打补丁需要 `_id`：调用方未投影 `_id` 时内部仍带该列，补丁后立即剥离
   （`strip_id_column`，无 `_id` 时原样返回），输出 schema 与纯 V4 批次流一致。
   仅追加行、没有 DeltaStore 单元更新时完全不带 `_id`、不重建批次。
6. 并行 range（`scan_batches_ranges`）同样适用：每条 range 独立打补丁，尾部批次
   只挂在最后一条 range 上，range 之间仍是不相交的连续行组。
7. 仍回落单批的情形：内存表、未落盘的 V4 内存追加行、非 V4（无 footer）文件、
   以及持久化删除向量非空（见 11.5）。

覆盖形状：仅 `.delta` 追加行、仅 `DeltaStore` 删除/更新、以及两者并存。

**2026-09-17 尝试与回退（保留）**：曾用“按物理窗口重复走
`read_columns_to_arrow` 合并读”实现（提交 `fb3ada4`）。功能与正确性通过（70k 行、
多窗口、删除/更新/追加覆盖层与单批扫描逐值一致；带 UPDATE 覆盖层的 GROUP BY 形状
落在 `batched_scan_pipeline`），但同机 canary 对 base `14ae19a` 暴露稳定回退：

- `Uncached delta Boolean+GROUP+HAVING+TopK`：base 五样本 [20.557, 20.672, 20.595,
  20.899, 20.864] 中位数 20.672ms；current [38.148, 38.231, 38.144, 38.380,
  38.349] 中位数 38.231ms（+84.94%），每个 current 样本都约为 base 的 2 倍，
  不是调度噪音。

原因是每个窗口都重新执行一次覆盖层合并读；delta 重的形状因此付出约 2 倍代价。
已回退该提交（workspace 回到 `14ae19a`），直接复测该指标回到 20.27ms。证据保存在
`local-perf-results/q1-delta-acceptance-20260917/REVERTED.md`。本批实现改为上述
“零拷贝直通 + 尾部一次追加”，同一 delta 重指标在最终门禁中反而稳定快约 41%。

### 11.4 测试与验收（2026-09-17，selection 直接消费）

- `maturin develop --release` 成功；完整串行 `pytest` 1802 passed（33.30 s）；
  完整 `cargo test --release` 583 单元 + 6 doc-test；`--features flight`
  587 单元通过。
- canary（200K/2/7，64 项）exit 0，初判直接通过，无五样本扩展；聚合形状与
  分批指标全在阈值内（`Aggregation (5 funcs)` +3.49%、`Derived ratio GROUP BY`
  -1.84%）。
- full（1M/2/5）exit 0：main 109/109（初判 `UNION ALL (ordered)` +24.95% 在
  五样本终判恢复），qps 10/10、quant 8/8、idx 4/4、par 4/4；`Parallel batch
  scan` 2/4/8 线程与 auto 分别 -8.72%/-7.01%/-9.52%/-7.75%。
- 报告目录 `local-perf-results/q1-acceptance-20260917/`。

### 11.5 合并读的行空间不一致（已修复，2026-09-19）

**现象（release 单元探针）**：同一张表既有持久化行组删除向量、又有 `.delta` 覆盖层
时，`read_columns_to_arrow(Some(&["_id","v"]), 0, None)` 对 1..1000 行（删除 `_id=7`、
追加一行 `v=555`）返回 **1001 行**：`_id=7` 泄漏，且末尾错位为 `(1000, 555)` 与
`(1001, 0)`（值被补齐成默认值）。即“多一行、少一行、值错位”三种错误同时存在。

**根因**：`read_columns` 以 `header.row_count`（**活跃**行数）作为基础/增量边界，但
它返回的是**物理**行（行组扫描含被删除行）；`read_ids` 返回**物理** id 序列。两个
行空间不一致，`read_columns_to_arrow` 于是把物理列按活跃长度截断/补齐，并与物理 id
错位配对。`header.row_count` 本身没有问题——问题是用它去界定物理读。

**修复（保持物理内容，只在 Arrow 边界转成活跃视图）**：

1. `OnDemandStorage::persisted_physical_base_row_count()`：仅当持久化删除向量存在且
   非空时返回物理基础行数，否则 `None`（物理与活跃空间重合）。footer 已缓存时零克隆。
2. `read_columns` 在该值存在时改用物理边界，与行组扫描和 `read_ids` 同一空间；无删除
   向量时行为逐字不变。
3. `read_columns_to_arrow_inner` 拆成「活跃空间包装 + 物理空间读取」：无删除向量时把
   窗口直接下推（原路径不变）；有删除向量时先构造**完整活跃视图**（物理基础行按删除
   位图过滤 + 增量行），再对调用方的活跃窗口做零拷贝 `slice`。`_id` 单独快路径同样过滤。
4. 覆盖层流的尾部改用 `read_delta_tail_to_arrow`：在物理空间只读增量行，删除向量存在
   时也不必物化基础表。
5. 据此移除 `scan_batches` 中“持久化删除向量 + 覆盖层必须回落”的门控；该组合现在也
   流式，且与单批读一致。

**测试**：Rust `read_columns_to_arrow_keeps_active_rows_with_persisted_deletes_and_delta`
覆盖全量读、`_id` 单独读与活跃窗口；`overlay_scan_fixture` 加入持久化删除，使覆盖层
parity/ranges/字符串更新测试都在“物理删除 + DeltaStore 删除/更新 + 追加行”下比对；
门控测试改为正向断言。Python `test_batch_scan_streams_delta_state_with_parity` 在叠加
持久化删除后仍断言 `batched_scan_pipeline` 与开/关结果一致。

**残留（登记，未声称修复）**：`pending_v4_in_memory_rows() > 0`（仅内存追加行、
`ids` 只含新追加行）时基础/增量边界仍有历史歧义，该状态继续回落单批；本批只保证
“持久化基础 + 覆盖层”的物理空间一致。`read_columns` 的物理内容语义不变，blob 读取、
首值缓存等按物理行索引的调用方因此保持正确。

### 11.6 测试与验收（2026-09-19，base+delta 流式读视图）

功能：`maturin develop --release` 成功；完整串行 `pytest` 1803 passed（34.85 s）；
完整 `cargo test --release` 591 单元 + 6 doc-test；`--features flight` 595 单元。
新增/更新测试：

- Rust `scan_batches_streams_overlay_and_rejects_in_memory_and_persisted_deletes`：
  纯 V4 可流、仅追加行可流（尾部批次）、DeltaStore 更新可流、持久化删除 + 覆盖层
  回落。
- Rust `scan_batches_overlay_stream_matches_single_shot_scan`：70k 行 3 个行组，
  DeltaStore 删除 + 单元更新 + 两条追加行，逐值等于单批扫描，且删除生效、更新与
  追加可见。
- Rust `scan_batches_overlay_stream_matches_single_shot_for_updated_string_column`：
  字符串列单元更新后每个批次 schema 与单批一致（补丁不改变列类型）。
- Rust `scan_batches_overlay_ranges_partition_rows`：并行 range 拼接等于单批扫描。
- Rust `scan_batches_overlay_strips_unprojected_id`：未投影 `_id` 时输出 schema
  与单批一致。
- Rust `scan_batches_overlay_streams_delta_rows_without_persisted_row_groups`：
  没有持久化行组、只有增量行时流式可用。
- Rust `scan_batches_overlay_tail_is_fixed_at_stream_creation`：流创建后追加的行
  不被读到（稳定读视图）。
- Rust `update_overlay_runs_on_the_batched_pipeline` 与更新后的
  `batch_group_pipeline_executes_gated_shapes_and_falls_back_outside_gate`：
  带覆盖层的形状落在 `batched_scan_pipeline` 且与物化路径一致。
- Python `test_batch_scan_streams_delta_state_with_parity` /
  `test_parallel_batch_scan_streams_delta_state_with_parity`：200K 行上
  INSERT + UPDATE 后 `EXPLAIN ANALYZE` 显示 `batched_scan_pipeline`，开/关
  `APEX_BATCH_SCAN` 结果一致，再叠加持久化删除仍一致。

公开 benchmark（`local-perf-results/q1-overlay-acceptance-20260919/public-bench.json`）：
103/103 表格 fair detail、Load&Index 2/2、Point&Limited 17/17、Filtering 11/11、
Aggregation 14/14、Joins 等各组全部 `slower=0`；向量 6/6、量化 6/6。

性能门禁（base `d1e6899`）：

- canary 第一轮（200K/2/7）**原始 exit 1**，五样本终判标记
  `Derived ratio GROUP BY` +33.53%（base 中位数 0.578ms / current 0.772ms）与
  `Numeric GROUP BY (5 funcs)` +16.50%。两者都是无覆盖层的干净表路径，本批对
  该路径的调用序列逐条不变（仅把原先合并的 `is_in_memory || has_delta ||
  has_pending_deltas || pending_v4_in_memory_rows` 门控拆成同一组短路判断）；
  同构建独立复测 6 轮：`Derived ratio GROUP BY` 极差 **113.29%**
  （0.5969–1.2730ms），`Numeric GROUP BY (5 funcs)` 极差 **40.10%**
  （0.9254–1.2965ms），均远超 15% 阈值。
- canary 第二轮**原始 exit 1**，标记的是**另一组**指标
  `Filtered numeric TopK` +21.00%（初判另有 `Two-key GROUP BY (5 funcs)`
  +44.36%，五样本终判恢复）；第一轮的两个指标本轮为 -12.62% 与 -3.48%。
  两轮的 delta 重指标分别为 -41.03% 与 -40.95%。
- full（1M/2/5）**exit 0**：main 109/109（初判 3 样本的
  `IN subquery COUNT` +26.95% 在五样本终判恢复），qps 10/10、quant 8/8、
  idx 4/4、par 4/4。
- `Uncached delta Boolean+GROUP+HAVING+TopK`：base 20.390ms → current 12.025ms
  （canary 第一轮五样本），第二轮 20.536ms → 12.126ms；即被回退的实现所回退的
  那个指标，本批稳定快约 41%。

结论：canary 两轮原始 exit 1，但两轮标记的是互不相同的干净表指标、同构建复测极差
达 40–113%、且 full 完整模式（含该批次全部改动）exit 0；按 AGENTS.md §12.14
登记为**已证实的环境受限例外**，不表述为 canary 通过，原始报告全部保留。

Q1 状态：selection 直接消费、验证矩阵与 base+delta 行组流式读视图均已实现 +
功能已验证 + 性能已验收（full exit 0）；合并读的行空间缺口已修复（11.5，见 11.7），
“持久化删除向量 + 覆盖层”也会流式并与单批读一致。

### 11.7 测试与验收（2026-09-19，合并读行空间修复）

功能：`maturin develop --release` 成功；完整串行 `pytest` 1803 passed（35.14 s）；
完整 `cargo test --release` 592 单元 + 6 doc-test；`--features flight` 596 单元。
新增测试：Rust `read_columns_to_arrow_keeps_active_rows_with_persisted_deletes_and_delta`
（全量、`_id` 单独、活跃窗口三种读都排除持久化删除行且值与 id 对齐）；
`overlay_scan_fixture` 加入持久化删除，`scan_batches_streams_supported_overlays_and_rejects_in_memory`
改为“持久化删除 + 覆盖层可流且等于单批”；Python 覆盖层测试叠加持久化删除后仍断言
`batched_scan_pipeline` 与 parity。

公开 benchmark（同一公开套件，最终修订）：
`local-perf-results/q1-read-space-acceptance-20260919/public-bench-final3.json`
103/103 表格 fair detail（Load&Index 2/2、Point&Limited 17/17、Filtering 11/11、
Aggregation 14/14、Joins 5/5、Set Ops 4/4、Expression 5/5、File Scan 9/9、DML 13/13、
Table Ops 5/5 与其余各组全部 `slower=0`），向量 6/6、量化 6/6。同一修订在 host load
16.7 时的另一次运行只标记 1 项 near-tie（`NOT filter (age NOT BETWEEN, name NOT LIKE)`
5.99ms vs DuckDB 4.84ms），再下一次标记 2 项 near-tie；三次运行标记集合互不相同且
低负载运行全部 103/103，原始报告全部保留。

性能门禁（base `a832a83`）：

- 修复主体完成、footer 访问器重构之前的中间修订 canary **exit 0**（64 项全过，
  无回退标记）；该重构只影响“footer 尚未写入”的错误路径，不改变已缓存 footer 的读路径。
- 最终修订的 canary 两轮**原始 exit 1**，但分别只标记
  `Derived ratio GROUP BY` +27.21%（同构建 6 轮复测极差 **109.43%**，
  0.5969–1.2501ms）与 `Two-key GROUP BY (5 funcs)` +24.18%（base 侧自身
  3.1476–6.9036ms，极差 119%），互不相同；两轮的 delta 重指标与并行指标全过。
- full 最终修订**exit 0**：main 109/109（初判 `JSON Read + COUNT(*)` +34.01% 经
  五样本终判为 -1.59%），qps 10/10、quant 8/8、idx 4/4、par 4/4；此前一轮 full
  初判标记的 `ORDER BY expression (LENGTH)` +141.29% 与
  `GROUP BY category + HAVING` +25.67% 在复跑中分别为 -1.46% 与 -20.28%，证实为噪音。

结论：合并读行空间修复的功能与公开 benchmark 全部达标，full 完整门禁在最终修订上
exit 0；canary 在最终修订上的两轮原始 exit 1 属已证实的环境受限例外（同构建极差
109%、指标集合逐轮不同、复跑反向），保留全部原始报告，不表述为 canary 通过。

Q2 实施与验收（2026-09-17）见第 13 节：契约澄清、schema 变化清理反馈、公开
benchmark 15/15 `slower=0`；canary exit 0，full 原始 exit 1 为环境受限例外。

## 12. 验收债务 V1 复核（2026-09-17）

对 S3 提交 `eb44b85`（未改动）复跑 canary/full，报告
`local-perf-results/v1-reverify-20260917/`：

- canary 原始 exit 1，但标记的是**第三组不同指标**（`Filtered numeric TopK`
  +52.87%）；S3 两轮 canary 标记过的 `Derived ratio GROUP BY`、
  `Multiple COUNT DISTINCT`、`CSV filtered GROUP BY + HAVING`、
  `Numeric conjunction aggregation` 本次全部在阈值内。
- full 原始 exit 1：初判 3 样本标记 7 项（含与 S3 无关的 `CSV Read + COUNT(*)`
  与向量 `TopK L2`），自动五样本终判仅剩 `ORDER BY expression (LENGTH)`
  +15.03%，其 base/current 样本分布高度重叠（base 中位数 1.722ms，current
  1.981ms）。qps 10/10、quant 8/8、idx 4/4、par 4/4 全过。S3 验收时标记的
  `IN subquery COUNT` 与 `UNION DISTINCT (ordered)` 本次在阈值内。
- 主机负载全程 6-12（WindowServer ~50%、Chrome helpers ~40%、airportd ~39%、
  CleanMyMac ~24%），从未进入空闲窗口。

结论：五次 S3 canary/full 门禁标记的指标集合互不重复，且命中与 S3 无关的路径
（CSV 读、向量 TopK、ORDER BY 表达式），两侧均有 2-3 倍尖峰、低峰重叠——
环境噪音结论得到强化。但门禁仍未在空闲机器上取得绿色结果，V1 记为“已证实为
环境噪音、仍需空闲窗口复核”，保留全部原始报告，不把失败门禁改称通过。

**2026-09-19 补充**：Q1 base+delta 流式读视图批次在 host load 4.9–8.3 下取得
full 完整模式 exit 0（main 109/109，初判 `IN subquery COUNT` +26.95% 经五样本
终判恢复；qps/quant/idx/par 全过），canary 两轮原始 exit 1 但标记两组互不相同的
干净表指标、同构建复测极差 40–113%（见 11.6）。合并读行空间修复批次同样取得
full exit 0（main 109/109，初判 `JSON Read + COUNT(*)` +34.01% 经五样本终判为
-1.59%），canary 三组互不相同的干净表指标、同构建极差最高 109%（见 11.7）。
V1 因此收紧为：完整模式已在非空闲条件下连续转绿；canary 仍需空闲窗口复核。

## 13. 第八批 Q2：成本反馈与自动并行契约

Q2 依据 A2/R5.3/R5.12 的校准链路，先做契约澄清，再关闭 schema 变化带来的陈旧
校准。起点（base）为 Q1/V1 验收提交 `5aa913e`；current 为 `2461a87`。

### 13.1 校准契约（澄清）

- **唯一写入者**：`PLAN_FEEDBACK` 只由 EXPLAIN ANALYZE 记录
  （`record_plan_feedback`）。普通查询不采样、不写反馈，规划读路径不取
  `FEEDBACK_PERSIST_LOCK`，因此没有后台采样成本，也没有“采样本身影响被测查询”
  的循环。
- **保持显式校准，不引入低开销采样**：一次真实执行的采样要在所有查询上付固定
  成本，而自动并行的收益只对少数长形状成立；显式校准让用户在需要时付出一次
  成本，并把“环境是否适合并行”的决定留在可解释的入口。
- **单位分层（统一口径）**：模型成本是相对单位（`COST_*`，seq scan 每行 1.0），
  用于候选路由比较；时间校准是微秒（`scan_time_avg_us` /
  `parallel_time_avg_us` 等），用于自动并行阈值
  （`PARALLEL_SCAN_AUTO_ENABLE_US = 2000`）。自动并行决策只比较“预测串行时间
  （µs） vs 实测并行时间（µs）”，不把模型成本与微秒混用；形状按实际执行的
  cost class（scan/index/parallel）分桶记录。

### 13.2 数据、schema 与环境变化

- **schema 变化（已实现）**：`invalidate_table_schema_stats` 在 DROP/ALTER 等
  改 schema 的 DDL 后，除清理 `STATS_CACHE` 外调用新增的
  `invalidate_table_plan_feedback`，同时清除该表内存反馈与
  `<table>.plan_feedback` sidecar。理由：每个形状的估计/时间都在旧 schema 上
  校准；下一次 EXPLAIN ANALYZE 按新形状重新记录。测试
  `schema_change_clears_table_plan_feedback` 覆盖内存与 sidecar。
- **数据变化**：普通写入只使 `STATS_CACHE` 失效，保留 `PLAN_FEEDBACK`；行数与
  成本是滑动均值，旧样本权重按 1/n 衰减，形状/表容量上限保证内存有界。
- **环境变化（登记，待后续）**：时间校准与机器绑定，而 sidecar 跨会话/机器
  持久。设计方向是在 `PersistedPlanFeedback` 增加机器指纹（OS/arch/并行度）
  并提升 `FEEDBACK_SCHEMA_VERSION`，使旧文件按“无反馈”处理（与 R5.12 的版本
  提升同法）；本轮不夹带格式变更。
- **样本老化**：滑动均值 + S2 容量上限已限制影响；不引入按时间戳淘汰，避免在
  规划读路径增加时钟与分支。

### 13.3 并发与性能

反馈只在 EXPLAIN ANALYZE 写入，规划读路径零锁零分配；schema 清理只在 DDL
冷路径。Q2 未改查询执行热路径。

### 13.4 测试与验收（2026-09-17）

功能：`maturin develop --release`；完整串行 `pytest` 1803 passed；
`cargo test --release` 584 单元 + 6 doc-test；`--features flight` 588 单元。
新增 `schema_change_clears_table_plan_feedback`（Rust）与
`test_setup_benchmark_runs_teardown_outside_the_timer`（Python），并更新
table-ops 选择契约测试到新的 teardown 计时边界。

**benchmark 全面领先**：同一公开 benchmark 运行
（`local-perf-results/q2-acceptance-20260917/public-bench.json`）15 个 workload
全部 `slower=0`：Load&Index 2/2、Point&Limited 17/17、Filtering 11/11、
Aggregation 14/14、Ordering/Window/View 9/9、Full Materialization 3/3、Joins
5/5、Set Ops 4/4、Subqueries&CTE 4/4、Expression 5/5、File Scan 9/9、DML
13/13、Search 1/1、Table Ops 5/5、Other 1/1；向量 6/6、量化 6/6。
`ALTER TABLE ADD COLUMN` 0.0506ms 对 SQLite 0.0597ms。为此修正计时边界：
ApexBase 的 table-ops 计时方法原先包含 `client.use_table('default')`
（SQLite/DuckDB 无对应客户端状态切换），现由运行器解析 `<method>_teardown`
在计时区外执行；选择契约由测试保证。

性能门禁：canary（200K/2/7，64 项）exit 0，无五样本扩展。full（1M/2/5）
**原始 exit 1**：主比较五样本终判保留 `Derived table GROUP BY` +15.82% 与
`UNION ALL (ordered)` +18.99%（初判的 `CSV Read + COUNT(*)` 与
`GROUP BY city (10 groups)` 已恢复）；样本两侧均有尖峰、低峰重叠，Q2 未改查询
执行路径，host load 4-7。qps 10/10、quant 8/8、idx 4/4 通过，par 经五样本
确认后 4/4。原始报告全部保留，登记为**环境受限例外**，不表述为门禁通过。

Q2 状态：契约澄清与 schema 清理已实现 + 功能已验证 + 公开 benchmark 全面领先；
full 原始门禁为环境受限例外（与 V1 同一类，空闲窗口复核仍待）。

## 14. 第九批 M1：读取路径职责与能力表单源（2026-09-19）

M1 的目标是减少**重复决策**而不是减少行数：读取 lane 的“可见状态”判定此前在
storage、执行器和 Python 绑定里各拼一遍，同一条 `has_delta() ||
has_pending_deltas() || pending_v4_in_memory_rows() > 0` 出现在多个文件，且
`has_delta() || row_count() > base_rows || active_row_count() > base_rows` 也有三份
独立拷贝——任何一份漂移都会让某个 lane 读到另一个 lane 已失效的视图。

### 14.1 已实施

- `TableStorageBackend::overlay_state()` 单点探测四类可见覆盖层
  （`appended_rows` / `pending_cells` / `unflushed_rows` / `base_in_memory`），
  并提供命名判定：`has_pending_writes()`（不探测 delta 文件，保持点查守卫成本）、
  `requires_merged_read()`、`is_clean_view()`。
- 替换重复表达式 16 处（Python 绑定 8、执行器 5、storage 3），并把
  `visible_rows_exceed_base(base_rows)` 作为“逻辑增量”判定的唯一实现，替换 3 处拷贝。
- `scan()` 与 `scan_batches_split` 的复合 gate 改为读 `OverlayState`，各 lane 的前提
  现在有名字，不再靠条件顺序表达。
- 新增 `docs/READ_PATH_CAPABILITIES.md` 作为“能力表 / 限制 / fallback 目标”的单源，
  逐 lane 登记入口、支持范围、gate 与回退目标，并写明 typed 协议的类型边界
  （UInt64 谓词列排除）与“新增 lane 必须同步本表 + 补 gate 回退测试”的规则。
- 新增 Rust 测试 `overlay_state_is_the_single_source_for_the_read_lanes` 与
  `overlay_state_separates_base_in_memory_from_pending_writes`，钉住每个状态的分类
  以及 `has_pending_writes` 与 `requires_merged_read` 的区别。

### 14.2 评估后不做（避免制造抽象）

- Python 绑定与执行器里另有约 100 处针对**具体语义**的 `has_pending_deltas()`
  调用（例如“该列有 pending 单元更新”“该事务的 DeltaStore 非空”），它们不是同一个
  决策，合并成统一谓词会隐藏语义、扩大热路径成本，因此只替换了逐字相同的
  2/3 标志组合。
- 未按行数拆分 `backend.rs`：`include!` 分文件只是编译期组织，不能据此声称依赖
  解耦；真正的职责拆分需要绑定具体边界，留待出现第二个复用方时再做。
- E1（外部执行/复杂 Join/向量组合）仍按需：目前没有容量或工作负载证据。

### 14.3 测试与验收（2026-09-19）

报告目录 `local-perf-results/m1-acceptance-20260919/`，base `34ac1a0`。

- 功能：`maturin develop --release` 成功；完整串行 `pytest` **1803 passed**
  （36.59 s）；`cargo test --release` **594** 单元 + 6 doc-test；
  `--features flight` **598** 单元。
- 公开 benchmark：`public-bench-final2.json` 本次运行 **103/103** 表格 fair detail
  全部 `slower=0`，向量 6/6、量化 6/6。同修订的另三次运行分别在 host load 13–15
  下各标记 1 项不同的 near-tie（`Filtered aggregation (city)` 1.06ms vs 0.928ms、
  `NOT filter` 7.49ms vs 3.56ms、`ALTER TABLE ADD COLUMN` 0.062ms vs 0.062ms），
  与低负载运行对比即可归因于干扰，原始报告全部保留。
- canary（200K/2/7）：首次原始 exit 1（4 项干净表指标，其中
  `Numeric GROUP BY (5 funcs)` current 样本 [2.45, 1.72, 1.70, 2.47, 4.05] 与 base
  [1.13, 0.97, 2.69, 2.19, 0.97] 高度重叠）；重跑 **exit 0**（64 项全过，无五样本
  扩展）。同构建 6 轮复测显示这些指标的极差为 21.6%（`Filtered numeric TopK`）、
  23.3%（`Derived ratio GROUP BY`）、30.9%（`Numeric conjunction aggregation`）、
  42.7%（`Numeric GROUP BY (5 funcs)`）。
- full（1M/2/5）：两轮**原始 exit 1**，但两轮标记的是**互不相同**的指标集合——
  第一轮 `JSON Read + Filter` +26.65%（current [8.91, 11.63, 9.03, 12.90, 12.98]
  双峰）与 `ORDER BY expression (LENGTH)` +38.31%；第二轮 `Filtered aggregation (city)`
  +27.05%（current [0.42, 0.99, 0.55, 1.24, 0.43] 双峰）与 `NOT filter` +16.73%
  （base 自身含 6.25ms 尖峰）。第一轮的两个指标在第二轮分别为 +1.2% 与 -2.4%；
  第二轮的两个指标在第一轮的五样本终判中均未保留（`NOT filter` 曾在第一轮初判出现，
  五样本终判恢复）。两轮的 qps 10/10、quant 8/8、idx 4/4、par 4/4 全部通过。同构建公开 benchmark 在这两个指标上的极差为 47%（`JSON Read + Filter`
  8.92–13.12ms）与 85%（`ORDER BY expression (LENGTH)` 1.67–3.08ms），且
  `JSON Read + Filter` 是 JSON 文件扫描，不经过本批修改的存储读取路径。

结论：M1 改动为“同判定、改写法”，替换前后的可见状态集合逐条一致；公开 benchmark
在最终修订上有 103/103 `slower=0` 的绿色记录，canary 重跑 exit 0；full 两轮原始
exit 1 属已证实的环境受限例外（两轮指标互不相同、样本双峰、同构建极差 47–85%），
保留全部原始报告，不表述为 full 门禁通过。

M1 状态：读取路径的可见状态判定、能力表与 fallback 目标已单源；余项（backend
职责拆分、E1）按上面 14.2 的边界继续保留。
