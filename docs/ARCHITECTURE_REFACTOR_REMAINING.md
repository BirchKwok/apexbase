# ApexBase 架构重构剩余工作与执行计划

更新日期：2026-09-10。起点：`1b60ae8f916c48e25ae2dc2e1354ec586ca7133a`。
依据：[架构评审](ARCHITECTURE_REVIEW_2026_09.md)、当前源码及已保留的本地验收报告。
本文维护剩余工作；历史评审的阶段记录不等于当前整体完成状态。

## 1. 当前结论与完成口径

方向保持：保留 Rust/V4/mmap/融合内核，逐步收敛提交协议、共享执行与资源归属。
整体重构尚未完成。分别记录“已实现”“功能已验证”“性能已验收”，不得互相替代。

| 原阶段 | 当前状态 | 尚未关闭的目标 |
| --- | --- | --- |
| R0 | 有环境、固定 base 和原始报告 | 当前源码快照与证据清单统一；历史失败保持可追溯 |
| R1 | 错误传播和有限 INSERT/DELETE 恢复已实现 | 提交结果分类、UPDATE 恢复、Max 持久性、跨表/索引恢复契约 |
| R2 | SELECT 文件拆分完成 | backend 委托和状态所有权收敛；同模块 include 不等于依赖解耦 |
| R3 | 纯持久化 V4 上的受限分批聚合已实现 | overlay 稳定读视图、selection 直接消费、更广查询形状 |
| R4 | 状态清单、上下文传播、有界队列、局部取消已实现 | 内存预算、缓存容量与唯一 owner、协议背压/分批交付 |
| R5 | 索引计划执行、路径跟踪、并行扫描和显式校准后自动选路已实现 | 校准失效/反馈边界、资源前提、最新 canary 验收 |
| R6 | 按需求保留 | 外部聚合/排序/Join、FTS/向量组合、构建 feature，非基础收尾前置项 |

最新 R5.12 完整模式 `local-perf-results/20260910-155135/` 比较通过；
canary `local-perf-results/20260910-152709/` 五样本最终仍有 3 项回退。
专项 A/B 是归因证据，不能把失败门禁改称通过。同构建自 A/B 只能解释波动，不能替代旧版/新版比较。

## 2. 优先级、依赖和交付边界

| 顺序 | 优先级 / 编号 | 工作 | 完成条件 | 状态 |
| --- | --- | --- | --- | --- |
| 1 | P0 / C1 | 提交失败结果分类与提交后错误处理 | Rust 可识别、Python 可辨认“未提交/结果不确定/已提交”；保留原始错误；不把 WAL marker 写失败误判可安全重试；提交后维护失败仍发布可见性与失效；真实 I/O 故障测试 | 已实现并验收通过（2026-09-10，canary/full 对 base `1b60ae8` 均 exit 0）；C2 可开始 |
| 2 | P0 / C2 | UPDATE、Safe/Max 与恢复一致性 | 沿 WAL/数据/索引/水位画时序；补真实 UPDATE 和 fsync 失败测试；保证或明确拒绝无法支持的语义；文件格式变化独立设计 | 实施中；C2.1 先拒绝 WAL-backed 事务 UPDATE，C2.2/C2.3 待实施 |
| 3 | P0 / C3 | 跨表与索引恢复契约 | 覆盖各表 marker 间故障、索引保存失败及 compact 后重开；明确按表收敛与原子提交区别；若引入数据库提交记录，先完成兼容与恢复设计 | 待实施，依赖 C1/C2 |
| 4 | P1 / S1 | 查询内存预算与资源准入 | 先约束高基数聚合及并行局部状态；预算按字节计量，超预算明确报错或走已验证回退；取消和失败释放资源；峰值 RSS/并发/回收验收 | 待实施，C1–C3 后 |
| 5 | P1 / S2 | 缓存容量与状态 owner | 逐项关闭 RESOURCE_OWNERSHIP 的 G1/G2/G3；保留 epoch 引用缓存；无新全局大锁；close/reopen/跨客户端/跨进程/持有结果生命周期测试 | 待实施，按缓存拆批 |
| 6 | P1 / S3 | Flight 分批桥接与协议资源边界 | 查询执行至输出端有界；慢消费者背压、断连取消；schema 请求避免重复完整执行；不为嵌入式点查增加固定锁成本 | 待实施，依赖 S1 |
| 7 | P1 / Q1 | 完善分批物理执行 | base+delta 稳定读视图；selection 直接消费，减少 gather；扩展形状前验证 NULL/UInt64/精确整数/更新删除/schema 一致性 | 待实施，依赖 C/S 基础 |
| 8 | P1 / Q2 | 成本反馈与自动并行契约 | 明确只由 EXPLAIN ANALYZE 校准的当前行为；评估低开销采样或保持显式校准；处理数据/schema/环境变化及历史样本老化；统一候选成本单位 | 待实施，依赖 S1，文档澄清先做 |
| 9 | P2 / M1 | 剩余职责与文档收敛 | 以重复决策/依赖减少为标准拆 backend 和路由；能力表、限制和 fallback 单源；不按行数制造抽象 | 待实施，与对应边界一起推进 |
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

## 6. 第二批 C2：UPDATE、Safe/Max 与恢复一致性

### 6.1 当前时序与已确认缺口

本节以 C1 验收后的 `229b596` 为审查快照。当前事务提交时序如下：

```text
prepare/OCC
  -> 每表 WAL TxnBegin
  -> INSERT/DELETE 写 WAL；UPDATE 不写 WAL
  -> 每表 WAL TxnCommit（Safe=flush；当前协调器无法获知调用方 Max）
  -> apply_txn_writes
       INSERT -> .delta
       DELETE -> 删除状态
       UPDATE -> 原位覆盖或 .deltastore 原子替换
  -> 索引保存
  -> MVCC finalize + epoch/cache 发布
  -> WAL applied watermark
```

由此得到两个不能靠调整调用顺序消除的缺口：

1. WAL commit marker 成功而 UPDATE 应用尚未完成时，恢复只会重放 INSERT/DELETE，
   无法重建 UPDATE；把这种事务报告为可恢复的持久提交是不成立的。
2. `open_txn_wal_backend()` 当前仅根据 `.wal` 是否存在判断 WAL-backed，并固定按
   Safe 打开。Max 调用方要求的 commit-marker fsync 没有传播到协调层；在该边界
   明确前，不能声称 Max 事务提交已经满足其公开语义。

UPDATE WAL 记录属于持久化格式变化。它还需要解决 applied watermark 之后只重放
未应用后缀的问题，否则旧 UPDATE 可能覆盖 watermark 之后的较新非事务更新。因此
不在 C2.1 中直接追加一个记录类型并宣称恢复完成。

### 6.2 子批与完成口径

| 子批 | 范围 | 完成条件 | 状态 |
| --- | --- | --- | --- |
| C2.1 | 无格式变化的安全边界 | WAL-backed 表的显式事务含 UPDATE 时，在任何 TxnBegin/DML/Commit WAL 写入前拒绝；结果为 `not_committed`；事务状态清除；原值及 WAL 长度不变；Fast 事务 UPDATE 兼容 | 实施中 |
| C2.2 | durability 传播与真实同步失败 | Session/嵌入式/Python 到提交协调层保留 Safe/Max；Max commit marker 使用真实 fsync；真实写入/flush/fsync 故障分别验证结果分类、重开与后续事务 | 待实施，依赖 C2.1 |
| C2.3 | UPDATE WAL 与后缀恢复 | 独立记录格式/版本/旧文件兼容设计；watermark 按偏移解析；只按 WAL 顺序幂等重放未应用且已提交的 UPDATE；覆盖崩溃、重复打开、更新后再更新、compact | 待设计，依赖 C2.2 |

C2.1 只关闭“不把不可恢复 UPDATE 当作可持久提交”的漏洞，不代表 C2 整体完成，
也不把 Safe/Max 的事务 UPDATE 描述为已支持。拒绝发生在 prepare/OCC 之后、首次
WAL 写入之前；现有 C1 `CommitOutcome` 契约因此允许稳定返回 `not_committed`。

C2 最终验收仍使用第 4 节固定 base 和完整链；C2.1 原子步骤仅运行聚焦 release
功能检查，所有文件修改结束后再统一执行完整验收。
