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
| R1 | 错误传播和 INSERT/DELETE/UPDATE 恢复已实现 | 提交结果分类、跨表/索引恢复契约 |
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
| 2 | P0 / C2 | UPDATE、Safe/Max 与恢复一致性 | 沿 WAL/数据/索引/水位画时序；补真实 UPDATE 和 fsync 失败测试；保证或明确拒绝无法支持的语义；文件格式变化独立设计 | C2.1–C2.3 已实现，功能验收完成；性能证据已收口并登记 full 原始噪音例外；C3 可开始 |
| 3 | P0 / C3 | 跨表与索引恢复契约 | 覆盖各表 marker 间故障、索引保存失败及 compact 后重开；明确按表收敛与原子提交区别；若引入数据库提交记录，先完成兼容与恢复设计 | C3.1–C3.2 已实现并聚焦验证；最终统一验收待执行 |
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

C3.1–C3.2 至此达到实现和聚焦功能验证边界，但尚未把 C3 描述为最终验收通过。下一步
统一执行 release 安装、完整 pytest/cargo test、公开 benchmark、canary 与核心恢复/索引
路径要求的 full 同机比较；性能异常按保留原始报告、结合源码路径和样本分布辨别噪音的
规则处理，不为已证实噪音机械增加轮次。
