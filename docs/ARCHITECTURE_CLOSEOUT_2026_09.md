# 架构收尾与验收证据计划

日期：2026-09-20。起点：`41f34001f91477af74921a46401847e289d807e3`。
依据：`ARCHITECTURE_REVIEW_2026_09.md`、`ARCHITECTURE_REFACTOR_REMAINING.md` 第 15 节及
`local-perf-results/remaining-acceptance-20260920/` 原始报告。

## 1. 交付范围与完成口径

本轮关闭验收证据缺口、修正文档状态漂移，并复核 V1。先写本计划，再修改代码。
主要架构实施已基本收口，但最新 canary 原始 exit 1；full 135 项通过不能代替 canary。
S2 G2/G3、backend 拆分、E1 保留已评审的触发条件；本轮不扩展其实现范围。
不改变事务协议、存储格式、查询热路径或性能判定规则，不修改 AGENTS.md。

## 2. 按顺序实施

1. **证据持久化（代码）**：增强现有 `benchmarks/run_local_perf_guard.py`，在报告目录
   保存机器可读的运行状态、固定 base/current 身份、实际构建源码文件摘要、工作区 patch、
   构建依赖锁与 wheel 摘要、各比较阶段原始退出状态。完成、构建/采样失败和中断均留痕；
   未完成状态不得被解释为通过。临时 worktree 删除后仍可核对受测源码。只在计时区外处理。
2. **证据测试（Python）**：使用临时真实 Git 仓库和文件验证已跟踪/未跟踪修改、二进制
   内容和源码摘要；覆盖成功、回退、不兼容/执行失败、中断的状态留存。保留既有默认规模、
   交错采样顺序、五样本扩展和参数校验测试。无 Rust 行为变更，不为 Python 工具虚构 Rust
   生产代码或测试；最终完整 Rust 测试仍必须运行。
3. **文档同步**：原始评审保留历史；更新导航与当前结论，纠正 15.5 对 M1 full 通过的误引。
   明确跨表按表恢复、有限流式覆盖、内存预算边界及延期项；不得将登记例外写为门禁通过。
4. **统一验收**：全部实现/测试修改完成后按第 3 节执行。记录所有原始退出状态与日志，
   Rust 默认/Flight 原始日志必须落盘，不用历史文档中的计数替代。
5. **结果登记**：记录本次真实计数、失败项、公开旧基线对比和 V1 状态。若有失败，区分实现
   回退、报告不兼容、机器噪音和未验证；不挑样本、不反复运行直到绿色。

## 3. 验收基线与顺序

使用 conda base。新增工具改动固定比较 base 为 `41f34001f91477af74921a46401847e289d807e3`。
V1 原批次的 canary 复核保持原 base `4d85ed29e397a6a3a1a957d70c00387f05fd7de4`，
单独保留报告，不用新 base 消除旧失败。该比较还包含本轮计时区外的工具改动。
原始 R5.12 等历史批次失败不由本次比较自动撤销。

依次执行：

1. `maturin develop --release`。
2. 完整串行 `pytest`，记录首次冷态时间。
3. `cargo test --release` 和 `cargo test --release --features flight`。
4. `python benchmarks/bench_vs_sqlite_duckdb.py --output <report>/public-bench.json`，
   用现有比较器对照 `benchmarks/latest_public_baseline.json`；不覆盖旧基线。
5. 本轮 canary（固定本轮 base），V1 canary（固定原 base）。
6. 本轮 full（固定本轮 base），完整默认指标集合。

全部日志/JSON/摘要保留在新的 `local-perf-results/closeout-acceptance-<timestamp>/`。
canary 200K/2/7、full 1M/2/5、30 秒静置、B-C-C-B-B-C、必要时五样本、
15% AND 0.005ms 阈值保持。测试、编译与 benchmark 不并发；不终止用户后台应用。
记录主机负载，繁忙环境下的结果不称为空闲窗口验收。

## 4. 当前执行状态

- 计划已先于代码落地；门禁证据持久化、对应 Python 测试和文档同步已实施，最终统一验收已执行。
- 原子步骤回顾：仅修改计时区外工具和文档；既有采样、阈值、指标与测试均保持，未修改
  Rust 运行时或 AGENTS.md。full 与原基线 V1 canary 仍按计划执行。
- 历史 Rust 测试计数仅有文档记录的批次，维持证据缺失标注；新运行不能补造旧日志。
- 本轮不承诺全架构完成或发布就绪，完成与否以实际验收记录为准。

## 5. 实施与最终验收结果（2026-09-20）

报告：`local-perf-results/closeout-acceptance-20260920-134803/`。
执行时间 13:48:10–15:44:08（Asia/Shanghai），完整命令、原始退出码、耗时和负载见
`acceptance-status.json`，各阶段 stdout/stderr 保存在同名 `.log`。

代码：`GateEvidence` 在计时区外写入 `run-manifest.json`（原子替换），保存运行中、
通过、回退、错误和中断状态；保留各比较阶段初判与终判退出码。临时 worktree 清理后仍
保留 `source-{base,current}.json` 文件内容/可执行位/符号链接清单、binary patch、
双方 Cargo.lock 及 wheel SHA-256。执行子命令失败时记录其原始退出码，门禁返回 2；
中断返回 130；强制终止未能收尾时 manifest 留在 running，不视为通过。
未跟踪文件保存内容摘要，未另存其完整内容；摘要用于核对，不宣称可从摘要还原源码。

| 验收 | 原始结果 |
| --- | --- |
| `maturin develop --release` | exit 0；332.04 秒 |
| 完整串行 pytest | exit 0；1811 passed，首次冷态 37.58 秒（进程 38.28 秒） |
| `cargo test --release` | exit 0；596 单元 + 6 doc-test，0 failed/ignored |
| `cargo test --release --features flight` | exit 0；600 单元 + 6 doc-test，0 failed/ignored |
| 公开 benchmark | exit 0；103/103 表格、6/6 向量、6/6 量化全部胜出 |
| 公开旧基线比较 | **exit 1，5 项超阈值**，详见下表；不称为全绿 |
| 本轮 canary，base `41f3400` | **exit 0，64/64**；初判 2 项，自动五样本终判全部通过 |
| V1 canary，base `4d85ed2` | **exit 0，64/64**；初判 3 项，自动五样本终判全部通过 |
| 本轮 full，base `41f3400` | **exit 0，135/135**：main 109、qps 10、quant 8、idx 4、par 4；全部三样本通过 |

canary 200K/2/7、full 1M/2/5、辅助套件默认配置、30 秒静置和阈值均保持。未选择或
删除样本；两组 canary 初判失败报告与全部五样本保留。本轮 canary 初判的
Numeric GROUP BY (5 funcs) +35.73%、Numeric conjunction aggregation +40.93%，
终判分别为 +13.21%、+5.34%；V1 初判的 CSV filtered scalar aggregation +23.43%、
CSV string GROUP BY numeric agg +41.92%、NULL profile +21.67%，终判分别为
-6.45%、+14.30%、-1.50%。

公开比较以未覆盖的 `benchmarks/latest_public_baseline.json`（`492956bb`，macOS
26.6.2）为基线，本次系统为 macOS 27.0。以下单次差异全部保留；同机 full 对应指标未
确认本轮回退，不能将跨日期/系统的单次比较当成本轮因果证明，也不能把旧基线比较改为通过。

| 指标 | 相对旧公开基线 | 本轮 full 同机比较 |
| --- | ---: | ---: |
| Batch TopK Dot (10 queries) | +17.23% | -2.42% |
| Batch TopK L2 (10 queries) | +28.94% | -3.49% |
| GROUP BY city ORDER BY count | +42.06% | +1.78% |
| INTERSECT (ordered) | +20.70% | -1.31% |
| UNION DISTINCT (ordered) | +80.45% | -1.58% |

证据完整性：三组门禁的 inventory、patch、Cargo.lock SHA-256 均经复核；每组双方
Cargo.lock 完全相同。本轮 base/current 仅工具、测试和三份文档不同，Rust/Python 运行时
与被计时 workload 文件一致；V1 保留上一批 planner/backend/arrow_io 的实际改动。
验收结束时所有非文档输入与 full 受测清单一致；之后仅补写本文和状态导航。

V1 边界：remaining-acceptance 批次相对 `4d85ed2` 的 canary 绿色证据已补齐。
该次运行首尾 load 约 7.67/9.39，不能称为严格空闲窗口验收；原 R5.12/S3 等历史批次
失败未被本次新比较撤销，原基线上的历史债务仍单独保留。本轮不为已收敛的噪音重复门禁。
