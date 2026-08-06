# ApexBase 问题总结

> 整理日期：2026-08-05
> 环境：ApexBase `1.27.0`（editable 安装于 `/Users/guobingming/projects/ApexBase`，pip 记录 1.26.0）、Python 3.12.2、macOS arm64（Apple Silicon）
> 来源：视频画面搜索项目（ChineseCLIP + 人脸聚类 + ApexBase 存储）开发与全量索引过程中实测发现。
> 复核（2026-08-05）：在独立 venv 安装官方线上 ApexBase `1.27.0`（PyPI 与 GitHub `v1.27.0` 一致）逐条复测，以下 5 个问题**均仍存在**。

---

## 问题 1【严重】`create_table` 会覆盖磁盘上已存在的表，造成数据丢失

**现象**

新进程打开已有数据库后，如果直接调用 `ApexClient.create_table(name, schema)` 创建“已存在”的表，该表磁盘文件会被重建，原有行全部丢失；且不报错（因为客户端缓存 `table_paths` 中查不到该表名，跳过了存在性检查）。

**复现**

```python
from apexbase import ApexClient

# 进程 1：建表并写入
c = ApexClient(dirpath=db, drop_if_exists=True)
c.create_table("videos", {"name": "string"})
c.store({"name": "v1"})   # 1 行
c.close()

# 进程 2：同一路径直接 create_table
c2 = ApexClient(dirpath=db)
c2.create_table("videos", {"name": "string"})  # 不报错
print(c2.count_rows("videos"))  # 0 —— 数据已被重建清空
```

**原因定位**

`apexbase/src/python/bindings/sql.rs` 的 `create_table` 只检查了客户端缓存 `table_paths`；缓存里没有该表名时不会检查磁盘文件，直接调用 `Database::create_table_with_schema` 重建文件。与 `use_table` 的“磁盘惰性发现”逻辑不一致。

**影响**

任何“启动时初始化 schema”的代码都可能静默清库。本项目首次运行时因 `init_schema` 每次都 `create_table`，导致重启后索引数据被清空（videos/frames/faces 全部归零）。

**本项目绕过方案**

`db.py::init_schema` 改为先 `use_table(name)` 探测，失败（表不存在）才 `create_table`。

**建议**

`create_table` 应像 `use_table` 一样先检查 `current_base_dir()/{name}.apex` 是否存在于磁盘；已存在时返回“Table already exists”，绝不重建。

---

## 问题 2【严重】`ORDER BY _id ... LIMIT` 只返回首个行组的数据，漏掉后续写入

**现象**

同一客户端分批 `store` + `flush` 后（每批形成一个落盘行组），**带 WHERE 条件的** `SELECT ... ORDER BY ... DESC LIMIT n` 在 `n` 不超过首批行组大小时，只对“扫描顺序前 n 行”做排序返回，而不是全局排序后的前 n 行；`count_rows()`、`MAX(_id)`、不带 `ORDER BY` 的普通扫描均正确。`flush()` / `flush_cache()` 无法修复。

**复现**

完整可运行脚本（`python repro_issue2.py`，无需额外依赖，仅需 `pip install apexbase`）：

```python
"""问题 2 精确复现：WHERE + ORDER BY DESC + LIMIT 只返回前 N 行组的行。

环境：apexbase 1.27.0（PyPI 官方 wheel，2026-08-05 复测仍存在）
"""
import tempfile

from apexbase import ApexClient

db = tempfile.mkdtemp(prefix="apex_issue2_")
c = ApexClient(dirpath=db, drop_if_exists=True)
c.create_table("t", {"k": "int64"})

# 分 3 批写入，每批 flush + flush_cache（每批形成一个落盘行组，共 12 行）
for batch in (range(0, 4), range(10, 14), range(20, 24)):
    c.store([{"k": k} for k in batch])
    c.flush()
    c.flush_cache()


def ids(sql):
    return [r["_id"] for r in c.execute(sql, show_internal_id=True).to_dict()]


print("count:", c.count_rows())
print("WHERE DESC L4  :", ids("SELECT _id, k FROM t WHERE k >= 0 ORDER BY _id DESC LIMIT 4"))
print("WHERE DESC L8  :", ids("SELECT _id, k FROM t WHERE k >= 0 ORDER BY _id DESC LIMIT 8"))
print("WHERE ASC  L4  :", ids("SELECT _id, k FROM t WHERE k >= 0 ORDER BY _id LIMIT 4"))
print("no-WHERE DESC L4:", ids("SELECT _id, k FROM t ORDER BY _id DESC LIMIT 4"))
print("WHERE DESC L12 :", ids("SELECT _id, k FROM t WHERE k >= 0 ORDER BY _id DESC LIMIT 12"))
```

实测输出（apexbase 1.27.0，2026-08-05）：

```text
count: 12
WHERE DESC L4  : [4, 3, 2, 1]                      # ✗ 期望 [12, 11, 10, 9]
WHERE DESC L8  : [8, 7, 6, 5, 4, 3, 2, 1]          # ✗ 期望 [12, 11, 10, 9, 8, 7, 6, 5]
WHERE ASC  L4  : [1, 2, 3, 4]                      # 看似正确，恰好前 4 个小 id 在前行组，掩盖了 bug
no-WHERE DESC L4: [12, 11, 10, 9]                  # 无 WHERE 时正确（另一条执行路径）
WHERE DESC L12 : [12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]  # LIMIT 覆盖全部行组时正确
```

关键点：

- 触发条件：**WHERE + ORDER BY DESC + LIMIT 小于总行数且 ≤ 首批行组大小**。此时只对扫描顺序前 n 行排序返回，等价于把 LIMIT 错误地下推到“排序之前/行组扫描过程中”，后续行组根本没参与排序。
- 升序查询在这份数据上“看似正确”（前 4/8 个小 id 恰好都在前行组），所以必须用 DESC 才能暴露；这是本项目索引器最初只查 DESC 却仍然踩中的原因。
- 无 WHERE 时 DESC 正确（无 WHERE 走了另一条执行路径）；另有一个变体：无 WHERE 但 SELECT 不投影 `_id` 时也可能出错（如 `SELECT ts FROM t ORDER BY _id DESC LIMIT 4` 返回首个行组的自然顺序 `[0,1,2,3]`），说明相关执行路径不止一处存在行组截断问题。
- `flush()` / `flush_cache()` 在查询前再次调用无法修复。

**影响**

本项目索引器原来用 `WHERE video_id = ? ORDER BY _id DESC LIMIT n` 回查刚写入批次的帧 id，而 `n` 恰好等于每个写入批次/行组的大小（16），于是永远只返回第一个行组的 id → **人脸只写入了每集开头约 20 秒**（全库 47.9 万帧只有 782 张人脸）。属于高影响的数据完整性问题。

**原因推测**

带 WHERE 的 `ORDER BY ... LIMIT` 执行路径把 LIMIT 下推到逐行组扫描中：扫描到前 n 行即停止，再对这 n 行排序，导致后续行组未参与排序；无 WHERE 路径与投影路径各自实现不同，结果也不一致（详见“关键点”）。

**本项目绕过方案**

`indexer.py::flush()`：写入后 `flush()` + `flush_cache()`，再按本批时间范围 `WHERE video_id = ? AND ts >= ? AND ts <= ?` 查询（不依赖 `ORDER BY`）取 `_id`。

**建议**

修复 `ORDER BY` 对多行组的扫描；并为该项目补充“分批写入后 ORDER BY 可见性”的回归测试。

---

## 问题 3【中】`SELECT _id` 的可见性规则不一致（`show_internal_id` 行为随路径变化）

**现象**

- 未显式指定 `show_internal_id` 时，部分路径（如写入未 flush 前的懒查询）会自动返回 `_id`；
- `flush()` 后同一句 `SELECT _id, ts` 却不返回 `_id`，必须显式传 `show_internal_id=True`；
- `SELECT *` 与显式列在不同状态下返回的列也不一致。

**复现**

```python
c.store({...})
c.execute("SELECT _id, ts FROM t LIMIT 1").to_dict()   # 可能含 _id
c.flush()
c.execute("SELECT _id, ts FROM t LIMIT 1").to_dict()   # 不含 _id
c.execute("SELECT _id, ts FROM t LIMIT 1",
          show_internal_id=True).to_dict()             # 含 _id
```

**影响**

调用方无法依赖“显式列出 `_id` 就返回 `_id`”，容易写出在 flush 前后行为不同的代码。

**本项目绕过方案**

`db.py::ApexStore.execute` 统一强制 `show_internal_id=True`。

**建议**

统一规则：显式列出 `_id` 时始终返回；内部 id 是否展示只由 `show_internal_id` 控制，与写入/flush 状态无关。

---

## 问题 4【中】`execute_batch` 批量 UPDATE 性能差（10k 行约 28 秒）

**现象**

对 10k 行逐条构造 `UPDATE ... WHERE _id = n` 并用 `execute_batch` 执行，耗时约 28 秒（约 350 行/秒），无法用于大规模回填/改写。

**复现**

```python
queries = [f"UPDATE faces SET cluster_id = {i % 7} WHERE _id = {i + 1}"
           for i in range(10_000)]
t0 = time.time()
c.execute_batch(queries)
print(time.time() - t0)   # ≈ 27.9s
```

**影响**

本项目最初计划聚类后把 `cluster_id` 回写到 faces 表；10 万级人脸会需要数分钟到数十分钟，体验不可接受。

**本项目绕过方案**

聚类结果不逐行 UPDATE，而是写入独立的 `face_clusters` 映射表（批量 `store` + 查询时扫描/过滤），实测 1 万行过滤查询约 4ms。

**建议**

为批量 UPDATE 提供列式/批式写入路径（如 UPDATE ... SET col = CASE 或直接重建映射表），避免逐条 SQL 解析开销。

---

## 问题 5【低】SQL 参数绑定只支持 TopK 向量查询的单 `?` 占位

**现象**

`ApexClient.execute(sql, params=...)` 的 `params` 仅匹配 `topk_distance` 形态（一个 `?` 作为查询向量直接走 FFI），普通 SQL 的 `?` 占位符不支持，需自行内联转义字面量。

**影响**

项目内所有 WHERE 条件只能手工拼接 SQL（本项目限定本地可信输入，整数/浮点内联安全；字符串已做单引号转义）。

**建议**

提供通用参数化绑定（位置参数/命名参数），至少覆盖字符串、数值和 IN 列表。
