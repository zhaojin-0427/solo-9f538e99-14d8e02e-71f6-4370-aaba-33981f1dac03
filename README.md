# OSM 道路图层批次入仓系统

面向城市 GIS 数据员的 Web 服务：把只含 `node` / `way` 的 OpenStreetMap 分区摘录
（`.osm`）和离线变更文件（`.osc`）按**批次**入仓到 SQLite，分析人员可按入仓批次
查看道路图层并下载当时的 GeoJSON 快照。

技术栈：Flask 3 + SQLite（WAL 模式）+ gunicorn，纯 Docker Compose 部署。

## 快速启动

```bash
docker compose up --build
# 打开 http://localhost:8000
```

数据库持久化在命名卷 `osm_batch_data` 中；`samples/` 以只读方式挂载到容器
`/samples`，可直接在网页上传试用。已有旧版本数据卷无需迁移命令，容器启动时
自动原地升级（见下文「批次不可变与并发」）。

本地无 Docker 时的开发启动：

```bash
pip install -r requirements.txt
python -m gunicorn --bind 0.0.0.0:8000 --worker-class gthread --workers 2 \
    --threads 8 wsgi:app
# 或：flask --app wsgi run --debug
```

## 网页操作

1. 首页选择 `.osm` / `.osc` 文件上传，每个文件形成一个批次（上传开始即编号）。
2. 批次列表显示状态（已提交 / 已拒绝 / 处理中）、本批对象版本数、拒绝原因；
   解析中的批次也立即列出，详情链接固定。
3. 批次详情页显示：可见/已删除对象统计、可导出道路、待补全道路及缺失节点 id。
4. 点击「下载本批道路 GeoJSON」得到 `batch_<id>_highways.geojson`
   （GeoJSON FeatureCollection，几何为 LineString，坐标按 lon/lat 顺序）。
5. `GET /health` 健康检查；拒绝或未提交批次的导出返回 HTTP 409。

## 合并与版本规则

按对象 `(类型, id, version)` 合并，所有规则在单事务内整批生效：

| 情况 | 处理 |
| --- | --- |
| 版本号低于库中最高版本（旧版重传） | **静默忽略**，不能修改或恢复任何对象，包括已删除对象 |
| 同版本、内容完全相同 | 重复跳过（重复上传同一文件幂等） |
| 同版本、内容不同 | **整批拒绝**，回滚全部对象写入，批次留拒绝原因 |
| 更高版本、可见 | 成为对象当前状态 |
| 更高版本、不可见（delete） | 墓碑版本：对象撤下，版本历史永久保留 |
| 含 `relation` 等非 node/way 元素、XML 非法、字段缺失 | 整批拒绝 |

## 道路生成规则

- 仅带 `highway` 标签的可见 way 参与道路图层，按 `<nd>` 有序节点生成 LineString。
- way 引用的节点缺失（从未上传）或已删除 → **整条 way 列为待补全**，不进 GeoJSON；
  后续批次补齐可见节点后，在新批次导出中自动恢复。
- 共享节点被移动或删除时，即使引用它的 way 没有重传，所有相关道路的几何同步更新、
  或整条撤下；way 改引用（新版本换 `<nd>` 列表）后，旧节点不再影响它。

## 批次不可变与并发

- 上传开始即预留批次号并写入 `processing` 批次行，因此解析期间该批已出现在列表中，
  详情链接 `/batches/<id>` 立即可用，**批次号此后永不变更**（即使提交失败也不改号）。
- 批次号只反映上传开始顺序。每个批次在提交事务内额外分配一个单调递增的
  `commit_seq`（提交序号）。某批 B 的快照 = 所有 `commit_seq <= B.commit_seq` 的
  已提交批次中每对象的最高版本。它固定为 B **提交那一刻**的完整入仓状态：含 B 自身
  及此前已提交批次，**不含当时仍在解析或已失败的批次**。
- 因此并行上传时，先取得较小批次号但解析较慢的 A 即使晚于 B 提交，也不会改变 B 已
  发布的道路坐标、待补全列表或统计；A 自己的快照则包含 B（B 在 A 提交前已完成），
  不会漏掉 B 已补齐的节点。版本合并仍严格按 `(类型, id, version)` 规则执行。
- `object_versions` 是只追加事件表；已提交批次的 `commit_seq` 永不改变，后续上传、
  导出与进程重启都不会改变任何历史批次的快照。
- 每批在一个 `BEGIN IMMEDIATE` 事务内完成对象写入、提交序号分配与状态翻转；失败即
  ROLLBACK，只留下标记 rejected 的批次审计行，不留半批，也不获得 `commit_seq`。
- 解析在数据库写事务之外进行；gunicorn 使用 gthread 多线程 worker，慢批解析时另一批
  仍可完成提交；导出只做只读快照查询，不会阻塞也不被提交阻塞。
- 进程重启时，启动钩子只回滚**属主进程已不存在**的 `processing` 批次（按主机名 +
  pid + 进程启动时刻判断），不会误杀其他 worker 正在解析的慢批。
- **兼容已有数据卷**：`init_db` 以增量 `ALTER TABLE` 原地升级旧库，保留全部批次号、
  对象历史与链接；旧批次（升级前已可见的结果）的 `commit_seq` 回填为其批次号，
  精确复现升级前 `batch_id <= B` 的顺序视图，升级后仍可在详情页看到与升级前相同的
  道路与待补全结果。升级后新上传的批次从更大的序号继续，且不会改变这些旧批快照。

## 样例文件（`samples/`，建议按编号顺序上传）

| 文件 | 场景 |
| --- | --- |
| `01_base_extract.osm` | 基础摘录：2 条 highway 道路 + 1 条非 highway way，含共享节点 n3 |
| `02_missing_node.osm` | **缺节点**：w102 引用未入仓的 n9 → 待补全；含同版重传 |
| `03_fill_gap_and_move_shared.osc` | 补齐 n9 道路恢复；**共享节点** n3 移动，两条路几何联动更新 |
| `04_delete_shared_node.osc` | 删除共享节点 n4（保留墓碑版本），w101 未重传仍被整条撤下 |
| `05_old_version_ignored.osc` | 旧版 n4 v1 重传被忽略、不能恢复对象；同版重复跳过 |
| `06_way_reroute.osc` | w101 改引用绕开 n4，旧节点不再影响它，道路恢复 |
| `07_out_of_order.osc` | **乱序变更**：way 先于节点、同批前向引用、同文件重复、同对象多版本 |
| `08_same_version_conflict.osc` | 同版异内容冲突，整批拒绝（同批合法 way 也回滚） |
| `09_recreate_node_higher_version.osc` | 只有更高版本才能重建已删除节点，墓碑历史保留 |
| `10_relation_rejected.osm` | 含 relation，整批拒绝 |

## 测试

```bash
pip install -r requirements.txt pytest
python -m pytest tests/ -v
```

除版本合并、道路生成、HTTP 上传/下载等原有用例外，还包含针对并行上传快照的回归：

- 慢批（低批次号）晚提交不改变先提交批次的快照，且自身快照包含先提交批次；
- 解析中的批次不计入其他批次的快照；
- 失败批不获得提交序号、不进入任何快照；
- 重启前后各批快照一致、批次号不变；
- 旧数据卷原地升级后，每个旧批次的可见结果与升级前 `batch_id <= B` 完全一致；
- HTTP 层：解析中批次即出现在列表、详情链接固定可用，另一批上传与导出不被阻塞；
- 启动钩子只回收属主已死的 `processing` 批次，保留存活 worker 的解析中批次。

## 目录结构

```
app/
  config.py       环境变量配置
  db.py           SQLite 连接、建表、原地迁移、崩溃恢复
  osm_parser.py   .osm/.osc 严格解析（defusedxml）
  ingestion.py    批次合并规则、提交序号与单事务入仓
  roads.py        按 commit_seq 重建快照、待补全判定、GeoJSON 序列化
  web.py          Flask 路由
  templates/      中文网页模板
samples/          10 个场景样例
tests/            pytest 测试
wsgi.py           gunicorn 入口
Dockerfile / docker-compose.yml
```
