# 日志采集与事件追踪平台

面向应用日志收集、事件埋点追踪、流式采集与结构化归集的多源日志采集平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 日志源管理 | `log_source` | 日志源 | 源编号、源名称、日志类型 |
| 日志解析 | `log_parser` | 解析规则 | 规则编号、关联源、正则表达式 |
| 事件埋点 | `event_tracker` | 埋点事件 | 事件编号、事件名称、埋点页面 |
| 流式缓冲 | `stream_buffer` | 缓冲配置 | 配置编号、关联源、缓冲区大小 |
| 字段映射 | `field_mapping` | 映射规则 | 规则编号、源字段、目标字段 |
| 采集代理 | `agent_heartbeat` | 采集代理 | 代理编号、部署主机、代理版本 |
| 事件校验 | `event_validate` | 校验规则 | 规则编号、关联事件、字段检查 |
| 日志轮转 | `log_rotate` | 轮转策略 | 策略编号、关联源、轮转间隔 |
| 日志富化 | `enrich_join` | 富化规则 | 规则编号、目标字段、维表来源 |
| 告警规则 | `alert_rule` | 告警规则 | 规则编号、关联源、触发条件 |
| 日志分区 | `log_partition` | 分区规则 | 规则编号、关联源、分区键 |
| 采集位点 | `collect_checkpoint` | 位点记录 | 记录编号、关联源、文件偏移 |
| 窗口去重 | `dedup_window` | 去重窗口 | 窗口编号、关联源、去重键 |
| 聚合计算 | `aggregate_olap` | 聚合规则 | 规则编号、关联源、聚合维度 |
| 格式输出 | `format_output` | 输出配置 | 配置编号、关联源、输出格式 |
| 回溯采集 | `backfill_range` | 回溯任务 | 任务编号、关联源、时间范围 |
| 采样策略 | `sampling_rule` | 采样规则 | 规则编号、关联源、采样方式 |
| 质量诊断 | `log_quality` | 诊断报告 | 报告编号、关联源、解析成功率 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

### 去重成品与采样回执

- 一次去重按输入内容生成批次号 `BATCH-<摘要>`；同一份输入重跑/重送只会复用同一批次，
  不会把已确认的保留行再次追加。
- 每个批次的成品与回执落在 `data/logs/batches/<源>/<批次>/`：
  `deduped.jsonl`（成品）、`receipt.json`（回执）、`manifest.json`（成品身份旁证）、
  `sampled.jsonl` / `sample_receipt.json`（采样）。
  回执中的保留数、保留行号范围与成品行数、摘要必须互相核对通过。
- 成品先原子整写、回执后提交。若写出后响应断开（`POST /api/dedup/run/{源}?simulate_disconnect=true`
  返回 503），重送相同输入会从 manifest 恢复同一份回执，而不是重新追加数据。
- 采样只消费“已核对成品 + 已确认回执”，回执记录采样基数（批次、输入摘要、保留数与行范围）
  与样本产物身份（摘要、行号）；同一批次采样幂等。
- 新批次各自计算保留集，旧批次成品与回执原样保留；晚到的旧批次重送不会替换磁盘上的最新成品。
  相关入口：`GET /api/dedup/batches/{源}`、`GET /api/dedup/receipt/{源}`、
  `POST /api/dedup/run/{源}`、`POST /api/sampling/run/{源}`、`GET /api/sampling/receipt/{源}`。

领域：数据工程
子方向：数据采集、爬虫与RPA
