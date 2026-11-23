"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for
from app.services.dedup_window import DedupDeliveryInterrupted
from app.services.pipeline import pipeline

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


# -- 去重成品 / 采样回执入口 --
# 注意：这些固定路径必须注册在 /{module}/{entry_id} 之前，避免被动态段抢先匹配。

@router.get("/dedup/batches/{source_key}")
def list_dedup_batches(source_key: str) -> dict[str, Any]:
    """列出某日志源已落盘的去重批次及其状态。"""
    service = require_service("dedup_window")
    return {"source_key": source_key, "batches": service.list_batches(source_key)}


@router.get("/dedup/receipt/{source_key}")
def get_dedup_receipts(source_key: str, batch_id: str | None = Query(default=None)) -> dict[str, Any]:
    """读取去重回执：显式批次或最新已确认批次。"""
    service = require_service("dedup_window")
    receipt = (
        service.get_receipt(source_key, batch_id)
        if batch_id
        else service.get_latest_receipt(source_key)
    )
    if receipt is None:
        raise HTTPException(status_code=404, detail=f"{source_key} 无已确认的去重回执")
    return receipt


@router.post("/dedup/run/{source_key}")
def run_dedup(
    source_key: str,
    batch_id: str | None = Query(default=None),
    simulate_disconnect: bool = Query(default=False),
    force: bool = Query(default=False),
) -> dict[str, Any]:
    """执行一次可重跑核对的去重；simulate_disconnect 可在成品落盘后不提交回执。"""
    service = require_service("dedup_window")
    try:
        return service.deduplicate_source(
            source_key,
            batch_id,
            interrupt_after_write=simulate_disconnect,
            force_rebuild=force,
        )
    except DedupDeliveryInterrupted as exc:
        # 成品已写出，但本次不交付回执：客户端应重送相同输入。
        raise HTTPException(
            status_code=503,
            detail={"message": str(exc), "outcome": exc.result},
        ) from exc


@router.get("/sampling/receipt/{source_key}")
def get_sampling_receipt(source_key: str, batch_id: str | None = Query(default=None)) -> dict[str, Any]:
    """读取采样回执：采样基数、采样参数与样本产物身份。"""
    service = require_service("sampling_rule")
    return service.get_sampling_stats(source_key, batch_id)


@router.post("/sampling/run/{source_key}")
def run_sampling(
    source_key: str,
    batch_id: str | None = Query(default=None),
    force: bool = Query(default=False),
) -> dict[str, Any]:
    """在已核对通过的去重成品上采样。"""
    service = require_service("sampling_rule")
    return service.sample_source(source_key, batch_id, force_rebuild=force)


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)


# -- 跨模块管道端点 --

@router.post("/pipelines/collect")
def run_collect_pipeline(source_id: int = Query(default=0)) -> dict[str, Any]:
    """执行完整采集管道：采集 -> 解析 -> 映射 -> 输出。"""
    return pipeline.full_collect_pipeline(source_id)


@router.post("/pipelines/event/{event_code}")
def run_event_pipeline(event_code: str) -> dict[str, Any]:
    """执行事件追踪管道：跟踪 -> 缓冲 -> 校验。"""
    return pipeline.event_tracking_pipeline([event_code])


@router.post("/pipelines/backfill/{source_key}")
def run_backfill_pipeline(source_key: str, time_range: str = Query(default="2026-09-01~2026-09-30")) -> dict[str, Any]:
    """执行回溯采集管道：创建任务 -> 分批执行 -> 位点更新。"""
    return pipeline.backfill_pipeline(source_key, time_range)


@router.post("/pipelines/quality-alert/{source_key}")
def run_quality_alert_pipeline(source_key: str) -> dict[str, Any]:
    """执行质量诊断+告警管道：诊断 -> 评估 -> 触发告警 -> 位点回滚。"""
    return pipeline.quality_alert_pipeline(source_key)


@router.post("/pipelines/enrich-aggregate/{source_key}")
def run_enrich_aggregate_pipeline(source_key: str) -> dict[str, Any]:
    """执行富化+聚合管道：构建维表 -> 富化 -> 聚合 -> 告警评估。"""
    return pipeline.enrichment_aggregation_pipeline(source_key)


@router.post("/pipelines/dedup-sample/{source_key}")
def run_dedup_sample_pipeline(source_key: str) -> dict[str, Any]:
    """执行去重+采样管道：窗口去重 -> 采样 -> 格式化输出。"""
    return pipeline.dedup_sample_pipeline(source_key)


@router.post("/pipelines/rotate-partition/{source_key}")
def run_rotate_partition_pipeline(source_key: str) -> dict[str, Any]:
    """执行轮转+分区管道：轮转 -> 分区路由 -> 归并。"""
    return pipeline.rotate_partition_pipeline(source_key)


@router.post("/pipelines/agent-checkpoint/{agent_code}")
def run_agent_checkpoint_pipeline(agent_code: str) -> dict[str, Any]:
    """执行代理心跳+位点管道：心跳上报 -> 位点同步 -> 离线检测。"""
    return pipeline.agent_checkpoint_pipeline(agent_code)


@router.post("/pipelines/run-all")
def run_all_pipelines() -> dict[str, Any]:
    """运行所有管道组合。"""
    return pipeline.run_all_pipelines()
