"""采样策略：在可重跑核对的去重成品上确定性降采样，并记录采样基数与产物身份。

采样只消费“批次成品 + 已确认回执”这一对：

* 读取前先用回执核对成品摘要与保留行范围，核对不通过就不采样；
* 采样对同一批次是幂等的，重跑/重送只复用同一份采样回执，不会追加样本；
* 采样回执写明本次采样基数（批次号、输入摘要、保留数与保留行范围）、采样参数、
  样本数与样本产物摘要，便于和上游回执逐项对上。
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService, source_lock
from app.services.dedup_window import DedupWindowService
from app.store import store

SCHEMA_VERSION = 1


class SamplingRuleService(BaseModuleService):
    MODULE = "sampling_rule"
    ENTITY = "采样规则"
    OPERATION_TERM = "采样"
    LIST_FIELDS = ["规则编号", "关联源", "采样方式", "采样比例", "保留条件", "降级策略", "生效时段", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "关联源", "采样方式"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def _sample_paths(self, source_key: str, batch_id: str):
        batch_dir = DedupWindowService()._batch_dir(source_key, batch_id)
        return batch_dir / "sampled.jsonl", batch_dir / "sample_receipt.json"

    def sample_source(
        self,
        source_key: str,
        batch_id: str | None = None,
        *,
        force_rebuild: bool = False,
    ) -> dict[str, Any]:
        """对最新已确认的去重成品采样；也可指定历史批次重跑核对。"""
        with source_lock(source_key):
            return self._sample_locked(
                source_key, batch_id=batch_id, force_rebuild=force_rebuild
            )

    def _sample_locked(
        self, source_key: str, *, batch_id: str | None, force_rebuild: bool
    ) -> dict[str, Any]:
        dedup = DedupWindowService()
        receipt = (
            dedup.get_receipt(source_key, batch_id)
            if batch_id
            else dedup.get_latest_receipt(source_key)
        )

        if receipt is None:
            # 没有可用成品时，明确区分“还没去重”和“批次不存在/未确认”。
            if batch_id:
                return {
                    "error": f"批次 {batch_id} 无已确认的去重成品，拒绝在未核对的数据上采样",
                    "source_key": source_key,
                    "total": 0,
                    "sampled": 0,
                }
            pointer = dedup._read_json(dedup._latest_pointer(source_key)) or {}
            if pointer:
                return {
                    "error": "最新去重成品与回执核对不通过，拒绝采样",
                    "source_key": source_key,
                    "total": 0,
                    "sampled": 0,
                }
            return {"error": "无可用数据，请先执行去重", "total": 0, "sampled": 0}

        # 复核成品：采样基数必须与回执、持久化成品三方一致。
        product_path = dedup._batch_dir(source_key, receipt["batch_id"]) / "deduped.jsonl"
        if not product_path.exists():
            return {
                "error": "去重成品缺失，拒绝采样",
                "source_key": source_key, "total": 0, "sampled": 0,
            }
        product_bytes = product_path.read_bytes()
        if dedup._digest_bytes(product_bytes) != receipt["product"]["digest"]:
            return {
                "error": "去重成品摘要与回执不一致，拒绝采样",
                "source_key": source_key, "total": 0, "sampled": 0,
            }
        base_records = dedup._read_jsonl(product_path)
        if len(base_records) != int(receipt["retained"]["count"]):
            return {
                "error": "去重成品保留数与回执不一致，拒绝采样",
                "source_key": source_key,
                "total": 0,
                "sampled": 0,
            }

        rule = self._resolve_rule(source_key)
        batch = receipt["batch_id"]
        sampled_path, receipt_path = self._sample_paths(source_key, batch)

        if not force_rebuild:
            existing = self._read_json(receipt_path)
            if existing and self._verify_sample_receipt(
                existing, receipt, rule, sampled_path
            ):
                return self._sample_result(existing, replayed=True)

        sampled = self._apply_sampling(
            base_records, rule["method"], rule["rate"], source_key, receipt
        )

        sample_bytes = self._jsonl_bytes(sampled)
        sample_digest = self._digest_bytes(sample_bytes)
        retained_line_nos = receipt["retained"].get("line_nos", [])
        sampled_line_nos = [rec.get("line_no", i) for i, rec in enumerate(sampled, start=1)]

        sample_receipt = {
            "schema": SCHEMA_VERSION,
            "receipt_id": "SAMPLE-" + self._digest_payload({
                "source_key": source_key,
                "batch_id": batch,
                "dedup_receipt": receipt["receipt_id"],
                "sample": sample_digest,
                "method": rule["method"],
                "rate": rule["rate"],
            })[:16],
            "source_key": source_key,
            "status": "committed",
            "committed": True,
            "base": {
                "kind": "dedup_batch",
                "batch_id": batch,
                "dedup_receipt_id": receipt["receipt_id"],
                "input_digest": receipt["input"]["digest"],
                "product_digest": receipt["product"]["digest"],
                "retained_count": receipt["retained"]["count"],
                "retained_line_range": receipt["retained"]["line_range"],
                "retained_line_nos": retained_line_nos,
            },
            "sampling": {
                "method": rule["method"],
                "rate": rule["rate"],
                "rule_id": rule["rule_id"],
                "seed": self._sample_seed(source_key, batch),
            },
            "sample": {
                "path": str(sampled_path),
                "relpath": self._relpath(sampled_path),
                "digest": sample_digest,
                "bytes": len(sample_bytes),
                "count": len(sampled),
                "line_nos": sampled_line_nos,
                "line_range": dedup._range(sampled_line_nos),
            },
            "counts": {
                "base": len(base_records),
                "sampled": len(sampled),
                "dropped": len(base_records) - len(sampled),
            },
            "completed_at": time.time(),
        }
        sample_receipt["sampled"] = sample_receipt["counts"]["sampled"]
        sample_receipt["base_count"] = sample_receipt["counts"]["base"]

        # 先写样本，再提交采样回执；两者都在源锁内完成，读取方不会看到半份样本。
        self._write_jsonl_atomic(sampled_path, sampled)
        self._write_json_atomic(receipt_path, sample_receipt)

        mirror = self._data_dir("logs") / f"{source_key}_sampled.jsonl"
        self._write_jsonl_atomic(mirror, sampled)
        self._write_json_atomic(
            self._data_dir("logs") / f"{source_key}_sampled.latest.json",
            {
                "batch_id": batch,
                "dedup_receipt_id": receipt["receipt_id"],
                "sample_receipt_id": sample_receipt["receipt_id"],
                "base_count": len(base_records),
                "sampled": len(sampled),
                "sample_digest": sample_digest,
            },
        )

        self._write_checkpoint(f"sample_{source_key}", {
            "batch_id": batch,
            "dedup_receipt_id": receipt["receipt_id"],
            "receipt_id": sample_receipt["receipt_id"],
            "total": len(base_records),
            "sampled": len(sampled),
            "rate": rule["rate"],
            "method": rule["method"],
            "sample_digest": sample_digest,
            "completed_at": sample_receipt["completed_at"],
        })
        return self._sample_result(sample_receipt, replayed=False)

    def _resolve_rule(self, source_key: str) -> dict[str, Any]:
        rules = [
            r for r in store.rows(self.MODULE)
            if r.get("关联源") == source_key and r.get("status") == "已生效"
        ]
        if not rules:
            return {"method": "hash", "rate": 0.5, "rule_id": ""}
        rule = rules[0]
        try:
            rate = float(str(rule.get("采样比例", "0.5")))
        except (TypeError, ValueError):
            rate = 0.5
        rate = min(max(rate, 0.0), 1.0)
        method = str(rule.get("采样方式", "hash") or "hash")
        if method not in ("random", "systematic", "hash", "head"):
            method = "hash"
        return {"method": method, "rate": rate, "rule_id": str(rule.get("规则编号", ""))}

    @staticmethod
    def _sample_seed(source_key: str, batch_id: str) -> str:
        return hashlib.sha256(f"{source_key}|{batch_id}".encode("utf-8")).hexdigest()[:16]

    def _apply_sampling(
        self,
        records: list[dict[str, Any]],
        method: str,
        rate: float,
        source_key: str,
        receipt: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if rate >= 1.0:
            return list(records)
        if rate <= 0:
            return []

        if method == "random":
            # 用批次摘要做种子：同一批次重跑得到同一份样本，便于核对。
            seed_material = (
                self._sample_seed(source_key, receipt["batch_id"])
                + receipt["product"]["digest"]
            )
            sampled = []
            for rec in records:
                ticket = int(
                    hashlib.sha256(
                        (seed_material + str(rec.get("line_no", ""))).encode("utf-8")
                    ).hexdigest()[:8], 16
                )
                if (ticket % 10_000) < rate * 10_000:
                    sampled.append(rec)
            return sampled

        if method == "systematic":
            step = max(1, int(round(1 / rate)))
            return [r for i, r in enumerate(records) if i % step == 0]

        if method == "hash":
            sampled = []
            for r in records:
                raw = str(r.get("raw", r.get("line_no", "")))
                rand_seed = int(hashlib.md5((raw + source_key).encode()).hexdigest()[:8], 16)
                if (rand_seed % 100) < rate * 100:
                    sampled.append(r)
            return sampled

        if method == "head":
            count = max(1, int(len(records) * rate))
            return records[:count]

        return list(records)

    def _verify_sample_receipt(
        self,
        sample_receipt: dict[str, Any],
        dedup_receipt: dict[str, Any],
        rule: dict[str, Any],
        sampled_path,
    ) -> bool:
        if sample_receipt.get("status") != "committed":
            return False
        base = sample_receipt.get("base", {})
        if base.get("batch_id") != dedup_receipt["batch_id"]:
            return False
        if base.get("dedup_receipt_id") != dedup_receipt["receipt_id"]:
            return False
        if base.get("product_digest") != dedup_receipt["product"]["digest"]:
            return False
        sampling = sample_receipt.get("sampling", {})
        if sampling.get("method") != rule["method"] or sampling.get("rate") != rule["rate"]:
            return False
        if not sampled_path.exists():
            return False
        if sample_receipt.get("sample", {}).get("digest") != self._digest_bytes(
            sampled_path.read_bytes()
        ):
            return False
        return True

    def _sample_result(
        self, sample_receipt: dict[str, Any], *, replayed: bool
    ) -> dict[str, Any]:
        result = dict(sample_receipt)
        base = sample_receipt["base"]
        sample = sample_receipt["sample"]
        result.update({
            "batch_id": base["batch_id"],
            "dedup_receipt_id": base["dedup_receipt_id"],
            "total": base["retained_count"],
            "base_count": base["retained_count"],
            "sampled": sample["count"],
            "sample_rate": sample_receipt["sampling"]["rate"],
            "method": sample_receipt["sampling"]["method"],
            "sample_path": sample["path"],
            "sample_digest": sample["digest"],
            "retained_line_range": base["retained_line_range"],
            "sampled_line_nos": sample["line_nos"],
            "replayed": replayed,
            "verified": True,
        })
        return result

    def get_sampling_stats(self, source_key: str, batch_id: str | None = None) -> dict[str, Any]:
        """获取采样回执；未指定批次时取最新。"""
        dedup = DedupWindowService()
        receipt = (
            dedup.get_receipt(source_key, batch_id)
            if batch_id
            else dedup.get_latest_receipt(source_key)
        )
        if receipt is None:
            return {"source_key": source_key, "status": "未采样"}
        sample_receipt = self._read_json(self._sample_paths(source_key, receipt["batch_id"])[1])
        if sample_receipt is None:
            return {
                "source_key": source_key,
                "batch_id": receipt["batch_id"],
                "status": "未采样",
            }
        return self._sample_result(sample_receipt, replayed=False)
