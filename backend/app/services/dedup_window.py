"""窗口去重：按批次对日志记录去重，并产出可重跑核对、可恢复的成品与回执。

设计要点：

* **批次身份**：一次去重的输入按内容取摘要得到 ``batch_id``。同一份输入重送
  会落到同一个批次，重复处理只返回同一份回执，不会把已确认的保留行再次追加。
* **成品**：每个批次的保留记录单独落在
  ``logs/batches/<源>/<批次>/deduped.jsonl``，用整份原子替换写入；
  ``logs/<源>_deduped.jsonl`` 只是最新已确认批次的镜像，内容与批次成品逐字节一致。
* **回执**：``receipt.json`` 记录输入范围、保留行号范围、保留数、成品摘要和成品
  行数。回执中的保留数与持久化成品必须互相核对通过，才算本次可交付结果。
* **中断恢复**：成品先写、回执后提交。若在两次写之间断开（或回执被丢弃），
  重送同一输入时可从 ``manifest.json`` 恢复出同一份回执，不会重新追加数据。
* **新批次**：输入变化即生成新的批次，各自计算保留集；旧批次成品和回执保持
  原样，不会因为后续刷新而被换成别的输入。
"""
from __future__ import annotations

import hashlib
import re
import time
from collections import OrderedDict
from typing import Any

from app.services.base import BaseModuleService, source_lock
from app.store import store

SCHEMA_VERSION = 1


class DedupDeliveryInterrupted(RuntimeError):
    """成品已落盘但回执未提交时抛出，模拟响应断开。"""

    def __init__(self, result: dict[str, Any]) -> None:
        self.result = result
        super().__init__(
            f"去重成品已写入 {result.get('product_path')}，回执尚未提交，可重送同一输入"
        )


def _safe_segment(value: str) -> str:
    """把日志源编号/批次号转成安全的目录名。"""
    cleaned = re.sub(r"[^A-Za-z0-9_.-]", "_", str(value)).strip("._")
    return cleaned or "source"


class DedupWindowService(BaseModuleService):
    MODULE = "dedup_window"
    ENTITY = "去重窗口"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["窗口编号", "关联源", "去重键", "窗口大小", "滑动间隔", "指纹算法", "冲突策略", "窗口状态"]
    REQUIRED_FIELDS = ["窗口编号", "关联源", "去重键"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用窗口": "已停用"}
    NEGATIVE_ACTIONS = ["停用窗口"]

    # -- 路径布局 --

    def _batch_root(self, source_key: str):
        return self._data_dir("logs") / "batches" / _safe_segment(source_key)

    def _batch_dir(self, source_key: str, batch_id: str):
        return self._batch_root(source_key) / _safe_segment(batch_id)

    def _legacy_product(self, source_key: str):
        return self._data_dir("logs") / f"{source_key}_deduped.jsonl"

    def _latest_pointer(self, source_key: str):
        return self._batch_root(source_key) / "latest.json"

    def _input_file(self, source_key: str):
        return self._data_dir("logs") / f"{source_key}.jsonl"

    # -- 执行入口 --

    def deduplicate_source(
        self,
        source_key: str,
        batch_id: str | None = None,
        *,
        interrupt_after_write: bool = False,
        simulate_disconnect: bool | None = None,
        force_rebuild: bool = False,
    ) -> dict[str, Any]:
        """对指定日志源当前输入执行一次可重跑核对的窗口去重。

        ``batch_id`` 可显式指定历史批次（只读取该批次成品，不回算新输入）；
        ``interrupt_after_write`` / ``simulate_disconnect`` 用于模拟“写出数据但
        响应断开”：成品落盘后不提交回执并抛出 :class:`DedupDeliveryInterrupted`。
        """
        if simulate_disconnect is not None:
            interrupt_after_write = bool(simulate_disconnect)
        with source_lock(source_key):
            return self._deduplicate_locked(
                source_key,
                batch_id=batch_id,
                interrupt_after_write=interrupt_after_write,
                force_rebuild=force_rebuild,
            )

    def _deduplicate_locked(
        self,
        source_key: str,
        *,
        batch_id: str | None,
        interrupt_after_write: bool,
        force_rebuild: bool,
    ) -> dict[str, Any]:
        # 显式指定批次：优先只读该批次自己的成品与 manifest；已确认的回执直接复用，
        # 只有成品的批次按其 manifest 恢复回执。批次不可核对时，只有当前输入确实
        # 就是该批次输入（摘要一致）才允许原地重建，绝不把别的输入写成该批次。
        explicit_batch = batch_id
        if explicit_batch:
            existing = self._load_confirmed_batch(source_key, explicit_batch)
            if existing is not None:
                if existing["source"] == "manifest":
                    return self._commit_from_manifest(
                        source_key, explicit_batch, existing["manifest"],
                        replayed=True, recovered=True,
                    )
                return self._replay_result(existing["receipt"], replayed=True, recovered=False)

        log_file = self._input_file(source_key)
        if not log_file.exists():
            return {"error": "无采集数据", "deduped": 0}

        records = self._read_jsonl(log_file)
        if not records:
            return {"error": "无采集数据", "deduped": 0}

        input_digest, computed_batch_id = self._input_identity(source_key, records)

        if explicit_batch and explicit_batch != computed_batch_id:
            return {
                "error": (
                    f"批次 {explicit_batch} 没有可核对的成品，"
                    "且当前输入与该批次身份不一致，拒绝用别的输入覆盖该批次"
                ),
                "batch_id": explicit_batch,
                "current_batch_id": computed_batch_id,
            }

        batch_id = explicit_batch or computed_batch_id

        if not force_rebuild:
            existing = self._load_confirmed_batch(
                source_key, batch_id, input_digest=input_digest
            )
            if existing is not None:
                # 回执被丢弃但成品仍在：按 manifest 恢复回执，保留行不追加。
                if existing["source"] == "manifest":
                    return self._commit_from_manifest(
                        source_key, batch_id, existing["manifest"],
                        replayed=True, recovered=True,
                    )
                return self._replay_result(existing["receipt"], replayed=True, recovered=False)

        window = self._resolve_window(source_key)
        kept, duplicates, retained_line_nos = self._retain(records, window)

        batch_dir = self._batch_dir(source_key, batch_id)
        product_path = batch_dir / "deduped.jsonl"
        product_bytes = self._jsonl_bytes(kept)
        product_digest = self._digest_bytes(product_bytes)
        product_sha = hashlib.sha256(product_bytes).hexdigest()

        existing_manifest = self._read_json(batch_dir / "manifest.json") or {}
        sequence = int(existing_manifest.get("sequence") or 0) or self._next_sequence(source_key)
        manifest = {
            "schema": SCHEMA_VERSION,
            "source_key": source_key,
            "batch_id": batch_id,
            "sequence": sequence,
            "status": "product_written",
            "input": self._input_meta(log_file, records, input_digest),
            "retained": self._retained_meta(kept, retained_line_nos),
            "duplicates": len(duplicates),
            "dedup_ratio": round(len(duplicates) / max(len(records), 1) * 100, 2),
            "product": {
                "path": str(product_path),
                "relpath": self._relpath(product_path),
                "digest": product_digest,
                "sha256": product_sha,
                "bytes": len(product_bytes),
                "records": len(kept),
            },
            "window": window,
            "attempts": 1,
            "product_written_at": time.time(),
        }

        # 1) 成品整份原子替换（重跑/重送都不会在旧成品后追加）。
        self._write_jsonl_atomic(product_path, kept)
        # 2) 先写 manifest，作为成品身份的旁证；回执随后提交。
        self._write_json_atomic(batch_dir / "manifest.json", manifest)

        if interrupt_after_write:
            partial = dict(manifest)
            partial.update(self._receipt_aliases({
                "input": manifest["input"],
                "retained": manifest["retained"],
                "product": manifest["product"],
            }))
            partial.update({
                "receipt_id": self._receipt_id(batch_id, product_digest),
                "status": "product_written",
                "committed": False,
                "output_file": str(product_path),
                "product_path": str(product_path),
            })
            raise DedupDeliveryInterrupted(partial)

        return self._commit_manifest(source_key, batch_id, manifest)

    # -- 保留集 --

    def _resolve_window(self, source_key: str) -> dict[str, Any]:
        windows = [
            w for w in store.rows(self.MODULE)
            if w.get("关联源") == source_key and w.get("status") == "已生效"
        ]
        size = 1000
        key_field = "raw"
        window_code = ""
        if windows:
            window_code = str(windows[0].get("窗口编号", ""))
            key_field = str(windows[0].get("去重键", "raw") or "raw")
            try:
                size = int(str(windows[0].get("窗口大小", "1000")))
            except (TypeError, ValueError):
                size = 1000
        return {
            "mode": "count",
            "size": size,
            "dedup_key": key_field,
            "window_code": window_code,
            "fingerprint": "md5-16",
        }

    def _retain(
        self, records: list[dict[str, Any]], window: dict[str, Any]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[Any]]:
        """确定性保留集：按输入顺序保留每个指纹的首次出现。

        不依赖进程内缓存或墙上时间，因此同一份输入无论重跑多少次，
        保留集、重复数和成品摘要都相同。
        """
        key_field = window.get("dedup_key", "raw")
        size = int(window.get("size", 1000) or 0)
        seen: OrderedDict[str, Any] = OrderedDict()
        kept: list[dict[str, Any]] = []
        duplicates: list[dict[str, Any]] = []
        retained_line_nos: list[Any] = []

        for index, rec in enumerate(records, start=1):
            fp = self._compute_fingerprint(rec, key_field)
            if fp in seen:
                duplicates.append(rec)
                continue
            seen[fp] = rec.get("line_no", index)
            kept.append(rec)
            retained_line_nos.append(rec.get("line_no", index))
            if size > 0 and len(seen) > size:
                # 滑动窗口只限制指纹记忆长度，不改变已保留行的判定。
                seen.popitem(last=False)

        return kept, duplicates, retained_line_nos

    def _compute_fingerprint(self, rec: dict[str, Any], key_field: str) -> str:
        if key_field == "fields":
            fields = rec.get("fields", {})
            if fields:
                val = str(sorted(fields.items()))
            else:
                val = str(rec.get("raw", ""))
        else:
            val = str(rec.get(key_field, rec.get("raw", "")))
        return hashlib.md5(val.encode("utf-8")).hexdigest()[:16]

    # -- 身份与元数据 --

    def _input_identity(
        self, source_key: str, records: list[dict[str, Any]]
    ) -> tuple[str, str]:
        identities = [
            {
                "line_no": rec.get("line_no", index),
                "raw": rec.get("raw", ""),
            }
            for index, rec in enumerate(records, start=1)
        ]
        digest = self._digest_payload({
            "source_key": source_key,
            "records": identities,
        })
        return digest, f"BATCH-{digest[:16]}"

    def _input_meta(
        self, log_file, records: list[dict[str, Any]], input_digest: str
    ) -> dict[str, Any]:
        line_nos = [rec.get("line_no", i) for i, rec in enumerate(records, start=1)]
        return {
            "path": str(log_file),
            "relpath": self._relpath(log_file),
            "digest": input_digest,
            "count": len(records),
            "line_range": self._range(line_nos),
        }

    def _retained_meta(
        self, kept: list[dict[str, Any]], retained_line_nos: list[Any]
    ) -> dict[str, Any]:
        return {
            "count": len(kept),
            "line_nos": retained_line_nos,
            "line_range": self._range(retained_line_nos),
        }

    @staticmethod
    def _range(line_nos: list[Any]) -> dict[str, Any]:
        if not line_nos:
            return {"first": None, "last": None, "count": 0}
        return {"first": line_nos[0], "last": line_nos[-1], "count": len(line_nos)}

    def _receipt_id(self, batch_id: str, product_digest: str) -> str:
        return "DEDUP-" + self._digest_payload(
            {"batch_id": batch_id, "product": product_digest}
        )[:16]

    # -- 提交、恢复与核对 --

    def _commit_manifest(
        self,
        source_key: str,
        batch_id: str,
        manifest: dict[str, Any],
        *,
        replayed: bool = False,
        recovered: bool = False,
    ) -> dict[str, Any]:
        product = manifest["product"]
        receipt = {
            "schema": SCHEMA_VERSION,
            "receipt_id": self._receipt_id(batch_id, product["digest"]),
            "source_key": source_key,
            "batch_id": batch_id,
            "sequence": manifest.get("sequence", 0),
            "status": "committed",
            "committed": True,
            "input": manifest["input"],
            "retained": manifest["retained"],
            "duplicates": manifest.get("duplicates", 0),
            "dedup_ratio": manifest.get("dedup_ratio", 0.0),
            "product": product,
            "window": manifest.get("window", {}),
            "attempts": int(manifest.get("attempts", 1)),
            "completed_at": manifest.get("product_written_at") or time.time(),
        }
        receipt.update(self._receipt_aliases(receipt))

        batch_dir = self._batch_dir(source_key, batch_id)
        manifest = dict(manifest)
        manifest.update({
            "status": "committed",
            "receipt_id": receipt["receipt_id"],
            "completed_at": receipt["completed_at"],
        })
        self._write_json_atomic(batch_dir / "manifest.json", manifest)
        self._write_json_atomic(batch_dir / "receipt.json", receipt)
        self._publish_latest(source_key, receipt, manifest)
        return self._replay_result(receipt, replayed=replayed, recovered=recovered)

    def _commit_from_manifest(
        self,
        source_key: str,
        batch_id: str,
        manifest: dict[str, Any],
        *,
        replayed: bool,
        recovered: bool,
    ) -> dict[str, Any]:
        """回执被丢弃后，用 manifest + 成品重建同一份回执。"""
        manifest = dict(manifest)
        manifest["attempts"] = int(manifest.get("attempts", 1)) + 1
        return self._commit_manifest(
            source_key, batch_id, manifest, replayed=replayed, recovered=recovered
        )

    @staticmethod
    def _receipt_aliases(receipt: dict[str, Any]) -> dict[str, Any]:
        product = receipt["product"]
        retained = receipt["retained"]
        return {
            "total": receipt["input"]["count"],
            "kept": retained["count"],
            "deduped": retained["count"],
            "output_file": product["path"],
            "product_path": product["path"],
            "product_digest": product["digest"],
            "retained_line_nos": retained["line_nos"],
            "kept_line_range": retained["line_range"],
            "input_line_range": receipt["input"]["line_range"],
        }

    def _replay_result(
        self,
        receipt: dict[str, Any],
        *,
        replayed: bool,
        recovered: bool = False,
    ) -> dict[str, Any]:
        result = dict(receipt)
        result.update(self._receipt_aliases(receipt))
        result["replayed"] = replayed
        result["recovered"] = recovered
        result["verified"] = True
        result["dedup_result"] = "same_batch"
        return result

    def _load_confirmed_batch(
        self,
        source_key: str,
        batch_id: str,
        *,
        input_digest: str | None = None,
    ) -> dict[str, Any] | None:
        """读取并核对指定批次的成品；返回回执或 manifest 恢复依据。"""
        batch_dir = self._batch_dir(source_key, batch_id)
        product_path = batch_dir / "deduped.jsonl"
        if not product_path.exists():
            return None

        product_bytes = product_path.read_bytes()
        receipt = self._read_json(batch_dir / "receipt.json")
        if receipt is not None:
            if self._verify_receipt(receipt, source_key, batch_id, product_bytes, input_digest):
                return {"source": "receipt", "receipt": receipt}
            return None

        manifest = self._read_json(batch_dir / "manifest.json")
        if manifest is None:
            return None
        if manifest.get("source_key") != source_key or manifest.get("batch_id") != batch_id:
            return None
        if input_digest is not None and manifest.get("input", {}).get("digest") != input_digest:
            return None
        product_meta = manifest.get("product", {})
        if product_meta.get("digest") != self._digest_bytes(product_bytes):
            return None
        product_records = self._read_jsonl(product_path)
        if int(product_meta.get("records", -1)) != len(product_records):
            return None
        # manifest 是回执的恢复依据，保留行范围同样必须和成品对得上。
        retained = manifest.get("retained", {})
        if int(retained.get("count", -1)) != len(product_records):
            return None
        if product_records and "line_no" in product_records[0]:
            if [rec.get("line_no") for rec in product_records] != list(retained.get("line_nos", [])):
                return None
        return {"source": "manifest", "manifest": manifest}

    def _verify_receipt(
        self,
        receipt: dict[str, Any],
        source_key: str,
        batch_id: str,
        product_bytes: bytes,
        input_digest: str | None,
    ) -> bool:
        if receipt.get("status") != "committed":
            return False
        if receipt.get("source_key") != source_key or receipt.get("batch_id") != batch_id:
            return False
        if input_digest is not None and receipt.get("input", {}).get("digest") != input_digest:
            return False
        product = receipt.get("product", {})
        if product.get("digest") != self._digest_bytes(product_bytes):
            return False
        kept_records = self._read_jsonl(self._batch_dir(source_key, batch_id) / "deduped.jsonl")
        retained = receipt.get("retained", {})
        if int(retained.get("count", -1)) != len(kept_records):
            return False
        if int(product.get("records", -1)) != len(kept_records):
            return False
        if "line_no" in (kept_records[0] if kept_records else {}):
            actual = [rec.get("line_no") for rec in kept_records]
            if actual != list(retained.get("line_nos", [])):
                return False
        return True

    # -- 最新指针与兼容镜像 --

    def _next_sequence(self, source_key: str) -> int:
        """序号在所有已落盘批次之间单调递增（含未提交批次）。

        仅看 latest 指针会在“中断批次 + 后续新批次”时给出重复序号，
        导致旧批次重送时按序号比较不出先后。调用方必须持有源锁。
        """
        highest = 0
        pointer = self._read_json(self._latest_pointer(source_key)) or {}
        highest = max(highest, int(pointer.get("sequence", 0)))
        root = self._batch_root(source_key)
        if root.exists():
            for child in root.iterdir():
                if not child.is_dir():
                    continue
                for name in ("receipt.json", "manifest.json"):
                    meta = self._read_json(child / name) or {}
                    highest = max(highest, int(meta.get("sequence", 0)))
        return highest + 1

    def _publish_latest(
        self, source_key: str, receipt: dict[str, Any], manifest: dict[str, Any]
    ) -> None:
        pointer_path = self._latest_pointer(source_key)
        pointer = self._read_json(pointer_path) or {}
        sequence = int(receipt.get("sequence", 0))
        product = receipt["product"]

        # 重送旧批次不回退最新指针：原结果不会被刷新成别的输入。
        if pointer and sequence < int(pointer.get("sequence", 0)):
            return

        self._write_json_atomic(pointer_path, {
            "batch_id": receipt["batch_id"],
            "sequence": sequence,
            "receipt_id": receipt["receipt_id"],
            "product_digest": product["digest"],
            "retained": receipt["retained"]["count"],
            "completed_at": receipt["completed_at"],
        })

        legacy = self._legacy_product(source_key)
        source_product = self._batch_dir(source_key, receipt["batch_id"]) / "deduped.jsonl"
        if legacy.exists() and legacy.read_bytes() == source_product.read_bytes():
            pass
        else:
            self._write_jsonl_atomic(
                legacy, self._read_jsonl(source_product)
            )

        self._write_checkpoint(f"dedup_{source_key}", {
            "batch_id": receipt["batch_id"],
            "receipt_id": receipt["receipt_id"],
            "total": receipt["input"]["count"],
            "kept": receipt["retained"]["count"],
            "duplicates": receipt["duplicates"],
            "dedup_ratio": receipt["dedup_ratio"],
            "window_size": receipt.get("window", {}).get("size"),
            "product_digest": product["digest"],
            "completed_at": receipt["completed_at"],
        })

    # -- 查询辅助 --

    def get_latest_receipt(self, source_key: str) -> dict[str, Any] | None:
        pointer = self._read_json(self._latest_pointer(source_key))
        if not pointer:
            return None
        found = self._load_confirmed_batch(source_key, pointer["batch_id"])
        return found["receipt"] if found and found["source"] == "receipt" else None

    def get_receipt(self, source_key: str, batch_id: str) -> dict[str, Any] | None:
        found = self._load_confirmed_batch(source_key, batch_id)
        return found["receipt"] if found and found["source"] == "receipt" else None

    def list_batches(self, source_key: str) -> list[dict[str, Any]]:
        root = self._batch_root(source_key)
        if not root.exists():
            return []
        batches = []
        for child in sorted(root.iterdir()):
            if not child.is_dir():
                continue
            receipt = self._read_json(child / "receipt.json")
            manifest = self._read_json(child / "manifest.json") or {}
            batches.append({
                "batch_id": child.name,
                "status": (receipt or manifest).get("status", "unknown"),
                "sequence": (receipt or manifest).get("sequence", 0),
                "kept": ((receipt or {}).get("retained", {}) or {}).get("count"),
                "completed_at": (receipt or manifest).get("completed_at"),
            })
        return batches

    def clear_window(self, source_key: str) -> dict[str, Any]:
        """窗口去重不再依赖进程内指纹缓存；保留接口用于显式重算。"""
        return {"source_key": source_key, "cleared": True, "persistent": True}
