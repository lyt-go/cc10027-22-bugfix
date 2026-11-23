"""去重成品与采样回执的回归测试：重跑可核对、断线重送、批次隔离。

这些用例使用独立的临时数据目录，不污染 test_cross_module.py 的运行产物。
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import app.services.base as base
from app.services.dedup_window import DedupDeliveryInterrupted, DedupWindowService
from app.services.sampling_rule import SamplingRuleService
from app.store import store

SOURCE_KEY = "LOG_-DEDUP-REG"


def _use_temp_data_root() -> Path:
    import tempfile

    root = Path(tempfile.mkdtemp(prefix="dedup-regression-"))
    base.DATA_ROOT = root
    return root


def _seed_rules() -> None:
    rows = store.rows("dedup_window")
    if not any(r.get("窗口编号") == "DEDU-REG" for r in rows):
        rows.append({
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "窗口编号": "DEDU-REG",
            "关联源": SOURCE_KEY,
            "去重键": "raw",
            "窗口大小": "1000",
            "滑动间隔": "1",
            "指纹算法": "md5",
            "冲突策略": "keep_first",
            "窗口状态": "已生效",
        })
    rows = store.rows("sampling_rule")
    if not any(r.get("规则编号") == "SAMP-REG" for r in rows):
        rows.append({
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "规则编号": "SAMP-REG",
            "关联源": SOURCE_KEY,
            "采样方式": "random",
            "采样比例": "0.5",
            "保留条件": "",
            "降级策略": "",
            "生效时段": "",
            "规则版本": "v1",
        })


def _write_input(lines: list[str]) -> Path:
    path = base.DATA_ROOT / "logs" / f"{SOURCE_KEY}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for index, raw in enumerate(lines, start=1):
            f.write(json.dumps({
                "source_key": SOURCE_KEY,
                "source_name": "去重回归源",
                "line_no": index,
                "offset": index * 10,
                "raw": raw,
                "collected_at": float(index),
            }, ensure_ascii=False) + "\n")
    return path


def _base_lines() -> list[str]:
    # 12 行、4 种内容，各重复 3 次；首次出现的行号应为 1、2、3、4。
    return [f"2026-11-23T00:00:00 [svc] INFO event_id={i % 4} msg=repeat" for i in range(12)]


def _product_lines(dedup: DedupWindowService, batch_id: str, name: str = "deduped.jsonl") -> list[dict]:
    return dedup._read_jsonl(dedup._batch_dir(SOURCE_KEY, batch_id) / name)


def _assert_receipt_matches_product(dedup: DedupWindowService, receipt: dict) -> None:
    product = _product_lines(dedup, receipt["batch_id"])
    assert receipt["retained"]["count"] == len(product)
    assert receipt["product"]["records"] == len(product)
    assert receipt["kept"] == len(product)
    assert receipt["retained"]["line_nos"] == [rec["line_no"] for rec in product]
    assert receipt["product"]["digest"] == dedup._digest_bytes(
        (dedup._batch_dir(SOURCE_KEY, receipt["batch_id"]) / "deduped.jsonl").read_bytes()
    )


def setup_module():
    _use_temp_data_root()
    _seed_rules()
    _write_input(_base_lines())


def test_dedup_rerun_is_idempotent_and_receipt_matches_product():
    """同一份输入重跑：保留数不归零，回执与成品逐项对得上。"""
    svc = DedupWindowService()

    first = svc.deduplicate_source(SOURCE_KEY)
    assert first["total"] == 12
    assert first["kept"] == 4, first
    assert first["duplicates"] == 8
    assert first["retained"]["line_nos"] == [1, 2, 3, 4]
    _assert_receipt_matches_product(svc, first)

    second = svc.deduplicate_source(SOURCE_KEY)
    assert second["kept"] == 4, f"重跑不应把已保留行判成重复: {second}"
    assert second["batch_id"] == first["batch_id"]
    assert second["receipt_id"] == first["receipt_id"]
    assert second["replayed"] is True
    assert len(_product_lines(svc, second["batch_id"])) == 4, "重送不得追加已确认的保留行"
    _assert_receipt_matches_product(svc, second)


def test_sampling_uses_verified_dedup_product_and_records_base():
    """采样读已核对成品，回执写明采样基数与产物身份；重跑复用同一份样本。"""
    dedup = DedupWindowService()
    dedup_receipt = dedup.deduplicate_source(SOURCE_KEY)
    sampling = SamplingRuleService()

    first = sampling.sample_source(SOURCE_KEY, dedup_receipt["batch_id"])
    assert first["base_count"] == dedup_receipt["kept"] == 4
    assert first["sampled"] == len(_product_lines(dedup, first["batch_id"], "sampled.jsonl"))
    assert first["base"]["dedup_receipt_id"] == dedup_receipt["receipt_id"]
    assert first["base"]["product_digest"] == dedup_receipt["product"]["digest"]
    assert first["retained_line_range"] == dedup_receipt["retained"]["line_range"]

    second = sampling.sample_source(SOURCE_KEY, dedup_receipt["batch_id"])
    assert second["replayed"] is True
    assert second["sample_digest"] == first["sample_digest"]
    assert second["sampled"] == first["sampled"]
    assert len(_product_lines(dedup, first["batch_id"], "sampled.jsonl")) == first["sampled"]


def test_disconnected_write_then_resend_commits_one_result():
    """写出成品但响应断开，重送相同输入只得到一个可核对结果。"""
    svc = DedupWindowService()
    _write_input(_base_lines() + ["2026-11-23T03:00:00 [svc] INFO event_id=5 msg=interrupted"])

    # 第一次请求：成品写出后连接断开，本次没有可交付回执。
    try:
        svc.deduplicate_source(SOURCE_KEY, interrupt_after_write=True)
        raise AssertionError("应模拟出一次断开的交付")
    except DedupDeliveryInterrupted as exc:
        batch_id = exc.result["batch_id"]
        assert exc.result["status"] == "product_written"
        assert exc.result["retained"]["count"] == 5
    before = (svc._batch_dir(SOURCE_KEY, batch_id) / "deduped.jsonl").read_bytes()
    assert svc.get_receipt(SOURCE_KEY, batch_id) is None

    # 第二次请求：重送相同输入，恢复到同一个批次，只产生一个可核对结果。
    resent = svc.deduplicate_source(SOURCE_KEY)
    assert resent["batch_id"] == batch_id
    assert resent["kept"] == 5
    assert resent["recovered"] is True
    assert (svc._batch_dir(SOURCE_KEY, batch_id) / "deduped.jsonl").read_bytes() == before
    _assert_receipt_matches_product(svc, resent)

    # 重送之后仍只有一份回执，且与最新成品一致。
    receipts = list(svc._batch_dir(SOURCE_KEY, batch_id).glob("receipt.json"))
    assert len(receipts) == 1
    assert svc.get_latest_receipt(SOURCE_KEY)["receipt_id"] == resent["receipt_id"]


def test_dropped_receipt_is_recovered_without_appending():
    """回执被丢弃（模拟写入中断）：仅凭成品 + manifest 恢复回执，保留行不重复。"""
    svc = DedupWindowService()
    first = svc.deduplicate_source(SOURCE_KEY)
    batch_id = first["batch_id"]
    product_path = svc._batch_dir(SOURCE_KEY, batch_id) / "deduped.jsonl"
    product_before = product_path.read_bytes()

    (svc._batch_dir(SOURCE_KEY, batch_id) / "receipt.json").unlink()
    assert svc.get_receipt(SOURCE_KEY, batch_id) is None

    recovered = svc.deduplicate_source(SOURCE_KEY)
    assert recovered["batch_id"] == batch_id
    assert recovered["kept"] == first["kept"]
    assert recovered["recovered"] is True
    assert recovered["receipt_id"] == first["receipt_id"], "恢复出的回执应与首次一致"
    assert product_path.read_bytes() == product_before
    _assert_receipt_matches_product(svc, recovered)

    # 采样仍能读到该批次的核对基数。
    sample = SamplingRuleService().sample_source(SOURCE_KEY, batch_id)
    assert sample["base_count"] == first["kept"]


def test_new_batch_gets_its_own_retained_set_without_touching_old():
    """提交新批时重新计算保留集，旧批次成品与回执不被替换。"""
    svc = DedupWindowService()
    old = svc.deduplicate_source(SOURCE_KEY)
    old_product = (svc._batch_dir(SOURCE_KEY, old["batch_id"]) / "deduped.jsonl").read_bytes()
    old_receipt = svc.get_receipt(SOURCE_KEY, old["batch_id"])

    _write_input(_base_lines() + [
        "2026-11-23T01:00:00 [svc] INFO event_id=9 msg=new",
        "2026-11-23T01:00:01 [svc] INFO event_id=9 msg=new",
    ])
    new = svc.deduplicate_source(SOURCE_KEY)
    assert new["batch_id"] != old["batch_id"]
    assert new["total"] == 14
    # 前 4 行是新内容的首现；第 13、14 行时间戳不同，是两条不同的新日志。
    assert new["kept"] == 6, new
    assert new["retained"]["line_nos"] == [1, 2, 3, 4, 13, 14]
    assert new["replayed"] is False
    _assert_receipt_matches_product(svc, new)

    # 旧批次原样保留，用旧批次号仍能取回原成品与回执。
    assert (svc._batch_dir(SOURCE_KEY, old["batch_id"]) / "deduped.jsonl").read_bytes() == old_product
    assert svc.get_receipt(SOURCE_KEY, old["batch_id"])["receipt_id"] == old_receipt["receipt_id"]
    assert svc.get_latest_receipt(SOURCE_KEY)["batch_id"] == new["batch_id"]

    # 采样可分别按两个批次的基数取数。
    sampling = SamplingRuleService()
    old_sample = sampling.sample_source(SOURCE_KEY, old["batch_id"])
    assert old_sample["base_count"] == old["kept"]
    new_sample = sampling.sample_source(SOURCE_KEY, new["batch_id"])
    assert new_sample["base_count"] == new["kept"]

    # 重送旧批次不会把最新指针改回旧输入。
    replay_old = svc.deduplicate_source(SOURCE_KEY)
    assert replay_old["batch_id"] == new["batch_id"]
    assert svc.get_latest_receipt(SOURCE_KEY)["batch_id"] == new["batch_id"]


def test_stale_resend_does_not_replace_newer_batch_on_disk():
    """中断的旧批次晚到的重送，不能把磁盘上的最新成品换成旧输入。"""
    svc = DedupWindowService()
    dedup = DedupWindowService()

    # 批次 A：正常交付。
    _write_input(_base_lines())
    batch_a = dedup.deduplicate_source(SOURCE_KEY)
    # 批次 B：写出成品但响应断开。
    _write_input(_base_lines() + ["2026-11-23T04:00:00 [svc] INFO event_id=8 msg=second"])
    try:
        dedup.deduplicate_source(SOURCE_KEY, interrupt_after_write=True)
        raise AssertionError("应模拟出一次断开的交付")
    except DedupDeliveryInterrupted as exc:
        batch_b = exc.result["batch_id"]
    # 批次 C：新输入正常交付，成为最新。
    _write_input(_base_lines() + [
        "2026-11-23T04:00:00 [svc] INFO event_id=8 msg=second",
        "2026-11-23T05:00:00 [svc] INFO event_id=9 msg=third",
    ])
    batch_c = dedup.deduplicate_source(SOURCE_KEY)
    assert batch_c["batch_id"] not in (batch_a["batch_id"], batch_b)

    # 现在补送旧批次 B：可以交付 B 自己的结果，但最新指针/镜像保持 C。
    replay_b = dedup.deduplicate_source(SOURCE_KEY, batch_b)
    assert replay_b.get("error") is None, replay_b
    assert replay_b["batch_id"] == batch_b
    assert dedup.get_latest_receipt(SOURCE_KEY)["batch_id"] == batch_c["batch_id"]

    pointer = dedup._read_json(dedup._latest_pointer(SOURCE_KEY))
    mirror = dedup._data_dir("logs") / f"{SOURCE_KEY}_deduped.jsonl"
    assert pointer["batch_id"] == batch_c["batch_id"]
    assert mirror.read_bytes() == (
        dedup._batch_dir(SOURCE_KEY, batch_c["batch_id"]) / "deduped.jsonl"
    ).read_bytes()
    _assert_receipt_matches_product(dedup, replay_b)


def test_unrecorded_batch_recovery_never_mixes_in_new_input():
    """未确认批次恢复时只认自己的成品，不会把变化后的当前输入混进来。"""
    svc = DedupWindowService()
    first = svc.deduplicate_source(SOURCE_KEY)
    batch_id = first["batch_id"]
    product_before = (svc._batch_dir(SOURCE_KEY, batch_id) / "deduped.jsonl").read_bytes()
    (svc._batch_dir(SOURCE_KEY, batch_id) / "receipt.json").unlink()

    _write_input(_base_lines() + ["2026-11-23T02:00:00 [svc] INFO event_id=7 msg=changed"])
    recovered = svc.deduplicate_source(SOURCE_KEY, batch_id)
    assert recovered["batch_id"] == batch_id
    assert recovered["input"]["digest"] == first["input"]["digest"], "回执应记录该批次原有输入"
    assert recovered["input"]["count"] == first["input"]["count"]
    assert (svc._batch_dir(SOURCE_KEY, batch_id) / "deduped.jsonl").read_bytes() == product_before
    _assert_receipt_matches_product(svc, recovered)


def test_sampling_refuses_when_receipt_and_product_disagree():
    """成品被改动后回执核对不通过，采样拒绝在不可核对的数据上产出样本。"""
    svc = DedupWindowService()
    receipt = svc.deduplicate_source(SOURCE_KEY)
    product = svc._batch_dir(SOURCE_KEY, receipt["batch_id"]) / "deduped.jsonl"
    product.write_text(product.read_text() + '{"tampered": true}\n', encoding="utf-8")

    result = SamplingRuleService().sample_source(SOURCE_KEY, receipt["batch_id"])
    assert "error" in result, result
    assert result["sampled"] == 0
