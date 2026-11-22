"""数据归并同键行修复与调用审计逐笔核对测试。

场景（与值班恢复流程对应）：
1. 先让从源使用另一个订单号，核对主源货值 8 和 9 两行都出现在结果里
   （同侧同键不再互相覆盖，来源身份独立）。
2. 再增加从源同键行，在归并结果写出而审计尚未确认时重启，随后再次提交，
   比对原行去向、审计计数以及既有主从冲突策略的结果。
3. 进程重启后只认已确认的审计结果，任何行重新提交都不能少记或记两遍。
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

temp_data = tempfile.mkdtemp(prefix='cc10026_merge_')
os.environ['DATA_ROOT'] = temp_data

ORDER_NO = "订单号"
AMOUNT = "货值"


def _make_rule(rule_id: int = 100, conflict_policy: str = "主源优先",
               dedup_policy: str = "全保留"):
    """在内存仓库登记一条已生效的归并规则，归并键为订单号。"""
    from app.store import store
    rows = store.rows("data_merge")
    rule = {
        "id": rule_id,
        "status": "已生效",
        "pending": False,
        "abnormal": False,
        "规则编号": f"DATA-TEST-{rule_id}",
        "主API": "订单主源API",
        "从API": "订单从源API",
        "归并键": ORDER_NO,
        "冲突策略": conflict_policy,
        "去重策略": dedup_policy,
        "结果排序": "",
        "规则版本": "v1",
    }
    rows.append(rule)
    return rule_id


class TestSameKeyMergeAndAudit:

    def setup_method(self):
        from app.seed import SEED_ROWS
        from app.store import store as s
        import shutil
        s._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
        if os.path.exists(temp_data):
            shutil.rmtree(temp_data, ignore_errors=True)

    # --------------------------------------------------------
    # 场景一：从源使用另一个订单号，8 和 9 必须都保留
    # --------------------------------------------------------

    def test_same_side_same_key_rows_are_both_kept(self):
        """主源同一订单号货值8和9两行，全保留时不得覆盖，逐行可核对。"""
        from app.services.data_merge import DataMergeService
        rule_id = _make_rule()
        svc = DataMergeService()

        source_a = [
            {ORDER_NO: "ORD-1", AMOUNT: 8},
            {ORDER_NO: "ORD-1", AMOUNT: 9},
        ]
        # 从源先用另一个订单号，不触发冲突
        source_b = [{ORDER_NO: "ORD-2", AMOUNT: 7}]

        result = svc.merge_sources(rule_id, source_a, source_b)

        amounts = sorted(r[AMOUNT] for r in result if r.get(ORDER_NO) == "ORD-1")
        assert amounts == [8, 9], f"主源同键两行必须都保留，实际: {amounts}"
        # 每行保留独立的数据源身份和行位置
        primary_rows = [r for r in result if r["_source"] == "primary"]
        assert {r["_line_uid"] for r in primary_rows} == {"primary:0", "primary:1"}
        assert any(r[ORDER_NO] == "ORD-2" and r["_source"] == "secondary" for r in result)

    def test_merge_plan_records_every_source_line(self):
        """归并明细逐行登记来源与行位置，行数与输入一致。"""
        from app.services.data_merge import (DataMergeService, OUTCOME_PRIMARY_KEPT,
                                             OUTCOME_SECONDARY_KEPT)
        rule_id = _make_rule()
        svc = DataMergeService()
        source_a = [{ORDER_NO: "ORD-1", AMOUNT: 8}, {ORDER_NO: "ORD-1", AMOUNT: 9}]
        source_b = [{ORDER_NO: "ORD-2", AMOUNT: 7}]

        built = svc.build_merge_plan(rule_id, source_a, source_b)
        lines = built["plan"]["lines"]

        assert len(lines) == 3
        assert [l["source_position"] for l in lines if l["source"] == "primary"] == [0, 1]
        assert {l["outcome"] for l in lines} == {OUTCOME_PRIMARY_KEPT, OUTCOME_SECONDARY_KEPT}
        # 归并明细与结果行可通过 result_index 互相定位
        for line in lines:
            assert line["result_index"] is not None
            assert built["rows"][line["result_index"]]["_line_uid"] == line["line_uid"]

    # --------------------------------------------------------
    # 场景二：从源同键 + 结果已写出但审计未确认时重启
    # --------------------------------------------------------

    def test_crash_between_result_and_audit_then_resubmit(self):
        """归并结果写出、审计未确认时重启：再次提交不重记不漏记。"""
        from app.services.data_merge import DataMergeService
        from app.services.call_audit import CallAuditService
        from app.services.data_manager import read_merge_batch, list_audit
        rule_id = _make_rule(conflict_policy="主源优先")
        svc = DataMergeService()
        audit_svc = CallAuditService()

        source_a = [{ORDER_NO: "ORD-1", AMOUNT: 8}, {ORDER_NO: "ORD-1", AMOUNT: 9}]
        # 从源稍后交来同号记录，触发真实主从冲突
        source_b = [{ORDER_NO: "ORD-1", AMOUNT: 10}]

        batch_id = "BATCH-CRASH-1"
        crashed = svc.merge_and_audit(rule_id, source_a, source_b,
                                      batch_id=batch_id, crash_before_audit=True)

        # 崩溃点：归并结果与明细已持久化，但没有任何审计落盘
        on_disk = read_merge_batch(batch_id)
        assert on_disk["status"] == "待确认"
        assert list_audit(caller="data_merge") == []
        assert crashed["audit_count"] == 0
        # 主源 8 和 9 在已写出的结果里仍然都在
        assert sorted(r[AMOUNT] for r in crashed["rows"]
                      if r[ORDER_NO] == "ORD-1" and r["_source"] == "primary") == [8, 9]

        # 值班人员重启后重新提交同一批次：应补齐审计并确认，不重记
        recovered = svc.merge_and_audit(rule_id, source_a, source_b, batch_id=batch_id)

        assert recovered["status"] == "已确认"
        # 3 条来源行 -> 3 条逐行审计，逐笔对齐
        assert recovered["line_count"] == 3
        assert recovered["audit_count"] == 3
        assert recovered["reconciled"] is True

        # 主源优先策略：从源同键行被跳过，主源 8/9 维持原去向
        outcomes = {l["line_uid"]: l["outcome"] for l in recovered["lines"]}
        assert outcomes["secondary:0"].endswith("secondary_skipped")
        primary_amounts = sorted(
            r[AMOUNT] for r in recovered["rows"] if r["_source"] == "primary")
        assert primary_amounts == [8, 9]

        # 再次重复提交：已确认批次只认原审计，审计数量保持 3，绝不记两遍
        again = svc.merge_and_audit(rule_id, source_a, source_b, batch_id=batch_id)
        assert again["audit_count"] == 3
        assert again.get("reused") is True
        assert len(list_audit(caller="data_merge")) == 3

        # 调用审计工作台能按原批次打开并核对计数
        summary = audit_svc.batch_audit_summary(batch_id)
        assert summary["total_audit"] == 3
        assert summary["confirmed_count"] == 3
        assert summary["pending_count"] == 0
        recon = audit_svc.reconcile_merge_batch(batch_id)
        assert recon["reconciled"] is True
        assert recon["missing_audit"] == [] and recon["extra_audit"] == []

    def test_recover_batch_after_crash_before_confirm(self):
        """审计已逐行登记但未确认时重启：recover_batch 补齐确认且不重复登记。"""
        from app.services.data_merge import DataMergeService
        from app.services.data_manager import list_audit
        rule_id = _make_rule()
        svc = DataMergeService()
        source_a = [{ORDER_NO: "ORD-1", AMOUNT: 8}, {ORDER_NO: "ORD-1", AMOUNT: 9}]
        source_b = [{ORDER_NO: "ORD-3", AMOUNT: 6}]

        batch_id = "BATCH-CRASH-2"
        svc.merge_and_audit(rule_id, source_a, source_b,
                            batch_id=batch_id, crash_before_confirm=True)
        before = list_audit(caller="data_merge")
        assert len(before) == 3
        assert all(not r.get("confirmed") for r in before)

        recovered = svc.recover_batch(batch_id)
        after = list_audit(caller="data_merge")
        assert len(after) == 3, "恢复处理不得重复登记审计"
        assert all(r.get("confirmed") for r in after)
        assert recovered["status"] == "已确认"
        assert recovered["reconciled"] is True

    # --------------------------------------------------------
    # 主从冲突策略：从源覆盖 / 合并字段 在同键多行下仍按原策略
    # --------------------------------------------------------

    def test_secondary_override_policy_with_duplicate_primary_key(self):
        """从源覆盖：同键主源行登记为被覆盖，从源行进入结果，逐行可追溯。"""
        from app.services.data_merge import (DataMergeService, OUTCOME_PRIMARY_REPLACED,
                                             OUTCOME_SECONDARY_REPLACED)
        rule_id = _make_rule(conflict_policy="从源覆盖")
        svc = DataMergeService()
        source_a = [{ORDER_NO: "ORD-1", AMOUNT: 8}, {ORDER_NO: "ORD-1", AMOUNT: 9}]
        source_b = [{ORDER_NO: "ORD-1", AMOUNT: 10}]

        view = svc.merge_and_audit(rule_id, source_a, source_b, batch_id="BATCH-OVERRIDE")

        outcomes = {l["line_uid"]: l["outcome"] for l in view["lines"]}
        assert outcomes["primary:0"] == OUTCOME_PRIMARY_REPLACED
        assert outcomes["primary:1"] == OUTCOME_PRIMARY_REPLACED
        assert outcomes["secondary:0"] == OUTCOME_SECONDARY_REPLACED
        # 结果中 ORD-1 只有从源那一行，货值为10
        ord1_rows = [r for r in view["rows"] if r[ORDER_NO] == "ORD-1"]
        assert len(ord1_rows) == 1
        assert ord1_rows[0][AMOUNT] == 10 and ord1_rows[0]["_source"] == "secondary"
        # 被覆盖的主源行在审计里仍有独立记录，原始货值不丢
        audit_svc = __import__("app.services.call_audit", fromlist=["CallAuditService"]).CallAuditService()
        records = audit_svc.list_merge_audits("BATCH-OVERRIDE")
        assert len(records) == 3
        assert {r["line_uid"] for r in records} == {"primary:0", "primary:1", "secondary:0"}

    def test_field_merge_policy_keeps_every_primary_line(self):
        """合并字段：主源同键两行各自独立，从源记录并入首个主源行。"""
        from app.services.data_merge import (DataMergeService, OUTCOME_PRIMARY_FIELD_MERGED,
                                             OUTCOME_SECONDARY_FIELD_MERGED)
        rule_id = _make_rule(conflict_policy="合并字段")
        svc = DataMergeService()
        source_a = [{ORDER_NO: "ORD-1", AMOUNT: 8}, {ORDER_NO: "ORD-1", AMOUNT: 9}]
        source_b = [{ORDER_NO: "ORD-1", AMOUNT: 10}]

        view = svc.merge_and_audit(rule_id, source_a, source_b, batch_id="BATCH-MERGE-FIELDS")

        outcomes = {l["line_uid"]: l["outcome"] for l in view["lines"]}
        assert outcomes["secondary:0"] == OUTCOME_SECONDARY_FIELD_MERGED
        # 两条主源行都还在，其中首条吸收了从源记录
        primary = [r for r in view["rows"] if r["_source"] == "primary"]
        assert sorted(r[AMOUNT] for r in primary) == [8, 9]
        merged_primary = next(r for r in primary if r.get("from_secondary"))
        assert merged_primary["from_secondary"][0][AMOUNT] == 10
        assert outcomes["primary:0"] == OUTCOME_PRIMARY_FIELD_MERGED
        # 从源明细行指向承接它的主源结果行
        sec_line = next(l for l in view["lines"] if l["line_uid"] == "secondary:0")
        assert sec_line["result_index"] is not None
        assert view["rows"][sec_line["result_index"]]["_source"] == "primary"

    # --------------------------------------------------------
    # 重启语义与幂等
    # --------------------------------------------------------

    def test_resubmit_with_changed_rows_is_rejected(self):
        """未确认批次重新提交但原始行变了：拒绝自动恢复，防止错账。"""
        import pytest
        from app.services.data_merge import DataMergeService
        rule_id = _make_rule()
        svc = DataMergeService()
        source_a = [{ORDER_NO: "ORD-1", AMOUNT: 8}, {ORDER_NO: "ORD-1", AMOUNT: 9}]
        source_b = [{ORDER_NO: "ORD-2", AMOUNT: 7}]

        svc.merge_and_audit(rule_id, source_a, source_b,
                            batch_id="BATCH-DIFF", crash_before_audit=True)

        changed_b = [{ORDER_NO: "ORD-2", AMOUNT: 77}]
        with pytest.raises(ValueError):
            svc.merge_and_audit(rule_id, source_a, changed_b, batch_id="BATCH-DIFF")

    def test_audit_workbench_hydrates_after_restart(self):
        """进程重启后工作台只凭磁盘持久记录即可恢复审计计数。"""
        from app.services.call_audit import CallAuditService
        from app.services.data_merge import DataMergeService
        rule_id = _make_rule()
        svc = DataMergeService()
        source_a = [{ORDER_NO: "ORD-1", AMOUNT: 8}, {ORDER_NO: "ORD-1", AMOUNT: 9}]
        source_b = [{ORDER_NO: "ORD-4", AMOUNT: 5}]
        svc.merge_and_audit(rule_id, source_a, source_b, batch_id="BATCH-RESTART")

        # 模拟重启：重置内存仓库（种子数据），磁盘 data/ 保留
        from app.seed import SEED_ROWS
        from app.store import store as s
        s._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}

        audit_svc = CallAuditService()
        items, total = audit_svc.list_entries(page=1, size=200)
        merge_items = [r for r in items if r.get("batch_id") == "BATCH-RESTART"]
        assert len(merge_items) == 3
        assert all(r["status"] == "已校验" for r in merge_items)

        snapshot = audit_svc.domain_snapshot()
        assert snapshot["merge_audit_count"] == 3
        assert snapshot["merge_confirmed_count"] == 3

        # 值班视角：列出批次并辨认状态
        batches = svc.list_merge_batches()
        assert any(b["batch_id"] == "BATCH-RESTART" and b["status"] == "已确认" for b in batches)

    def test_dedup_policy_does_not_swallow_primary_lines_silently(self):
        """按键去重时被去掉的行也在归并明细与审计中登记原因，可逐笔核对。"""
        from app.services.data_merge import DataMergeService, OUTCOME_DEDUP_DROPPED
        rule_id = _make_rule(dedup_policy="按键去重")
        svc = DataMergeService()
        source_a = [{ORDER_NO: "ORD-1", AMOUNT: 8}, {ORDER_NO: "ORD-1", AMOUNT: 9}]
        source_b = []

        view = svc.merge_and_audit(rule_id, source_a, source_b, batch_id="BATCH-DEDUP")

        dropped = [l for l in view["lines"] if l["outcome"] == OUTCOME_DEDUP_DROPPED]
        assert len(dropped) == 1
        assert dropped[0]["line_uid"] == "primary:1"
        # 结果只剩一行，但审计仍为两行（8被保留、9登记为去重丢弃）
        assert len(view["rows"]) == 1
        assert view["audit_count"] == 2
        assert view["line_count"] == 2
        assert view["reconciled"] is True
