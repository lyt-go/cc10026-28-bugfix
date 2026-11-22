"""数据归并领域服务：管理多API数据源的归并、冲突处理和去重策略。

跨模块数据流：
- 消费 api_registry 快照 -> 归并源API核验
- 消费 data_filter 快照 -> 各源数据归并前先过滤
- 被 export_task 消费 -> 归并后数据导出
- 归并结果写入共享快照

可靠性约定（重启安全）：
- 同侧（同数据源）同键的每一行都是独立来源行，全保留，不允许互相覆盖
- 主从冲突仍按规则上配置的业务策略处理（主源优先/从源覆盖/合并字段）
- 每次归并生成一个持久化归并批次，逐行登记来源、行位置、处理结果与原因
- 调用审计按批次逐行登记处理结果；重启后只认已确认的审计结果
- 重复提交同一批次严格幂等：任何行都不会少记或记两遍
"""
from __future__ import annotations

import copy
import hashlib
import json
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 每行处理结果的标准取值，归并明细与调用审计共用同一套口径。
OUTCOME_PRIMARY_KEPT = "primary_kept"
OUTCOME_SECONDARY_KEPT = "secondary_kept"
OUTCOME_PRIMARY_CONFLICT_KEPT = "primary_conflict_kept"
OUTCOME_SECONDARY_SKIPPED = "secondary_skipped"
OUTCOME_PRIMARY_REPLACED = "primary_replaced"
OUTCOME_SECONDARY_REPLACED = "secondary_replaced"
OUTCOME_PRIMARY_FIELD_MERGED = "primary_field_merged"
OUTCOME_SECONDARY_FIELD_MERGED = "secondary_field_merged"
OUTCOME_DEDUP_DROPPED = "dedup_dropped"

BATCH_PENDING = "待确认"
BATCH_CONFIRMED = "已确认"


def line_uid(source: str, position: int) -> str:
    """来源行的稳定身份：数据源 + 原始行位置。"""
    return f"{source}:{position}"


def line_fingerprint(row: dict[str, Any]) -> str:
    """行内容指纹，用于重新提交时识别同一批原始行。"""
    payload = json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


class DataMergeService(BaseModuleService):
    MODULE = "data_merge"
    ENTITY = "归并规则"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["规则编号", "主API", "从API", "归并键", "冲突策略", "去重策略", "结果排序", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "主API", "从API"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    # ========================================================
    # 归并规划：逐行保留来源身份与处理结果
    # ========================================================

    def build_merge_plan(self, rule_id: int, source_a_rows: list[dict[str, Any]],
                         source_b_rows: list[dict[str, Any]]) -> dict[str, Any]:
        """构建逐行归并计划。

        计划中的每一条 line 都对应一条真实来源行，记录来源、原始行位置、
        处理结果（outcome）与原因（reason），保证可与调用审计逐笔核对。
        """
        rule = store.find(self.MODULE, rule_id)
        active = rule is not None and rule.get("status") == "已生效"
        merge_key = rule.get("归并键", "id") if rule else "id"
        conflict_policy = rule.get("冲突策略", "主源优先") if rule else "主源优先"
        dedup_policy = rule.get("去重策略", "全保留") if rule else "全保留"
        sort_field = rule.get("结果排序", "") if rule else ""

        lines: list[dict[str, Any]] = []
        primary_by_key: dict[str, list[int]] = {}
        working_rows: list[dict[str, Any]] = []

        # ---- 主源：同键的每一行独立保留，不互相覆盖 ----
        for position, raw_row in enumerate(source_a_rows):
            key = str(raw_row.get(merge_key, ""))
            row = {"_source": "primary", "_line_uid": line_uid("primary", position),
                   **copy.deepcopy(raw_row)}
            idx = len(working_rows)
            working_rows.append(row)
            primary_by_key.setdefault(key, []).append(idx)
            lines.append({
                "line_uid": line_uid("primary", position),
                "source": "primary",
                "source_position": position,
                "merge_key": key,
                "fingerprint": line_fingerprint(raw_row),
                "outcome": OUTCOME_PRIMARY_KEPT,
                "reason": "主源合法行独立保留",
                "result_index": idx,
            })

        # ---- 从源：仅在与主源发生真实冲突时套用业务冲突策略 ----
        for position, raw_row in enumerate(source_b_rows):
            key = str(raw_row.get(merge_key, ""))
            uid = line_uid("secondary", position)
            row = {"_source": "secondary", "_line_uid": uid, **copy.deepcopy(raw_row)}
            primary_indexes = primary_by_key.get(key, [])

            if not active:
                # 规则缺失或未生效：不做冲突裁决，两侧来源行原样保留
                idx = len(working_rows)
                working_rows.append(row)
                lines.append(self._line(uid, "secondary", position, key, raw_row,
                                        OUTCOME_SECONDARY_KEPT,
                                        "归并规则未生效，来源行原样保留", idx))
                continue

            if not primary_indexes:
                idx = len(working_rows)
                working_rows.append(row)
                lines.append(self._line(uid, "secondary", position, key, raw_row,
                                        OUTCOME_SECONDARY_KEPT,
                                        "从源独有键，直接保留", idx))
                continue

            first_idx = primary_indexes[0]
            if conflict_policy == "主源优先":
                lines.append(self._line(uid, "secondary", position, key, raw_row,
                                        OUTCOME_SECONDARY_SKIPPED,
                                        "主源优先：与主源同键，从源行不进入归并结果", None))
            elif conflict_policy == "从源覆盖":
                # 同键的全部主源行登记为被覆盖；从源行替代首个主源行的位置，
                # 其余同键主源行从归并结果中移除（审计明细仍逐行保留）
                keep_idx = primary_indexes[0]
                for order, p_idx in enumerate(primary_indexes):
                    p_uid = working_rows[p_idx]["_line_uid"]
                    p_line = lines[self._line_index(lines, p_uid)]
                    p_line["outcome"] = OUTCOME_PRIMARY_REPLACED
                    p_line["reason"] = "从源覆盖：主源同键行被从源记录替代"
                    p_line["result_index"] = None
                    if order > 0:
                        working_rows[p_idx] = None
                working_rows[keep_idx] = row
                lines.append(self._line(uid, "secondary", position, key, raw_row,
                                        OUTCOME_SECONDARY_REPLACED,
                                        "从源覆盖：从源记录替代主源同键行", keep_idx))
            elif conflict_policy == "合并字段":
                # 合并到首个主源行；同键的其余主源行仍独立保留
                merged = working_rows[first_idx]
                merged.setdefault("from_secondary", []).append(copy.deepcopy(raw_row))
                p_uid = merged["_line_uid"]
                p_line = lines[self._line_index(lines, p_uid)]
                p_line["outcome"] = OUTCOME_PRIMARY_FIELD_MERGED
                p_line["reason"] = "合并字段：主源行吸收从源同键记录"
                lines.append(self._line(uid, "secondary", position, key, raw_row,
                                        OUTCOME_SECONDARY_FIELD_MERGED,
                                        "合并字段：从源同键记录并入主源行",
                                        p_line["result_index"]))
            else:
                # 未知策略按主源优先兜底，避免静默丢行
                lines.append(self._line(uid, "secondary", position, key, raw_row,
                                        OUTCOME_SECONDARY_SKIPPED,
                                        f"未知冲突策略「{conflict_policy}」按主源优先处理", None))

        # ---- 去重：仅作用于显式选择按键去重的场景，全保留时不丢任何行 ----
        result_rows = [row for row in working_rows if row is not None]
        if active and dedup_policy == "按键去重":
            seen: set[str] = set()
            deduped: list[dict[str, Any]] = []
            for row in result_rows:
                key = str(row.get(merge_key, ""))
                if key in seen:
                    p_line = lines[self._line_index(lines, row["_line_uid"])]
                    p_line["outcome"] = OUTCOME_DEDUP_DROPPED
                    p_line["reason"] = "按键去重：同键的后续来源行被去重"
                    p_line["result_index"] = None
                    continue
                seen.add(key)
                deduped.append(row)
            result_rows = deduped

        # ---- 排序 ----
        if active and sort_field:
            result_rows = sorted(result_rows, key=lambda r: str(r.get(sort_field, "")))

        # 以最终结果顺序回填 result_index，保证归并明细与结果行可互相定位
        uid_to_index = {row["_line_uid"]: i for i, row in enumerate(result_rows)}
        for line in lines:
            uid = line["line_uid"]
            if uid in uid_to_index:
                line["result_index"] = uid_to_index[uid]
            elif line["outcome"] == OUTCOME_SECONDARY_FIELD_MERGED:
                # 从源行被并入主源行，定位到承接它的主源结果行
                host = next((l for l in lines
                             if l["source"] == "primary"
                             and l["merge_key"] == line["merge_key"]
                             and l["result_index"] is not None), None)
                line["result_index"] = host["result_index"] if host else None
            else:
                line["result_index"] = None

        plan = {
            "rule_id": rule_id,
            "rule_no": rule.get("规则编号", "") if rule else "",
            "primary_api": rule.get("主API", "") if rule else "",
            "secondary_api": rule.get("从API", "") if rule else "",
            "merge_key": merge_key,
            "conflict_policy": conflict_policy,
            "dedup_policy": dedup_policy,
            "sort_field": sort_field,
            "active": active,
            "primary_count": len(source_a_rows),
            "secondary_count": len(source_b_rows),
            "lines": lines,
        }
        return {"plan": plan, "rows": result_rows}

    @staticmethod
    def _line(uid: str, source: str, position: int, key: str, raw_row: dict[str, Any],
              outcome: str, reason: str, result_index: int | None) -> dict[str, Any]:
        return {
            "line_uid": uid,
            "source": source,
            "source_position": position,
            "merge_key": key,
            "fingerprint": line_fingerprint(raw_row),
            "outcome": outcome,
            "reason": reason,
            "result_index": result_index,
        }

    @staticmethod
    def _line_index(lines: list[dict[str, Any]], uid: str) -> int:
        for i, line in enumerate(lines):
            if line["line_uid"] == uid:
                return i
        raise KeyError(uid)

    def merge_sources(self, rule_id: int, source_a_rows: list[dict[str, Any]],
                      source_b_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """按归并规则合并两个数据源（保留逐行来源身份，同侧同键不覆盖）。"""
        return self.build_merge_plan(rule_id, source_a_rows, source_b_rows)["rows"]

    # ========================================================
    # 归并 + 调用审计：持久化批次、逐行审计、重启幂等
    # ========================================================

    def merge_and_audit(self, rule_id: int, source_a_rows: list[dict[str, Any]],
                        source_b_rows: list[dict[str, Any]], *,
                        batch_id: str | None = None,
                        crash_before_audit: bool = False,
                        crash_before_confirm: bool = False) -> dict[str, Any]:
        """归并两个数据源并把逐行处理结果登记到调用审计。

        返回批次视图，包含 batch_id、归并明细 lines、归并结果 rows 与审计计数。

        崩溃注入：
        - crash_before_audit=True：归并结果已写出（批次已持久化）但尚未写审计，
          模拟服务停在“提交结果与写审计之间”，重启后走恢复路径。
        - crash_before_confirm=True：审计已逐行登记但批次尚未确认，
          模拟审计未确认时的重启。

        幂等：同一 batch_id 重新提交时，已确认批次直接返回原结果；待确认批次
        复用已持久化的归并计划，只补齐缺失的审计记录，任何行都不重记或漏记。
        """
        from app.services import data_manager as dm

        batch_id = batch_id or self._new_batch_id()
        existing = dm.read_merge_batch(batch_id)

        if existing and existing.get("status") == BATCH_CONFIRMED:
            # 进程重启后只认已确认的审计结果：原批次原样返回，绝不重算重记
            existing["reused"] = True
            return self._batch_view(existing)

        if existing:
            # 待确认批次：重新提交必须仍是同一批原始行，否则拒绝自动恢复
            current_fingerprint = self._batch_fingerprint(rule_id, source_a_rows, source_b_rows)
            if existing.get("fingerprint") != current_fingerprint:
                raise ValueError(
                    f"批次 {batch_id} 存在未确认记录，但重新提交的原始行与原批次不一致，"
                    "请先核对原批次后再处理")
            batch = existing
        else:
            built = self.build_merge_plan(rule_id, source_a_rows, source_b_rows)
            plan, merged = built["plan"], built["rows"]
            batch = {
                "batch_id": batch_id,
                "status": BATCH_PENDING,
                "fingerprint": self._batch_fingerprint(rule_id, source_a_rows, source_b_rows),
                **plan,
                "result_rows": merged,
                "merged_count": len(merged),
                "audit_registered": [],
            }
            # 先持久化归并明细与结果（提交结果），再写审计
            dm.write_merge_batch(batch_id, batch)

        if crash_before_audit:
            batch["crash_point"] = "before_audit"
            dm.write_merge_batch(batch_id, batch)
            return self._batch_view(batch)

        # 逐行登记调用审计；已登记的行跳过，保证重启补处理不重复
        registered = self._register_line_audits(batch_id, batch)
        batch["audit_registered"] = registered
        dm.write_merge_batch(batch_id, batch)

        if crash_before_confirm:
            batch["crash_point"] = "before_confirm"
            dm.write_merge_batch(batch_id, batch)
            return self._batch_view(batch)

        # 全部逐行审计落盘后才确认批次
        self.confirm_batch(batch_id)
        final_batch = dm.read_merge_batch(batch_id)
        return self._batch_view(final_batch)

    def _register_line_audits(self, batch_id: str, batch: dict[str, Any]) -> list[int]:
        """为批次中每条来源行登记一条调用审计，已登记的跳过。"""
        from app.services import data_manager as dm

        existing_audits = dm.list_audit(caller="data_merge")
        already = {int(a) for a in batch.get("audit_registered", [])}
        registered = list(batch.get("audit_registered", []))
        for line in batch["lines"]:
            uid = line["line_uid"]
            # 同批次同行已有审计则不重记（按 batch_id + line_uid 去重）
            duplicate = any(
                rec.get("batch_id") == batch_id and rec.get("line_uid") == uid
                for rec in existing_audits
            )
            if duplicate:
                continue
            audit_id = dm.next_audit_id()
            while audit_id in already:
                audit_id += 1
            already.add(audit_id)
            dm.write_audit(audit_id, {
                "audit_id": audit_id,
                "record_no": f"CALL-MERGE-{batch_id}-{uid}",
                "batch_id": batch_id,
                "line_uid": uid,
                "source": line["source"],
                "source_position": line["source_position"],
                "merge_key": line["merge_key"],
                "outcome": line["outcome"],
                "reason": line["reason"],
                "result_index": line["result_index"],
                "api_name": batch.get("primary_api", ""),
                "caller": "data_merge",
                "event": "merge_line_processed",
                "rule_no": batch.get("rule_no", ""),
                "status": "已记录",
            })
            registered.append(audit_id)
        return sorted(set(registered))

    def confirm_batch(self, batch_id: str) -> dict[str, Any]:
        """确认一个归并批次：其全部逐行审计均已登记，状态置为已确认。"""
        from app.services import data_manager as dm

        batch = dm.read_merge_batch(batch_id)
        if not batch:
            raise KeyError(f"归并批次 {batch_id} 不存在")
        audit_records = [
            rec for rec in dm.list_audit(caller="data_merge")
            if rec.get("batch_id") == batch_id
        ]
        line_uids = {line["line_uid"] for line in batch["lines"]}
        audited_uids = {rec.get("line_uid") for rec in audit_records}
        missing = line_uids - audited_uids
        if missing:
            raise ValueError(f"批次 {batch_id} 仍有 {len(missing)} 行未登记审计，不能确认")
        for rec in audit_records:
            rec["status"] = "已校验"
            rec["confirmed"] = True
            dm.write_audit(int(rec["audit_id"]), rec)
        batch["status"] = BATCH_CONFIRMED
        batch["confirmed_at"] = dm._now_iso()
        batch.pop("crash_point", None)
        dm.write_merge_batch(batch_id, batch)
        return batch

    def recover_batch(self, batch_id: str) -> dict[str, Any]:
        """值班人员按原批次号重新打开审计记录并恢复处理。

        - 已确认：直接返回，不产生任何新审计
        - 待确认：补齐缺失的逐行审计后确认，行数与审计计数逐笔对齐
        """
        from app.services import data_manager as dm

        batch = dm.read_merge_batch(batch_id)
        if not batch:
            raise KeyError(f"归并批次 {batch_id} 不存在")
        if batch.get("status") == BATCH_CONFIRMED:
            view = self._batch_view(batch)
            view["reused"] = True
            return view
        registered = self._register_line_audits(batch_id, batch)
        batch["audit_registered"] = registered
        batch.pop("crash_point", None)
        dm.write_merge_batch(batch_id, batch)
        self.confirm_batch(batch_id)
        return self._batch_view(dm.read_merge_batch(batch_id))

    def get_merge_batch(self, batch_id: str) -> dict[str, Any]:
        """读取持久化的归并处理记录，供辨认已确认/待补处理。"""
        from app.services import data_manager as dm
        batch = dm.read_merge_batch(batch_id)
        if not batch:
            raise KeyError(f"归并批次 {batch_id} 不存在")
        return self._batch_view(batch)

    def list_merge_batches(self) -> list[dict[str, Any]]:
        """列出全部归并批次及其确认状态。"""
        from app.services import data_manager as dm
        return [self._batch_view(b) for b in dm.list_merge_batches()]

    def _batch_view(self, batch: dict[str, Any]) -> dict[str, Any]:
        """对外批次视图：归并明细、结果行与可核对的审计计数。"""
        from app.services import data_manager as dm
        line_audits = [
            rec for rec in dm.list_audit(caller="data_merge")
            if rec.get("batch_id") == batch.get("batch_id")
        ]
        view = dict(batch)
        view["rows"] = [self._row_from_result(r) for r in batch.get("result_rows", [])]
        view["audit_count"] = len(line_audits)
        view["line_count"] = len(batch.get("lines", []))
        view["confirmed_audit_count"] = sum(1 for r in line_audits if r.get("confirmed"))
        view["reconciled"] = view["audit_count"] == view["line_count"]
        return view

    @staticmethod
    def _row_from_result(stored: dict[str, Any]) -> dict[str, Any]:
        return copy.deepcopy(stored)

    @staticmethod
    def _new_batch_id() -> str:
        from datetime import datetime
        stamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"MB-{stamp}-{uuid.uuid4().hex[:8]}"

    @staticmethod
    def _batch_fingerprint(rule_id: int, source_a_rows: list[dict[str, Any]],
                           source_b_rows: list[dict[str, Any]]) -> str:
        payload = json.dumps(
            {"rule_id": rule_id, "a": source_a_rows, "b": source_b_rows},
            ensure_ascii=False, sort_keys=True, default=str)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def merge_from_exports(self, rule_id: int) -> list[dict[str, Any]]:
        """从导出产物直接读取两个数据源并进行归并。

        跨模块增强：被 export_task 完成后调用，实现导出->归并->审计链路。
        """
        from app.services.data_manager import list_exports
        exports = list_exports()
        exports.sort(key=lambda x: x.get("_created_at", ""), reverse=True)
        source_a = exports[0] if len(exports) > 0 else {}
        source_b = exports[1] if len(exports) > 1 else {}

        rows_a = []
        rows_b = []
        cols_a = source_a.get("columns", [])
        cols_b = source_b.get("columns", [])
        for row_vals in source_a.get("rows", []):
            row = {col: val for col, val in zip(cols_a, row_vals)} if cols_a else {}
            rows_a.append(row)
        for row_vals in source_b.get("rows", []):
            row = {col: val for col, val in zip(cols_b, row_vals)} if cols_b else {}
            rows_b.append(row)

        return self.merge_and_audit(rule_id, rows_a, rows_b)["rows"]

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        if action == "标记生效":
            from app.services.data_manager import read_api_snapshot
            registry = read_api_snapshot()
            primary_api = entry.get("主API", "")
            secondary_api = entry.get("从API", "")
            api_names = {v.get("api_name") for v in (registry.values() if isinstance(registry, dict) else []) if isinstance(v, dict)}
            if primary_api not in api_names and secondary_api not in api_names:
                return None, "至少一个源API尚未注册"

        result = super().run_action(entry_id, action)
        from app.services.data_manager import write_module_snapshot
        write_module_snapshot(self.MODULE, self.domain_snapshot())
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        merge_list = []
        for row in rows:
            merge_list.append({
                "id": row.get("id"),
                "rule_no": row.get("规则编号"),
                "primary_api": row.get("主API"),
                "secondary_api": row.get("从API"),
                "merge_key": row.get("归并键"),
                "conflict_policy": row.get("冲突策略"),
                "dedup_policy": row.get("去重策略"),
                "sort": row.get("结果排序"),
                "version": row.get("规则版本"),
                "status": row.get("status"),
            })
        try:
            batches = self.list_merge_batches()
        except Exception:
            batches = []
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(merge_list),
            "merges": merge_list,
            "active_count": sum(1 for r in merge_list if r.get("status") == "已生效"),
            "merge_batch_count": len(batches),
            "pending_batch_count": sum(1 for b in batches if b.get("status") == BATCH_PENDING),
            "confirmed_batch_count": sum(1 for b in batches if b.get("status") == BATCH_CONFIRMED),
        }
