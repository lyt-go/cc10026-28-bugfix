"""调用审计领域服务：管理API调用的审计记录、归档和查询追溯。

跨模块数据流：
- 消费 usage_billing 快照 → 用量异常触发审计核查
- 消费 subscription 共享记录 → 审批通过的订阅记录审计
- 消费 export_task 快照 → 导出操作记录审计
- 审计记录定期归档 → 写入 data/audit/ 共享目录
- 被 error_classify 消费 → 异常调用触发错误归类
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CallAuditService(BaseModuleService):
    MODULE = "call_audit"
    ENTITY = "调用记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["记录编号", "调用方", "目标API", "请求参数", "响应码", "响应耗时", "返回行数", "时间戳"]
    REQUIRED_FIELDS = ["记录编号", "调用方", "目标API"]
    STATUS_ORDER = ["已记录", "已校验", "已归档", "已过期"]
    ACTION_RULES = {"确认校验": "已校验", "提交归档": "已归档", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = []

    def record_api_call(self, caller: str, api_name: str, params: str = "",
                        response_code: str = "200", latency_ms: float = 0.0,
                        result_rows: int = 0) -> dict[str, Any]:
        """记录一次API调用到审计日志。"""
        from app.services.data_manager import write_audit
        from app.store import store as s

        audit_rows = s.rows(self.MODULE)
        new_id = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
        record_no = f"CALL-{new_id:04d}"

        audit_data = {
            "audit_id": new_id,
            "record_no": record_no,
            "caller": caller,
            "api_name": api_name,
            "request_params": params,
            "response_code": response_code,
            "latency_ms": latency_ms,
            "result_rows": result_rows,
            "timestamp": self._now_iso(),
        }
        write_audit(new_id, audit_data)

        # 同时记录到内存
        entry = {
            "id": new_id,
            "记录编号": record_no,
            "调用方": caller,
            "目标API": api_name,
            "请求参数": params,
            "响应码": response_code,
            "响应耗时": str(latency_ms),
            "返回行数": str(result_rows),
            "时间戳": self._now_iso(),
            "status": "已记录",
            "pending": True,
            "abnormal": response_code != "200",
        }
        audit_rows.append(entry)
        return entry

    def _now_iso(self) -> str:
        from datetime import datetime, timedelta, timezone
        return datetime.now(timezone(timedelta(hours=8))).isoformat()

    def verify_audits(self, caller: str, api_name: str) -> list[dict[str, Any]]:
        """校验调用方对指定API的审计记录。"""
        from app.services.data_manager import list_audit, read_api_snapshot

        # 检查API是否已注册
        registry = read_api_snapshot()
        api_exists = any(
            isinstance(v, dict) and v.get("api_name") == api_name
            for v in (registry.values() if isinstance(registry, dict) else []))

        if not api_exists:
            return []

        audits = list_audit(api_name=api_name, caller=caller)
        # 校验订阅状态
        from app.services.data_manager import list_subscriptions
        subs = list_subscriptions()
        valid_sub = any(
            s.get("api_name") == api_name and s.get("applicant") == caller and s.get("approval_status") == "已通过"
            for s in subs
        )

        for audit in audits:
            audit["subscription_valid"] = valid_sub
            audit["verified"] = valid_sub
        return audits

    # ========================================================
    # 归并批次关联：把持久化处理记录与逐行审计打通
    # ========================================================

    def list_merge_audits(self, batch_id: str) -> list[dict[str, Any]]:
        """按原归并批次号调出关联的全部逐行审计记录。

        值班人员据此辨认哪些记录已确认（confirmed=True）、哪些仍待补处理。
        """
        from app.services.data_manager import list_audit
        return [
            rec for rec in list_audit(caller="data_merge")
            if rec.get("batch_id") == batch_id
        ]

    def batch_audit_summary(self, batch_id: str) -> dict[str, Any]:
        """汇总批次的审计计数，供与归并明细逐笔核对。"""
        records = self.list_merge_audits(batch_id)
        return {
            "batch_id": batch_id,
            "total_audit": len(records),
            "confirmed_count": sum(1 for r in records if r.get("confirmed")),
            "pending_count": sum(1 for r in records if not r.get("confirmed")),
            "by_outcome": self._tally(records, "outcome"),
            "by_source": self._tally(records, "source"),
        }

    @staticmethod
    def _tally(records: list[dict[str, Any]], field: str) -> dict[str, int]:
        result: dict[str, int] = {}
        for rec in records:
            key = str(rec.get(field, ""))
            result[key] = result.get(key, 0) + 1
        return result

    def reconcile_merge_batch(self, batch_id: str) -> dict[str, Any]:
        """核对归并批次的来源行数与审计登记数是否逐笔对齐。"""
        from app.services.data_manager import read_merge_batch
        batch = read_merge_batch(batch_id)
        if not batch:
            return {"batch_id": batch_id, "found": False, "reconciled": False}
        line_uids = {line["line_uid"] for line in batch.get("lines", [])}
        audit_uids = {rec.get("line_uid") for rec in self.list_merge_audits(batch_id)}
        return {
            "batch_id": batch_id,
            "found": True,
            "status": batch.get("status"),
            "line_count": len(line_uids),
            "audit_count": len(audit_uids),
            "missing_audit": sorted(line_uids - audit_uids),
            "extra_audit": sorted(audit_uids - line_uids),
            "reconciled": line_uids == audit_uids,
        }

    def _hydrate_from_disk(self) -> None:
        """把磁盘上的归并审计记录同步进内存表，进程重启后工作台仍可查询。

        磁盘 audit_id 是全局编号，可能超出内存表分页窗口；这里用内存表自己的
        连续 id 建档，同时保留 disk_audit_id 以便回溯持久化记录。
        """
        from app.services.data_manager import list_audit

        rows = store.rows(self.MODULE)
        known_disk_ids = {r.get("disk_audit_id") for r in rows if r.get("disk_audit_id") is not None}
        next_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        for rec in list_audit(caller="data_merge"):
            disk_audit_id = int(rec.get("audit_id", 0))
            if disk_audit_id in known_disk_ids:
                continue
            confirmed = bool(rec.get("confirmed"))
            rows.append({
                "id": next_id,
                "disk_audit_id": disk_audit_id,
                "记录编号": rec.get("record_no", f"CALL-{disk_audit_id:04d}"),
                "调用方": rec.get("caller", "data_merge"),
                "目标API": rec.get("api_name", ""),
                "请求参数": f"batch={rec.get('batch_id', '')};line={rec.get('line_uid', '')}",
                "响应码": "200",
                "响应耗时": "0",
                "返回行数": str(1 if rec.get("result_index") is not None else 0),
                "时间戳": rec.get("_logged_at", ""),
                "status": "已校验" if confirmed else "已记录",
                "pending": not confirmed,
                "abnormal": False,
                "batch_id": rec.get("batch_id"),
                "line_uid": rec.get("line_uid"),
                "outcome": rec.get("outcome"),
                "reason": rec.get("reason"),
            })
            next_id += 1
            known_disk_ids.add(disk_audit_id)

    def list_entries(self, *args: Any, **kwargs: Any) -> tuple[list[dict[str, Any]], int]:
        """查询前先把持久化审计补齐到内存，保证重启后计数与磁盘一致。"""
        self._hydrate_from_disk()
        return super().list_entries(*args, **kwargs)

    def archive_old_records(self, before_date: str) -> dict[str, Any]:
        """归档指定日期之前的审计记录。"""
        from app.services.data_manager import archive_audit_records
        return archive_audit_records(before_date)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "提交归档":
            self.archive_old_records(self._today_str())

        result = super().run_action(entry_id, action)
        from app.services.data_manager import write_module_snapshot
        write_module_snapshot(self.MODULE, self.domain_snapshot())
        return result

    def _today_str(self) -> str:
        from datetime import datetime, timedelta, timezone
        return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")

    def domain_snapshot(self) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        audit_list = []
        for row in rows:
            audit_list.append({
                "id": row.get("id"),
                "record_no": row.get("记录编号"),
                "caller": row.get("调用方"),
                "api": row.get("目标API"),
                "params": row.get("请求参数"),
                "response_code": row.get("响应码"),
                "latency": row.get("响应耗时"),
                "result_rows": row.get("返回行数"),
                "timestamp": row.get("时间戳"),
                "status": row.get("status"),
            })
        # 归并逐行审计以磁盘持久记录为准，重启后计数仍与归并明细一致
        from app.services.data_manager import list_audit
        merge_audits = list_audit(caller="data_merge")
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(audit_list),
            "audits": audit_list,
            "archived_count": sum(1 for r in audit_list if r.get("status") == "已归档"),
            "abnormal_count": sum(1 for r in rows if r.get("abnormal")),
            "merge_audit_count": len(merge_audits),
            "merge_confirmed_count": sum(1 for r in merge_audits if r.get("confirmed")),
            "merge_pending_count": sum(1 for r in merge_audits if not r.get("confirmed")),
        }