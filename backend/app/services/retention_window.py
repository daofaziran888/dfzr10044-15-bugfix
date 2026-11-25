"""保留窗口领域服务：保留粒度、保留周期、到期回收与审计标记。

到期回收链修复要点：
- 回收前按湖表+分区读取审计解除记录，仍在审核的分区一律拦截
- 回收提交幂等：同一 round_id 只保留一份释放记录，回执丢失可安全重提
- 存储侧按已删除文件逐项确认，回收预算只累计实际确认的字节
- 解除审计/重新保留申请与回收提交做冲突检测，读取侧可核对资格与回执
"""
from __future__ import annotations

import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.lake_store import lake_store, PartitionFile


class RetentionWindowService(BaseModuleService):
    MODULE = "retention_window"
    ENTITY = "保留窗口"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["窗口编号", "所属湖表", "保留粒度", "保留周期", "到期分区数", "受审计标记", "回收字节数", "最近回收"]
    REQUIRED_FIELDS = ["窗口编号", "所属湖表", "保留粒度"]
    STATUS_ORDER = ["生效中", "即将到期", "已回收", "已暂停"]
    ACTION_RULES = {"确认生效": "即将到期", "启动回收": "已回收", "暂停窗口": "已暂停"}
    NEGATIVE_ACTIONS = []

    # ---- 窗口定义与到期识别 ----

    def define_retention_window(self, lake_table: str, granularity: str,
                                retention_days: int, audit_tagged: bool = False) -> dict[str, Any]:
        """定义保留窗口：粒度、保留天数、是否受审计标记。"""
        return {
            "lake_table": lake_table,
            "granularity": granularity,
            "retention_days": retention_days,
            "audit_tagged": audit_tagged,
            "created_at": time.time(),
        }

    def identify_expired_partitions(self, lake_table: str,
                                    retention_days: int) -> list[str]:
        """识别到期分区。"""
        files = lake_store.list_files(lake_table)
        now = time.time()
        expired = []
        for f in files:
            age_days = (now - f.created_at) / 86400
            if age_days > retention_days:
                expired.append(f.file_id)
        return expired

    def check_audit_block(self, lake_table: str, audit_tagged: bool) -> dict[str, Any]:
        """检查审计标记是否阻塞回收。"""
        return {
            "lake_table": lake_table,
            "audit_tagged": audit_tagged,
            "block_reclaim": audit_tagged,
            "message": "受审计标记约束，回收前需先解除标记" if audit_tagged else "无审计阻塞",
        }

    # ---- 分区审计约束 ----

    def mark_partition_audit(self, lake_table: str, partition_key: str,
                             reason: str = "合规稽核") -> dict[str, Any]:
        """登记分区审计约束：解除之前该分区不得被回收。"""
        rec = lake_store.mark_partition_audit(lake_table, partition_key)
        return {
            "lake_table": lake_table,
            "partition_key": partition_key,
            "audit_status": rec.status,
            "reason": reason,
            "block_reclaim": True,
        }

    def release_partition_audit(self, lake_table: str, partition_key: str,
                                operator: str = "system") -> dict[str, Any]:
        """登记审计解除记录：严格按湖表+分区生效。"""
        rec = lake_store.release_partition_audit(lake_table, partition_key, operator)
        return {
            "lake_table": lake_table,
            "partition_key": partition_key,
            "audit_status": rec.status,
            "release_id": rec.release_id,
            "block_reclaim": False,
        }

    def re_retain_partition(self, lake_table: str, partition_key: str,
                            operator: str = "system") -> dict[str, Any]:
        """重新保留：把已登记过的分区重新纳入保留约束。"""
        rec = lake_store.re_retain_partition(lake_table, partition_key, operator)
        if rec is None:
            return {
                "lake_table": lake_table,
                "partition_key": partition_key,
                "re_retained": False,
                "message": "该分区没有审计约束记录，无需重新保留",
            }
        return {
            "lake_table": lake_table,
            "partition_key": partition_key,
            "audit_status": rec.status,
            "re_retained": True,
            "block_reclaim": True,
        }

    def partition_audit_state(self, lake_table: str, partition_key: str) -> dict[str, Any]:
        """查询分区当前审计状态与可回收性。"""
        rec = lake_store.get_partition_audit(lake_table, partition_key)
        return {
            "lake_table": lake_table,
            "partition_key": partition_key,
            "audit_status": rec.status if rec else "none",
            "release_id": rec.release_id if rec else None,
            "reclaimable": lake_store.partition_reclaimable(lake_table, partition_key),
        }

    # ---- 解除/重新保留申请（与回收提交冲突检测） ----

    def submit_release_request(self, lake_table: str, partition_key: str,
                               operator: str = "system") -> dict[str, Any]:
        """提交解除审计申请；与已提交的回收冲突时标记 conflict。"""
        req = lake_store.submit_retention_request(lake_table, partition_key, "release_audit", operator)
        return self._request_view(req)

    def submit_re_retain_request(self, lake_table: str, partition_key: str,
                                 operator: str = "system") -> dict[str, Any]:
        """提交重新保留申请；与已提交的回收冲突时标记 conflict。"""
        req = lake_store.submit_retention_request(lake_table, partition_key, "re_retain", operator)
        return self._request_view(req)

    @staticmethod
    def _request_view(req) -> dict[str, Any]:
        return {
            "request_id": req.request_id,
            "lake_table": req.lake_table,
            "partition_key": req.partition_key,
            "request_type": req.request_type,
            "status": req.status,
            "conflict_with": req.conflict_with,
        }

    # ---- 回收主流程：计划 → 幂等提交 → 逐项确认 ----

    def plan_reclaim(self, lake_table: str, file_ids: list[str] | None = None,
                     partitions: list[str] | None = None,
                     retention_days: int | None = None) -> dict[str, Any]:
        """按湖表+分区读取审计解除记录，划分可回收文件与被拦截分区。"""
        candidates = self._candidate_files(lake_table, file_ids, partitions, retention_days)
        eligible_files: list[PartitionFile] = []
        blocked: set[str] = set()
        qualification: dict[str, str] = {}
        for pf in candidates:
            if lake_store.partition_reclaimable(lake_table, pf.partition_key):
                eligible_files.append(pf)
                qualification.setdefault(pf.partition_key, "无审计约束或审计已解除")
            else:
                blocked.add(pf.partition_key)
                qualification[pf.partition_key] = "审计约束未解除，回收被拦截"
        return {
            "lake_table": lake_table,
            "eligible_files": eligible_files,
            "eligible_partitions": sorted({f.partition_key for f in eligible_files}),
            "blocked_partitions": sorted(blocked),
            "qualification": qualification,
        }

    def _candidate_files(self, lake_table: str, file_ids: list[str] | None,
                         partitions: list[str] | None,
                         retention_days: int | None) -> list[PartitionFile]:
        if file_ids is not None:
            files: list[PartitionFile] = []
            for fid in file_ids:
                pf = lake_store.get_file(lake_table, fid)
                if pf is not None:
                    files.append(pf)
            return files
        if partitions is not None:
            files = []
            for part in partitions:
                files.extend(lake_store.list_files(lake_table, part))
            return files
        if retention_days is not None:
            return [
                pf for fid in self.identify_expired_partitions(lake_table, retention_days)
                if (pf := lake_store.get_file(lake_table, fid)) is not None
            ]
        return lake_store.list_files(lake_table)

    def submit_reclaim(self, lake_table: str, file_ids: list[str] | None = None,
                       partitions: list[str] | None = None, round_id: str | None = None,
                       retention_days: int | None = None) -> dict[str, Any]:
        """提交一轮回收（幂等）。

        同一 round_id 只保留一份释放记录：确认结果已写出但回执丢失时，
        重复提交不会新建记录，也不会重复累计回执字节；
        再次提交只覆盖仍在存储且尚未确认的那部分文件。
        """
        round_id = round_id or f"reclaim-{lake_table}-{uuid.uuid4().hex[:8]}"
        existing = lake_store.get_reclaim_round(round_id)
        plan = self.plan_reclaim(lake_table, file_ids, partitions, retention_days)
        confirmed = set(existing.confirmed) if existing else set()
        planned: dict[str, int] = {}
        file_partitions: dict[str, str] = {}
        submitted: list[str] = []
        for pf in plan["eligible_files"]:
            if pf.file_id in confirmed:
                continue  # 已确认删除的不再提交
            planned[pf.file_id] = pf.file_bytes
            file_partitions[pf.file_id] = pf.partition_key
            submitted.append(pf.file_id)
        if existing is not None:
            # 上一轮已计划但未确认且仍在存储的文件，本次继续提交；
            # 分区被重新拦截的除外
            blocked_now = set(plan["blocked_partitions"])
            for fid, fbytes in existing.planned.items():
                if fid in confirmed or fid in planned:
                    continue
                if existing.file_partitions.get(fid, "") in blocked_now:
                    continue
                if lake_store.get_file(lake_table, fid) is not None:
                    planned[fid] = fbytes
                    file_partitions[fid] = existing.file_partitions.get(fid, "")
                    submitted.append(fid)
        if existing is None and not planned and not plan["blocked_partitions"]:
            return {
                "lake_table": lake_table,
                "round_id": None,
                "created": False,
                "submitted_file_ids": [],
                "submitted_bytes": 0,
                "eligible_partitions": [],
                "blocked_partitions": [],
                "release_record_count": len(lake_store.list_reclaim_rounds(lake_table)),
            }
        rnd, created = lake_store.submit_reclaim_round(
            round_id, lake_table, planned, file_partitions,
            plan["eligible_partitions"], plan["blocked_partitions"])
        return {
            "lake_table": lake_table,
            "round_id": rnd.round_id,
            "created": created,
            "submitted_file_ids": submitted,
            "submitted_bytes": sum(planned[fid] for fid in submitted),
            "receipt": {"round_id": rnd.round_id, "receipt_bytes": rnd.receipt_bytes},
            "receipt_bytes": rnd.receipt_bytes,
            "eligible_partitions": plan["eligible_partitions"],
            "blocked_partitions": plan["blocked_partitions"],
            "qualification": plan["qualification"],
            "release_record_count": len(lake_store.list_reclaim_rounds(lake_table)),
        }

    def confirm_reclaim(self, round_id: str, deleted_file_ids: list[str]) -> dict[str, Any]:
        """存储侧确认删除结果：按已删除文件逐项记录，预算只增加实际确认字节。"""
        rnd = lake_store.get_reclaim_round(round_id)
        if rnd is None:
            return {
                "found": False,
                "round_id": round_id,
                "confirmed_files": {},
                "confirmed_bytes": 0,
            }
        lock = lake_store.acquire_write_lock(rnd.lake_table)
        try:
            known = [fid for fid in deleted_file_ids if fid in rnd.planned]
            unknown = [fid for fid in deleted_file_ids if fid not in rnd.planned]
            lake_store.remove_files(rnd.lake_table, known)
            rnd = lake_store.confirm_reclaim_files(round_id, known)
        finally:
            lake_store.release_write_lock(rnd.lake_table)
        confirmed_now = {fid: rnd.confirmed[fid] for fid in known}
        return {
            "found": True,
            "round_id": round_id,
            "confirmed_files": confirmed_now,
            "confirmed_bytes": sum(confirmed_now.values()),
            "unknown_file_ids": unknown,
            "total_confirmed_bytes": sum(rnd.confirmed.values()),
            "remaining_file_ids": [fid for fid in rnd.planned if fid not in rnd.confirmed],
            "status": rnd.status,
        }

    def execute_reclaim(self, lake_table: str, file_ids: list[str] | None = None,
                        partitions: list[str] | None = None, round_id: str | None = None,
                        retention_days: int | None = None) -> dict[str, Any]:
        """提交并确认一轮回收：存储侧确认全部提交文件，返回实际回收字节。"""
        submission = self.submit_reclaim(
            lake_table, file_ids=file_ids, partitions=partitions,
            round_id=round_id, retention_days=retention_days)
        if not submission.get("round_id"):
            return {
                **submission,
                "reclaimed_count": 0,
                "reclaimed_bytes": 0,
                "total_confirmed_bytes": 0,
            }
        confirmation = self.confirm_reclaim(submission["round_id"], submission["submitted_file_ids"])
        return {
            **submission,
            "reclaimed_count": len(confirmation["confirmed_files"]),
            "reclaimed_bytes": confirmation["confirmed_bytes"],
            "total_confirmed_bytes": confirmation["total_confirmed_bytes"],
            "remaining_file_ids": confirmation["remaining_file_ids"],
            "reclaim_time": time.time(),
        }

    def reclaim_partitions(self, lake_table: str, file_ids: list[str],
                           round_id: str | None = None) -> dict[str, Any]:
        """回收到期分区文件：按湖表+分区核对审计解除记录，报告实际回收字节。"""
        result = self.execute_reclaim(lake_table, file_ids=file_ids, round_id=round_id)
        return {
            "lake_table": lake_table,
            "round_id": result.get("round_id"),
            "reclaimed_count": result.get("reclaimed_count", 0),
            "reclaimed_bytes": result.get("reclaimed_bytes", 0),
            "blocked_partitions": result.get("blocked_partitions", []),
            "reclaim_time": time.time(),
        }

    # ---- 读取核对与预算 ----

    def verify_reclaim(self, round_id: str) -> dict[str, Any]:
        """读取核对：本轮回收是否取得有效资格，回执与实际回收是否对得上。"""
        rnd = lake_store.get_reclaim_round(round_id)
        if rnd is None:
            return {"found": False, "round_id": round_id}
        confirmed_partitions = {rnd.file_partitions.get(fid, "") for fid in rnd.confirmed}
        eligible = set(rnd.eligible_partitions)
        qualified = all(part in eligible for part in confirmed_partitions)
        confirmed_bytes = sum(rnd.confirmed.values())
        return {
            "found": True,
            "round_id": round_id,
            "lake_table": rnd.lake_table,
            "qualified": qualified,
            "qualification": {
                "eligible_partitions": list(rnd.eligible_partitions),
                "blocked_partitions": list(rnd.blocked_partitions),
                "confirmed_partitions": sorted(confirmed_partitions),
            },
            "receipt_bytes": rnd.receipt_bytes,
            "confirmed_bytes": confirmed_bytes,
            "receipt_mismatch": rnd.receipt_bytes != confirmed_bytes,
            "pending_file_ids": [fid for fid in rnd.planned if fid not in rnd.confirmed],
            "conflict_request_ids": list(rnd.conflict_request_ids),
            "status": rnd.status,
        }

    def reclaim_budget(self, lake_table: str) -> dict[str, Any]:
        """回收预算：只累计存储侧实际确认删除的字节。"""
        rounds = lake_store.list_reclaim_rounds(lake_table)
        confirmed = sum(sum(rnd.confirmed.values()) for rnd in rounds)
        receipt = sum(rnd.receipt_bytes for rnd in rounds)
        return {
            "lake_table": lake_table,
            "budget_bytes": confirmed,
            "confirmed_bytes": confirmed,
            "receipt_bytes": receipt,
            "round_count": len(rounds),
        }

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        """窗口详情：同步核对实际收回的字节（以存储侧确认结果为准）。"""
        entry = super().get_entry(entry_id)
        if entry is None:
            return None
        detail = dict(entry)
        lake_table = str(detail.get("所属湖表", "") or "")
        budget = self.reclaim_budget(lake_table)
        if budget["round_count"]:
            detail["回收字节数"] = budget["confirmed_bytes"]
        detail["回收核对"] = {
            "实际确认字节": budget["confirmed_bytes"],
            "回执字节": budget["receipt_bytes"],
            "释放记录份数": budget["round_count"],
            "核对一致": budget["receipt_bytes"] == budget["confirmed_bytes"],
        }
        return detail
