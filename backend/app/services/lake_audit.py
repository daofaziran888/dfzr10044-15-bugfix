"""湖表审计领域服务：记录结构演进、快照回退、过期清理、分层迁移的变更前后值。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import lake_store


class LakeAuditService(BaseModuleService):
    MODULE = "lake_audit"
    ENTITY = "审计条目"
    OPERATION_TERM = "迁移"
    LIST_FIELDS = ["条目编号", "所属湖表", "操作类型", "操作对象", "变更前值", "变更后值", "操作者标识", "操作时间"]
    REQUIRED_FIELDS = ["条目编号", "所属湖表", "操作类型"]
    STATUS_ORDER = ["已记录", "待核查", "已核查", "已封存"]
    ACTION_RULES = {"提交核查": "待核查", "完成核查": "已核查", "封存条目": "已封存"}
    NEGATIVE_ACTIONS = []

    def record_audit_entry(self, lake_table: str, operation_type: str,
                           target: str, value_before: Any,
                           value_after: Any, operator_id: str) -> dict[str, Any]:
        """记录一条审计条目：变更前后的完整快照。"""
        return {
            "lake_table": lake_table,
            "operation_type": operation_type,
            "target": target,
            "value_before": str(value_before),
            "value_after": str(value_after),
            "operator_id": operator_id,
            "recorded_at": time.time(),
        }

    def verify_audit_chain(self, lake_table: str, operation_type: str) -> list[dict[str, Any]]:
        """验证审计链完整性：列出指定湖表某种操作类型的全部审计条目。"""
        rows = store.rows(self.MODULE)
        chain = [
            r for r in rows
            if str(r.get("所属湖表", "")) == lake_table
               and str(r.get("操作类型", "")) == operation_type
        ]
        chain.sort(key=lambda r: str(r.get("操作时间", "")))
        return chain

    def check_audit_gap(self, lake_table: str,
                        expected_before: str, actual_after: str) -> dict[str, Any]:
        """检查审计缺口：预期变更前值与实际变更后值是否闭合。"""
        gap = expected_before != actual_after
        return {
            "lake_table": lake_table,
            "has_gap": gap,
            "expected_before": expected_before,
            "actual_after": actual_after,
            "severity": "HIGH" if gap else "OK",
        }

    def seal_audit_chain(self, lake_table: str) -> dict[str, Any]:
        """封存审计链：将待核查条目封存为不可变档案。"""
        sealed = 0
        for row in store.rows(self.MODULE):
            if row.get("status") == "待核查" and str(row.get("所属湖表", "")) == lake_table:
                row["status"] = "已封存"
                sealed += 1
        return {
            "lake_table": lake_table,
            "sealed_count": sealed,
            "seal_time": time.time(),
        }

    def list_retention_requests(self, lake_table: str | None = None,
                                request_type: str | None = None) -> list[dict[str, Any]]:
        """从审计入口查询解除审计/重新保留申请及其与回收提交的冲突状态。"""
        type_labels = {"release_audit": "解除审计", "re_retain": "重新保留"}
        requests = lake_store.list_retention_requests(lake_table, request_type)
        return [
            {
                "request_id": req.request_id,
                "lake_table": req.lake_table,
                "partition_key": req.partition_key,
                "request_type": req.request_type,
                "request_type_label": type_labels.get(req.request_type, req.request_type),
                "status": req.status,
                "conflict_with": req.conflict_with,
                "operator": req.operator,
                "created_at": req.created_at,
            }
            for req in requests
        ]