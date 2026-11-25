"""保留窗口到期回收链修复的回归测试。

场景锚点：
- 湖表两个已过期分区：F 1200 字节审计仍在审核，G 1100 字节无限制
- 执行回收只处理 G，F 仍能从所属分区读取
- 回执丢失重复提交同一回收，只保留一份释放记录
- 另一张湖表同名分区的解除记录不能放行 F
- 解除 F / 重新保留 F 的申请与回收提交冲突
- 读取核对本轮回收资格、回执与实际回收是否对得上
- 详情页同步核对实际收回字节，审计入口可查解除/重新保留申请
- 部分删除逐项确认，第二次执行只提交仍在的部分，预算只加实际确认字节
"""
from __future__ import annotations

import time

from app.lake_store import lake_store
from app.pipeline.chain_core import run_chain
from app.pipeline.chain_runner import get_chain_steps
from app.services.lake_audit import LakeAuditService
from app.services.retention_window import RetentionWindowService


def _register_expired(table: str, partition: str, total_bytes: int, files: int = 1):
    """注册一组已过期分区文件（创建时间回拨 120 天）。"""
    out = []
    per = total_bytes // files
    for i in range(files):
        size = per if i < files - 1 else total_bytes - per * (files - 1)
        pf = lake_store.register_file(table, partition, rows=10, bytes_=size)
        pf.created_at = time.time() - 120 * 86400
        out.append(pf)
    return out


class TestReclaimAuditGate:
    """回收只处理已解除约束的分区，审核中的分区保持可读。"""

    def test_reclaim_skips_auditing_partition(self):
        svc = RetentionWindowService()
        table = "rw_tbl_fg"
        _register_expired(table, "F", 1200)
        _register_expired(table, "G", 1100)
        svc.mark_partition_audit(table, "F")

        result = svc.execute_reclaim(table, partitions=["F", "G"], round_id="rw-fg-1")

        assert result["reclaimed_bytes"] == 1100
        assert result["reclaimed_count"] == 1
        assert result["blocked_partitions"] == ["F"]
        # G 已回收，F 仍能从所属分区读取
        assert lake_store.list_files(table, "G") == []
        f_files = lake_store.list_files(table, "F")
        assert len(f_files) == 1
        assert f_files[0].file_bytes == 1200

    def test_cross_table_release_does_not_unlock(self):
        svc = RetentionWindowService()
        table = "rw_tbl_cross"
        other = "rw_tbl_other"
        _register_expired(table, "F", 1200)
        _register_expired(other, "F", 900)
        svc.mark_partition_audit(table, "F")
        svc.mark_partition_audit(other, "F")
        # 另一张湖表同名分区的解除记录
        svc.release_partition_audit(other, "F")

        assert not lake_store.partition_reclaimable(table, "F")
        result = svc.execute_reclaim(table, partitions=["F"], round_id="rw-cross-1")
        assert result["reclaimed_bytes"] == 0
        assert result["blocked_partitions"] == ["F"]
        assert len(lake_store.list_files(table, "F")) == 1

    def test_proper_release_unlocks_partition(self):
        svc = RetentionWindowService()
        table = "rw_tbl_rel"
        _register_expired(table, "F", 1200)
        svc.mark_partition_audit(table, "F")
        svc.release_partition_audit(table, "F")

        result = svc.execute_reclaim(table, partitions=["F"], round_id="rw-rel-1")
        assert result["reclaimed_bytes"] == 1200
        assert lake_store.list_files(table, "F") == []


class TestReclaimIdempotency:
    """回执丢失后重复提交同一回收，只保留一份释放记录。"""

    def test_resubmit_keeps_single_release_record(self):
        svc = RetentionWindowService()
        table = "rw_tbl_idem"
        _register_expired(table, "G", 1100)

        first = svc.execute_reclaim(table, partitions=["G"], round_id="rw-idem-1")
        assert first["reclaimed_bytes"] == 1100

        # 第一次确认结果已写出但回执丢失，重复提交同一回收
        second = svc.submit_reclaim(table, partitions=["G"], round_id="rw-idem-1")
        assert not second["created"]
        assert second["submitted_file_ids"] == []
        assert second["receipt_bytes"] == 1100

        rounds = lake_store.list_reclaim_rounds(table)
        assert len(rounds) == 1
        # 预算不因重复提交而翻倍
        assert svc.reclaim_budget(table)["confirmed_bytes"] == 1100


class TestRetentionRequestConflict:
    """解除/重新保留申请与回收提交冲突，并可从审计入口查询。"""

    def test_requests_conflict_with_reclaim_submission(self):
        svc = RetentionWindowService()
        audit_svc = LakeAuditService()
        table = "rw_tbl_conf"
        _register_expired(table, "F", 1200)
        _register_expired(table, "G", 1100)
        svc.mark_partition_audit(table, "F")
        svc.execute_reclaim(table, partitions=["F", "G"], round_id="rw-conf-1")

        release_req = svc.submit_release_request(table, "F")
        re_retain_req = svc.submit_re_retain_request(table, "F")
        assert release_req["status"] == "conflict"
        assert re_retain_req["status"] == "conflict"
        assert release_req["conflict_with"] == "rw-conf-1"
        assert re_retain_req["conflict_with"] == "rw-conf-1"

        # 从审计入口查询解除和重新保留的申请
        reqs = audit_svc.list_retention_requests(table)
        labels = {r["request_type_label"] for r in reqs}
        assert labels == {"解除审计", "重新保留"}
        assert all(r["status"] == "conflict" for r in reqs)

        # 随后的读取核对：本轮回收资格有效、冲突申请可见、回执与实际一致
        verification = svc.verify_reclaim("rw-conf-1")
        assert verification["qualified"]
        assert len(verification["conflict_request_ids"]) == 2
        assert not verification["receipt_mismatch"]


class TestReclaimVerification:
    """读取核对回收资格与回执对账，部分删除逐项确认。"""

    def test_partial_delete_confirmed_per_file(self):
        svc = RetentionWindowService()
        table = "rw_tbl_part"
        files = _register_expired(table, "G", 1100, files=2)  # 550 + 550

        submission = svc.submit_reclaim(table, partitions=["G"], round_id="rw-part-1")
        assert submission["receipt_bytes"] == 1100

        # 存储侧确认 G 只有一部分已删除：按已删除文件逐项记录
        confirmation = svc.confirm_reclaim("rw-part-1", [files[0].file_id])
        assert confirmation["confirmed_bytes"] == 550
        assert confirmation["remaining_file_ids"] == [files[1].file_id]
        assert svc.reclaim_budget(table)["confirmed_bytes"] == 550

        # 回执 1100 与实际 550 对不上
        verification = svc.verify_reclaim("rw-part-1")
        assert verification["receipt_mismatch"]
        assert verification["confirmed_bytes"] == 550
        assert verification["qualified"]

        # 第二次执行只提交仍在的那部分
        second = svc.submit_reclaim(table, partitions=["G"], round_id="rw-part-1")
        assert not second["created"]
        assert second["submitted_file_ids"] == [files[1].file_id]
        svc.confirm_reclaim("rw-part-1", second["submitted_file_ids"])

        # 回收预算只增加实际确认的字节，回执与实际对齐
        assert svc.reclaim_budget(table)["confirmed_bytes"] == 1100
        assert not svc.verify_reclaim("rw-part-1")["receipt_mismatch"]
        assert len(lake_store.list_reclaim_rounds(table)) == 1
        assert lake_store.list_files(table, "G") == []


class TestWindowDetailSync:
    """保留窗口详情页同步核对实际收回的字节。"""

    def test_detail_syncs_confirmed_bytes(self):
        svc = RetentionWindowService()
        table = "rw_tbl_detail"
        _register_expired(table, "G", 1100)
        svc.execute_reclaim(table, partitions=["G"], round_id="rw-detail-1")

        entry, missing = svc.create_entry({
            "窗口编号": "RW-DETAIL-1", "所属湖表": table, "保留粒度": "DAY",
            "保留周期": 90, "到期分区数": 1, "受审计标记": False,
            "回收字节数": 0, "最近回收": "",
        })
        assert not missing
        detail = svc.get_entry(entry["id"])
        assert detail["回收字节数"] == 1100
        assert detail["回收核对"]["实际确认字节"] == 1100
        assert detail["回收核对"]["释放记录份数"] == 1
        assert detail["回收核对"]["核对一致"]


class TestChainRetentionReclaimFixed:
    """链路10端到端：审核中的分区不被回收，释放字节按实际确认上报。"""

    def test_chain_reclaims_only_cleared_partitions(self):
        table = "rw_chain_fg"
        _register_expired(table, "F", 1200)
        _register_expired(table, "G", 1100)
        lake_store.mark_partition_audit(table, "F")

        result = run_chain(
            "chain_retention_audit_reclaim",
            table,
            get_chain_steps("chain_retention_audit_reclaim"),
        )
        assert result.success
        assert result.final_output["reclaimed_bytes"] == 1100
        assert result.final_output["reclaimed_file_count"] == 1
        assert result.final_output["blocked_partitions"] == ["F"]
        assert not result.final_output["audit_cleared"]
        # F 仍能从所属分区读取
        assert len(lake_store.list_files(table, "F")) == 1
        assert lake_store.list_files(table, "G") == []

    def test_chain_resubmit_is_idempotent(self):
        table = "rw_chain_idem"
        _register_expired(table, "G", 1100)
        steps = get_chain_steps("chain_retention_audit_reclaim")
        run_chain("chain_retention_audit_reclaim", table, steps)
        # 回执丢失后重跑同一链路：只保留一份释放记录
        run_chain("chain_retention_audit_reclaim", table, steps)
        rounds = lake_store.list_reclaim_rounds(table)
        assert len(rounds) == 1
        assert rounds[0].receipt_bytes == 1100
        assert sum(rounds[0].confirmed.values()) == 1100
