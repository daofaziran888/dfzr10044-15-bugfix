"""湖表存储引擎：文件注册表、分区清单、快照链、水位存储与批次数据。

本模块提供跨服务共享的不可变数据层：
- 文件注册：每个分区文件有唯一 file_id、所属分区、所属湖表、行数、字节数
- 快照链：每个湖表按时间线记录快照，含 file_ids、manifest、父快照指针
- 水位：每张湖表的已提交/待提交水位记录
- 批次数据：入湖批次的行级数据存储，含校验值
- 隔离区：被质量规则拦截的数据行，按批次隔离
- 写锁：并发提交冲突检测

设计原则：
- 所有写操作在提交前必须拿到全局锁
- 快照只在事务完整落湖后标记为可用
- 归并时先写入新文件清单，再原子地替换旧文件列表
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


# ----------------------------- 文件注册 -----------------------------

@dataclass
class PartitionFile:
    """分区文件：每个文件属于一个分区。"""
    file_id: str
    lake_table: str
    partition_key: str
    file_rows: int
    file_bytes: int
    file_manifest: str  # 文件内容摘要（MD5）
    created_at: float


# ----------------------------- 快照 -----------------------------

@dataclass
class LakeSnapshot:
    """湖表快照：记录某个时刻整个湖表的分区文件清单。"""
    snapshot_id: str
    lake_table: str
    parent_snapshot: str | None
    file_ids: list[str]
    added_rows: int
    deleted_rows: int
    manifest_checksum: str
    status: str  # generating | available | expired | rolled_back
    created_at: float


# ----------------------------- 水位 -----------------------------

@dataclass
class WatermarkRecord:
    """入湖水位追踪：记录已提交和待提交水位。"""
    lake_table: str
    field_name: str
    committed_value: str
    pending_value: str | None
    strategy: str  # append | merge | overwrite
    last_advanced_at: float


# ----------------------------- 批次数据 -----------------------------

@dataclass
class BatchRecord:
    """入湖批次：一次入湖操作导出的一组数据行。"""
    batch_id: str
    lake_table: str
    source_system: str
    rows: list[dict[str, Any]]  # 实际数据行
    row_count: int
    window_start: str
    window_end: str
    checksum: str
    snapshot_id: str | None  # 落湖后分配的 snap
    status: str  # pending | ingesting | ingested | failed
    created_at: float


# ----------------------------- 分区审计约束 -----------------------------

@dataclass
class PartitionAuditRecord:
    """分区审计约束：按湖表+分区记录审计状态与解除轨迹。

    解除记录严格按 (lake_table, partition_key) 生效，
    另一张湖表同名分区的解除记录不能放行本表分区。
    """
    lake_table: str
    partition_key: str
    status: str  # auditing | released | re_retained
    release_id: str | None
    operator: str
    updated_at: float


@dataclass
class RetentionRequest:
    """保留申请：解除审计 / 重新保留，需与回收提交做冲突检测。"""
    request_id: str
    lake_table: str
    partition_key: str
    request_type: str  # release_audit | re_retain
    status: str  # pending | conflict | approved
    conflict_with: str | None
    operator: str
    created_at: float


@dataclass
class ReclaimRound:
    """一轮回收提交：幂等键 + 计划清单 + 逐项确认 + 回执。

    - planned: 计划回收的文件及字节（提交时的快照）
    - confirmed: 存储侧实际确认删除的文件及字节（逐项记录）
    - receipt_bytes: 回执累计声称的释放字节，用于和实际回收对账
    """
    round_id: str
    lake_table: str
    planned: dict[str, int] = field(default_factory=dict)
    confirmed: dict[str, int] = field(default_factory=dict)
    file_partitions: dict[str, str] = field(default_factory=dict)
    eligible_partitions: list[str] = field(default_factory=list)
    blocked_partitions: list[str] = field(default_factory=list)
    receipt_bytes: int = 0
    status: str = "submitted"  # submitted | partially_confirmed | confirmed
    conflict_request_ids: list[str] = field(default_factory=list)
    created_at: float = 0.0
    updated_at: float = 0.0


# ----------------------------- 写锁 -----------------------------

class WriteLock:
    """全局写入锁：同一湖表同时只能有一个写入者提交。"""

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

    def acquire(self, lake_table: str) -> threading.Lock:
        with self._lock:
            if lake_table not in self._locks:
                self._locks[lake_table] = threading.Lock()
            table_lock = self._locks[lake_table]
        table_lock.acquire()
        return table_lock

    def release(self, lake_table: str) -> None:
        with self._lock:
            lock = self._locks.get(lake_table)
        if lock is not None:
            try:
                lock.release()
            except RuntimeError:
                pass


# ----------------------------- 存储引擎 -----------------------------

class LakeStore:
    """湖表存储引擎主类。

    管理三大维度：
    - files: 分区文件注册表
    - snapshots: 快照链
    - watermarks: 水位
    - batches: 批次数据
    - quarantine: 隔离区数据
    - write_lock: 并发提交的控制锁
    """

    def __init__(self) -> None:
        # 文件注册表：lake_table -> {file_id -> PartitionFile}
        self._files: dict[str, dict[str, PartitionFile]] = {}

        # 快照链：lake_table -> list[LakeSnapshot]（时间序）
        self._snapshots: dict[str, list[LakeSnapshot]] = {}

        # 水位：lake_table -> WatermarkRecord
        self._watermarks: dict[str, WatermarkRecord] = {}

        # 批次数据：batch_id -> BatchRecord
        self._batches: dict[str, BatchRecord] = {}

        # 隔离区：batch_id -> list[dict]（被拦截的数据行）
        self._quarantine: dict[str, list[dict[str, Any]]] = {}

        # 写锁
        self._write_lock = WriteLock()

        # 归并中断恢复检查点：merge_id -> {partial_file_ids: [...], ...}
        self._merge_checkpoints: dict[str, dict[str, Any]] = {}

        # 分区审计约束：lake_table -> {partition_key -> PartitionAuditRecord}
        self._partition_audits: dict[str, dict[str, PartitionAuditRecord]] = {}

        # 保留申请（解除审计/重新保留）：request_id -> RetentionRequest
        self._retention_requests: dict[str, RetentionRequest] = {}

        # 回收轮次台账（释放记录）：round_id -> ReclaimRound
        self._reclaim_rounds: dict[str, ReclaimRound] = {}

    # ---- 文件操作 ----

    def register_file(
        self, lake_table: str, partition_key: str, rows: int, bytes_: int, manifest: str | None = None
    ) -> PartitionFile:
        file_id = f"{lake_table}-p{partition_key}-{uuid.uuid4().hex[:8]}"
        checksum = manifest or hashlib.md5(f"{lake_table}:{partition_key}:{time.time()}".encode()).hexdigest()
        pf = PartitionFile(
            file_id=file_id,
            lake_table=lake_table,
            partition_key=partition_key,
            file_rows=rows,
            file_bytes=bytes_,
            file_manifest=checksum,
            created_at=time.time(),
        )
        self._files.setdefault(lake_table, {})[file_id] = pf
        return pf

    def list_files(self, lake_table: str, partition_key: str | None = None) -> list[PartitionFile]:
        files = list(self._files.get(lake_table, {}).values())
        if partition_key is not None:
            files = [f for f in files if f.partition_key == partition_key]
        return files

    def get_file(self, lake_table: str, file_id: str) -> PartitionFile | None:
        return self._files.get(lake_table, {}).get(file_id)

    def remove_files(self, lake_table: str, file_ids: list[str]) -> int:
        table_files = self._files.get(lake_table, {})
        removed = 0
        for fid in file_ids:
            if fid in table_files:
                del table_files[fid]
                removed += 1
        return removed

    def replace_file_list(
        self, lake_table: str, old_file_ids: list[str], new_files: list[PartitionFile]
    ) -> None:
        """原子替换：先注册新文件，再删除旧文件。保证归并不留半份清单。"""
        table_files = self._files.setdefault(lake_table, {})
        for nf in new_files:
            table_files[nf.file_id] = nf
        for fid in old_file_ids:
            table_files.pop(fid, None)

    # ---- 快照操作 ----

    def create_snapshot(
        self,
        lake_table: str,
        file_ids: list[str],
        parent_snapshot: str | None = None,
        added_rows: int = 0,
        deleted_rows: int = 0,
    ) -> LakeSnapshot:
        snap_id = f"snap-{lake_table}-{uuid.uuid4().hex[:8]}"
        manifest = hashlib.md5(
            ",".join(sorted(file_ids)).encode()
        ).hexdigest()
        snap = LakeSnapshot(
            snapshot_id=snap_id,
            lake_table=lake_table,
            parent_snapshot=parent_snapshot,
            file_ids=list(file_ids),
            added_rows=added_rows,
            deleted_rows=deleted_rows,
            manifest_checksum=manifest,
            status="generating",
            created_at=time.time(),
        )
        chain = self._snapshots.setdefault(lake_table, [])
        chain.append(snap)
        return snap

    def mark_snapshot_available(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "available"
                return snap
        return None

    def mark_snapshot_expired(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "expired"
                return snap
        return None

    def get_snapshot(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                return snap
        return None

    def list_snapshots(self, lake_table: str) -> list[LakeSnapshot]:
        return list(self._snapshots.get(lake_table, []))

    def get_snapshot_at_time(self, lake_table: str, timestamp: float) -> LakeSnapshot | None:
        """时间旅行：返回 <=timestamp 的最近可用快照。"""
        candidates = [
            s for s in self._snapshots.get(lake_table, [])
            if s.status == "available" and s.created_at <= timestamp
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda s: s.created_at)

    # ---- 水位操作 ----

    def set_watermark(
        self, lake_table: str, field_name: str, committed: str,
        strategy: str = "append"
    ) -> WatermarkRecord:
        wm = WatermarkRecord(
            lake_table=lake_table,
            field_name=field_name,
            committed_value=committed,
            pending_value=None,
            strategy=strategy,
            last_advanced_at=time.time(),
        )
        self._watermarks[lake_table] = wm
        return wm

    def advance_watermark(self, lake_table: str, new_value: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None:
            return None
        wm.pending_value = new_value
        return wm

    def commit_watermark(self, lake_table: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None or wm.pending_value is None:
            return None
        wm.committed_value = wm.pending_value
        wm.pending_value = None
        wm.last_advanced_at = time.time()
        return wm

    def get_watermark(self, lake_table: str) -> WatermarkRecord | None:
        return self._watermarks.get(lake_table)

    def list_watermarks(self) -> list[WatermarkRecord]:
        return list(self._watermarks.values())

    # ---- 批次数据 ----

    def store_batch(
        self, batch_id: str, lake_table: str, source_system: str,
        rows: list[dict[str, Any]], window_start: str, window_end: str,
    ) -> BatchRecord:
        checksum = hashlib.md5(
            str(sorted([str(r) for r in rows])).encode()
        ).hexdigest()
        br = BatchRecord(
            batch_id=batch_id,
            lake_table=lake_table,
            source_system=source_system,
            rows=list(rows),
            row_count=len(rows),
            window_start=window_start,
            window_end=window_end,
            checksum=checksum,
            snapshot_id=None,
            status="pending",
            created_at=time.time(),
        )
        self._batches[batch_id] = br
        return br

    def get_batch(self, batch_id: str) -> BatchRecord | None:
        return self._batches.get(batch_id)

    def list_batches(self, lake_table: str) -> list[BatchRecord]:
        return [b for b in self._batches.values() if b.lake_table == lake_table]

    def update_batch_status(self, batch_id: str, status: str, snapshot_id: str | None = None) -> BatchRecord | None:
        br = self._batches.get(batch_id)
        if br is None:
            return None
        br.status = status
        if snapshot_id is not None:
            br.snapshot_id = snapshot_id
        return br

    # ---- 隔离区 ----

    def quarantine_rows(
        self, batch_id: str, rule_id: str, rule_name: str,
        rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """将被质量规则拦截的行放入隔离区。"""
        quarantined = []
        for row in rows:
            qr = dict(row)
            qr["_quarantine_batch"] = batch_id
            qr["_quarantine_rule"] = rule_id
            qr["_quarantine_rule_name"] = rule_name
            qr["_quarantine_time"] = time.time()
            quarantined.append(qr)
        self._quarantine.setdefault(batch_id, []).extend(quarantined)
        return quarantined

    def list_quarantine(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        if batch_id is not None:
            return list(self._quarantine.get(batch_id, []))
        result: list[dict[str, Any]] = []
        for rows in self._quarantine.values():
            result.extend(rows)
        return result

    def release_quarantine(self, batch_id: str) -> list[dict[str, Any]]:
        """回炉重入：从隔离区取出数据行。"""
        rows = self._quarantine.pop(batch_id, [])
        cleaned: list[dict[str, Any]] = []
        for r in rows:
            cleaned_row = {k: v for k, v in r.items() if not k.startswith("_quarantine_")}
            cleaned.append(cleaned_row)
        return cleaned

    # ---- 归并检查点 ----

    def save_merge_checkpoint(self, merge_id: str, partial_file_ids: list[str]) -> None:
        self._merge_checkpoints[merge_id] = {
            "partial_file_ids": list(partial_file_ids),
            "saved_at": time.time(),
        }

    def get_merge_checkpoint(self, merge_id: str) -> dict[str, Any] | None:
        return self._merge_checkpoints.get(merge_id)

    def clear_merge_checkpoint(self, merge_id: str) -> None:
        self._merge_checkpoints.pop(merge_id, None)

    # ---- 分区审计约束 ----

    def mark_partition_audit(self, lake_table: str, partition_key: str,
                             operator: str = "system") -> PartitionAuditRecord:
        """登记分区审计约束：解除之前该分区不得被回收。"""
        rec = PartitionAuditRecord(
            lake_table=lake_table,
            partition_key=partition_key,
            status="auditing",
            release_id=None,
            operator=operator,
            updated_at=time.time(),
        )
        self._partition_audits.setdefault(lake_table, {})[partition_key] = rec
        return rec

    def release_partition_audit(self, lake_table: str, partition_key: str,
                                operator: str = "system") -> PartitionAuditRecord:
        """登记审计解除记录：严格按湖表+分区生效。

        另一张湖表同名分区的解除记录存放在不同的键下，不能放行本表分区。
        无约束时补记一条解除记录，保证解除轨迹可查。
        """
        table_audits = self._partition_audits.setdefault(lake_table, {})
        rec = table_audits.get(partition_key)
        if rec is None:
            rec = PartitionAuditRecord(
                lake_table=lake_table,
                partition_key=partition_key,
                status="released",
                release_id=None,
                operator=operator,
                updated_at=time.time(),
            )
            table_audits[partition_key] = rec
        rec.status = "released"
        rec.release_id = f"rel-{lake_table}-{partition_key}-{uuid.uuid4().hex[:8]}"
        rec.operator = operator
        rec.updated_at = time.time()
        # 同步放行待处理的解除申请
        for req in self._retention_requests.values():
            if (req.lake_table == lake_table and req.partition_key == partition_key
                    and req.request_type == "release_audit" and req.status == "pending"):
                req.status = "approved"
        return rec

    def re_retain_partition(self, lake_table: str, partition_key: str,
                            operator: str = "system") -> PartitionAuditRecord | None:
        """重新保留：把已登记过的分区重新纳入保留约束。"""
        rec = self._partition_audits.get(lake_table, {}).get(partition_key)
        if rec is None:
            return None
        rec.status = "re_retained"
        rec.operator = operator
        rec.updated_at = time.time()
        return rec

    def get_partition_audit(self, lake_table: str, partition_key: str) -> PartitionAuditRecord | None:
        return self._partition_audits.get(lake_table, {}).get(partition_key)

    def partition_reclaimable(self, lake_table: str, partition_key: str) -> bool:
        """分区是否可回收：无审计约束或已解除，且没有待处理的重新保留申请。"""
        rec = self.get_partition_audit(lake_table, partition_key)
        if rec is not None and rec.status != "released":
            return False
        for req in self._retention_requests.values():
            if (req.lake_table == lake_table and req.partition_key == partition_key
                    and req.request_type == "re_retain" and req.status == "pending"):
                return False
        return True

    def list_partition_audits(self, lake_table: str | None = None) -> list[PartitionAuditRecord]:
        records: list[PartitionAuditRecord] = []
        for table, audits in self._partition_audits.items():
            if lake_table is not None and table != lake_table:
                continue
            records.extend(audits.values())
        return records

    # ---- 保留申请（解除审计 / 重新保留） ----

    def submit_retention_request(self, lake_table: str, partition_key: str,
                                 request_type: str, operator: str = "system") -> RetentionRequest:
        """提交解除审计/重新保留申请。

        若该分区已被某轮回收提交触及（无论被回收还是被拦截），
        申请与回收提交冲突，标记为 conflict 并回挂到对应轮次。
        """
        conflict_with: str | None = None
        for rnd in self._reclaim_rounds.values():
            if rnd.lake_table != lake_table:
                continue
            touched = set(rnd.eligible_partitions) | set(rnd.blocked_partitions)
            if partition_key in touched:
                conflict_with = rnd.round_id
                break
        req = RetentionRequest(
            request_id=f"req-{request_type}-{uuid.uuid4().hex[:8]}",
            lake_table=lake_table,
            partition_key=partition_key,
            request_type=request_type,
            status="conflict" if conflict_with else "pending",
            conflict_with=conflict_with,
            operator=operator,
            created_at=time.time(),
        )
        self._retention_requests[req.request_id] = req
        if conflict_with is not None:
            self._reclaim_rounds[conflict_with].conflict_request_ids.append(req.request_id)
        return req

    def get_retention_request(self, request_id: str) -> RetentionRequest | None:
        return self._retention_requests.get(request_id)

    def list_retention_requests(self, lake_table: str | None = None,
                                request_type: str | None = None) -> list[RetentionRequest]:
        requests = list(self._retention_requests.values())
        if lake_table is not None:
            requests = [r for r in requests if r.lake_table == lake_table]
        if request_type is not None:
            requests = [r for r in requests if r.request_type == request_type]
        return sorted(requests, key=lambda r: r.created_at)

    # ---- 回收轮次（释放记录台账） ----

    def submit_reclaim_round(self, round_id: str, lake_table: str,
                             planned: dict[str, int],
                             file_partitions: dict[str, str],
                             eligible_partitions: list[str],
                             blocked_partitions: list[str]) -> tuple[ReclaimRound, bool]:
        """提交一轮回收（幂等）：同一 round_id 只保留一份释放记录。

        已存在的轮次只追加新计划文件并累加回执字节；
        已确认删除的文件不会重复进入计划，确认结果不因重复提交而丢失。
        返回 (轮次, 是否新建)。
        """
        existing = self._reclaim_rounds.get(round_id)
        if existing is not None:
            new_bytes = 0
            for fid, fbytes in planned.items():
                if fid not in existing.planned and fid not in existing.confirmed:
                    existing.planned[fid] = fbytes
                    existing.file_partitions[fid] = file_partitions.get(fid, "")
                    new_bytes += fbytes
            for part in eligible_partitions:
                if part not in existing.eligible_partitions:
                    existing.eligible_partitions.append(part)
            for part in blocked_partitions:
                if part not in existing.blocked_partitions:
                    existing.blocked_partitions.append(part)
            existing.receipt_bytes += new_bytes
            existing.updated_at = time.time()
            existing.status = self._reclaim_round_status(existing)
            return existing, False
        rnd = ReclaimRound(
            round_id=round_id,
            lake_table=lake_table,
            planned=dict(planned),
            confirmed={},
            file_partitions=dict(file_partitions),
            eligible_partitions=list(eligible_partitions),
            blocked_partitions=list(blocked_partitions),
            receipt_bytes=sum(planned.values()),
            status="submitted",
            conflict_request_ids=[],
            created_at=time.time(),
            updated_at=time.time(),
        )
        self._reclaim_rounds[round_id] = rnd
        return rnd, True

    def confirm_reclaim_files(self, round_id: str, file_ids: list[str]) -> ReclaimRound | None:
        """存储侧确认：按已删除文件逐项记录实际回收字节。"""
        rnd = self._reclaim_rounds.get(round_id)
        if rnd is None:
            return None
        for fid in file_ids:
            if fid in rnd.planned and fid not in rnd.confirmed:
                rnd.confirmed[fid] = rnd.planned[fid]
        rnd.updated_at = time.time()
        rnd.status = self._reclaim_round_status(rnd)
        return rnd

    @staticmethod
    def _reclaim_round_status(rnd: ReclaimRound) -> str:
        if not rnd.confirmed:
            return "submitted"
        if set(rnd.confirmed) >= set(rnd.planned):
            return "confirmed"
        return "partially_confirmed"

    def get_reclaim_round(self, round_id: str) -> ReclaimRound | None:
        return self._reclaim_rounds.get(round_id)

    def list_reclaim_rounds(self, lake_table: str | None = None) -> list[ReclaimRound]:
        rounds = list(self._reclaim_rounds.values())
        if lake_table is not None:
            rounds = [r for r in rounds if r.lake_table == lake_table]
        return sorted(rounds, key=lambda r: r.created_at)

    def confirmed_reclaim_bytes(self, lake_table: str) -> int:
        """湖表累计实际确认回收的字节（回收预算只认这部分）。"""
        return sum(
            sum(rnd.confirmed.values())
            for rnd in self._reclaim_rounds.values()
            if rnd.lake_table == lake_table
        )

    # ---- 并发写锁 ----

    def acquire_write_lock(self, lake_table: str) -> threading.Lock:
        return self._write_lock.acquire(lake_table)

    def release_write_lock(self, lake_table: str) -> None:
        self._write_lock.release(lake_table)

    # ---- 统计快照 ----

    def stats_snapshot(self, lake_table: str) -> dict[str, Any]:
        """生成湖表统计画像快照。"""
        files = self.list_files(lake_table)
        snapshots = self.list_snapshots(lake_table)
        wm = self.get_watermark(lake_table)
        total_rows = sum(f.file_rows for f in files)
        total_bytes = sum(f.file_bytes for f in files)
        partition_count = len(set(f.partition_key for f in files))
        return {
            "lake_table": lake_table,
            "file_count": len(files),
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "partition_count": partition_count,
            "available_snapshots": sum(1 for s in snapshots if s.status == "available"),
            "watermark": wm.committed_value if wm else None,
        }


# 全局单例
lake_store = LakeStore()