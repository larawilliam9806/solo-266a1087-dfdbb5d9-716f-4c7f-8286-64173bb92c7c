"""交接业务：建包、逐片验收、缺片恢复、拼接下载、按字节区间取件。"""

from __future__ import annotations

import hashlib
import os
import sqlite3

from flask import current_app

from . import codec, db, storage
from .manifest import Manifest, manifest_digest, parse_manifest


class HandoverError(Exception):
    def __init__(
        self,
        message: str,
        status: int = 400,
        details: list[str] | None = None,
        extra: dict | None = None,
    ):
        super().__init__(message)
        self.message = message
        self.status = status
        self.details = details or []
        # 结构化补充字段（如缺片号、最早受阻偏移），随 JSON 错误一并返回。
        self.extra = extra or {}


def _data_dir() -> str:
    return current_app.config["DATA_DIR"]


def _chunk() -> int:
    return int(current_app.config["CHUNK_SIZE"])


def _delivery(conn, delivery_id: int) -> dict:
    row = conn.execute(
        "SELECT * FROM deliveries WHERE id = ?", (int(delivery_id),)
    ).fetchone()
    if row is None:
        raise HandoverError("交付包不存在", 404)
    return dict(row)


def _shard_rows(conn, delivery_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM shards WHERE delivery_id = ? ORDER BY shard_index",
        (int(delivery_id),),
    ).fetchall()
    return [dict(r) for r in rows]


def _manifest_of(delivery: dict) -> Manifest:
    manifest, _label = parse_manifest(
        delivery["manifest_json"].encode("utf-8"), delivery.get("manifest_name")
    )
    return manifest


def delivery_state(delivery_id: int) -> dict:
    """汇总页面所需的完整状态。"""
    conn = db.get_db()
    delivery = _delivery(conn, delivery_id)
    manifest = _manifest_of(delivery)
    rows = _shard_rows(conn, delivery_id)
    shards = []
    missing = []
    accepted = 0
    accepted_bytes = 0
    offset = 0
    for row in rows:
        spec = manifest.by_index[row["shard_index"]]
        done = row["accepted_at"] is not None
        if done:
            accepted += 1
            accepted_bytes += spec.size
        else:
            missing.append(spec.index)
        shards.append(
            {
                "index": spec.index,
                "size": spec.size,
                "offset": offset,
                "sha256": spec.sha256,
                "accepted": done,
                "source": row["source"],
                "accepted_at": row["accepted_at"],
                "stored_size": row["stored_size"],
                "stored_sha256": row["stored_sha256"],
                "is_longest": spec.size == manifest.max_size,
            }
        )
        offset += spec.size
    return {
        "delivery": delivery,
        "manifest": manifest,
        "shards": shards,
        "missing": missing,
        "accepted_count": accepted,
        "accepted_bytes": accepted_bytes,
        "total_count": len(shards),
        "complete": not missing,
        "recovery_allowed": len(missing) == 1,
        "recovery_target": next(
            (s for s in shards if missing and s["index"] == missing[0]), None
        ),
    }


def _canonical_manifest_json(manifest: Manifest, label: str) -> str:
    import json

    return json.dumps(
        {
            "label": label,
            "shards": [s.as_dict() for s in manifest.shards],
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def create_delivery(manifest_bytes: bytes, filename: str | None) -> tuple[int, bool]:
    """建包或沿用已有记录（同一原片清单摘要相同）。返回 (delivery_id, 是否新建)。"""
    manifest, label = parse_manifest(manifest_bytes, filename)
    digest = manifest_digest(manifest)
    conn = db.get_db()

    existing = conn.execute(
        "SELECT id FROM deliveries WHERE manifest_hash = ?", (digest,)
    ).fetchone()
    if existing is not None:
        return int(existing["id"]), False

    now = db.utcnow()
    try:
        with db.write_tx(conn):
            cursor = conn.execute(
                """INSERT INTO deliveries
                   (manifest_hash, manifest_name, label, manifest_json,
                    total_shards, total_bytes, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    digest,
                    (filename or "")[:255] or None,
                    label,
                    _canonical_manifest_json(manifest, label),
                    len(manifest.shards),
                    manifest.total_bytes,
                    now,
                    now,
                ),
            )
            delivery_id = int(cursor.lastrowid)
            conn.executemany(
                """INSERT INTO shards
                   (delivery_id, shard_index, declared_size, declared_sha256)
                   VALUES (?, ?, ?, ?)""",
                [(delivery_id, s.index, s.size, s.sha256) for s in manifest.shards],
            )
    except sqlite3.IntegrityError:
        # 并发建包：另一方先写入，沿用它的记录。
        row = conn.execute(
            "SELECT id FROM deliveries WHERE manifest_hash = ?", (digest,)
        ).fetchone()
        if row is None:
            raise
        return int(row["id"]), False

    try:
        storage.ensure_package_dirs(_data_dir(), delivery_id)
    except storage.StorageError:
        with db.write_tx(conn):
            conn.execute("DELETE FROM deliveries WHERE id = ?", (delivery_id,))
        raise
    return delivery_id, True


def _staging_path(delivery_id: int, tag: str) -> str:
    directory = os.path.join(_data_dir(), "tmp")
    storage.ensure_dir(directory)
    return os.path.join(directory, storage.temp_name(f"d{int(delivery_id)}-{tag}-"))


def _stream_to_staging(file_storage, staging: str, limit: int | None = None):
    """把上传流写入暂存文件，返回 (sha256, 字节数)。

    limit 为允许的最大字节数：超出即停止读取，避免为超限上传耗尽磁盘。
    """
    digest = hashlib.sha256()
    total = 0
    try:
        with open(staging, "wb") as out:
            while True:
                block = file_storage.stream.read(_chunk())
                if not block:
                    break
                total += len(block)
                if limit is not None and total > limit:
                    break
                out.write(block)
                digest.update(block)
            out.flush()
            os.fsync(out.fileno())
    except OSError as exc:
        raise HandoverError(f"写入暂存文件失败：{exc.strerror or exc}", 507) from exc
    return digest.hexdigest(), total


def _safe_replace(source: str, dest: str) -> None:
    try:
        storage.ensure_dir(os.path.dirname(dest))
        storage.replace_file(source, dest)
        storage.fsync_dir(os.path.dirname(dest))
    except storage.StorageError as exc:
        raise HandoverError(str(exc), 507) from exc


def accept_shard(delivery_id: int, index: int, file_storage) -> dict:
    """验收单片：先核验长度与 SHA-256 再入账；失败不影响已验收片。"""
    conn = db.get_db()
    delivery = _delivery(conn, delivery_id)
    manifest = _manifest_of(delivery)
    spec = manifest.by_index.get(int(index))
    if spec is None:
        raise HandoverError(f"片号 {index} 不在清单内", 404)
    if file_storage is None or not file_storage.filename:
        raise HandoverError("缺少上传文件", 400)

    row = conn.execute(
        "SELECT accepted_at, source FROM shards WHERE delivery_id = ? AND shard_index = ?",
        (int(delivery_id), spec.index),
    ).fetchone()
    if row is not None and row["accepted_at"] is not None:
        return _reuse_result(row, spec.index, "该片此前已验收，沿用已有记录")

    staging = _staging_path(delivery_id, f"shard{spec.index}")
    try:
        actual_sha, total = _stream_to_staging(file_storage, staging, limit=spec.size)
        if total != spec.size:
            raise HandoverError(
                f"片 {spec.index} 字节数不符：清单 {spec.size}，实收 {total}",
                422,
            )
        if actual_sha != spec.sha256:
            raise HandoverError(
                f"片 {spec.index} SHA-256 不符：清单 {spec.sha256}，实收 {actual_sha}",
                422,
            )

        final_path = storage.part_path(_data_dir(), delivery_id, spec.index)
        now = db.utcnow()
        with db.write_tx(conn):
            current = conn.execute(
                """SELECT accepted_at, source FROM shards
                   WHERE delivery_id = ? AND shard_index = ?""",
                (int(delivery_id), spec.index),
            ).fetchone()
            if current is not None and current["accepted_at"] is not None:
                return _reuse_result(
                    current, spec.index, "该片已由并发的另一次提交完成，沿用已有记录"
                )
            _safe_replace(staging, final_path)
            staging = None
            conn.execute(
                """UPDATE shards
                   SET stored_size = ?, stored_sha256 = ?, source = 'upload', accepted_at = ?
                   WHERE delivery_id = ? AND shard_index = ?""",
                (spec.size, actual_sha, now, int(delivery_id), spec.index),
            )
            conn.execute(
                "UPDATE deliveries SET updated_at = ? WHERE id = ?",
                (now, int(delivery_id)),
            )
        return {
            "status": "accepted",
            "index": spec.index,
            "source": "upload",
            "reused": False,
            "message": f"片 {spec.index} 已验收",
        }
    finally:
        if staging is not None:
            storage.remove_file(staging)


def _reuse_result(row, index: int, message: str) -> dict:
    return {
        "status": "already-accepted",
        "index": index,
        "source": row["source"],
        "reused": True,
        "message": message,
    }


def _part_sources(manifest: Manifest, data_dir: str, delivery_id: int, indices):
    return [
        (storage.part_path(data_dir, delivery_id, i), manifest.by_index[i].size)
        for i in indices
    ]


def recover_missing(delivery_id: int, index: int, file_storage) -> dict:
    """用校验片恢复唯一缺片；与实交并发时只完成一次。"""
    conn = db.get_db()
    delivery = _delivery(conn, delivery_id)
    manifest = _manifest_of(delivery)
    spec = manifest.by_index.get(int(index))
    if spec is None:
        raise HandoverError(f"片号 {index} 不在清单内", 404)
    if file_storage is None or not file_storage.filename:
        raise HandoverError("缺少校验片文件", 400)

    rows = _shard_rows(conn, delivery_id)
    accepted_indices = [r["shard_index"] for r in rows if r["accepted_at"] is not None]
    missing = [r["shard_index"] for r in rows if r["accepted_at"] is None]
    if not missing:
        raise HandoverError("清单内所有片均已验收，无需恢复", 409)
    if len(missing) != 1:
        raise HandoverError(
            f"当前缺 {len(missing)} 片（{', '.join(map(str, missing))}），"
            "仅当恰好缺一片时才可用校验片恢复",
            409,
        )
    if missing[0] != spec.index:
        raise HandoverError(f"应恢复缺片 {missing[0]}，而非片 {spec.index}", 409)

    staging = _staging_path(delivery_id, f"checkshard{spec.index}")
    recovered_tmp = None
    try:
        # 1. 校验片落暂存，长度必须等于最长片长度。
        _upload_sha, upload_size = _stream_to_staging(
            file_storage, staging, limit=manifest.max_size
        )
        if upload_size != manifest.max_size:
            raise HandoverError(
                f"校验片长度应为 {manifest.max_size} 字节（最长片长度），实收 {upload_size}",
                422,
            )

        # 2. 校验片 ⊕ 其余已验收片 = 缺片；按清单长度截断并核验 SHA-256。
        data_dir = _data_dir()
        recovered_tmp = _staging_path(delivery_id, f"recovered{spec.index}")
        try:
            rec_size, rec_sha = codec.recover_part(
                staging,
                manifest.max_size,
                _part_sources(manifest, data_dir, delivery_id, accepted_indices),
                spec.size,
                recovered_tmp,
                chunk_size=_chunk(),
            )
        except storage.StorageError as exc:
            raise HandoverError(str(exc), 507) from exc
        if rec_size != spec.size:
            raise HandoverError(
                f"恢复所得片长度 {rec_size} 与清单 {spec.size} 不符", 422
            )
        if rec_sha != spec.sha256:
            raise HandoverError(
                "校验片无法恢复出清单中的原片：恢复结果 SHA-256 不符"
                f"（得到 {rec_sha}，清单 {spec.sha256}），请核对校验片与已验收片",
                422,
            )

        # 3. 写锁内复核：若实交已先完成，则恢复不写入、沿用已有记录。
        final_path = storage.part_path(data_dir, delivery_id, spec.index)
        now = db.utcnow()
        with db.write_tx(conn):
            current = conn.execute(
                """SELECT accepted_at, source FROM shards
                   WHERE delivery_id = ? AND shard_index = ?""",
                (int(delivery_id), spec.index),
            ).fetchone()
            still_missing = conn.execute(
                """SELECT COUNT(*) AS n FROM shards
                   WHERE delivery_id = ? AND accepted_at IS NULL""",
                (int(delivery_id),),
            ).fetchone()["n"]
            if current is not None and current["accepted_at"] is not None:
                return _reuse_result(
                    current,
                    spec.index,
                    "该片已由实交完成，本次恢复未写入，沿用已有记录",
                )
            if still_missing != 1:
                raise HandoverError("缺片状态已变化，未完成恢复，请刷新后重试", 409)
            _safe_replace(recovered_tmp, final_path)
            recovered_tmp = None
            conn.execute(
                """UPDATE shards
                   SET stored_size = ?, stored_sha256 = ?, source = 'recovered',
                       accepted_at = ?
                   WHERE delivery_id = ? AND shard_index = ?""",
                (spec.size, rec_sha, now, int(delivery_id), spec.index),
            )
            conn.execute(
                """UPDATE deliveries
                   SET updated_at = ?, recovered_index = ?, recovered_at = ?
                   WHERE id = ?""",
                (now, spec.index, now, int(delivery_id)),
            )
        return {
            "status": "recovered",
            "index": spec.index,
            "source": "recovered",
            "reused": False,
            "used_recovery": True,
            "message": f"片 {spec.index} 已由校验片恢复并通过哈希核验",
        }
    finally:
        if staging is not None:
            storage.remove_file(staging)
        if recovered_tmp is not None:
            storage.remove_file(recovered_tmp)


def _download_snapshot(delivery_id: int):
    """读取受理时的交付状态快照：清单、各片验收情况、已验收片集合。

    下载/区间取件都只在本次请求内使用这份快照；此后并发上传或恢复
    不会改变本次的返回结果。
    """
    conn = db.get_db()
    delivery = _delivery(conn, delivery_id)
    manifest = _manifest_of(delivery)
    rows = _shard_rows(conn, delivery_id)
    accepted = {r["shard_index"] for r in rows if r["accepted_at"] is not None}
    return conn, delivery, manifest, rows, accepted


def _count_download(conn, delivery_id: int) -> None:
    now = db.utcnow()
    with db.write_tx(conn):
        conn.execute(
            """UPDATE deliveries
               SET download_count = download_count + 1, last_download_at = ?
               WHERE id = ?""",
            (now, int(delivery_id)),
        )


def open_download(delivery_id: int):
    """拼接下载：集齐后才开放；按片号升序、固定块大小流式输出。"""
    conn, delivery, manifest, rows, accepted = _download_snapshot(delivery_id)
    missing = [r["shard_index"] for r in rows if r["shard_index"] not in accepted]
    if missing:
        raise HandoverError(
            f"尚缺 {len(missing)} 片（{', '.join(map(str, missing))}），暂不能下载",
            409,
        )

    chunk = _chunk()
    data_dir = _data_dir()
    # 快照内各片路径固定；生成器只在本次请求内读取。
    plan = [
        (storage.part_path(data_dir, delivery_id, row["shard_index"]), manifest.by_index[row["shard_index"]].size)
        for row in rows
    ]

    def generate():
        for path, size in plan:
            yield from codec.iter_part_slice(path, 0, size, chunk)

    _count_download(conn, delivery_id)
    return delivery, manifest, generate


def delivery_total(delivery_id: int) -> int:
    """清单总长度（字节）。用于区间校验与 416 的 Content-Range。"""
    conn = db.get_db()
    delivery = _delivery(conn, delivery_id)
    return _manifest_of(delivery).total_bytes


def _range_plan(manifest: Manifest, accepted: set[int], start: int, end: int):
    """把半开区间 [start,end) 映射为各片的读取切片，并找出阻挡的缺片。"""
    plan = []
    blocked = []
    position = 0
    for spec in manifest.shards:
        shard_start = position
        shard_end = position + spec.size
        position = shard_end
        if shard_end <= start or shard_start >= end:
            continue
        if spec.index not in accepted:
            blocked.append((spec.index, max(start, shard_start)))
            continue
        slice_start = max(start, shard_start) - shard_start
        slice_end = min(end, shard_end) - shard_start
        plan.append((spec.index, slice_start, slice_end - slice_start))
    return plan, blocked


def open_range(delivery_id: int, start: int, end: int):
    """按清单总长度取半开区间 [start,end) 的原始字节。

    以受理时的验收快照为准：仅当区间覆盖到的片全部已验收时才返回 206 与
    Content-Range；碰到缺片则返回 409，携带缺片号与最早受阻偏移，不返回正文。
    非法区间与缺片都不会增加下载次数。
    """
    conn, delivery, manifest, _rows, accepted = _download_snapshot(delivery_id)
    total = manifest.total_bytes

    if start < 0 or end <= start or end > total:
        raise HandoverError(
            f"字节区间 [{start}, {end}) 非法：须满足 0 ≤ start < end ≤ {total}",
            416,
            extra={"start": start, "end": end, "total_bytes": total},
        )

    plan, blocked = _range_plan(manifest, accepted, start, end)
    if blocked:
        missing_indices = sorted({index for index, _offset in blocked})
        first_index, blocked_offset = blocked[0]
        raise HandoverError(
            f"区间 [{start}, {end}) 碰到缺片 {first_index}，"
            f"最早受阻偏移 {blocked_offset}，本次不返回正文",
            409,
            extra={
                "start": start,
                "end": end,
                "total_bytes": total,
                "missing": missing_indices,
                "missing_shards": missing_indices,
                "missing_shard": first_index,
                "missing_index": first_index,
                "blocked_offset": blocked_offset,
                "first_blocked_offset": blocked_offset,
            },
        )

    chunk = _chunk()
    data_dir = _data_dir()
    # 快照内已验收片的路径与切片长度在此固定，生成器不再查库。
    sources = [
        (storage.part_path(data_dir, delivery_id, index), offset, length)
        for index, offset, length in plan
    ]

    def generate():
        for path, offset, length in sources:
            yield from codec.iter_part_slice(path, offset, length, chunk)

    _count_download(conn, delivery_id)
    return delivery, manifest, generate, start, end
