"""页面与 JSON 接口。"""

from __future__ import annotations

import re

from flask import (
    Blueprint,
    Response,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from . import db, service
from .manifest import ManifestError

main_bp = Blueprint("main", __name__)
api_bp = Blueprint("api", __name__, url_prefix="/api")

_RANGE_HEADER_RE = re.compile(r"^bytes=(\d*)-(\d*)$", re.IGNORECASE)


def _wants_json() -> bool:
    if request.path.startswith("/api/"):
        return True
    best = request.accept_mimetypes.best
    return (
        request.is_json
        or best == "application/json"
        or request.headers.get("X-Requested-With") == "XMLHttpRequest"
    )


def _error_response(exc: service.HandoverError):
    if _wants_json():
        payload = {"error": exc.message}
        if exc.details:
            payload["details"] = exc.details
        if exc.extra:
            payload.update(exc.extra)
        response = jsonify(payload)
        response.status_code = exc.status
        if exc.status == 416 and "total_bytes" in exc.extra:
            response.headers["Content-Range"] = (
                f"bytes */{int(exc.extra['total_bytes'])}"
            )
        return response
    return render_template("error.html", code=exc.status, message=exc.message), exc.status


def _manifest_upload():
    upload = request.files.get("manifest")
    if upload is not None and upload.filename:
        data = upload.read(current_app.config["MAX_MANIFEST_BYTES"] + 1)
        if len(data) > current_app.config["MAX_MANIFEST_BYTES"]:
            raise service.HandoverError("清单超过 4 MiB，拒绝处理", 413)
        return data, upload.filename
    text = request.form.get("manifest_text")
    if text and text.strip():
        payload = text.encode("utf-8")
        if len(payload) > current_app.config["MAX_MANIFEST_BYTES"]:
            raise service.HandoverError("清单超过 4 MiB，拒绝处理", 413)
        return payload, "inline-manifest.json"
    raise service.HandoverError("请上传清单文件或粘贴清单内容", 400)


def _state_json(state: dict) -> dict:
    delivery = state["delivery"]
    manifest = state["manifest"]
    target = None
    if state["recovery_allowed"]:
        for shard in state["shards"]:
            if shard["index"] == state["missing"][0]:
                target = {
                    "index": shard["index"],
                    "size": shard["size"],
                    "sha256": shard["sha256"],
                }
                break
    return {
        "id": delivery["id"],
        "label": delivery["label"],
        "manifest_name": delivery["manifest_name"],
        "manifest_hash": delivery["manifest_hash"],
        "created_at": delivery["created_at"],
        "updated_at": delivery["updated_at"],
        "total_shards": state["total_count"],
        "total_bytes": manifest.total_bytes,
        "accepted_count": state["accepted_count"],
        "accepted_bytes": state["accepted_bytes"],
        "complete": state["complete"],
        "recovery_allowed": state["recovery_allowed"],
        "missing": state["missing"],
        "checkshard": {
            "length": manifest.max_size,
            "base_shard": manifest.longest_indices[0] if manifest.longest_indices else None,
        },
        "recovery_target": target,
        "recovered_index": delivery["recovered_index"],
        "used_recovery": delivery["recovered_index"] is not None,
        "download_ready": state["complete"],
        "download_url": url_for("api.download_package", delivery_id=delivery["id"])
        if state["complete"]
        else None,
        # 详情页/接口都按清单总长度接收半开区间 [start,end)。
        "range_download_url": url_for(
            "api.download_package", delivery_id=delivery["id"]
        ),
        "range_supported": True,
        "range_endpoint": url_for(
            "main.package_detail", delivery_id=delivery["id"]
        ),
        "range_parameters": "start/end（半开区间，单位字节）",
        "shards": [
            {
                "index": s["index"],
                "size": s["size"],
                "offset": s["offset"],
                "sha256": s["sha256"],
                "accepted": s["accepted"],
                "source": s["source"],
                "accepted_at": s["accepted_at"],
            }
            for s in state["shards"]
        ],
    }


# ---------------------------------------------------------------- 页面


@main_bp.get("/healthz")
def healthz():
    try:
        conn = db.get_db()
        conn.execute("SELECT 1").fetchone()
    except Exception:  # noqa: BLE001 - 健康检查只关心可用性
        return jsonify({"status": "degraded", "database": "unavailable"}), 503
    return jsonify({"status": "ok", "database": "ok"})


@main_bp.get("/")
def index():
    conn = db.get_db()
    rows = conn.execute(
        """SELECT d.*,
                  (SELECT COUNT(*) FROM shards s
                    WHERE s.delivery_id = d.id AND s.accepted_at IS NULL) AS missing_count
           FROM deliveries d
           ORDER BY d.updated_at DESC, d.id DESC
           LIMIT 200"""
    ).fetchall()
    return render_template("index.html", deliveries=[dict(r) for r in rows])


@main_bp.post("/packages")
def create_package_page():
    try:
        manifest_bytes, filename = _manifest_upload()
        delivery_id, created = service.create_delivery(manifest_bytes, filename)
    except ManifestError as exc:
        if _wants_json():
            return jsonify({"error": exc.message, "details": exc.details}), 422
        for detail in (exc.details or [exc.message])[:10]:
            flash(detail, "error")
        return redirect(url_for("main.index"))
    except service.HandoverError as exc:
        if _wants_json():
            return _error_response(exc)
        flash(exc.message, "error")
        return redirect(url_for("main.index"))

    if _wants_json():
        state = service.delivery_state(delivery_id)
        payload = _state_json(state)
        payload["created"] = created
        payload["reused"] = not created
        payload["status"] = "created" if created else "existing"
        return jsonify(payload), (201 if created else 200)
    flash(
        "交接包已建立" if created else "同一份清单已有交付记录，已沿用原记录",
        "ok",
    )
    return redirect(url_for("main.package_detail", delivery_id=delivery_id))


@main_bp.get("/packages/<int:delivery_id>")
def package_detail(delivery_id: int):
    # 详情页同时承担按字节区间取件：带 start/end 查询参数时返回原始字节。
    # （HTTP Range 头只在下载地址上生效，避免浏览器加载页面时被误判。）
    if "start" in request.args or "end" in request.args:
        return _download_response(delivery_id)
    try:
        state = service.delivery_state(delivery_id)
    except service.HandoverError as exc:
        return _error_response(exc)
    return render_template("package.html", state=state)


@main_bp.post("/packages/<int:delivery_id>/shards/<int:index>")
def upload_shard_page(delivery_id: int, index: int):
    upload = request.files.get("file") or request.files.get("shard")
    try:
        result = service.accept_shard(delivery_id, index, upload)
    except service.HandoverError as exc:
        if _wants_json():
            return _error_response(exc)
        flash(exc.message, "error")
        return redirect(url_for("main.package_detail", delivery_id=delivery_id))
    if _wants_json():
        result["state"] = _state_json(service.delivery_state(delivery_id))
        return jsonify(result), (200 if result["reused"] else 201)
    flash(result["message"], "ok" if not result["reused"] else "info")
    return redirect(url_for("main.package_detail", delivery_id=delivery_id))


@main_bp.post("/packages/<int:delivery_id>/recover")
def recover_page(delivery_id: int):
    raw_index = request.form.get("index") or request.form.get("shard_index")
    upload = request.files.get("checkshard") or request.files.get("file")
    try:
        if raw_index is None or not str(raw_index).strip().isdigit():
            raise service.HandoverError("请指定要恢复的缺片号", 400)
        result = service.recover_missing(delivery_id, int(raw_index), upload)
    except service.HandoverError as exc:
        if _wants_json():
            return _error_response(exc)
        flash(exc.message, "error")
        return redirect(url_for("main.package_detail", delivery_id=delivery_id))
    if _wants_json():
        result["state"] = _state_json(service.delivery_state(delivery_id))
        return jsonify(result), (200 if result["reused"] else 201)
    flash(result["message"], "ok" if not result["reused"] else "info")
    return redirect(url_for("main.package_detail", delivery_id=delivery_id))


def _parse_range_header(value: str, total: int) -> tuple[int, int, bool]:
    """解析单段 HTTP Range，转成半开区间 (start, end, is_suffix)。

    HTTP 的 `bytes=a-b` 语义包含末字节 b，这里转换为半开区间 [a, b+1)。
    `bytes=-N` 表示末尾 N 字节，返回 (0, N, True)，由调用方按总长度换算。
    任何非法写法都以 416 拒绝，并附上清单总长度。
    """
    match = _RANGE_HEADER_RE.match(value.strip())
    if match is None:
        raise service.HandoverError(
            "字节区间格式应为 bytes=start-end 或 bytes=start- 或 bytes=-N",
            416,
            extra={"total_bytes": total},
        )
    raw_start, raw_end = match.group(1), match.group(2)
    if not raw_start and not raw_end:
        raise service.HandoverError(
            "字节区间缺少起止位置", 416, extra={"total_bytes": total}
        )
    if not raw_start:
        # bytes=-N：最后 N 字节。
        suffix = int(raw_end)
        if suffix <= 0:
            raise service.HandoverError(
                "末尾长度必须为正数", 416, extra={"total_bytes": total}
            )
        return 0, suffix, True
    start = int(raw_start)
    if start >= total:
        raise service.HandoverError(
            f"字节区间起点 {start} 超出清单总长度 {total}",
            416,
            extra={"start": start, "total_bytes": total},
        )
    if not raw_end:
        end = total
    else:
        # HTTP 语义：末尾越界按实际总长度截断。
        end = min(int(raw_end) + 1, total)
    if end <= start:
        raise service.HandoverError(
            f"字节区间 [{start}, {end}) 非法：结束必须大于开始",
            416,
            extra={"start": start, "end": end, "total_bytes": total},
        )
    return start, end, False


def _range_query():
    """取请求中的区间：优先 Range 头，其次 start/end 查询参数。"""
    header = request.headers.get("Range")
    if header:
        return header, None
    start = request.args.get("start")
    end = request.args.get("end")
    if start is not None or end is not None:
        return None, (start, end)
    return None, None


def _range_error_response(exc: service.HandoverError):
    """区间取件错误一律返回 JSON 元信息：缺片号与最早受阻偏移清晰可读。"""
    payload = {"error": exc.message}
    if exc.details:
        payload["details"] = exc.details
    payload.update(exc.extra)
    response = jsonify(payload)
    response.status_code = exc.status
    if exc.status == 416 and "total_bytes" in exc.extra:
        response.headers["Content-Range"] = f"bytes */{int(exc.extra['total_bytes'])}"
    if "missing_shard" in exc.extra:
        response.headers["X-Missing-Shard"] = str(int(exc.extra["missing_shard"]))
    if "blocked_offset" in exc.extra:
        response.headers["X-Blocked-Offset"] = str(int(exc.extra["blocked_offset"]))
    return response


def _download_response(delivery_id: int):
    try:
        total = service.delivery_total(delivery_id)
    except service.HandoverError as exc:
        return _error_response(exc)

    header, query = _range_query()
    if header is not None:
        try:
            start, end, is_suffix = _parse_range_header(header, total)
        except service.HandoverError as exc:
            return _range_error_response(exc)
        if is_suffix:
            start = max(0, total - end)
            end = total
            if start == end:
                return _range_error_response(
                    service.HandoverError(
                        "交付包为空", 416, extra={"total_bytes": 0}
                    )
                )
    elif query is not None:
        raw_start, raw_end = query
        try:
            if raw_start is None or raw_end is None:
                raise ValueError
            start = int(str(raw_start).strip())
            end = int(str(raw_end).strip())
        except (TypeError, ValueError):
            return _range_error_response(
                service.HandoverError(
                    "查询参数 start 与 end 必须是整数（半开区间）",
                    416,
                    extra={"total_bytes": total},
                )
            )
    else:
        return _full_download_response(delivery_id)

    try:
        _delivery, manifest, stream, start, end = service.open_range(
            delivery_id, start, end
        )
    except service.HandoverError as exc:
        return _range_error_response(exc)
    total = manifest.total_bytes
    response = Response(stream(), status=206, mimetype="application/octet-stream")
    response.headers["Content-Range"] = f"bytes {start}-{end - 1}/{total}"
    response.headers["Content-Length"] = str(end - start)
    response.headers["Accept-Ranges"] = "bytes"
    response.headers["Content-Disposition"] = (
        f'attachment; filename="flow-section-{delivery_id}-{start}-{end}.bin"'
    )
    response.headers["Cache-Control"] = "no-store"
    return response


def _full_download_response(delivery_id: int):
    try:
        _delivery, manifest, stream = service.open_download(delivery_id)
    except service.HandoverError as exc:
        return _error_response(exc)
    response = Response(stream(), mimetype="application/octet-stream")
    response.headers["Content-Disposition"] = (
        f'attachment; filename="flow-section-{delivery_id}.bin"'
    )
    response.headers["Content-Length"] = str(manifest.total_bytes)
    response.headers["Accept-Ranges"] = "bytes"
    response.headers["X-Shard-Order"] = ",".join(map(str, manifest.indices))
    response.headers["Cache-Control"] = "no-store"
    return response


@main_bp.get("/packages/<int:delivery_id>/download")
def download_page(delivery_id: int):
    return _download_response(delivery_id)


# ---------------------------------------------------------------- JSON API


@api_bp.post("/packages")
def create_package_api():
    try:
        manifest_bytes, filename = _manifest_upload()
        delivery_id, created = service.create_delivery(manifest_bytes, filename)
    except ManifestError as exc:
        return jsonify({"error": exc.message, "details": exc.details}), 422
    except service.HandoverError as exc:
        return _error_response(exc)
    payload = _state_json(service.delivery_state(delivery_id))
    payload["created"] = created
    payload["reused"] = not created
    return jsonify(payload), (201 if created else 200)


@api_bp.get("/packages")
def list_packages_api():
    conn = db.get_db()
    rows = conn.execute(
        """SELECT d.*,
                  (SELECT COUNT(*) FROM shards s
                    WHERE s.delivery_id = d.id AND s.accepted_at IS NULL) AS missing_count
           FROM deliveries d ORDER BY d.id"""
    ).fetchall()
    return jsonify(
        {
            "packages": [
                {
                    "id": r["id"],
                    "label": r["label"],
                    "manifest_hash": r["manifest_hash"],
                    "total_shards": r["total_shards"],
                    "accepted_shards": r["total_shards"] - r["missing_count"],
                    "missing_count": r["missing_count"],
                    "complete": r["missing_count"] == 0,
                    "recovered_index": r["recovered_index"],
                    "created_at": r["created_at"],
                    "updated_at": r["updated_at"],
                }
                for r in rows
            ]
        }
    )


@api_bp.get("/packages/<int:delivery_id>")
def package_detail_api(delivery_id: int):
    try:
        return jsonify(_state_json(service.delivery_state(delivery_id)))
    except service.HandoverError as exc:
        return _error_response(exc)


@api_bp.put("/packages/<int:delivery_id>/shards/<int:index>")
@api_bp.post("/packages/<int:delivery_id>/shards/<int:index>")
def upload_shard_api(delivery_id: int, index: int):
    upload = request.files.get("file") or request.files.get("shard")
    try:
        result = service.accept_shard(delivery_id, index, upload)
    except service.HandoverError as exc:
        return _error_response(exc)
    result["state"] = _state_json(service.delivery_state(delivery_id))
    return jsonify(result), (200 if result["reused"] else 201)


@api_bp.post("/packages/<int:delivery_id>/recover")
def recover_api(delivery_id: int):
    raw_index = request.form.get("index") or request.form.get("shard_index")
    if raw_index is None:
        payload = request.get_json(silent=True) or {}
        raw_index = payload.get("index")
    upload = request.files.get("checkshard") or request.files.get("file")
    try:
        if raw_index is None or not str(raw_index).strip().isdigit():
            raise service.HandoverError("请指定要恢复的缺片号", 400)
        result = service.recover_missing(delivery_id, int(raw_index), upload)
    except service.HandoverError as exc:
        return _error_response(exc)
    result["state"] = _state_json(service.delivery_state(delivery_id))
    return jsonify(result), (200 if result["reused"] else 201)


@api_bp.get("/packages/<int:delivery_id>/download")
def download_package(delivery_id: int):
    return _download_response(delivery_id)


@api_bp.get("/packages/<int:delivery_id>/manifest")
def manifest_api(delivery_id: int):
    try:
        state = service.delivery_state(delivery_id)
    except service.HandoverError as exc:
        return _error_response(exc)
    return Response(
        state["delivery"]["manifest_json"],
        mimetype="application/json",
        headers={
            "Content-Disposition": (
                f'attachment; filename="manifest-{delivery_id}.json"'
            )
        },
    )
