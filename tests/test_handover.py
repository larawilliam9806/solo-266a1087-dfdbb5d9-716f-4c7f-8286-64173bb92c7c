"""端到端测试：建包、验收、校验片恢复、并发与失败隔离。

运行：PYTHONPATH=<venv 或 vendor 解包目录> python3 -m pytest tests -q
无 pytest 时可用 python3 tests/test_handover.py 直接跑。
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
import threading
import unittest

from app import create_app
from app import codec
from app import service


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_manifest(parts: list[bytes], label: str = "测试断面") -> dict:
    return {
        "label": label,
        "shards": [
            {"index": i, "size": len(p), "sha256": sha(p)} for i, p in enumerate(parts)
        ],
    }


PART_SOURCE = "flow-section-source-2026-11-24"


class HandoverTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.app = create_app(
            {
                "DATA_DIR": self.tmp.name,
                "TESTING": True,
                "CHUNK_SIZE": 64,
            }
        )
        self.client = self.app.test_client()
        self.parts = [
            bytes(range(0, 200)),
            bytes(i % 256 for i in range(40, 300)),
            b"\x00\x01" * 33,  # 较短片，需要尾部补零
            bytes([255]) * 150,
        ]
        self.manifest = make_manifest(self.parts)
        self.label = self.manifest["label"]

    def tearDown(self):
        self.tmp.cleanup()

    # ---------------------------------------------------------------- helpers
    def create(self, manifest=None, name="manifest.json"):
        payload = manifest if manifest is not None else self.manifest
        return self.client.post(
            "/api/packages",
            data={"manifest": (io.BytesIO(json.dumps(payload).encode()), name)},
            content_type="multipart/form-data",
        )

    def upload(self, delivery_id, index, blob, expect=(201, 200)):
        response = self.client.put(
            f"/api/packages/{delivery_id}/shards/{index}",
            data={"file": (io.BytesIO(blob), f"shard-{index}.part")},
            content_type="multipart/form-data",
        )
        self.assertIn(response.status_code, expect, response.data)
        return response

    def upload_all_except(self, delivery_id, skip):
        for index, blob in enumerate(self.parts):
            if index == skip:
                continue
            self.upload(delivery_id, index, blob)

    def recover(self, delivery_id, index, checkshard, expect=(201, 200)):
        response = self.client.post(
            f"/api/packages/{delivery_id}/recover",
            data={
                "index": str(index),
                "checkshard": (io.BytesIO(checkshard), "checkshard.bin"),
            },
            content_type="multipart/form-data",
        )
        self.assertIn(response.status_code, expect, response.data)
        return response

    def checkshard(self):
        return codec.checkshard_bytes(self.parts, max(len(p) for p in self.parts))

    # ---------------------------------------------------------------- tests
    def test_create_and_dedupe_manifest(self):
        first = self.create()
        self.assertEqual(first.status_code, 201, first.data)
        delivery_id = first.get_json()["id"]
        self.assertTrue(first.get_json()["created"])

        again = self.create(name="renamed.json")
        self.assertEqual(again.status_code, 200)
        self.assertEqual(again.get_json()["id"], delivery_id)
        self.assertFalse(again.get_json()["created"])
        self.assertTrue(again.get_json()["reused"])

    def test_manifest_lists_missing_and_blocks_download(self):
        delivery_id = self.create().get_json()["id"]
        self.upload(delivery_id, 0, self.parts[0])
        state = self.client.get(f"/api/packages/{delivery_id}").get_json()
        self.assertEqual(state["missing"], [1, 2, 3])
        self.assertFalse(state["complete"])
        self.assertIsNone(state["download_url"])
        blocked = self.client.get(f"/api/packages/{delivery_id}/download")
        self.assertEqual(blocked.status_code, 409)

    def test_manifest_rejects_bad_entries(self):
        bad = {"label": "x", "shards": [{"index": 0, "size": 3, "sha256": "zz"}]}
        response = self.create(bad)
        self.assertEqual(response.status_code, 422)
        body = response.get_json()
        self.assertIn("SHA-256", body["error"] + " ".join(body.get("details", [])))

        duplicate = make_manifest(self.parts[:2])
        duplicate["shards"][1]["index"] = 0
        response = self.create(duplicate)
        self.assertEqual(response.status_code, 422)

    def test_full_upload_and_ordered_download(self):
        delivery_id = self.create().get_json()["id"]
        self.upload_all_except(delivery_id, skip=None)
        state = self.client.get(f"/api/packages/{delivery_id}").get_json()
        self.assertTrue(state["complete"])
        self.assertEqual(state["missing"], [])

        response = self.client.get(f"/api/packages/{delivery_id}/download")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"".join(self.parts))
        self.assertEqual(response.headers["Content-Length"], str(sum(map(len, self.parts))))

    def test_recovery_of_missing_tail_shard(self):
        delivery_id = self.create().get_json()["id"]
        self.upload_all_except(delivery_id, skip=3)

        recovered = self.recover(delivery_id, 3, self.checkshard())
        self.assertEqual(recovered.get_json()["status"], "recovered")
        state = recovered.get_json()["state"]
        self.assertTrue(state["complete"])
        self.assertEqual(state["recovered_index"], 3)
        self.assertTrue(state["used_recovery"])
        self.assertEqual(state["shards"][3]["source"], "recovered")

        response = self.client.get(f"/api/packages/{delivery_id}/download")
        self.assertEqual(response.data, b"".join(self.parts))

    def test_recovery_of_short_middle_shard_truncates(self):
        delivery_id = self.create().get_json()["id"]
        self.upload_all_except(delivery_id, skip=2)
        recovered = self.recover(delivery_id, 2, self.checkshard())
        self.assertEqual(recovered.status_code, 201)
        state = recovered.get_json()["state"]
        self.assertEqual(state["recovered_index"], 2)
        response = self.client.get(f"/api/packages/{delivery_id}/download")
        self.assertEqual(response.data, b"".join(self.parts))

    def test_recovery_only_when_exactly_one_missing(self):
        delivery_id = self.create().get_json()["id"]
        self.upload(delivery_id, 0, self.parts[0])
        response = self.recover(delivery_id, 1, self.checkshard(), expect=(409,))
        self.assertIn("恰好缺一片", response.get_json()["error"])

    def test_bad_checkshard_keeps_accepted_shards(self):
        delivery_id = self.create().get_json()["id"]
        self.upload_all_except(delivery_id, skip=1)
        corrupt = bytearray(self.checkshard())
        corrupt[0] ^= 0xFF
        response = self.recover(delivery_id, 1, bytes(corrupt), expect=(422,))
        self.assertIn("SHA-256", response.get_json()["error"])

        state = self.client.get(f"/api/packages/{delivery_id}").get_json()
        self.assertEqual(state["missing"], [1])
        self.assertEqual([s["accepted"] for s in state["shards"]], [True, False, True, True])

        good = self.recover(delivery_id, 1, self.checkshard())
        self.assertEqual(good.get_json()["status"], "recovered")

    def test_wrong_size_checkshard_rejected(self):
        delivery_id = self.create().get_json()["id"]
        self.upload_all_except(delivery_id, skip=1)
        response = self.recover(delivery_id, 1, self.checkshard()[:-1], expect=(422,))
        self.assertIn("校验片长度", response.get_json()["error"])

    def test_oversized_checkshard_rejected(self):
        delivery_id = self.create().get_json()["id"]
        self.upload_all_except(delivery_id, skip=1)
        response = self.recover(
            delivery_id, 1, self.checkshard() + b"tail", expect=(422,)
        )
        self.assertIn("校验片长度", response.get_json()["error"])

    def test_recovery_target_must_be_the_only_missing(self):
        delivery_id = self.create().get_json()["id"]
        self.upload(delivery_id, 0, self.parts[0])
        response = self.recover(delivery_id, 0, self.checkshard(), expect=(409,))
        self.assertIn("恰好缺一片", response.get_json()["error"])

    def test_manifest_endpoint_returns_normalized_listing(self):
        delivery_id = self.create().get_json()["id"]
        response = self.client.get(f"/api/packages/{delivery_id}/manifest")
        self.assertEqual(response.status_code, 200)
        doc = json.loads(response.get_data(as_text=True))
        self.assertEqual([s["index"] for s in doc["shards"]], [0, 1, 2, 3])
        self.assertEqual(
            [s["size"] for s in doc["shards"]], [len(p) for p in self.parts]
        )
        self.assertEqual(
            [s["sha256"] for s in doc["shards"]], [sha(p) for p in self.parts]
        )

    def test_bad_shard_hash_keeps_previous_shards(self):
        delivery_id = self.create().get_json()["id"]
        self.upload(delivery_id, 0, self.parts[0])
        bad = b"X" * len(self.parts[1])
        response = self.upload(delivery_id, 1, bad, expect=(422,))
        self.assertIn("SHA-256", response.get_json()["error"])

        state = self.client.get(f"/api/packages/{delivery_id}").get_json()
        self.assertTrue(state["shards"][0]["accepted"])
        self.assertFalse(state["shards"][1]["accepted"])

        self.upload(delivery_id, 1, self.parts[1])
        state = self.client.get(f"/api/packages/{delivery_id}").get_json()
        self.assertTrue(state["shards"][1]["accepted"])

    def test_oversized_shard_rejected_without_writing(self):
        delivery_id = self.create().get_json()["id"]
        response = self.upload(
            delivery_id, 0, self.parts[0] + b"extra", expect=(422,)
        )
        self.assertIn("字节数", response.get_json()["error"])
        state = self.client.get(f"/api/packages/{delivery_id}").get_json()
        self.assertFalse(state["shards"][0]["accepted"])

    def test_reupload_is_idempotent(self):
        delivery_id = self.create().get_json()["id"]
        first = self.upload(delivery_id, 0, self.parts[0], expect=(201,))
        self.assertEqual(first.get_json()["status"], "accepted")
        second = self.upload(delivery_id, 0, self.parts[0], expect=(200,))
        body = second.get_json()
        self.assertTrue(body["reused"])
        self.assertEqual(body["status"], "already-accepted")
        self.assertEqual(body["source"], "upload")
        self.assertEqual(body["state"]["accepted_count"], 1)

    def test_reupload_same_manifest_reuses_delivery_record(self):
        delivery_id = self.create().get_json()["id"]
        self.upload(delivery_id, 0, self.parts[0])
        again = self.create()
        again_id = again.get_json()["id"]
        self.assertEqual(again_id, delivery_id)
        state = again.get_json()
        self.assertEqual(state["accepted_count"], 1)
        self.assertFalse(state["created"])

    def test_recovery_and_upload_race_completes_once(self):
        delivery_id = self.create().get_json()["id"]
        self.upload_all_except(delivery_id, skip=3)
        checkshard = self.checkshard()

        results: list[tuple[str, int, dict]] = []
        barrier = threading.Barrier(2)
        lock = threading.Lock()

        def do_upload():
            barrier.wait()
            response = self.client.put(
                f"/api/packages/{delivery_id}/shards/3",
                data={"file": (io.BytesIO(self.parts[3]), "shard-3.part")},
                content_type="multipart/form-data",
            )
            with lock:
                results.append(("upload", response.status_code, response.get_json()))

        def do_recover():
            barrier.wait()
            response = self.client.post(
                f"/api/packages/{delivery_id}/recover",
                data={
                    "index": "3",
                    "checkshard": (io.BytesIO(checkshard), "checkshard.bin"),
                },
                content_type="multipart/form-data",
            )
            with lock:
                results.append(
                    ("recover", response.status_code, response.get_json() or {})
                )

        threads = [threading.Thread(target=do_upload), threading.Thread(target=do_recover)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(results), 2)
        writes = [
            r for r in results if r[1] in (200, 201) and not r[2].get("reused")
        ]
        self.assertEqual(len(writes), 1, results)

        state = self.client.get(f"/api/packages/{delivery_id}").get_json()
        self.assertTrue(state["complete"])
        self.assertEqual(state["accepted_count"], 4)
        final_source = state["shards"][3]["source"]
        self.assertEqual(
            state["used_recovery"], final_source == "recovered", state
        )
        if final_source == "recovered":
            self.assertEqual(state["recovered_index"], 3)
        response = self.client.get(f"/api/packages/{delivery_id}/download")
        self.assertEqual(response.data, b"".join(self.parts))

    def test_record_notes_recovery_index(self):
        delivery_id = self.create().get_json()["id"]
        self.upload_all_except(delivery_id, skip=0)
        self.recover(delivery_id, 0, self.checkshard())
        listing = self.client.get("/api/packages").get_json()["packages"]
        record = next(p for p in listing if p["id"] == delivery_id)
        self.assertEqual(record["recovered_index"], 0)
        self.assertTrue(record["complete"])

    def test_codec_matches_naive_xor(self):
        naive = bytearray(max(len(p) for p in self.parts))
        for part in self.parts:
            for i, value in enumerate(part):
                naive[i] ^= value
        self.assertEqual(codec.checkshard_bytes(self.parts, len(naive)), bytes(naive))

    def test_page_renders_and_shows_missing_indices(self):
        delivery_id = self.create().get_json()["id"]
        self.upload(delivery_id, 0, self.parts[0])
        page = self.client.get(f"/packages/{delivery_id}")
        self.assertEqual(page.status_code, 200)
        html = page.get_data(as_text=True)
        self.assertIn("尚缺", html)
        self.assertIn("1, 2, 3", html)
        self.assertNotIn("按片号升序拼接下载", html)

    def test_html_upload_flow(self):
        delivery_id = self.create().get_json()["id"]
        page = self.client.post(
            f"/packages/{delivery_id}/shards/0",
            data={"file": (io.BytesIO(self.parts[0]), "p0.part")},
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        self.assertEqual(page.status_code, 200)
        self.assertIn("已验收", page.get_data(as_text=True))

    # ------------------------------------------------------- 按字节区间取件
    def _offsets(self):
        offsets = []
        position = 0
        for part in self.parts:
            offsets.append(position)
            position += len(part)
        return offsets

    def _download_count(self, delivery_id):
        page = self.client.get(f"/packages/{delivery_id}").get_data(as_text=True)
        marker = "本包累计下载 "
        start = page.index(marker) + len(marker)
        return int(page[start : page.index(" 次", start)])

    def test_range_serves_accepted_bytes_from_incomplete_package(self):
        delivery_id = self.create().get_json()["id"]
        self.upload(delivery_id, 0, self.parts[0])  # 200 字节
        self.upload(delivery_id, 1, self.parts[1])  # 260 字节
        offsets = self._offsets()
        start = offsets[0] + 150  # 片 0 尾部
        end = offsets[1] + 40  # 片 1 内
        response = self.client.get(
            f"/api/packages/{delivery_id}/download?start={start}&end={end}"
        )
        self.assertEqual(response.status_code, 206, response.data)
        self.assertEqual(response.data, b"".join(self.parts)[start:end])
        self.assertEqual(
            response.headers["Content-Range"],
            f"bytes {start}-{end - 1}/{sum(map(len, self.parts))}",
        )
        self.assertEqual(response.headers["Content-Length"], str(end - start))
        self.assertEqual(response.headers["Accept-Ranges"], "bytes")

    def test_range_range_header_is_inclusive(self):
        delivery_id = self.create().get_json()["id"]
        self.upload_all_except(delivery_id, skip=None)
        blob = b"".join(self.parts)
        response = self.client.get(
            f"/api/packages/{delivery_id}/download",
            headers={"Range": "bytes=10-19"},
        )
        self.assertEqual(response.status_code, 206, response.data)
        self.assertEqual(response.data, blob[10:20])
        self.assertEqual(
            response.headers["Content-Range"], f"bytes 10-19/{len(blob)}"
        )

    def test_range_suffix_header(self):
        delivery_id = self.create().get_json()["id"]
        self.upload_all_except(delivery_id, skip=None)
        blob = b"".join(self.parts)
        response = self.client.get(
            f"/api/packages/{delivery_id}/download",
            headers={"Range": "bytes=-25"},
        )
        self.assertEqual(response.status_code, 206, response.data)
        self.assertEqual(response.data, blob[-25:])
        self.assertEqual(
            response.headers["Content-Range"],
            f"bytes {len(blob) - 25}-{len(blob) - 1}/{len(blob)}",
        )

    def test_range_blocked_by_missing_shard_returns_no_body(self):
        delivery_id = self.create().get_json()["id"]
        self.upload(delivery_id, 0, self.parts[0])
        offsets = self._offsets()
        start = offsets[1]  # 片 1 缺
        end = offsets[2] + 10
        before = self._download_count(delivery_id)
        response = self.client.get(
            f"/api/packages/{delivery_id}/download?start={start}&end={end}"
        )
        self.assertEqual(response.status_code, 409, response.data)
        body = response.get_json()
        self.assertEqual(body["missing_shard"], 1)
        self.assertEqual(body["missing"], [1, 2])
        self.assertEqual(body["blocked_offset"], offsets[1])
        self.assertEqual(body["start"], start)
        self.assertEqual(body["end"], end)
        self.assertIsNone(body.get("data"))
        self.assertEqual(
            response.headers["X-Missing-Shard"], "1"
        )
        self.assertEqual(response.headers["X-Blocked-Offset"], str(offsets[1]))
        # 缺片不增加下载次数，也不返回正文。
        self.assertEqual(response.mimetype, "application/json")
        self.assertEqual(self._download_count(delivery_id), before)

    def test_range_blocked_offset_starts_inside_range(self):
        delivery_id = self.create().get_json()["id"]
        self.upload(delivery_id, 0, self.parts[0])
        offsets = self._offsets()
        # 从片 1 中间开始，最早受阻偏移就是请求起点。
        start = offsets[1] + 5
        response = self.client.get(
            f"/api/packages/{delivery_id}/download?start={start}&end={offsets[1] + 30}"
        )
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["blocked_offset"], start)

    def test_invalid_ranges_rejected_without_counting(self):
        delivery_id = self.create().get_json()["id"]
        self.upload(delivery_id, 0, self.parts[0])
        total = sum(map(len, self.parts))
        before = self._download_count(delivery_id)
        for query in (
            "start=10&end=10",
            "start=20&end=10",
            "start=-1&end=5",
            f"start=0&end={total + 1}",
            "start=abc&end=5",
            "start=0",
            "end=5",
        ):
            response = self.client.get(
                f"/api/packages/{delivery_id}/download?{query}"
            )
            self.assertEqual(response.status_code, 416, (query, response.data))
            body = response.get_json()
            self.assertIn("total_bytes", body)
            self.assertEqual(response.headers["Content-Range"], f"bytes */{total}")
        for raw in ("bytes=5-1", "bytes=abc", "bytes=", f"bytes={total}-"):
            response = self.client.get(
                f"/api/packages/{delivery_id}/download", headers={"Range": raw}
            )
            self.assertEqual(response.status_code, 416, (raw, response.data))
            self.assertEqual(response.headers["Content-Range"], f"bytes */{total}")
        self.assertEqual(self._download_count(delivery_id), before)

    def test_range_header_clamps_past_end(self):
        delivery_id = self.create().get_json()["id"]
        self.upload_all_except(delivery_id, skip=None)
        blob = b"".join(self.parts)
        response = self.client.get(
            f"/api/packages/{delivery_id}/download",
            headers={"Range": f"bytes=600-{len(blob) + 500}"},
        )
        self.assertEqual(response.status_code, 206, response.data)
        self.assertEqual(response.data, blob[600:])
        self.assertEqual(
            response.headers["Content-Range"],
            f"bytes 600-{len(blob) - 1}/{len(blob)}",
        )

    def test_successful_range_counts_one_download(self):
        delivery_id = self.create().get_json()["id"]
        self.upload_all_except(delivery_id, skip=None)
        before = self._download_count(delivery_id)
        ok = self.client.get(
            f"/api/packages/{delivery_id}/download?start=0&end=5"
        )
        self.assertEqual(ok.status_code, 206)
        self.assertEqual(self._download_count(delivery_id), before + 1)

    def test_range_on_detail_page_returns_raw_bytes(self):
        delivery_id = self.create().get_json()["id"]
        self.upload(delivery_id, 0, self.parts[0])
        response = self.client.get(f"/packages/{delivery_id}?start=0&end=12")
        self.assertEqual(response.status_code, 206, response.data)
        self.assertEqual(response.data, self.parts[0][:12])
        # 不带区间参数仍是详情页。
        page = self.client.get(f"/packages/{delivery_id}")
        self.assertEqual(page.status_code, 200)
        self.assertIn("按字节区间取件", page.get_data(as_text=True))

    def test_range_uses_acceptance_snapshot(self):
        """受理快照之后完成的验收不会改变本次结果。"""
        delivery_id = self.create().get_json()["id"]
        self.upload(delivery_id, 0, self.parts[0])
        offsets = self._offsets()
        start, end = offsets[1], offsets[1] + len(self.parts[1])

        original = service._shard_rows
        snapshot_taken = threading.Event()
        release = threading.Event()

        def slow_rows(conn, target_id):
            rows = original(conn, target_id)
            snapshot_taken.set()
            release.wait(5)
            return rows

        service._shard_rows = slow_rows
        try:
            result = {}

            def request_range():
                result["response"] = self.client.get(
                    f"/api/packages/{delivery_id}/download?start={start}&end={end}"
                )

            thread = threading.Thread(target=request_range)
            thread.start()
            self.assertTrue(snapshot_taken.wait(5))
            # 快照已取：此时补交缺片 1。
            self.upload(delivery_id, 1, self.parts[1])
            release.set()
            thread.join(10)
        finally:
            service._shard_rows = original

        response = result["response"]
        self.assertEqual(response.status_code, 409, response.data)
        self.assertEqual(response.get_json()["missing_shard"], 1)

        # 之后的请求按新状态返回成功。
        again = self.client.get(
            f"/api/packages/{delivery_id}/download?start={start}&end={end}"
        )
        self.assertEqual(again.status_code, 206, again.data)
        self.assertEqual(again.data, self.parts[1])

    def test_range_streams_in_fixed_chunks(self):
        delivery_id = self.create().get_json()["id"]
        self.upload_all_except(delivery_id, skip=None)
        chunk = self.app.config["CHUNK_SIZE"]
        with self.app.app_context():
            _delivery, _manifest, stream, start, end = service.open_range(
                delivery_id, 0, sum(map(len, self.parts))
            )
            blocks = list(stream())
        self.assertEqual(b"".join(blocks), b"".join(self.parts))
        self.assertGreater(len(blocks), 1)
        self.assertTrue(all(len(block) <= chunk for block in blocks))

    def test_range_shard_offsets_exposed_in_state(self):
        delivery_id = self.create().get_json()["id"]
        state = self.client.get(f"/api/packages/{delivery_id}").get_json()
        self.assertEqual(
            [s["offset"] for s in state["shards"]], self._offsets()
        )
        self.assertTrue(state["range_supported"])
        self.assertEqual(
            state["range_download_url"],
            f"/api/packages/{delivery_id}/download",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
