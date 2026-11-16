"""HTTP behaviour while a batch is still being parsed.

Covers the user-visible half of the parallel-upload fix: the processing
batch is listed under its permanent id, its detail URL works immediately,
and another upload / an export of an already committed batch is served
without waiting for the slow parse.
"""
from __future__ import annotations

import io
import threading
import time

import pytest

from app import ingestion
from tests.conftest import sample_bytes


class ParseGate:
    """Wrap ``parse_osm`` so the first call blocks until released."""

    def __init__(self, blocked_name_marker: bytes):
        self.entered = threading.Event()
        self.release = threading.Event()
        self._marker = blocked_name_marker
        self._lock = threading.Lock()
        self._blocked_once = False
        self._real = ingestion.parse_osm

    def __call__(self, stream):
        # The web layer reads the upload into BytesIO before parsing, so we
        # can inspect the payload to decide which request is the slow one.
        head = stream.getvalue()[:4096]
        block = self._marker in head
        if block:
            with self._lock:
                first = not self._blocked_once
                self._blocked_once = True
            if first:
                self.entered.set()
                if not self.release.wait(20):
                    raise TimeoutError("test never released the slow parse")
        return self._real(stream)


def _upload(client, name):
    return client.post(
        "/upload",
        data={"osm_file": (io.BytesIO(sample_bytes(name)), name)},
        content_type="multipart/form-data",
        follow_redirects=True,
    )


def test_processing_batch_is_listed_with_working_detail_link(app, monkeypatch):
    gate = ParseGate(b"31.23520")
    monkeypatch.setattr(ingestion, "parse_osm", gate)
    client = app.test_client()

    # Committed batch B, created before the slow one.
    _upload(client, "01_base_extract.osm")

    slow = app.test_client()
    thread_result = {}

    def slow_upload():
        thread_result["resp"] = _upload(slow, "03_fill_gap_and_move_shared.osc")

    thread = threading.Thread(target=slow_upload)
    thread.start()
    assert gate.entered.wait(10)

    listing = client.get("/").data.decode()
    assert "处理中" in listing
    assert "/batches/2" in listing          # permanent detail link, id fixed
    detail = client.get("/batches/2").data.decode()
    assert "解析" in detail and "固定" in detail
    # No export yet for a batch whose commit has not landed.
    assert client.get("/batches/2/geojson").status_code == 409

    # Meanwhile batch B is fully usable: its snapshot is already fixed.
    assert client.get("/batches/1/geojson").status_code == 200

    gate.release.set()
    thread.join(timeout=20)
    assert thread_result["resp"].status_code == 200

    listing = client.get("/").data.decode()
    assert "已提交" in listing
    assert client.get("/batches/2").status_code == 200
    assert client.get("/batches/2/geojson").status_code == 200
    # Still the same id: the slow batch was never renumbered.
    assert "批次 #2" in listing or "#2" in listing


def test_export_and_new_upload_are_not_blocked_by_slow_parse(app, monkeypatch):
    gate = ParseGate(b"31.23520")
    monkeypatch.setattr(ingestion, "parse_osm", gate)

    setup = app.test_client()
    _upload(setup, "01_base_extract.osm")

    slow_client = app.test_client()
    slow_thread = threading.Thread(
        target=lambda: _upload(slow_client, "03_fill_gap_and_move_shared.osc")
    )
    slow_thread.start()
    assert gate.entered.wait(10)

    other = app.test_client()
    started = time.monotonic()
    resp = _upload(other, "02_missing_node.osm")
    elapsed = time.monotonic() - started
    body = resp.data.decode()
    assert resp.status_code == 200
    assert "已整体生效" in body
    # The slow upload reserved id 2, so the fast one is id 3 -- its id
    # follows start order, not commit order.
    assert "/batches/3" in body
    # The fast batch committed while the slow one was still parsing.
    assert gate.release.is_set() is False
    assert elapsed < 10

    export_started = time.monotonic()
    exported = other.get("/batches/3/geojson")
    assert exported.status_code == 200
    assert time.monotonic() - export_started < 10

    gate.release.set()
    slow_thread.join(timeout=20)

    # The slow batch (id 2) commits after the fast one (id 3): it must see
    # the fast batch, and the fast batch's view must not be affected.
    fast_before = exported.data
    committed = other.get("/batches/2").data.decode()
    assert "已提交" in committed
    assert "102" in committed  # the fast batch's road is in slow's snapshot
    assert other.get("/batches/3/geojson").data == fast_before
