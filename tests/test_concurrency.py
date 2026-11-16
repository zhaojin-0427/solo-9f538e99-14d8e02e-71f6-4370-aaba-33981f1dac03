"""Concurrency and crash-recovery guarantees."""
from __future__ import annotations

import io
import os
import socket
import threading

from app.db import connect, init_db
from app.ingestion import ingest_stream
from app.roads import build_road_layer
from tests.conftest import sample_bytes


def test_restart_recovers_interrupted_batch(db_path):
    # Simulate a crash: insert a processing batch with some object rows,
    # then reopen the database exactly as the application would on restart.
    conn = connect(db_path)
    cur = conn.execute(
        "INSERT INTO batches (filename, status) VALUES ('crashed.osc', 'processing')"
    )
    bid = cur.lastrowid
    conn.execute(
        "INSERT INTO object_versions (object_type, object_id, version, visible, "
        "lat, lon, tags_json, refs_json, batch_id) "
        "VALUES ('node', 999, 1, 1, 1.0, 2.0, '{}', NULL, ?)",
        (bid,),
    )
    conn.close()

    init_db(db_path)  # app startup hook

    conn2 = connect(db_path)
    batch = conn2.execute("SELECT * FROM batches WHERE id=?", (bid,)).fetchone()
    assert batch["status"] == "rejected"
    assert "rolled back" in (batch["error"] or "") or "回滚" in (batch["error"] or "")
    leftover = conn2.execute(
        "SELECT COUNT(*) AS n FROM object_versions WHERE batch_id=?", (bid,)
    ).fetchone()
    assert leftover["n"] == 0
    conn2.close()


def test_startup_hook_keeps_a_live_workers_in_flight_batch(db_path):
    """Another worker booting must not roll back a batch still being parsed."""
    conn = connect(db_path)
    cur = conn.execute(
        "INSERT INTO batches (filename, status, owner_host, owner_pid, owner_start) "
        "VALUES ('parsing.osc', 'processing', ?, ?, NULL)",
        (socket.gethostname(), os.getpid()),
    )
    bid = cur.lastrowid
    conn.close()

    init_db(db_path)  # this process is alive: the batch is not interrupted

    conn = connect(db_path)
    try:
        status = conn.execute(
            "SELECT status FROM batches WHERE id=?", (bid,)
        ).fetchone()["status"]
    finally:
        conn.close()
    assert status == "processing"


def test_startup_hook_rolls_back_a_dead_workers_batch(db_path):
    dead_pid = 2 ** 22 + 12345  # never a live process on Linux
    conn = connect(db_path)
    cur = conn.execute(
        "INSERT INTO batches (filename, status, owner_host, owner_pid, owner_start) "
        "VALUES ('dead.osc', 'processing', ?, ?, NULL)",
        (socket.gethostname(), dead_pid),
    )
    bid = cur.lastrowid
    conn.close()

    init_db(db_path)

    conn = connect(db_path)
    try:
        row = conn.execute("SELECT status, error FROM batches WHERE id=?", (bid,)).fetchone()
    finally:
        conn.close()
    assert row["status"] == "rejected"
    assert "rolled back" in row["error"]


def test_upload_reclaims_a_dead_workers_stale_batch(db_path):
    """A leftover row from a killed worker is cleared on the next upload."""
    dead_pid = 2 ** 22 + 54321
    conn = connect(db_path)
    cur = conn.execute(
        "INSERT INTO batches (filename, status, owner_host, owner_pid, owner_start) "
        "VALUES ('orphan.osc', 'processing', ?, ?, NULL)",
        (socket.gethostname(), dead_pid),
    )
    bid = cur.lastrowid
    conn.execute(
        "INSERT INTO object_versions (object_type, object_id, version, visible, "
        "lat, lon, tags_json, refs_json, batch_id) "
        "VALUES ('node', 777, 1, 1, 1.0, 2.0, '{}', NULL, ?)",
        (bid,),
    )
    try:
        ingest_stream(conn, "01_base_extract.osm", io.BytesIO(sample_bytes("01_base_extract.osm")))
        row = conn.execute("SELECT status FROM batches WHERE id=?", (bid,)).fetchone()
        assert row["status"] == "rejected"
        assert conn.execute(
            "SELECT COUNT(*) AS n FROM object_versions WHERE batch_id=?", (bid,)
        ).fetchone()["n"] == 0
        # The current upload is untouched and committed normally.
        assert conn.execute(
            "SELECT status FROM batches WHERE id=?", (bid + 1,)
        ).fetchone()["status"] == "committed"
    finally:
        conn.close()


def test_concurrent_uploads_all_commit_or_are_rejected_cleanly(db_path):
    errors = []

    def worker(filename):
        conn = connect(db_path)
        try:
            ingest_stream(conn, filename, io.BytesIO(sample_bytes(filename)))
        except Exception as exc:  # conflict batches are expected for some
            errors.append((filename, str(exc)))
        finally:
            conn.close()

    # Two identical batches racing: one commits, the other becomes duplicates
    # (still committed); plus an independent batch.
    threads = [
        threading.Thread(target=worker, args=("01_base_extract.osm",)),
        threading.Thread(target=worker, args=("01_base_extract.osm",)),
        threading.Thread(target=worker, args=("03_fill_gap_and_move_shared.osc",)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    conn = connect(db_path)
    statuses = conn.execute("SELECT status, COUNT(*) AS n FROM batches GROUP BY status").fetchall()
    counts = {r["status"]: r["n"] for r in statuses}
    # No batch may ever stay in the transitional 'processing' state.
    assert "processing" not in counts
    # Every stored object row belongs to a committed batch.
    orphan = conn.execute(
        "SELECT COUNT(*) AS n FROM object_versions o "
        "LEFT JOIN batches b ON b.id = o.batch_id "
        "WHERE b.status != 'committed'"
    ).fetchone()
    assert orphan["n"] == 0
    conn.close()


def test_export_while_writing_keeps_consistent_snapshot(db_path):
    # Seed batch 1.
    writer = connect(db_path)
    ingest_stream(writer, "01_base_extract.osm", io.BytesIO(sample_bytes("01_base_extract.osm")))
    writer.close()

    reader = connect(db_path)
    seen_during = []
    stop = threading.Event()

    def export_loop():
        while not stop.is_set():
            roads, pending = build_road_layer(reader, 1)
            seen_during.append((sorted(r.way_id for r in roads), len(pending)))

    t = threading.Thread(target=export_loop)
    t.start()

    writer = connect(db_path)
    for name in ["02_missing_node.osm", "03_fill_gap_and_move_shared.osc",
                 "04_delete_shared_node.osc", "06_way_reroute.osc"]:
        ingest_stream(writer, name, io.BytesIO(sample_bytes(name)))
    writer.close()
    stop.set()
    t.join()
    reader.close()

    # Batch 1's view never changes regardless of what writers committed.
    assert seen_during
    assert all(snapshot == ([100, 101], 0) for snapshot in seen_during)
