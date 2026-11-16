"""Snapshot ordering under parallel uploads, and legacy-volume upgrade.

The regression these tests pin down: batch ids are reserved when an upload
*starts*, so a slow upload A can have a lower id than a fast upload B that
commits first.  A's later commit must not alter B's snapshot, and A's own
snapshot must contain B because B was already committed when A finished.
"""
from __future__ import annotations

import io
import json
import sqlite3
import threading
import time

import pytest

from app.db import connect, init_db
from app.ingestion import ingest_stream
from app.roads import batch_summary, build_road_layer

from tests.conftest import sample_bytes


class BlockingStream(io.BytesIO):
    """A byte stream whose first ``read`` waits until released.

    ``ElementTree.parse`` pulls bytes from here, which lets a test hold a
    batch in the parsing phase for as long as it needs while other batches
    run to completion.
    """

    def __init__(self, data: bytes, started: threading.Event, release: threading.Event):
        super().__init__(data)
        self._started = started
        self._release = release
        self._blocked = False

    def read(self, *args, **kwargs):  # noqa: D401 - stdlib signature
        if not self._blocked:
            self._blocked = True
            self._started.set()
            if not self._release.wait(20):
                raise TimeoutError("test never released the slow stream")
        return super().read(*args, **kwargs)


def _wait_for_batch(db_path, filename, timeout=10.0) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        conn = connect(db_path)
        try:
            row = conn.execute(
                "SELECT id FROM batches WHERE filename = ? ORDER BY id DESC LIMIT 1",
                (filename,),
            ).fetchone()
        finally:
            conn.close()
        if row is not None:
            return row["id"]
        time.sleep(0.01)
    raise AssertionError(f"batch {filename} never appeared")


def _snapshot(db_path, batch_id):
    conn = connect(db_path)
    try:
        roads, pending = build_road_layer(conn, batch_id)
        summary = batch_summary(conn, batch_id)
    finally:
        conn.close()
    return (
        sorted((r.way_id, r.version, tuple(map(tuple, r.coordinates))) for r in roads),
        sorted((p.way_id, tuple(p.missing_node_ids)) for p in pending),
        summary,
    )


SLOW_A = b"""<?xml version="1.0"?>
<osmChange version="0.6">
  <create>
    <node id="6" version="1" lat="31.23440" lon="121.47770"/>
  </create>
  <modify>
    <node id="3" version="3" lat="31.29999" lon="121.49999"/>
    <way id="102" version="1">
      <nd ref="5"/><nd ref="6"/><nd ref="9"/>
      <tag k="highway" v="service"/>
      <tag k="name" v="pending lane"/>
    </way>
  </modify>
</osmChange>"""


def test_slow_lower_id_batch_does_not_change_earlier_committed_batch(db_path):
    """A (id 2, slow parse) commits after B (id 3).

    A moves shared node n3 and references n9, which only B supplies.  B must
    keep its published geometry after A commits, and A's own snapshot must
    contain B's already committed n9 -- the original bug made A miss it.
    """
    started = threading.Event()
    release = threading.Event()
    errors = []

    # Seed a committed base batch (id 1).
    writer = connect(db_path)
    try:
        base = ingest_stream(
            writer, "01_base_extract.osm", io.BytesIO(sample_bytes("01_base_extract.osm"))
        )
    finally:
        writer.close()

    def slow_upload():
        conn = connect(db_path)
        try:
            ingest_stream(
                conn, "slow_id2.osc", BlockingStream(SLOW_A, started, release)
            )
        except Exception as exc:  # pragma: no cover - failure detail
            errors.append(exc)
        finally:
            conn.close()

    slow = threading.Thread(target=slow_upload)
    slow.start()
    assert started.wait(10)

    slow_batch_id = _wait_for_batch(db_path, "slow_id2.osc")
    assert slow_batch_id == 2

    # B: a fast batch started later but commits first.
    writer = connect(db_path)
    try:
        fast = ingest_stream(
            writer,
            "03_fill_gap_and_move_shared.osc",
            io.BytesIO(sample_bytes("03_fill_gap_and_move_shared.osc")),
        )
    finally:
        writer.close()
    assert fast.batch_id == 3

    b_before = _snapshot(db_path, fast.batch_id)
    assert [r[0] for r in b_before[0]] == [100, 101]

    # While A is still parsing, the listing shows both, and B is exportable
    # and stable without waiting for A.
    reader = connect(db_path)
    try:
        listed = reader.execute("SELECT id, status FROM batches ORDER BY id").fetchall()
        assert [(r["id"], r["status"]) for r in listed] == [
            (1, "committed"),
            (2, "processing"),
            (3, "committed"),
        ]
        roads_while_parsing, _ = build_road_layer(reader, fast.batch_id)
        assert [r.way_id for r in roads_while_parsing] == [100, 101]
    finally:
        reader.close()

    release.set()
    slow.join(timeout=20)
    assert not errors, errors
    assert slow_batch_id == 2  # never renumbered

    # A's snapshot: itself + everything committed before it (base and B).
    # B's n9 is present, so A's w102 is a complete road; n3 is A's v3.
    a_snapshot = _snapshot(db_path, slow_batch_id)
    assert [r[0] for r in a_snapshot[0]] == [100, 101, 102]
    assert a_snapshot[1] == []
    w100_a = next(r for r in a_snapshot[0] if r[0] == 100)
    assert (121.49999, 31.29999) in w100_a[2]
    assert a_snapshot[2]["node_visible"] == 7  # 5 base + n6 + B's n9

    # B's snapshot is exactly what it was before A committed: A's later
    # lower-id commit must not move B's n3 or add A's objects.
    assert _snapshot(db_path, fast.batch_id) == b_before
    w100_b = next(r for r in b_before[0] if r[0] == 100)
    assert (121.47565, 31.23225) in w100_b[2]
    assert (121.49999, 31.29999) not in w100_b[2]
    assert b_before[2]["node_visible"] == 6  # 5 base + B's n9
    assert b_before[2]["way_visible"] == 3


def test_batch_still_processing_when_another_commits_is_excluded(db_path):
    """A snapshot contains only itself and batches committed before it."""
    started = threading.Event()
    release = threading.Event()
    errors = []

    # Seed a committed base batch.
    writer = connect(db_path)
    try:
        base = ingest_stream(
            writer, "01_base_extract.osm", io.BytesIO(sample_bytes("01_base_extract.osm"))
        )
    finally:
        writer.close()

    def late_upload():
        conn = connect(db_path)
        try:
            ingest_stream(
                conn,
                "late_missing_node.osm",
                BlockingStream(sample_bytes("02_missing_node.osm"), started, release),
            )
        except Exception as exc:  # pragma: no cover
            errors.append(exc)
        finally:
            conn.close()

    late = threading.Thread(target=late_upload)
    late.start()
    assert started.wait(10)
    late_id = _wait_for_batch(db_path, "late_missing_node.osm")
    assert late_id == 2

    # Commit a third batch while the second is still parsing.
    writer = connect(db_path)
    try:
        reroute = ingest_stream(
            writer, "06_way_reroute.osc", io.BytesIO(sample_bytes("06_way_reroute.osc"))
        )
    finally:
        writer.close()

    # Batch 3 committed while batch 2 was processing: batch 2 must not be in
    # its snapshot (its rows are not in the warehouse yet), and batch 2's id
    # stays reserved.
    # w101 is rerouted onto n6/n9, which no committed batch supplied, so the
    # rerouted way is pending in batch 3 and must not appear as a road.
    snap3 = _snapshot(db_path, reroute.batch_id)
    assert [r[0] for r in snap3[0]] == [100]
    assert [(p[0], p[1]) for p in snap3[1]] == [(101, (6, 9))]

    release.set()
    late.join(timeout=20)
    assert not errors, errors
    assert late_id == 2

    # Batch 2 saw base + batch 3 (which committed before it) + itself, but
    # not a batch that had not committed.  Batch 3's reroute of w101 points
    # at n9, which is not supplied by any of those batches, so w101 is
    # pending here; batch 2's own n6 does resolve.
    snap2 = _snapshot(db_path, late_id)
    assert [r[0] for r in snap2[0]] == [100]
    assert [(p[0], p[1]) for p in snap2[1]] == [(101, (9,)), (102, (9,))]

    # Batch 3's view is unchanged by batch 2's later commit.
    assert _snapshot(db_path, reroute.batch_id) == snap3


def test_rejected_batch_never_enters_any_snapshot_and_has_no_commit_seq(db_path):
    writer = connect(db_path)
    try:
        b1 = ingest_stream(
            writer, "01_base_extract.osm", io.BytesIO(sample_bytes("01_base_extract.osm"))
        )
    finally:
        writer.close()

    writer = connect(db_path)
    try:
        with pytest.raises(Exception):
            ingest_stream(
                writer,
                "08_same_version_conflict.osc",
                io.BytesIO(sample_bytes("08_same_version_conflict.osc")),
            )
    finally:
        writer.close()

    writer = connect(db_path)
    try:
        b3 = ingest_stream(
            writer, "03_fill_gap_and_move_shared.osc",
            io.BytesIO(sample_bytes("03_fill_gap_and_move_shared.osc")),
        )
    finally:
        writer.close()

    conn = connect(db_path)
    try:
        rows = conn.execute(
            "SELECT id, status, commit_seq FROM batches ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    assert [(r["id"], r["status"]) for r in rows] == [
        (1, "committed"), (2, "rejected"), (3, "committed"),
    ]
    assert rows[1]["commit_seq"] is None
    # Commit sequence is strictly increasing in commit order.
    assert rows[0]["commit_seq"] < rows[2]["commit_seq"]

    # The rejected batch contributes nothing; batch 3 still sees exactly
    # batch 1 + itself.
    snap3 = _snapshot(db_path, b3.batch_id)
    assert [r[0] for r in snap3[0]] == [100, 101]
    assert snap3[1] == []


def test_restart_keeps_snapshots_and_does_not_renumber(db_path):
    writer = connect(db_path)
    try:
        b1 = ingest_stream(
            writer, "01_base_extract.osm", io.BytesIO(sample_bytes("01_base_extract.osm"))
        )
        b2 = ingest_stream(
            writer, "02_missing_node.osm", io.BytesIO(sample_bytes("02_missing_node.osm"))
        )
    finally:
        writer.close()

    before = {b1.batch_id: _snapshot(db_path, b1.batch_id),
              b2.batch_id: _snapshot(db_path, b2.batch_id)}

    init_db(db_path)  # simulates a container restart over the same volume

    for bid, snap in before.items():
        assert _snapshot(db_path, bid) == snap

    writer = connect(db_path)
    try:
        b3 = ingest_stream(
            writer, "03_fill_gap_and_move_shared.osc",
            io.BytesIO(sample_bytes("03_fill_gap_and_move_shared.osc")),
        )
    finally:
        writer.close()
    assert b3.batch_id == 3
    for bid, snap in before.items():
        assert _snapshot(db_path, bid) == snap


LEGACY_SCHEMA = """
PRAGMA journal_mode = WAL;
CREATE TABLE batches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    filename    TEXT    NOT NULL,
    status      TEXT    NOT NULL CHECK (status IN ('processing', 'committed', 'rejected')),
    error       TEXT,
    created_at  TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE TABLE object_versions (
    object_type  TEXT    NOT NULL CHECK (object_type IN ('node', 'way')),
    object_id    INTEGER NOT NULL,
    version      INTEGER NOT NULL,
    visible      INTEGER NOT NULL,
    lat          REAL,
    lon          REAL,
    tags_json    TEXT    NOT NULL DEFAULT '{}',
    refs_json    TEXT,
    batch_id     INTEGER NOT NULL REFERENCES batches(id),
    PRIMARY KEY (object_type, object_id, version)
) WITHOUT ROWID;
CREATE INDEX idx_object_versions_batch ON object_versions (batch_id);
CREATE INDEX idx_batches_status ON batches (status, id);
"""


def _legacy_snapshot(legacy_path, batch_id):
    """The pre-upgrade ``batch_id <= B`` snapshot algorithm, kept for parity.

    Mirrors the old ``roads.build_road_layer`` / ``batch_summary`` exactly
    (per-object highest version among ``batch_id <= B``), including
    coordinates, pending ways and object statistics.
    """
    conn = sqlite3.connect(str(legacy_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT o.* FROM object_versions o
            JOIN (
                SELECT object_type, object_id, MAX(version) AS mv
                FROM object_versions WHERE batch_id <= ?
                GROUP BY object_type, object_id
            ) m ON o.object_type=m.object_type AND o.object_id=m.object_id
                 AND o.version=m.mv
            WHERE o.batch_id <= ?
            """,
            (batch_id, batch_id),
        ).fetchall()
        summary_rows = conn.execute(
            """
            SELECT object_type,
                   SUM(CASE WHEN visible = 1 THEN 1 ELSE 0 END) AS visible_count,
                   SUM(CASE WHEN visible = 0 THEN 1 ELSE 0 END) AS deleted_count
            FROM object_versions
            WHERE (object_type, object_id, version) IN (
                SELECT object_type, object_id, MAX(version)
                FROM object_versions WHERE batch_id <= ?
                GROUP BY object_type, object_id
            )
            GROUP BY object_type
            """,
            (batch_id,),
        ).fetchall()
    finally:
        conn.close()

    objects = {(r["object_type"], r["object_id"]): r for r in rows}
    roads, pending = [], []
    for (obj_type, obj_id), r in sorted(objects.items()):
        if obj_type != "way" or not r["visible"]:
            continue
        tags = json.loads(r["tags_json"])
        if "highway" not in tags:
            continue
        refs = json.loads(r["refs_json"]) if r["refs_json"] else []
        coordinates, missing = [], []
        for node_id in refs:
            node = objects.get(("node", node_id))
            if node is None or not node["visible"]:
                missing.append(node_id)
            else:
                coordinates.append([node["lon"], node["lat"]])
        if missing or len(coordinates) < 2:
            if not missing and len(coordinates) < 2:
                missing = list(refs)
            pending.append((obj_id, r["version"], tuple(missing)))
        else:
            roads.append((obj_id, r["version"], tuple(map(tuple, coordinates))))

    summary = {"node_visible": 0, "node_deleted": 0, "way_visible": 0, "way_deleted": 0}
    for row in summary_rows:
        prefix = "node" if row["object_type"] == "node" else "way"
        summary[f"{prefix}_visible"] = row["visible_count"]
        summary[f"{prefix}_deleted"] = row["deleted_count"]
    return sorted(roads), sorted(pending), summary


def _build_legacy_volume(tmp_path):
    """Create a DB the way the previous release left it on a mounted volume."""
    new_path = tmp_path / "new.db"
    init_db(new_path)
    conn = connect(new_path)
    try:
        for name in [
            "01_base_extract.osm",
            "02_missing_node.osm",
            "03_fill_gap_and_move_shared.osc",
            "04_delete_shared_node.osc",
            "06_way_reroute.osc",
        ]:
            ingest_stream(conn, name, io.BytesIO(sample_bytes(name)))
        batches = conn.execute("SELECT * FROM batches ORDER BY id").fetchall()
        objects = conn.execute("SELECT * FROM object_versions").fetchall()
    finally:
        conn.close()

    legacy_path = tmp_path / "legacy.db"
    legacy = sqlite3.connect(str(legacy_path))
    try:
        legacy.executescript(LEGACY_SCHEMA)
        for b in batches:
            legacy.execute(
                "INSERT INTO batches (id, filename, status, error, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (b["id"], b["filename"], b["status"], b["error"], b["created_at"]),
            )
        for o in objects:
            legacy.execute(
                "INSERT INTO object_versions (object_type, object_id, version, "
                "visible, lat, lon, tags_json, refs_json, batch_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    o["object_type"], o["object_id"], o["version"], o["visible"],
                    o["lat"], o["lon"], o["tags_json"], o["refs_json"], o["batch_id"],
                ),
            )
        legacy.commit()
    finally:
        legacy.close()
    return legacy_path, len(batches)


def test_legacy_volume_upgrades_in_place_and_keeps_old_batch_views(tmp_path):
    legacy_path, batch_count = _build_legacy_volume(tmp_path)
    assert batch_count == 5

    # What users saw before the upgrade, computed with the old algorithm.
    expected = {bid: _legacy_snapshot(legacy_path, bid) for bid in range(1, batch_count + 1)}

    conn = sqlite3.connect(str(legacy_path))
    cols_before = {r[1] for r in conn.execute("PRAGMA table_info(batches)")}
    conn.close()
    assert "commit_seq" not in cols_before  # genuinely the pre-upgrade schema

    init_db(legacy_path)  # the startup hook the new image runs

    conn = connect(legacy_path)
    try:
        rows = conn.execute(
            "SELECT id, status, commit_seq FROM batches ORDER BY id"
        ).fetchall()
        assert len(rows) == batch_count
        # Legacy committed batches backfill to their id: the old
        # ``batch_id <= B`` view is reproduced exactly.
        assert [(r["id"], r["commit_seq"]) for r in rows] == [
            (bid, bid) for bid in range(1, batch_count + 1)
        ]
        # Every stored object row survived the upgrade.
        n_objects = conn.execute("SELECT COUNT(*) AS n FROM object_versions").fetchone()["n"]
        assert n_objects > 0
    finally:
        conn.close()

    for bid in range(1, batch_count + 1):
        assert expected[bid][0], f"legacy batch {bid} should have roads"

    # Rebuild through the application code and compare, per old batch, the
    # complete visible result: roads (ids, versions, coordinates), pending
    # ways and object statistics must match what the old algorithm showed.
    conn = connect(legacy_path)
    try:
        for bid, (legacy_roads, legacy_pending, legacy_summary) in expected.items():
            roads, pending = build_road_layer(conn, bid)
            upgraded_roads = sorted(
                (r.way_id, r.version, tuple(map(tuple, r.coordinates))) for r in roads
            )
            upgraded_pending = sorted(
                (p.way_id, p.version, tuple(p.missing_node_ids)) for p in pending
            )
            assert upgraded_roads == legacy_roads, f"batch {bid} roads changed by upgrade"
            assert upgraded_pending == legacy_pending, f"batch {bid} pending changed"
            assert batch_summary(conn, bid) == legacy_summary, f"batch {bid} stats changed"
        # Batch links/ids are untouched: detail and export routes still work.
        assert conn.execute("SELECT filename FROM batches WHERE id=2").fetchone()[0] == \
            "02_missing_node.osm"

        # New uploads after the upgrade extend the volume without changing
        # any legacy snapshot.
        b_new = ingest_stream(
            conn, "09_recreate_node_higher_version.osc",
            io.BytesIO(sample_bytes("09_recreate_node_higher_version.osc")),
        )
        assert b_new.batch_id == batch_count + 1
        assert conn.execute(
            "SELECT commit_seq FROM batches WHERE id=?", (b_new.batch_id,)
        ).fetchone()["commit_seq"] > batch_count
        for bid, (legacy_roads, legacy_pending, legacy_summary) in expected.items():
            roads, pending = build_road_layer(conn, bid)
            assert sorted(
                (r.way_id, r.version, tuple(map(tuple, r.coordinates))) for r in roads
            ) == legacy_roads
            assert sorted(
                (p.way_id, p.version, tuple(p.missing_node_ids)) for p in pending
            ) == legacy_pending
            assert batch_summary(conn, bid) == legacy_summary
    finally:
        conn.close()
