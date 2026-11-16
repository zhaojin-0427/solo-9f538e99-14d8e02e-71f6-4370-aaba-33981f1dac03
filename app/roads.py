"""Reconstruct the road layer as it was when any given batch was complete.

``object_versions`` is append-only and every committed batch has a unique,
strictly increasing ``commit_seq`` assigned at commit time.  The state *as
of* batch B is obtained by taking, for every object, the highest version
among rows whose batch carries ``commit_seq <= B.commit_seq``.

That is deliberately commit order rather than batch-id order: ids are
reserved when an upload *starts*, so a slow-parsing batch may have a lower
id but commit after another batch.  Anchoring on ``commit_seq`` gives each
batch exactly the warehouse state at the instant it committed -- itself plus
every batch committed before it, and nothing that was still parsing at that
moment.  Consequences:

* a batch still parsing (or later rejected) never appears in any snapshot;
* a later commit can never retroactively change an earlier batch's geometry,
  pending list or statistics, no matter which id it was given;
* a shared node moving in batch 5 changes every affected way's geometry in
  batch 5's export even when the way itself is never re-uploaded;
* the same way's geometry in the batch 3 export is still built from the
  node coordinates fixed by batch 3;
* a deleted referenced node (or one not yet committed) makes the whole way
  pending until a later batch supplies a visible version;
* once a way's references change, its old node ids no longer influence it.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

HIGHWAY_TAG = "highway"


@dataclass
class RoadFeature:
    way_id: int
    version: int
    batch_id: int
    highway: str
    name: str | None
    coordinates: list[list[float]]
    tags: dict[str, str]


@dataclass
class PendingWay:
    way_id: int
    version: int
    batch_id: int
    highway: str
    missing_node_ids: list[int]


def _batch_commit_seq(conn: sqlite3.Connection, batch_id: int) -> int | None:
    row = conn.execute(
        "SELECT commit_seq FROM batches WHERE id = ?", (batch_id,)
    ).fetchone()
    return None if row is None else row["commit_seq"]


def _current_objects(conn: sqlite3.Connection, commit_seq: int) -> dict[tuple, sqlite3.Row]:
    """Return each object's highest-version state committed up to ``commit_seq``."""
    rows = conn.execute(
        """
        SELECT o.*
        FROM object_versions o
        JOIN batches b ON b.id = o.batch_id
        JOIN (
            SELECT ov.object_type, ov.object_id, MAX(ov.version) AS max_version
            FROM object_versions ov
            JOIN batches ob ON ob.id = ov.batch_id
            WHERE ob.commit_seq <= ? AND ob.status = 'committed'
            GROUP BY ov.object_type, ov.object_id
        ) m
          ON o.object_type = m.object_type
         AND o.object_id = m.object_id
         AND o.version = m.max_version
        WHERE b.commit_seq <= ? AND b.status = 'committed'
        """,
        (commit_seq, commit_seq),
    ).fetchall()
    return {(row["object_type"], row["object_id"]): row for row in rows}


def build_road_layer(conn: sqlite3.Connection, batch_id: int):
    """Build (roads, pending_ways) for the committed batch ``batch_id``.

    Returns ``([], [])`` for a batch that never committed; callers that need
    to distinguish "rejected" from "empty" check the batch status first.
    """
    seq = _batch_commit_seq(conn, batch_id)
    if seq is None:
        return [], []
    objects = _current_objects(conn, seq)
    roads: list[RoadFeature] = []
    pending: list[PendingWay] = []

    for (obj_type, obj_id), row in sorted(objects.items()):
        if obj_type != "way" or not row["visible"]:
            continue
        tags = json.loads(row["tags_json"])
        if HIGHWAY_TAG not in tags:
            continue  # only highway ways form the road layer

        refs = json.loads(row["refs_json"]) if row["refs_json"] is not None else []
        coordinates: list[list[float]] = []
        missing: list[int] = []
        for node_id in refs:
            node = objects.get(("node", node_id))
            if node is None or not node["visible"]:
                missing.append(node_id)
            else:
                coordinates.append([node["lon"], node["lat"]])

        if missing or len(coordinates) < 2:
            # Whole way waits until every referenced node is present.
            if not missing and len(coordinates) < 2:
                missing = list(refs)
            pending.append(PendingWay(
                way_id=obj_id,
                version=row["version"],
                batch_id=row["batch_id"],
                highway=tags[HIGHWAY_TAG],
                missing_node_ids=missing,
            ))
        else:
            roads.append(RoadFeature(
                way_id=obj_id,
                version=row["version"],
                batch_id=row["batch_id"],
                highway=tags[HIGHWAY_TAG],
                name=tags.get("name"),
                coordinates=coordinates,
                tags=tags,
            ))

    return roads, pending


def road_geojson(roads: list[RoadFeature]) -> dict:
    """Serialize roads as a GeoJSON FeatureCollection (lon, lat order)."""
    features = []
    for road in roads:
        props = dict(road.tags)
        props.setdefault("osm_way_id", road.way_id)
        props.setdefault("osm_version", road.version)
        props.setdefault("source_batch_id", road.batch_id)
        features.append({
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": road.coordinates},
            "properties": props,
        })
    return {"type": "FeatureCollection", "name": "highways", "features": features}


def batch_summary(conn: sqlite3.Connection, batch_id: int) -> dict:
    """Object counts fixed by a batch's commit, for the detail page."""
    summary = {"node_visible": 0, "node_deleted": 0, "way_visible": 0, "way_deleted": 0}
    seq = _batch_commit_seq(conn, batch_id)
    if seq is None:
        return summary
    rows = conn.execute(
        """
        SELECT o.object_type,
               SUM(CASE WHEN o.visible = 1 THEN 1 ELSE 0 END) AS visible_count,
               SUM(CASE WHEN o.visible = 0 THEN 1 ELSE 0 END) AS deleted_count
        FROM object_versions o
        JOIN batches b ON b.id = o.batch_id
        WHERE b.commit_seq <= ? AND b.status = 'committed'
          AND (o.object_type, o.object_id, o.version) IN (
              SELECT ov.object_type, ov.object_id, MAX(ov.version)
              FROM object_versions ov
              JOIN batches ob ON ob.id = ov.batch_id
              WHERE ob.commit_seq <= ? AND ob.status = 'committed'
              GROUP BY ov.object_type, ov.object_id
          )
        GROUP BY o.object_type
        """,
        (seq, seq),
    ).fetchall()
    for row in rows:
        prefix = "node" if row["object_type"] == "node" else "way"
        summary[f"{prefix}_visible"] = row["visible_count"]
        summary[f"{prefix}_deleted"] = row["deleted_count"]
    return summary
