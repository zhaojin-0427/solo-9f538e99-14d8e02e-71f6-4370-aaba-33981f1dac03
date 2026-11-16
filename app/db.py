"""SQLite persistence layer.

Design overview
---------------
The service keeps two kinds of history:

* ``batches``      -- one row per uploaded file / ingestion attempt.
* ``object_versions`` -- every distinct (object type, id, version) ever seen.

The object-version table is an append-only event log.

Snapshot ordering
-----------------
A batch id is reserved *before* parsing, so ids follow upload start order,
not commit order.  With two parallel uploads A (id 2, slow parser) and B
(id 3) it is entirely normal that B commits first.  Ordering snapshots by
``batch_id`` would then be wrong in both directions:

* A (``batch_id = 2``) would later add rows with an id below B's, silently
  changing B's already published geometry;
* A's own snapshot, taken as ``batch_id <= 2``, would miss B's rows even
  though B had already committed when A finished.

Therefore every batch also carries a monotonically increasing
``commit_seq``, allocated inside the write transaction at the moment the
batch commits.  A snapshot of batch B is the per-object highest version
among rows with ``commit_seq <= B.commit_seq``: it contains B itself and
exactly the batches that had committed before B, never a batch that was
still parsing, and never a later commit.  ``commit_seq`` never changes
after commit, so snapshots are stable across later uploads and restarts.

Legacy databases (created before ``commit_seq`` existed) are upgraded in
place at startup: committed batches are backfilled with ``commit_seq = id``,
which reproduces exactly the ``batch_id <= B`` view users saw before the
upgrade.  Batch ids, URLs and stored history are left untouched.

A batch is fully applied inside one SQLite transaction.  If validation or
commit fails the transaction is rolled back and the batch is marked
``rejected`` -- partial state can never survive a failure or a process
restart (rows left in the ``processing`` state by a crash are recovered at
startup).
"""
from __future__ import annotations

import os
import socket
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from . import config

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA synchronous = FULL;

CREATE TABLE IF NOT EXISTS batches (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    filename     TEXT    NOT NULL,
    status       TEXT    NOT NULL CHECK (status IN ('processing', 'committed', 'rejected')),
    error        TEXT,
    created_at   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    commit_seq   INTEGER,
    owner_host   TEXT,
    owner_pid    INTEGER,
    owner_start  TEXT
);

CREATE TABLE IF NOT EXISTS object_versions (
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

CREATE INDEX IF NOT EXISTS idx_object_versions_batch
    ON object_versions (batch_id);
CREATE INDEX IF NOT EXISTS idx_batches_status
    ON batches (status, id);
"""

# Columns added after the first released schema.  Applied with ALTER TABLE so
# an existing data volume keeps all of its rows, batch ids and links.
MIGRATIONS = {
    "batches": {
        "commit_seq": "INTEGER",
        "owner_host": "TEXT",
        "owner_pid": "INTEGER",
        "owner_start": "TEXT",
    },
}


def get_db_path() -> Path:
    return Path(config.DB_PATH)


def connect(db_path: Path | str | None = None) -> sqlite3.Connection:
    """Open a connection with the pragmas the application relies on."""
    path = Path(db_path) if db_path is not None else get_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(path),
        timeout=30.0,
        isolation_level=None,  # explicit transaction control
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = FULL")
    return conn


def init_db(db_path: Path | str | None = None) -> None:
    """Create/upgrade the schema and recover batches interrupted by a crash."""
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        _migrate(conn)
        _recover_interrupted(conn)
    finally:
        conn.close()


def _boot_time(pid: int | None = None) -> str | None:
    """A token identifying the current process lifetime on this host.

    Used to tell a batch whose owning worker is genuinely gone (crash,
    restart) from one that is merely slow and being parsed right now -- the
    latter must survive another worker's startup hook.
    """
    pid = os.getpid() if pid is None else pid
    try:
        # Linux: start time in clock ticks since boot, field 22 of stat.
        with open(f"/proc/{pid}/stat", encoding="utf-8") as fh:
            fields = fh.read().rsplit(")", 1)[1].split()
        return fields[19]
    except (OSError, IndexError):
        return None


def owner_token() -> tuple[str, int, str | None]:
    return (socket.gethostname(), os.getpid(), _boot_time())


def _owner_may_be_running(host: str | None, pid: int | None, start: str | None) -> bool:
    """True when the recorded worker could still be running.

    A *different* host means a previous container lifetime (docker-compose
    recreates the container with a new hostname) -- any ``processing`` row
    from it is stale by definition.  Same host: the pid must exist and its
    start token must match, otherwise the process crashed or its pid was
    recycled.  The volume is served by one container at a time.
    """
    if host is None or host != socket.gethostname() or pid is None:
        return False
    if pid == os.getpid():
        # Our own in-flight batch, or a stale row from a previous process
        # whose pid we reused.  Where the start token is unavailable, never
        # destroy work a live process may still be doing.
        if start is None:
            return True
        return start == _boot_time()
    try:
        os.kill(pid, 0)
    except PermissionError:
        return True  # exists, owned by another user: may be alive
    except OSError:
        return False
    current = _boot_time(pid)
    if start is not None and current is not None and start != current:
        return False  # pid reused by an unrelated process
    return True


def recover_stale_batches(conn: sqlite3.Connection) -> None:
    """Reclaim batches abandoned by dead workers, if any exist.

    Cheap pre-check first: the common case is no ``processing`` row at all,
    so normal page views never touch the write lock.
    """
    pending = conn.execute(
        "SELECT 1 FROM batches WHERE status = 'processing' LIMIT 1"
    ).fetchone()
    if pending is not None:
        _recover_interrupted(conn)


def _recover_interrupted(conn: sqlite3.Connection) -> None:
    """Roll back batches whose owning worker is gone.

    A ``processing`` batch is only an interrupted half-batch when its owner
    is no longer alive.  A batch still being parsed by a live worker -- the
    whole point of a slow parallel upload -- must not be touched by another
    worker's startup hook.  Every mutation re-checks ``processing`` while
    holding the write lock, and rows only ever become visible together with
    the committed status in one transaction.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        stuck = conn.execute(
            "SELECT id, owner_host, owner_pid, owner_start FROM batches "
            "WHERE status = 'processing' ORDER BY id"
        ).fetchall()
        for row in stuck:
            if _owner_may_be_running(
                row["owner_host"], row["owner_pid"], row["owner_start"]
            ):
                continue
            cur = conn.execute(
                "UPDATE batches SET status = 'rejected', "
                "error = COALESCE(error, '') || ' [interrupted by restart; rolled back]' "
                "WHERE id = ? AND status = 'processing'",
                (row["id"],),
            )
            if cur.rowcount:
                conn.execute(
                    "DELETE FROM object_versions WHERE batch_id = ?", (row["id"],)
                )
        conn.execute("COMMIT")
    except Exception:
        conn.rollback()
        raise


def _migrate(conn: sqlite3.Connection) -> None:
    """Bring a pre-existing database file up to the current schema in place.

    Only additive ``ALTER TABLE`` steps are used, so every existing row,
    batch id and history entry survives the upgrade.  After adding
    ``commit_seq`` we backfill legacy committed batches with their id: under
    the old semantics a snapshot was ``batch_id <= B``, and assigning
    ``commit_seq = id`` reproduces precisely that view for batches that were
    already visible before the upgrade.

    Runs under ``BEGIN IMMEDIATE`` because every gunicorn worker executes the
    startup hook; concurrent workers must not both try the same ALTER.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        for table, columns in MIGRATIONS.items():
            existing = {
                row["name"]
                for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
            }
            for column, ddl in columns.items():
                if column not in existing:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

        # Created here rather than in SCHEMA: on a legacy volume the column
        # only exists after the ALTER above.
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_batches_commit_seq ON batches (commit_seq)"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_batches_commit_seq "
            "ON batches (commit_seq) WHERE commit_seq IS NOT NULL"
        )

        needs_backfill = conn.execute(
            "SELECT 1 FROM batches "
            "WHERE status = 'committed' AND commit_seq IS NULL LIMIT 1"
        ).fetchone()
        if needs_backfill:
            already_assigned = conn.execute(
                "SELECT 1 FROM batches WHERE commit_seq IS NOT NULL LIMIT 1"
            ).fetchone()
            legacy = conn.execute(
                "SELECT id FROM batches "
                "WHERE status = 'committed' AND commit_seq IS NULL ORDER BY id"
            ).fetchall()
            if not already_assigned:
                # Pure legacy volume: seq == id reproduces the old
                # ``batch_id <= B`` view for every batch exactly.
                conn.executemany(
                    "UPDATE batches SET commit_seq = id WHERE id = ?",
                    [(row["id"],) for row in legacy],
                )
            else:
                # Partially upgraded volume: keep id order after the
                # sequences already handed out.
                max_seq = conn.execute(
                    "SELECT COALESCE(MAX(commit_seq), 0) AS m FROM batches"
                ).fetchone()["m"]
                for row in legacy:
                    max_seq += 1
                    conn.execute(
                        "UPDATE batches SET commit_seq = ? WHERE id = ?",
                        (max_seq, row["id"]),
                    )
        conn.execute("COMMIT")
    except Exception:
        conn.rollback()
        raise


def next_commit_seq(conn: sqlite3.Connection) -> int:
    """Allocate the next snapshot position.

    Must be called inside the batch's ``BEGIN IMMEDIATE`` transaction: the
    write lock serializes concurrent committers, so ``MAX+1`` is unique and
    strictly increasing in real commit order.
    """
    row = conn.execute(
        "SELECT COALESCE(MAX(commit_seq), 0) AS m FROM batches"
    ).fetchone()
    return int(row["m"]) + 1


@contextmanager
def db_session(db_path: Path | str | None = None) -> Iterator[sqlite3.Connection]:
    conn = connect(db_path)
    try:
        yield conn
    finally:
        conn.close()
