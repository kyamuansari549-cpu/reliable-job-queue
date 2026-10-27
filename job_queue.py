"""Core job queue engine.

SQLite-backed (Python standard library only), thread-safe, with:
  - priorities (higher priority is claimed first)
  - delayed jobs (run_at)
  - retries with exponential backoff
  - a dead-letter queue (DLQ): jobs that exhaust their retries land in
    status 'dead' with the last error recorded, and can be requeued
  - idempotent enqueue via a unique idempotency key

Handlers are callables ``fn(payload: dict, job: dict) -> result``.
The return value is stored as the job's result (JSON). Raising an
exception marks the attempt as failed.

Time is injectable (``now_fn``) and the backoff base is configurable,
so behaviour is fully deterministic in tests without real sleeps.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any, Callable, Optional

STATUSES = ("queued", "running", "succeeded", "failed", "dead")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    type            TEXT    NOT NULL,
    payload         TEXT    NOT NULL DEFAULT '{}',
    status          TEXT    NOT NULL DEFAULT 'queued',
    priority        INTEGER NOT NULL DEFAULT 0,
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 3,
    run_at          REAL    NOT NULL,
    idempotency_key TEXT    UNIQUE,
    last_error      TEXT,
    result          TEXT,
    created_at      REAL    NOT NULL,
    updated_at      REAL    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_due
    ON jobs (status, run_at, priority);
"""

Handler = Callable[[dict, dict], Any]


class JobQueue:
    """A persistent, thread-safe job queue stored in a SQLite file."""

    def __init__(
        self,
        db_path: str = "jobs.sqlite3",
        backoff_base: float = 1.0,
        now_fn: Callable[[], float] = time.time,
    ) -> None:
        if backoff_base < 0:
            raise ValueError("backoff_base must be >= 0")
        self.db_path = db_path
        self.backoff_base = float(backoff_base)
        self._now = now_fn
        self._handlers: dict[str, Handler] = {}
        self._lock = threading.RLock()
        # One shared connection guarded by a lock; isolation_level=None
        # (autocommit) so claim_next can use explicit BEGIN IMMEDIATE.
        self._conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(_SCHEMA)

    # ------------------------------------------------------------------
    # handlers
    # ------------------------------------------------------------------
    def register_handler(self, job_type: str, fn: Handler) -> None:
        """Register the callable that processes jobs of ``job_type``."""
        self._handlers[job_type] = fn

    # ------------------------------------------------------------------
    # enqueue / read
    # ------------------------------------------------------------------
    def enqueue(
        self,
        type: str,
        payload: Optional[dict] = None,
        priority: int = 0,
        max_retries: int = 3,
        delay_seconds: float = 0,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        """Add a job to the queue and return it.

        If ``idempotency_key`` was already used, the existing job is
        returned and no duplicate is created.
        """
        if not type:
            raise ValueError("job type is required")
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            raise ValueError("payload must be a dict (JSON object)")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        now = self._now()
        with self._lock:
            if idempotency_key is not None:
                existing = self._fetch_by_key(idempotency_key)
                if existing is not None:
                    return existing
            try:
                cur = self._conn.execute(
                    """
                    INSERT INTO jobs
                        (type, payload, status, priority, attempts,
                         max_retries, run_at, idempotency_key,
                         created_at, updated_at)
                    VALUES (?, ?, 'queued', ?, 0, ?, ?, ?, ?, ?)
                    """,
                    (
                        type,
                        json.dumps(payload),
                        int(priority),
                        int(max_retries),
                        now + float(delay_seconds),
                        idempotency_key,
                        now,
                        now,
                    ),
                )
                job_id = cur.lastrowid
            except sqlite3.IntegrityError:
                # Lost a race on the unique idempotency key.
                if idempotency_key is not None:
                    existing = self._fetch_by_key(idempotency_key)
                    if existing is not None:
                        return existing
                raise
        return self.get_job(job_id)

    def get_job(self, job_id: int) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
        return self._row_to_job(row) if row else None

    def list_jobs(self, status: Optional[str] = None) -> list[dict]:
        with self._lock:
            if status is None:
                rows = self._conn.execute(
                    "SELECT * FROM jobs ORDER BY id ASC"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM jobs WHERE status = ? ORDER BY id ASC",
                    (status,),
                ).fetchall()
        return [self._row_to_job(r) for r in rows]

    def stats(self) -> dict:
        """Job counts grouped by status, plus a total."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) AS n FROM jobs GROUP BY status"
            ).fetchall()
        counts = {s: 0 for s in STATUSES}
        for row in rows:
            counts[row["status"]] = row["n"]
        counts["total"] = sum(counts[s] for s in STATUSES)
        return counts

    # ------------------------------------------------------------------
    # worker side
    # ------------------------------------------------------------------
    def claim_next(self) -> Optional[dict]:
        """Atomically claim the highest-priority due job.

        Uses BEGIN IMMEDIATE so that even separate JobQueue instances
        (or processes) sharing one database file can never claim the
        same job twice.
        """
        now = self._now()
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")
                row = cur.execute(
                    """
                    SELECT * FROM jobs
                    WHERE status = 'queued' AND run_at <= ?
                    ORDER BY priority DESC, id ASC
                    LIMIT 1
                    """,
                    (now,),
                ).fetchone()
                if row is None:
                    self._conn.commit()
                    return None
                cur.execute(
                    "UPDATE jobs SET status = 'running', updated_at = ? "
                    "WHERE id = ?",
                    (now, row["id"]),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
            job_id = row["id"]
        return self.get_job(job_id)

    def process_next(self) -> Optional[dict]:
        """Claim one due job and execute it. Returns the final job
        state, or None when no job is due."""
        job = self.claim_next()
        if job is None:
            return None
        return self._execute(job)

    def requeue(self, job_id: int) -> Optional[dict]:
        """Move a dead job back to 'queued' with attempts reset.

        Returns the job, None if it does not exist, and raises
        ValueError if the job is not dead.
        """
        job = self.get_job(job_id)
        if job is None:
            return None
        if job["status"] != "dead":
            raise ValueError(
                f"only dead jobs can be requeued "
                f"(job {job_id} is '{job['status']}')"
            )
        now = self._now()
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET status = 'queued', attempts = 0, "
                "run_at = ?, last_error = NULL, updated_at = ? "
                "WHERE id = ?",
                (now, now, job_id),
            )
        return self.get_job(job_id)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------
    def _execute(self, job: dict) -> dict:
        handler = self._handlers.get(job["type"])
        if handler is None:
            self._mark_dead(
                job["id"],
                f"no handler registered for job type '{job['type']}'",
            )
            return self.get_job(job["id"])
        try:
            result = handler(job["payload"], job)
        except Exception as exc:  # noqa: BLE001 - any handler error = failure
            return self._handle_failure(job, exc)
        now = self._now()
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET status = 'succeeded', result = ?, "
                "last_error = NULL, updated_at = ? WHERE id = ?",
                (
                    json.dumps(result) if result is not None else None,
                    now,
                    job["id"],
                ),
            )
        return self.get_job(job["id"])

    def _handle_failure(self, job: dict, exc: Exception) -> dict:
        now = self._now()
        attempts = job["attempts"] + 1
        error = f"{type(exc).__name__}: {exc}"
        with self._lock:
            if attempts > job["max_retries"]:
                self._conn.execute(
                    "UPDATE jobs SET status = 'dead', attempts = ?, "
                    "last_error = ?, updated_at = ? WHERE id = ?",
                    (attempts, error, now, job["id"]),
                )
            else:
                delay = self.backoff_base * (2 ** (attempts - 1))
                self._conn.execute(
                    "UPDATE jobs SET status = 'queued', attempts = ?, "
                    "last_error = ?, run_at = ?, updated_at = ? "
                    "WHERE id = ?",
                    (attempts, error, now + delay, now, job["id"]),
                )
        return self.get_job(job["id"])

    def _mark_dead(self, job_id: int, error: str) -> None:
        now = self._now()
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET status = 'dead', last_error = ?, "
                "updated_at = ? WHERE id = ?",
                (error, now, job_id),
            )

    def _fetch_by_key(self, idempotency_key: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM jobs WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()
        return self._row_to_job(row) if row else None

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> dict:
        return {
            "id": row["id"],
            "type": row["type"],
            "payload": json.loads(row["payload"]),
            "status": row["status"],
            "priority": row["priority"],
            "attempts": row["attempts"],
            "max_retries": row["max_retries"],
            "run_at": row["run_at"],
            "idempotency_key": row["idempotency_key"],
            "last_error": row["last_error"],
            "result": json.loads(row["result"]) if row["result"] else None,
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }


class Worker:
    """Drives a JobQueue: processes jobs one at a time or in a loop."""

    def __init__(self, queue: JobQueue) -> None:
        self.queue = queue

    def run_once(self) -> Optional[dict]:
        """Process a single due job (used by tests and demos)."""
        return self.queue.process_next()

    def run_forever(
        self,
        stop_event: threading.Event,
        poll_interval: float = 0.5,
    ) -> None:
        """Process jobs until ``stop_event`` is set, sleeping
        ``poll_interval`` seconds whenever the queue is empty."""
        while not stop_event.is_set():
            job = self.queue.process_next()
            if job is None:
                stop_event.wait(poll_interval)
