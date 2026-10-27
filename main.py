"""Reliable Job Queue - FastAPI service.

Run:
    uvicorn main:app --port 8000

Configuration (environment variables):
    JOB_DB_PATH          SQLite file path        (default: jobs.sqlite3)
    JOB_BACKOFF_BASE     backoff base in seconds (default: 1.0)
    JOB_WORKER_COUNT     background worker threads (default: 1)
    JOB_POLL_INTERVAL    idle poll seconds       (default: 0.5)
"""

from __future__ import annotations

import os
import threading
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from job_queue import JobQueue, Worker


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        raise ValueError(f"{name} must be a number, got {raw!r}")


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {raw!r}")


DB_PATH = os.environ.get("JOB_DB_PATH", "jobs.sqlite3")
BACKOFF_BASE = _env_float("JOB_BACKOFF_BASE", 1.0)
WORKER_COUNT = _env_int("JOB_WORKER_COUNT", 1)
POLL_INTERVAL = _env_float("JOB_POLL_INTERVAL", 0.5)

queue = JobQueue(DB_PATH, backoff_base=BACKOFF_BASE)

# ----------------------------------------------------------------------
# Demo handlers
# ----------------------------------------------------------------------


def send_email(payload: dict, job: dict) -> dict:
    """Mock email sender: pretends to send and reports success."""
    return {
        "sent": True,
        "to": payload.get("to"),
        "subject": payload.get("subject"),
    }


def generate_report(payload: dict, job: dict) -> dict:
    """Mock report generator: returns a small summary of the payload."""
    name = payload.get("name", "report")
    return {
        "report": name,
        "summary": f"Generated report '{name}' from "
        f"{len(payload)} payload field(s)",
        "fields": sorted(payload.keys()),
    }


# flaky_task must remember, per job, how many times it has already run.
# Keyed by job id, so the count survives retries AND a DLQ requeue
# (a requeue resets `attempts`, which is exactly the "we fixed it, give
# it another chance" moment where the task can finally succeed).
_flaky_seen: dict[int, int] = {}
_flaky_lock = threading.Lock()


def flaky_task(payload: dict, job: dict) -> dict:
    """Fails the first ``fail_times`` times it runs for a given job,
    then succeeds. Demonstrates retries (and DLQ + requeue)."""
    fail_times = int(payload.get("fail_times", 1))
    with _flaky_lock:
        seen = _flaky_seen.get(job["id"], 0)
        _flaky_seen[job["id"]] = seen + 1
    if seen < fail_times:
        raise RuntimeError(
            f"flaky_task: simulated failure {seen + 1} of {fail_times}"
        )
    return {"ok": True, "failures_before_success": fail_times}


def always_fail(payload: dict, job: dict) -> dict:
    """Always raises. Demonstrates the dead-letter queue."""
    raise RuntimeError("always_fail: this task always fails (demo)")


queue.register_handler("send_email", send_email)
queue.register_handler("generate_report", generate_report)
queue.register_handler("flaky_task", flaky_task)
queue.register_handler("always_fail", always_fail)

# ----------------------------------------------------------------------
# Background workers
# ----------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    stop_event = threading.Event()
    threads = []
    for i in range(WORKER_COUNT):
        worker = Worker(queue)
        t = threading.Thread(
            target=worker.run_forever,
            args=(stop_event, POLL_INTERVAL),
            daemon=True,
            name=f"job-worker-{i}",
        )
        t.start()
        threads.append(t)
    app.state.stop_event = stop_event
    app.state.worker_threads = threads
    try:
        yield
    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=2)


app = FastAPI(title="Reliable Job Queue", lifespan=lifespan)

# ----------------------------------------------------------------------
# API
# ----------------------------------------------------------------------


class JobCreate(BaseModel):
    type: str
    payload: dict[str, Any] = {}
    priority: int = 0
    max_retries: int = 3
    delay_seconds: float = 0
    idempotency_key: Optional[str] = None


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/jobs", status_code=201)
def create_job(body: JobCreate) -> dict:
    try:
        return queue.enqueue(
            body.type,
            body.payload,
            priority=body.priority,
            max_retries=body.max_retries,
            delay_seconds=body.delay_seconds,
            idempotency_key=body.idempotency_key,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/jobs")
def list_jobs(status: Optional[str] = None) -> list[dict]:
    return queue.list_jobs(status=status)


@app.get("/jobs/{job_id}")
def get_job(job_id: int) -> dict:
    job = queue.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@app.get("/dlq")
def dead_letter_queue() -> list[dict]:
    """All dead jobs (the dead-letter queue)."""
    return queue.list_jobs(status="dead")


@app.post("/jobs/{job_id}/requeue")
def requeue_job(job_id: int) -> dict:
    try:
        job = queue.requeue(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@app.get("/stats")
def stats() -> dict:
    return queue.stats()
