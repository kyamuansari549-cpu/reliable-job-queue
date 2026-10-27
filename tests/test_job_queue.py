"""Tests for the job queue core and the FastAPI service.

Core tests are deterministic: they inject a FakeClock and a tiny
backoff base, and drive the worker synchronously with process_next().
API tests run the real app (background worker included) against a
temporary database and poll with a bounded deadline.
"""

import os
import threading
import time

import pytest

from job_queue import JobQueue

# ----------------------------------------------------------------------
# API environment must be set BEFORE importing main
# ----------------------------------------------------------------------
os.environ["JOB_DB_PATH"] = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "_api_test_jobs.sqlite3"
)
os.environ["JOB_BACKOFF_BASE"] = "0.01"
os.environ["JOB_POLL_INTERVAL"] = "0.02"
os.environ["JOB_WORKER_COUNT"] = "1"


class FakeClock:
    def __init__(self, start: float = 1_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture()
def clock():
    return FakeClock()


@pytest.fixture()
def q(tmp_path, clock):
    queue = JobQueue(
        str(tmp_path / "jobs.sqlite3"), backoff_base=1.0, now_fn=clock
    )
    yield queue
    queue.close()


def drain(queue: JobQueue, clock: FakeClock, max_steps: int = 50):
    """Process jobs until none are due; advance the fake clock to the
    next run_at whenever everything pending is scheduled in the future."""
    for _ in range(max_steps):
        job = queue.process_next()
        if job is not None:
            continue
        pending = [j for j in queue.list_jobs(status="queued")]
        if not pending:
            return
        clock.advance(min(j["run_at"] for j in pending) - clock.now + 0.001)


# ----------------------------------------------------------------------
# Core: happy path
# ----------------------------------------------------------------------


def test_enqueue_process_succeeds(q, clock):
    q.register_handler("echo", lambda payload, job: {"echo": payload})
    job = q.enqueue("echo", {"a": 1})
    assert job["status"] == "queued"
    done = q.process_next()
    assert done["id"] == job["id"]
    assert done["status"] == "succeeded"
    assert done["result"] == {"echo": {"a": 1}}
    assert done["attempts"] == 0
    assert q.process_next() is None


def test_delayed_job_not_due_until_delay_passes(q, clock):
    q.register_handler("echo", lambda payload, job: "ok")
    q.enqueue("echo", {}, delay_seconds=60)
    assert q.process_next() is None
    clock.advance(61)
    done = q.process_next()
    assert done["status"] == "succeeded"


# ----------------------------------------------------------------------
# Core: retries, backoff, DLQ
# ----------------------------------------------------------------------


def test_failing_job_retries_exactly_max_retries_then_dead(q, clock):
    calls = []

    def boom(payload, job):
        calls.append(1)
        raise RuntimeError("kaboom")

    q.register_handler("boom", boom)
    job = q.enqueue("boom", {}, max_retries=2)
    drain(q, clock)
    final = q.get_job(job["id"])
    assert final["status"] == "dead"
    # 1 initial run + exactly 2 retries
    assert len(calls) == 3
    assert final["attempts"] == 3
    assert "kaboom" in final["last_error"]
    assert q.stats()["dead"] == 1


def test_backoff_run_at_increases_exponentially(q, clock):
    def boom(payload, job):
        raise RuntimeError("x")

    q.register_handler("boom", boom)
    job = q.enqueue("boom", {}, max_retries=5)
    delays = []
    for _ in range(3):
        before = clock.now
        done = q.process_next()
        assert done["status"] == "queued"
        delays.append(done["run_at"] - before)
        clock.advance(delays[-1] + 0.001)
    # base = 1.0 -> delays 1, 2, 4 seconds
    assert delays == [1.0, 2.0, 4.0]


def test_flaky_job_succeeds_after_retries_without_going_dead(q, clock):
    seen = {"n": 0}

    def flaky(payload, job):
        seen["n"] += 1
        if seen["n"] <= payload["fail_times"]:
            raise RuntimeError("flaky")
        return "recovered"

    q.register_handler("flaky", flaky)
    job = q.enqueue("flaky", {"fail_times": 2}, max_retries=3)
    drain(q, clock)
    final = q.get_job(job["id"])
    assert final["status"] == "succeeded"
    assert final["result"] == "recovered"
    assert final["attempts"] == 2
    assert q.stats()["dead"] == 0


def test_unknown_type_goes_straight_to_dead(q):
    job = q.enqueue("no_such_type", {})
    done = q.process_next()
    assert done["id"] == job["id"]
    assert done["status"] == "dead"
    assert "no handler registered" in done["last_error"]


def test_requeue_dead_job_processes_to_success(q, clock):
    state = {"fixed": False}

    def sometimes(payload, job):
        if not state["fixed"]:
            raise RuntimeError("bug")
        return "done"

    q.register_handler("sometimes", sometimes)
    job = q.enqueue("sometimes", {}, max_retries=0)
    drain(q, clock)
    assert q.get_job(job["id"])["status"] == "dead"

    state["fixed"] = True  # simulate deploying the fix
    moved = q.requeue(job["id"])
    assert moved["status"] == "queued"
    assert moved["attempts"] == 0
    assert moved["last_error"] is None

    done = q.process_next()
    assert done["status"] == "succeeded"
    assert done["result"] == "done"


def test_requeue_rejects_non_dead_job(q):
    q.register_handler("echo", lambda payload, job: "ok")
    job = q.enqueue("echo", {})
    with pytest.raises(ValueError):
        q.requeue(job["id"])
    assert q.requeue(999999) is None


# ----------------------------------------------------------------------
# Core: priority, idempotency, persistence, concurrency
# ----------------------------------------------------------------------


def test_higher_priority_claimed_first(q):
    q.register_handler("echo", lambda payload, job: "ok")
    low = q.enqueue("echo", {"n": "low"}, priority=1)
    high = q.enqueue("echo", {"n": "high"}, priority=10)
    mid = q.enqueue("echo", {"n": "mid"}, priority=5)
    order = [q.claim_next()["id"] for _ in range(3)]
    assert order == [high["id"], mid["id"], low["id"]]


def test_idempotency_same_key_returns_same_job(q):
    q.register_handler("echo", lambda payload, job: "ok")
    a = q.enqueue("echo", {"v": 1}, idempotency_key="order-42")
    b = q.enqueue("echo", {"v": 1}, idempotency_key="order-42")
    assert a["id"] == b["id"]
    assert len(q.list_jobs()) == 1
    c = q.enqueue("echo", {"v": 2}, idempotency_key="order-43")
    assert c["id"] != a["id"]
    assert len(q.list_jobs()) == 2


def test_persistence_across_reopen(tmp_path, clock):
    db = str(tmp_path / "persist.sqlite3")
    first = JobQueue(db, backoff_base=1.0, now_fn=clock)
    first.register_handler("echo", lambda payload, job: "ok")
    job = first.enqueue("echo", {"keep": True})
    first.close()

    second = JobQueue(db, backoff_base=1.0, now_fn=clock)
    second.register_handler("echo", lambda payload, job: "ok")
    reopened = second.get_job(job["id"])
    assert reopened is not None
    assert reopened["status"] == "queued"
    assert reopened["payload"] == {"keep": True}
    done = second.process_next()
    assert done["status"] == "succeeded"
    second.close()


def test_concurrent_claimers_never_claim_same_job(tmp_path, clock):
    queue = JobQueue(
        str(tmp_path / "race.sqlite3"), backoff_base=1.0, now_fn=clock
    )
    queue.register_handler("echo", lambda payload, job: "ok")
    ids = {queue.enqueue("echo", {"i": i})["id"] for i in range(20)}
    claimed = []
    claimed_lock = threading.Lock()

    def claimer():
        while True:
            job = queue.claim_next()
            if job is None:
                return
            with claimed_lock:
                claimed.append(job["id"])

    threads = [threading.Thread(target=claimer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(claimed) == 20
    assert len(set(claimed)) == 20  # no job claimed twice
    assert set(claimed) == ids
    queue.close()


def test_stats_counts(q):
    q.register_handler("echo", lambda payload, job: "ok")
    q.enqueue("echo", {})
    q.enqueue("echo", {})
    q.process_next()
    s = q.stats()
    assert s["succeeded"] == 1
    assert s["queued"] == 1
    assert s["dead"] == 0
    assert s["total"] == 2


# ----------------------------------------------------------------------
# API tests (real app + background worker)
# ----------------------------------------------------------------------

from fastapi.testclient import TestClient  # noqa: E402

# Remove any stale API-test database BEFORE main opens its connection.
if os.path.exists(os.environ["JOB_DB_PATH"]):
    os.remove(os.environ["JOB_DB_PATH"])

import main  # noqa: E402


@pytest.fixture(scope="module")
def client():
    with TestClient(main.app) as c:
        yield c


def wait_for(client, job_id, status, timeout=5.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = client.get(f"/jobs/{job_id}").json()
        if last["status"] == status:
            return last
        time.sleep(0.05)
    raise AssertionError(f"job {job_id} never reached {status}: {last}")


def test_api_send_email_succeeds(client):
    r = client.post("/jobs", json={"type": "send_email",
                                   "payload": {"to": "a@b.c"}})
    assert r.status_code == 201
    job = wait_for(client, r.json()["id"], "succeeded")
    assert job["result"]["sent"] is True


def test_api_always_fail_lands_in_dlq_and_stats(client):
    r = client.post("/jobs", json={"type": "always_fail",
                                   "payload": {}, "max_retries": 1})
    assert r.status_code == 201
    job_id = r.json()["id"]
    job = wait_for(client, job_id, "dead")
    assert job["attempts"] == 2  # 1 run + 1 retry
    assert "always_fail" in job["last_error"]

    dlq = client.get("/dlq").json()
    assert any(j["id"] == job_id for j in dlq)

    dead = client.get("/jobs", params={"status": "dead"}).json()
    assert any(j["id"] == job_id for j in dead)

    stats = client.get("/stats").json()
    assert stats["dead"] >= 1
    assert stats["total"] >= 1


def test_api_requeue_dead_flaky_job_to_success(client):
    r = client.post("/jobs", json={"type": "flaky_task",
                                   "payload": {"fail_times": 1},
                                   "max_retries": 0})
    job_id = r.json()["id"]
    wait_for(client, job_id, "dead")

    r = client.post(f"/jobs/{job_id}/requeue")
    assert r.status_code == 200
    job = wait_for(client, job_id, "succeeded")
    assert job["result"]["ok"] is True


def test_api_get_missing_job_404(client):
    assert client.get("/jobs/999999").status_code == 404
    assert client.post("/jobs/999999/requeue").status_code == 404
