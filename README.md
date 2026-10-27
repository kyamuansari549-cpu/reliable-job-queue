# Reliable Job Queue with Dead-Letter Recovery

A small, dependency-light job queue service built with FastAPI and
SQLite (Python standard library only - no Redis, no Celery, no broker
to install). Jobs are stored in a SQLite file, processed by background
worker threads, retried with exponential backoff when they fail, and
moved to a dead-letter queue (DLQ) when their retries run out - from
where they can be inspected and requeued.

## Features

- **Persistent queue** - jobs live in SQLite and survive restarts.
- **Priorities** - higher-priority jobs are claimed first.
- **Delayed jobs** - schedule a job to become due in the future.
- **Retries with exponential backoff** - a failed job is retried after
  `base * 2^(attempt-1)` seconds instead of hammering a broken
  dependency.
- **Dead-letter queue** - jobs that exhaust retries land in status
  `dead` with the last error recorded; `GET /dlq` lists them and
  `POST /jobs/{id}/requeue` gives them another chance.
- **Idempotent enqueue** - pass an `idempotency_key` and submitting
  the same job twice creates only one job.
- **Atomic claiming** - workers claim jobs inside a `BEGIN IMMEDIATE`
  transaction, so two workers never process the same job twice.
- **Demo handlers** - `send_email`, `generate_report`, `flaky_task`
  (fails a configurable number of times, then succeeds) and
  `always_fail` (always raises - for demonstrating the DLQ).

## How retries, backoff and the DLQ work

A job starts as `queued`. A worker claims it (`running`) and runs its
handler:

- Handler succeeds -> `succeeded`, result stored.
- Handler raises -> `attempts` increases by 1.
  - If `attempts > max_retries` -> `dead` (the DLQ), error recorded.
  - Otherwise -> back to `queued`, due again at
    `now + backoff_base * 2^(attempts-1)`.

Worked example (`max_retries = 3`, `backoff_base = 1s`, always-failing
handler):

| Run | Result | attempts | What happens next        |
|-----|--------|----------|--------------------------|
| 1   | fail   | 1        | retry in 1s              |
| 2   | fail   | 2        | retry in 2s              |
| 3   | fail   | 3        | retry in 4s              |
| 4   | fail   | 4        | 4 > 3, so job goes `dead` |

Total executions = `max_retries + 1`. A dead job is never retried
automatically; a human (or a deployment of the fixed code) requeues
it, which resets `attempts` to 0 and makes it due immediately.

Jobs whose type has no registered handler go straight to `dead` with
a clear error - retrying them could never succeed.

## Project layout

```
job_queue.py            the queue engine (SQLite, claiming, retries)
main.py                 FastAPI app, demo handlers, background workers
tests/test_job_queue.py core + API tests (deterministic fake clock)
conftest.py             makes the project root importable for pytest
requirements.txt
LEARN.md                beginner-friendly walkthrough of the whole project
```

## Running

### Linux / macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --port 8000
```

### Windows (PowerShell)

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn main:app --port 8000
```

Open http://127.0.0.1:8000/docs for the interactive API docs.

## Demo (curl)

Start the server with a small backoff base so the demo is quick:

```bash
JOB_BACKOFF_BASE=0.5 uvicorn main:app --port 8000
```

1. A job that succeeds:

```bash
curl -X POST localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"send_email","payload":{"to":"a@b.c","subject":"hi"}}'
curl localhost:8000/jobs/1        # status becomes "succeeded"
```

2. A job that fails, retries and lands in the DLQ:

```bash
curl -X POST localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"always_fail","payload":{},"max_retries":2}'
curl localhost:8000/jobs/2        # watch attempts grow, then "dead"
curl localhost:8000/dlq           # the dead job, with last_error
curl localhost:8000/stats         # counts by status
```

3. DLQ recovery - requeue after the "fix":

`always_fail` would simply fail again (the bug is still there), so
the recovery demo uses `flaky_task`, which fails a fixed number of
times per job and then succeeds - a stand-in for a transient problem:

```bash
# fails once, and max_retries=0 means it goes straight to the DLQ
curl -X POST localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"flaky_task","payload":{"fail_times":1},"max_retries":0}'
curl localhost:8000/dlq                       # it is there
curl -X POST localhost:8000/jobs/3/requeue    # attempts reset
curl localhost:8000/jobs/3                    # now "succeeded"
```

In a real system the flow is the same: fix the cause, requeue the
dead jobs, and they process normally.

## API

| Method | Path                | Description                          |
|--------|---------------------|--------------------------------------|
| POST   | /jobs               | Enqueue a job (201)                  |
| GET    | /jobs?status=dead   | List jobs, optionally by status      |
| GET    | /jobs/{id}          | One job                              |
| GET    | /dlq                | Dead jobs (dead-letter queue)        |
| POST   | /jobs/{id}/requeue  | Requeue a dead job (attempts reset)  |
| GET    | /stats              | Counts by status                     |
| GET    | /health             | Health check                         |

`POST /jobs` body: `type` (required), `payload` (object),
`priority` (int, higher runs first), `max_retries` (int),
`delay_seconds` (number), `idempotency_key` (string, optional).

## Configuration

| Variable          | Default       | Meaning                             |
|-------------------|---------------|-------------------------------------|
| JOB_DB_PATH       | jobs.sqlite3  | SQLite database file                |
| JOB_BACKOFF_BASE  | 1.0           | Backoff base seconds (delay = base * 2^(attempt-1)) |
| JOB_WORKER_COUNT  | 1             | Background worker threads (0 = none, drive it yourself) |
| JOB_POLL_INTERVAL | 0.5           | Seconds a worker sleeps when idle   |

## Tests

```bash
pytest -q
```

The core tests inject a fake clock and a small backoff base, so retry
and backoff behaviour is verified exactly, with no real sleeps.
