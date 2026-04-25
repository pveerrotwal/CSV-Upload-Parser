# CSV Upload Processor

FastAPI-based CSV processor with async background jobs.

## What it does

- `POST /upload` accepts a CSV and returns a `jobId` immediately.
- Upload processing runs in a background worker via Redis queue.
- `GET /status/{jobId}` returns queued/processing/done/failed plus progress and summary.
- Invalid rows are counted and surfaced in the final result.
- Duplicate files are deduplicated using SHA-256 hash.
- Frontend shows all jobs and lets users open processed CSV output.

## Stack

- API: FastAPI
- Worker queue: RQ + Redis
- DB: PostgreSQL
- Infra: Docker Compose
- CI: GitHub Actions (lint + tests + build)

## Run locally

```bash
docker compose up --build
```

Open [http://localhost:8000](http://localhost:8000)

Use the provided demo files:

- `samples/sample1.csv`
- `samples/sample2.csv`
- `samples/sample3.csv`
- `samples/sample4.csv`

## API quick check

```bash
curl -F "file=@sample.csv" http://localhost:8000/upload
curl http://localhost:8000/status/<job-id>
curl -OJ http://localhost:8000/download/<job-id>
```

## CSV expectations

Required columns:

- `date`
- `description`
- `amount`
- `category`

Rows with malformed fields are not silently dropped:

- Invalid rows are counted in `invalid_rows`
- Error categories appear in `invalid_reasons`
- Output CSV contains normalized valid rows

## Important decisions

- **Fast upload endpoint:** file is saved and queued; processing is fully out-of-band.
- **Deduplication:** identical file content maps to one job using SHA-256 hash unique key.
- **Crash handling:** worker heartbeat is persisted; stale `processing` jobs are auto-marked failed.
- **Progress:** worker updates `processed_rows` while processing each row.

## With more time

- Add retry policy and dead-letter handling.
- Add authentication and per-user job history.
- Support very large files with chunked processing and batched DB updates.
- Add richer integration tests with full dockerized services.
