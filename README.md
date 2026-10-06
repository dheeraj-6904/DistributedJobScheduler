# LetsGossip — Distributed Job Scheduler

[![CI](https://github.com/dheeraj-6904/LetsGossip/actions/workflows/ci.yml/badge.svg)](https://github.com/dheeraj-6904/LetsGossip/actions/workflows/ci.yml)
![.NET](https://img.shields.io/badge/.NET-9.0-512BD4)
![License](https://img.shields.io/badge/license-MIT-green)

A fault-tolerant, horizontally scalable job scheduler built on **.NET 9**, **PostgreSQL**, and **Redis**.

You submit jobs over HTTP. The scheduler sends them to the right worker pool based on priority and topic, and workers run them under a lease. If a worker dies partway through a job, the job is picked up again automatically. You can run as many schedulers and workers as you like.

---

## ✨ Highlights

- **PostgreSQL is the source of truth.** Every job's state lives in Postgres. Redis only holds the dispatch queue, so you can rebuild it from the database.
- **Lock-free multi-scheduler dispatch.** Schedulers claim jobs with `FOR UPDATE SKIP LOCKED`, so any number of instances can run side by side without stepping on each other.
- **Priority with aging.** Queue scores combine `ScheduledAt` with `Priority × 600s`. High-priority jobs go first, but low-priority jobs still get their turn eventually.
- **Topic-based routing.** Jobs go to `jobs:ready:{topic}` sorted sets (`Default`, `CpuIntensive`, `GpuIntensive`). Workers block on several topics at once with `BZMPOP`.
- **Leases and heartbeats.** Workers hold a 30s lease and renew it every 10s. A sweeper re-queues any job whose lease runs out.
- **Retries with exponential backoff.** Failed jobs back off with `2^attempts` seconds between tries, up to `MaxAttempts`.
- **Idempotent submissions.** An optional `IdempotencyKey` blocks duplicate jobs (the API returns `409 Conflict`).
- **Built-in observability.** All services export OpenTelemetry metrics to Prometheus, and a Grafana dashboard is included.

---

## 🏗️ Architecture

```text
 [ Client ]
     │ 1. Submit Job
     ▼
 [   API  ]
     │ 2. Save to DB
     ▼
 [ PostgreSQL ] ◄──────────┐
     │                     │
     │ 3. Poll for Jobs    │ 6. Update Status
     ▼                     │    (and Renew Lease)
 [ Scheduler ]             │
     │                     │
     │ 4. Queue Job        │
     ▼                     │
 [   Redis  ]              │
     │                     │
     │ 5. Pop Job          │
     ▼                     │
 [  Worker  ] ─────────────┘
```

### Job lifecycle

```
Pending ──► Queued ──► Running ──► Completed
                          │
                          ├──► Retrying  (failure, attempts remaining)
                          ├──► Failed    (attempts exhausted)
                          └──► Queued    (lease expired → sweeper)
```

### Project layout

| Path                     | Description                                                                                                                                       |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/Core`               | Domain model (`Job`, `JobPriority`, `JobTopic`), scoring logic, repository/queue abstractions and their Postgres/Redis implementations, telemetry |
| `src/Api`                | Minimal API for submitting and querying jobs                                                                                                      |
| `src/Scheduler`          | `DispatcherService` (Postgres → Redis) and `LeaseSweeperService` (dead-worker recovery)                                                           |
| `src/Worker`             | `JobExecutorService`, `HeartbeatService`, `ActiveJobTracker`                                                                                      |
| `db/schema.sql`          | Database schema and partial indexes                                                                                                               |
| `tests/UnitTests`        | Fast, isolated tests using xUnit and Moq                                                                                                          |
| `tests/IntegrationTests` | Tests against real Postgres and Redis via Testcontainers                                                                                          |
| `tests/load`             | k6 load test script                                                                                                                               |

---

## 📦 Prerequisites

| Tool                                                           | Version | Needed for                                       |
| -------------------------------------------------------------- | ------- | ------------------------------------------------ |
| [.NET SDK](https://dotnet.microsoft.com/download/dotnet/9.0)   | 9.0+    | Building, running and testing locally            |
| [Docker](https://docs.docker.com/get-docker/) + Docker Compose | Recent  | Running the full stack and the integration tests |
| [k6](https://k6.io/) _(optional)_                              | Any     | Load testing (it can also run as a Docker image) |

> **Note:** You need Redis **7.0 or newer** because workers use `BZMPOP`. The provided Compose file already uses `redis:7-alpine`.

---

## 🚀 Getting Started

### Option 1 — Full stack with Docker Compose (recommended)

```bash
git clone https://github.com/dheeraj-6904/LetsGossip.git
cd LetsGossip
docker compose up --build
```

This starts the following:

| Service    | URL                   | Notes                                                 |
| ---------- | --------------------- | ----------------------------------------------------- |
| API        | http://localhost:8080 | Job submission and lookup                             |
| PostgreSQL | `localhost:5432`      | Schema applied automatically from `db/schema.sql`     |
| Redis      | `localhost:6379`      | Dispatch queues                                       |
| Prometheus | http://localhost:9090 | Scrapes `/metrics` from the API, Scheduler and Worker |
| Grafana    | http://localhost:3000 | Login `admin` / `admin`                               |

If 5432 or 6379 is already taken on your machine, set `POSTGRES_PORT` or `REDIS_PORT` to use different host ports.

**Scaling out:** to run more workers or schedulers, use `--scale`:

```bash
docker compose up --build --scale worker=3 --scale scheduler=2
```

### Option 2 — Run services locally with `dotnet`

Start only the infrastructure in Docker:

```bash
docker compose up -d postgres redis
```

Then run each service in its own terminal. The Scheduler and Worker are given explicit ports so they don't clash:

```bash
dotnet run --project src/Api                                   # http://localhost:5016
dotnet run --project src/Scheduler --urls http://localhost:5100
dotnet run --project src/Worker    --urls http://localhost:5200
```

By default the services connect to `localhost`. To point them somewhere else, set `ConnectionStrings__Postgres` and `ConnectionStrings__Redis`.

---

## 🔌 API

### Submit a job — `POST /jobs`

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
        "type": "EmailJob",
        "payload": "{\"recipient\":\"user@example.com\"}",
        "topic": "Default",
        "priority": 2,
        "maxAttempts": 3,
        "idempotencyKey": "welcome-email-42"
      }'
```

| Field            | Type        | Default   | Description                                     |
| ---------------- | ----------- | --------- | ----------------------------------------------- |
| `type`           | string      | —         | Job type, used to pick the handler              |
| `payload`        | JSON string | —         | Input passed to the handler (stored as `JSONB`) |
| `topic`          | string      | `Default` | `Default`, `CpuIntensive` or `GpuIntensive`     |
| `priority`       | int         | `5`       | `0` (highest) to `10` (lowest)                  |
| `maxAttempts`    | int         | `3`       | Retry budget                                    |
| `scheduledAt`    | ISO-8601    | now       | Earliest time the job may run                   |
| `idempotencyKey` | string      | `null`    | Blocks duplicate submissions                    |

**Responses:** `201 Created` · `400 Bad Request` (priority out of range) · `409 Conflict` (duplicate idempotency key)

### Get a job — `GET /jobs/{id}`

```bash
curl http://localhost:8080/jobs/<job-id>
```

Returns the full job record, including status, attempts, timestamps, result and error, or `404 Not Found`.

---

## 🧪 Testing

### Run everything

```bash
dotnet test DistributedJobScheduler.sln
```

### Unit tests

These are fast and need no external dependencies. They cover domain state transitions, score calculation, API endpoints, the dispatcher, the heartbeat service and the executor.

```bash
dotnet test tests/UnitTests/DistributedJobScheduler.UnitTests.csproj
```

### Integration tests

These use **Testcontainers** to start real PostgreSQL and Redis containers, so **Docker must be running**. They include a full end-to-end flow: API → Scheduler → Worker.

```bash
dotnet test tests/IntegrationTests/DistributedJobScheduler.IntegrationTests.csproj
```

To run just the end-to-end test:

```bash
dotnet test tests/IntegrationTests/DistributedJobScheduler.IntegrationTests.csproj \
  --filter FullFlow_ApiToSchedulerToWorker
```

> **Tip (Windows):** if Testcontainers has trouble with Docker Desktop, run the tests inside WSL. [`test_in_wsl.sh`](test_in_wsl.sh) installs the .NET 9 SDK if it's missing and runs the end-to-end test. You may need to change the repo path inside the script.

### Load testing

With the Compose stack running, run the k6 script on the same Docker network. Find the network name with `docker network ls`; it is usually `<folder-name>_default`.

```bash
docker run --rm --network <project>_default \
  -v "${PWD}/tests/load:/scripts" \
  grafana/k6 run /scripts/loadtest.js
```

The test ramps up to 5 virtual users. It passes if p95 latency is under 2s and fewer than 1% of requests fail.

### Continuous integration

Every push and pull request to `main` triggers [GitHub Actions](.github/workflows/ci.yml), which restores, builds (Release) and runs both test suites.

---

## 📊 Observability

Each service exposes Prometheus metrics at `/metrics`. Custom job metrics:

| Metric                      | Type           | Description                                |
| --------------------------- | -------------- | ------------------------------------------ |
| `jobs.queued`               | Counter        | Jobs pushed to Redis                       |
| `jobs.processed`            | Counter        | Jobs completed successfully                |
| `jobs.queue_time`           | Histogram (ms) | Time from `ScheduledAt` to execution start |
| `jobs.execution_time`       | Histogram (ms) | Handler execution time                     |
| `jobs.total_lifecycle_time` | Histogram (ms) | Time from creation to completion           |

To see the dashboard:

1. Open Grafana and add Prometheus as a data source with the URL `http://prometheus:9090`.
2. Import [`grafana-dashboard.json`](grafana-dashboard.json).

---

## 🗺️ Roadmap

- [ ] Pluggable `IJobHandler`s resolved by `job.Type` (execution is simulated today)
- [ ] Re-dispatch `Retrying` jobs once `NextRetryAt` has passed
- [ ] Worker topic subscriptions set through configuration
- [ ] Job cancellation endpoint
- [ ] Authentication on the API

---

## 🤝 Contributing

Contributions are welcome. See **[CONTRIBUTING.md](CONTRIBUTING.md)** for branch naming, the development workflow and pull request guidelines.

---

## 📄 License

Licensed under the [MIT License](LICENSE).
