# Notification Reliability Project

A Node.js backend for workshop registration where **notifications are never lost, never block the API, and are never sent twice**, even when the external gateway is flaky or the app crashes.

When a member registers for a workshop (or a workshop is cancelled), the system must notify people through an unreliable external service. This project shows how to do that safely using a database-backed outbox, a persistent Redis queue, retries with exponential backoff and jitter, and idempotent delivery.

---

## Table of Contents
1. [System Overview & Architecture](#1-system-overview--architecture)
2. [Reliability & Resilience Strategy](#2-reliability--resilience-strategy)
3. [Prerequisites & Environment Setup](#3-prerequisites--environment-setup)
4. [Execution & Testing Instructions](#4-execution--testing-instructions)
5. [E2E Demo Scenarios](#5-e2e-demo-scenarios)
6. [Project Structure](#6-project-structure)

---

## 1. System Overview & Architecture

### Tech stack

| Layer | Technology |
|---|---|
| Runtime / API | Node.js, Express |
| Database | PostgreSQL 16 via Prisma ORM |
| Queue | Redis 7 + BullMQ |
| HTTP client | Axios |
| Local infra | Docker Compose (PostgreSQL + Redis) |

### Components

```
 Client
   |  POST /api/workshops/:id/register      (returns 201 immediately)
   v
+------------------+   1. one DB transaction   +--------------+
|   App Server     | ------------------------> |  PostgreSQL  |
|  (Express :3000) |   registration +          |  (source of  |
|                  |   NotificationLog=PENDING |   truth)     |
|                  |                           +--------------+
|                  |   2. after commit                ^
|                  | -------- enqueue job ------>  +-------+   | status updates
+------------------+                               | Redis |   | (attempts,
   ^                                               |BullMQ |   |  DELIVERED,
   | on boot + every 60s:                          +-------+   |  FAILED)
   | outbox recovery re-enqueues                       | job   |
   | PENDING rows that have no job                     v       |
   |                                          +----------------+|
   +------------------------------------------|  Worker        |+
                                              |  (BullMQ)      |
                                              +--------+-------+
                                                       | POST /notify
                                                       | 3s timeout +
                                                       | Idempotency-Key
                                                       v
                                              +----------------+
                                              | Mock Notif.    |
                                              | Service (:4000)|
                                              | ~30% faults    |
                                              +----------------+
```

| Component | Role |
|---|---|
| **App Server** (`app/server.js`) | REST API. Writes the registration and a `PENDING` `NotificationLog` row in a single transaction, then enqueues a job and responds without waiting for delivery. |
| **BullMQ Worker** (`worker/index.js`) | Separate process that consumes jobs, calls the gateway with Axios, retries failures, and records `DELIVERED` / `FAILED`. |
| **Transactional Outbox Recovery** (`app/outbox.js`) | The `NotificationLog` table is the outbox. A recovery routine re-enqueues `PENDING` rows that never made it into Redis. Runs on app boot and as a periodic sweep. |
| **Mock Notification Service** (`mock-service/server.js`) | Deliberately unreliable gateway (`POST /notify`): about 70% success, 20% 5xx errors, 10% delayed past the worker's timeout. |

### Data model

- **Workshop**: `id`, `title`, `capacity`, `status` (`ACTIVE` / `CANCELLED`)
- **Member**: `id`, `name`, `email`
- **Registration**: `id`, `workshopId`, `memberId`, `status`; unique on `(workshopId, memberId)` to block double registration
- **NotificationLog**: `id`, `type` (`REGISTRATION_CONFIRMATION` / `CANCELLATION`), `recipientId`, `workshopId`, `status` (`PENDING` / `DELIVERED` / `FAILED`), `attempts`, `lastAttemptAt`

### API

| Method | Route | Purpose |
|---|---|---|
| `GET` | `/health` | Health check |
| `POST` | `/api/workshops` | Create a workshop: `{ "title": "...", "capacity": 10 }` |
| `GET` | `/api/workshops/:id` | Get a workshop |
| `POST` | `/api/workshops/:id/register` | Register a member: `{ "memberId": 1 }` |
| `POST` | `/api/workshops/:id/cancel` | Cancel a workshop and notify every registrant |
| `POST` | `/api/members` | Create a member: `{ "name": "...", "email": "..." }` |
| `GET` | `/api/members` | List members (`?workshopId=` filters to confirmed registrants) |

---

## 2. Reliability & Resilience Strategy

### Lost notifications & retries
The intent to notify is written to PostgreSQL **in the same transaction** as the registration, so it is durable the moment the API returns `201`. The job is then added to a Redis-backed BullMQ queue. Redis runs with append-only persistence (`--appendonly yes` in `docker-compose.yml`), so queued jobs also survive a Redis restart.

If the gateway returns an error or exceeds the worker's 3-second Axios timeout, the worker throws and BullMQ reschedules the job using a custom backoff strategy: **exponential backoff plus random jitter**.

```
delay = 2000 ms x 2^(attempt - 1) + random(0..999 ms)    ->  ~2s, ~4s, ~8s, ~16s
```

After the maximum of 5 attempts, the row is marked `FAILED`. The jitter spreads retries out in time, so many failed jobs do not hit the recovering gateway at the same instant.

### Duplicate delivery prevention
Delivery is **at-least-once**, so duplicates are guarded against in two places:

1. **Status check in the worker:** before sending, the worker loads the `NotificationLog` row and skips it if it is already `DELIVERED`. A final failure never overwrites a `DELIVERED` row.
2. **Idempotency key:** every gateway call carries an `Idempotency-Key` header set to the `NotificationLog` id. The id is the same on every retry, so the gateway can recognise and ignore a repeat of the same notification.

The remaining edge case is a crash after the gateway accepts a message but before the row is marked `DELIVERED`. The retry re-sends with the same idempotency key, which lets the gateway deduplicate it.

### App restarts / system crashes
All state lives in PostgreSQL and Redis, never in process memory.

- **Worker or app crash mid-job:** jobs persist in Redis and the `PENDING` row persists in PostgreSQL, so processing resumes when the process comes back.
- **Crash or Redis outage between DB commit and enqueue** (the classic outbox gap): the row exists as `PENDING` but has no job. If enqueueing fails, the API still returns `201` and logs the error. The outbox recovery routine then finds `PENDING` rows and re-enqueues them. It runs **once at app startup** and then as a **periodic sweep every 60 seconds**, so rows missed while Redis was down are picked up automatically.

### Bulk fan-out (workshop cancellation)
Cancelling a workshop runs in one transaction that locks the workshop row, marks it `CANCELLED`, cancels its confirmed registrations, and creates **one `NotificationLog` row per member**. After the commit, all jobs are queued with a single bulk call (`addBulk`), one job per member.

- **Isolation:** every member is an independent job. One member's failure is retried on its own and never delays or fails another member's notification.
- **Non-blocking:** the API only performs database writes and one bulk Redis call, then responds. The slow part, calling the gateway, happens in the worker process (up to 5 jobs concurrently), so the Express event loop stays free.

### Other safeguards
- `SELECT ... FOR UPDATE` on the workshop row makes the capacity check race-safe, so the last seat cannot be double-booked.
- A duplicate registration returns `409 Conflict` through the unique `(workshopId, memberId)` constraint.

---

## 3. Prerequisites & Environment Setup

### Prerequisites
- **Node.js v18+** and npm
- **Docker Desktop** (recommended; it provides PostgreSQL and Redis), or local PostgreSQL and Redis installs

### Setup

**1. Install dependencies**
```bash
npm install
```

**2. Start PostgreSQL and Redis**
```bash
docker compose up -d
```
This starts `workshop_postgres` (port 5432, database `workshops`) and `workshop_redis` (port 6379).

**3. Create your `.env` file** from the template

- Windows (PowerShell): `Copy-Item .env.example .env`
- macOS / Linux: `cp .env.example .env`

The defaults already match the Docker Compose setup:

```env
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/workshops?schema=public"
REDIS_URL="redis://localhost:6379"
```

Optional variables:

| Variable | Default | Purpose |
|---|---|---|
| `PORT` | `3000` | App server port |
| `MOCK_SERVICE_URL` | `http://localhost:4000` | Where the worker sends notifications |
| `APP_URL` | `http://localhost:3000` | API address used by `demo.js` |

**4. Create the database tables**
```bash
npx prisma db push
```

**5. (Optional) Seed sample data**
```bash
npm run seed
```

---

## 4. Execution & Testing Instructions

Run each service in its **own terminal**, from the project root:

```bash
npm run mock      # Mock Notification Service (port 4000)
npm run app       # Express API               (port 3000)
npm run worker    # BullMQ Worker
```

Then, in a fourth terminal, run the end-to-end suite:

```bash
npm run demo                # run all 4 scenarios
npm run demo -- --cleanup   # also delete the records this run created
```

The demo creates uniquely named workshops and members on every run, so it can be repeated safely. It takes about 1 to 2 minutes because it waits for real retry backoffs, and it exits with code `0` if all scenarios pass and `1` otherwise.

> Note: each notification has a small chance (about 0.24%) of failing all 5 attempts against the faulty mock. If a scenario fails for that reason, simply run the demo again.

---

## 5. E2E Demo Scenarios

`demo.js` first checks that the app is healthy and that a worker is attached to the queue, then runs these scenarios in order:

1. **Fast response (happy path).** Creates a workshop and members, registers a member, and asserts `201 Created` in under 200 ms. This shows that delivery is asynchronous and not part of the request. It also confirms a `NotificationLog` row was written in the same flow.
2. **Retry & recovery.** Registers 20 members at once against the faulty mock (about 30% of calls fail through 5xx errors or timeouts). It polls `NotificationLog` and prints the attempt timeline of retried notifications, showing `attempts > 1` with growing gaps, until every row reaches `DELIVERED` (or `FAILED` after 5 attempts).
3. **Outbox recovery after a crash.** Inserts 5 `PENDING` rows that were never enqueued, which is the state a crash between commit and enqueue leaves behind. It verifies no jobs exist for them, calls `recoverPendingNotifications()` (the same function the app runs on boot and every 60 seconds), and verifies all 5 are re-enqueued and reach `DELIVERED`.
4. **Mass cancellation fan-out.** Creates a workshop with capacity 10, registers 10 distinct members, and cancels it. It verifies 10 `CANCELLATION` rows for 10 distinct recipients, that the API reports 10 individually queued jobs, and that all 10 reach `DELIVERED` as independent jobs (some succeed on the first attempt while others retry).

---

## 6. Project Structure

```
.
├── docker-compose.yml     # PostgreSQL 16 + Redis 7 (with persistence)
├── .env.example           # Environment variable template
├── prisma/                # Prisma schema (and seed script)
├── app/                   # Express API, DB client, outbox recovery
├── worker/                # BullMQ worker (retries, idempotency)
├── mock-service/          # Faulty notification gateway
├── scripts/               # Helper scripts (queue test)
├── queue.js               # Shared BullMQ queue + Redis connection
├── demo.js                # End-to-end reliability suite
└── package.json
```

### npm scripts

| Script | Command |
|---|---|
| `npm run app` | Start the Express API |
| `npm run worker` | Start the BullMQ worker |
| `npm run mock` | Start the mock notification service |
| `npm run seed` | Seed sample data |
| `npm run demo` | Run the end-to-end reliability suite |
