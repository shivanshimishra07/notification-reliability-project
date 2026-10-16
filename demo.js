/**
 * demo.js: Step 12, end-to-end reliability verification.
 *
 * Prereqs (each in its own terminal): docker compose up -d, npm run mock,
 * npm run app, npm run worker. Then: npm run demo
 *
 * Every run creates fresh workshops/members with a unique RUN_ID, so it can
 * be re-run safely. Pass --cleanup to delete this run's records at the end.
 */
require("dotenv").config();
const axios = require("axios");
const { Queue } = require("bullmq");
const prisma = require("./app/db");
const { connection, QUEUE_NAME } = require("./queue");
const { recoverPendingNotifications } = require("./app/outbox");

const BASE_URL = process.env.APP_URL || "http://localhost:3000";
const RUN_ID = Date.now().toString(36);
const CLEANUP = process.argv.includes("--cleanup");

const http = axios.create({ baseURL: BASE_URL, validateStatus: () => true, timeout: 15000 });
const queue = new Queue(QUEUE_NAME, { connection });

const createdMemberIds = [];
const createdWorkshopIds = [];
const TERMINAL = ["DELIVERED", "FAILED"];

// ---------- helpers ----------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const header = (t) => console.log(`\n${"=".repeat(70)}\n${t}\n${"=".repeat(70)}`);
const info = (m) => console.log(`  ${m}`);
const pass = (m) => console.log(`  [PASS] ${m}`);

function assert(cond, msg) {
  if (!cond) throw new Error(`ASSERTION FAILED: ${msg}`);
  pass(msg);
}

async function waitFor(fn, { timeoutMs, intervalMs = 500, onTick } = {}) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const result = await fn();
    if (onTick) onTick(result, Date.now() - start);
    if (result.done) return { ...result, timedOut: false };
    await sleep(intervalMs);
  }
  return { ...(await fn()), timedOut: true };
}

async function createWorkshop(title, capacity) {
  const res = await http.post("/api/workshops", { title: `${title} [${RUN_ID}]`, capacity });
  if (res.status !== 201) throw new Error(`create workshop failed: ${res.status} ${JSON.stringify(res.data)}`);
  createdWorkshopIds.push(res.data.id);
  return res.data;
}

async function createMembers(count, tag) {
  const members = [];
  for (let i = 1; i <= count; i++) {
    const res = await http.post("/api/members", {
      name: `Demo ${tag} ${i}`,
      email: `demo-${RUN_ID}-${tag}-${i}@example.com`,
    });
    if (res.status !== 201) throw new Error(`create member failed: ${res.status} ${JSON.stringify(res.data)}`);
    createdMemberIds.push(res.data.id);
    members.push(res.data);
  }
  return members;
}

const register = (workshopId, memberId) =>
  http.post(`/api/workshops/${workshopId}/register`, { memberId });

async function fetchLogs(where) {
  return prisma.notificationLog.findMany({ where, orderBy: { id: "asc" } });
}

// Best-effort look at BullMQ jobs (completed jobs may already be auto-removed).
async function findJobsFor(logIds) {
  const wanted = new Set(logIds);
  const jobs = await queue.getJobs(
    ["waiting", "active", "delayed", "prioritized", "completed", "failed"],
    0,
    1000
  );
  return jobs.filter((j) => j && wanted.has(j.data?.notificationLogId));
}

async function preflight() {
  header("PREFLIGHT");
  const health = await http.get("/health");
  if (health.status !== 200) throw new Error(`App server not healthy at ${BASE_URL}`);
  pass(`App server reachable at ${BASE_URL}`);
  try {
    const workers = await queue.getWorkers();
    if (workers.length === 0) {
      console.warn("  [WARN] No BullMQ worker connected! Start it with: npm run worker");
    } else {
      pass(`${workers.length} worker connection(s) attached to "${QUEUE_NAME}"`);
    }
  } catch (e) {
    info(`(could not list workers: ${e.message})`);
  }
}

// ---------- Scenario 1 ----------
async function runScenario1() {
  header("SCENARIO 1: Fast API response (async enqueue)");
  const workshop = await createWorkshop("Demo S1", 10);
  const [warmup, member] = await createMembers(2, "s1");

  // Warm-up call so Prisma/connection setup does not skew the measurement.
  await register(workshop.id, warmup.id);

  const t0 = performance.now();
  const res = await register(workshop.id, member.id);
  const ms = performance.now() - t0;

  info(`POST /api/workshops/${workshop.id}/register -> ${res.status} in ${ms.toFixed(1)} ms`);
  assert(res.status === 201, "HTTP status is 201 Created");
  assert(ms < 200, `Response time ${ms.toFixed(1)} ms is < 200 ms (notification not awaited)`);

  const logs = await fetchLogs({ workshopId: workshop.id, recipientId: member.id });
  assert(logs.length === 1, "A NotificationLog row was written in the same flow");
  info(`Notification status right now: ${logs[0].status} (delivery continues in background)`);
}

// ---------- Scenario 2 ----------
async function runScenario2() {
  header("SCENARIO 2: Exponential backoff retries on failure");
  info("The mock fails ~30% of calls (20% 5xx + 10% slow > 3s timeout).");
  info("We register 20 members at once so retries are practically guaranteed.");

  const N = 20;
  const workshop = await createWorkshop("Demo S2", N + 5);
  const members = await createMembers(N, "s2");

  const results = await Promise.all(members.map((m) => register(workshop.id, m.id)));
  assert(results.every((r) => r.status === 201), `All ${N} registrations returned 201`);

  const history = new Map(); // logId -> [{ attempts, t }]
  const result = await waitFor(
    async () => {
      const logs = await fetchLogs({ workshopId: workshop.id, type: "REGISTRATION_CONFIRMATION" });
      return { logs, done: logs.length === N && logs.every((l) => TERMINAL.includes(l.status)) };
    },
    {
      timeoutMs: 120000,
      intervalMs: 300,
      onTick: ({ logs }, elapsed) => {
        for (const l of logs) {
          const h = history.get(l.id) || [];
          if (h.length === 0 || h[h.length - 1].attempts !== l.attempts) {
            h.push({ attempts: l.attempts, t: elapsed });
            history.set(l.id, h);
          }
        }
      },
    }
  );

  const retried = result.logs.filter((l) => l.attempts > 1);
  info(`Final: ${result.logs.filter((l) => l.status === "DELIVERED").length} DELIVERED, ` +
       `${result.logs.filter((l) => l.status === "FAILED").length} FAILED, ` +
       `${retried.length} needed retries`);

  console.log("\n  Retry timeline (seconds since polling began; gaps should grow ~2s, 4s, 8s, 16s + jitter):");
  for (const l of retried.slice(0, 5)) {
    const h = history.get(l.id).filter((x) => x.attempts > 0);
    const line = h.map((x) => `attempt ${x.attempts} @ ${(x.t / 1000).toFixed(1)}s`).join("  ->  ");
    info(`logId=${l.id} [${l.status}]: ${line}`);
  }

  assert(!result.timedOut, "All notifications reached a terminal state (DELIVERED/FAILED)");
  assert(retried.length > 0, "At least one notification retried (attempts > 1)");
  assert(Math.max(...result.logs.map((l) => l.attempts)) <= 5, "No notification exceeded 5 attempts");
}

// ---------- Scenario 3 ----------
async function runScenario3() {
  header("SCENARIO 3: Outbox recovery after crash / restart");
  info("Simulating a crash: 5 rows are committed as PENDING but never enqueued to Redis.");

  const workshop = await createWorkshop("Demo S3", 10);
  const members = await createMembers(5, "s3");

  const logs = [];
  for (const m of members) {
    await prisma.registration.create({
      data: { workshopId: workshop.id, memberId: m.id, status: "CONFIRMED" },
    });
    const base = {
      type: "REGISTRATION_CONFIRMATION",
      recipientId: m.id,
      workshopId: workshop.id,
      status: "PENDING",
    };
    let row;
    try {
      // Back-dated so recovery logic that ignores very fresh rows still picks them up.
      row = await prisma.notificationLog.create({
        data: { ...base, createdAt: new Date(Date.now() - 10 * 60 * 1000) },
      });
    } catch (_e) {
      row = await prisma.notificationLog.create({ data: base });
    }
    logs.push(row);
  }
  const ids = logs.map((l) => l.id);

  const orphanJobs = await findJobsFor(ids);
  assert(orphanJobs.length === 0, "Before recovery: 0 jobs exist in Redis for these 5 rows (orphans)");

  info("Restart simulated -> calling recoverPendingNotifications() (same code the app runs on boot)...");
  await recoverPendingNotifications();

  const jobsAfter = await findJobsFor(ids);
  info(`Jobs found in Redis after recovery: ${jobsAfter.length} (completed jobs may already be auto-removed)`);

  const poll = () =>
    waitFor(
      async () => {
        const rows = await fetchLogs({ id: { in: ids } });
        return { rows, done: rows.length === 5 && rows.every((r) => TERMINAL.includes(r.status)) };
      },
      { timeoutMs: 20000 }
    );

  let result = await poll();
  if (result.timedOut) {
    info("Some rows still pending; running another recovery sweep (like the 60s periodic sweep)...");
    await recoverPendingNotifications();
    result = await waitFor(
      async () => {
        const rows = await fetchLogs({ id: { in: ids } });
        return { rows, done: rows.every((r) => TERMINAL.includes(r.status)) };
      },
      { timeoutMs: 90000 }
    );
  }

  for (const r of result.rows) info(`logId=${r.id}: ${r.status} (attempts=${r.attempts})`);
  assert(!result.timedOut, "All 5 recovered notifications reached a terminal state");
  const delivered = result.rows.filter((r) => r.status === "DELIVERED").length;
  assert(delivered === 5, `All 5 orphaned notifications were DELIVERED (got ${delivered})`);
}

// ---------- Scenario 4 ----------
async function runScenario4() {
  header("SCENARIO 4: Bulk workshop cancellation (fan-out, isolated jobs)");
  const workshop = await createWorkshop("Demo S4", 10);
  const members = await createMembers(10, "s4");

  for (const m of members) {
    const res = await register(workshop.id, m.id);
    if (res.status !== 201) throw new Error(`registration failed: ${res.status} ${JSON.stringify(res.data)}`);
  }
  pass("10 distinct members registered");

  const res = await http.post(`/api/workshops/${workshop.id}/cancel`);
  info(`POST /api/workshops/${workshop.id}/cancel -> ${res.status} ${JSON.stringify(res.data)}`);
  assert(res.status === 200, "Cancel returned 200");
  assert(res.data.notificationsCreated === 10, "API reports 10 notifications created");
  assert(res.data.notificationsQueued === 10, "API reports 10 individual jobs queued");

  const cancelLogs = await fetchLogs({ workshopId: workshop.id, type: "CANCELLATION" });
  assert(cancelLogs.length === 10, "10 CANCELLATION rows exist in NotificationLog");
  assert(new Set(cancelLogs.map((l) => l.recipientId)).size === 10, "Each row targets a distinct member");

  const jobs = await findJobsFor(cancelLogs.map((l) => l.id));
  info(`BullMQ jobs still visible in Redis: ${jobs.length}/10 (finished jobs may be auto-removed)`);

  const result = await waitFor(
    async () => {
      const rows = await fetchLogs({ workshopId: workshop.id, type: "CANCELLATION" });
      return { rows, done: rows.every((r) => TERMINAL.includes(r.status)) };
    },
    { timeoutMs: 120000 }
  );

  console.log("\n  Per-member result (each is an independent job):");
  for (const r of result.rows) {
    info(`recipient=${r.recipientId} logId=${r.id}: ${r.status}, attempts=${r.attempts}`);
  }
  const attempts = result.rows.map((r) => r.attempts);
  if (Math.max(...attempts) > 1 && Math.min(...attempts) === 1) {
    info("Isolation visible: some members were delivered on attempt 1 while others retried.");
  }

  assert(!result.timedOut, "All 10 notifications reached a terminal state");
  const delivered = result.rows.filter((r) => r.status === "DELIVERED").length;
  assert(delivered === 10, `All 10 cancellation notifications DELIVERED (got ${delivered})`);
}

// ---------- cleanup ----------
async function cleanup() {
  header("CLEANUP (--cleanup)");
  await prisma.notificationLog.deleteMany({ where: { workshopId: { in: createdWorkshopIds } } });
  await prisma.registration.deleteMany({ where: { workshopId: { in: createdWorkshopIds } } });
  await prisma.workshop.deleteMany({ where: { id: { in: createdWorkshopIds } } });
  await prisma.member.deleteMany({ where: { id: { in: createdMemberIds } } });
  info(`Removed ${createdWorkshopIds.length} workshops and ${createdMemberIds.length} members from this run.`);
}

// ---------- main ----------
async function main() {
  console.log(`Reliability demo, run id: ${RUN_ID}`);
  try {
    await preflight();
  } catch (err) {
    console.error(`\nPreflight failed: ${err.message}`);
    console.error("Make sure docker compose, the app, the worker and the mock service are running.");
    process.exit(1);
  }

  const scenarios = [
    ["Scenario 1: Fast response", runScenario1],
    ["Scenario 2: Exponential retries", runScenario2],
    ["Scenario 3: Outbox recovery", runScenario3],
    ["Scenario 4: Bulk cancellation", runScenario4],
  ];

  const summary = [];
  for (const [name, fn] of scenarios) {
    try {
      await fn();
      summary.push({ name, ok: true });
    } catch (err) {
      console.error(`\n  [FAIL] ${name}: ${err.message}`);
      summary.push({ name, ok: false, error: err.message });
    }
  }

  if (CLEANUP) {
    try { await cleanup(); } catch (e) { console.error("Cleanup failed:", e.message); }
  }

  header("SUMMARY");
  for (const s of summary) console.log(`  ${s.ok ? "PASS" : "FAIL"}  ${s.name}${s.ok ? "" : ` -> ${s.error}`}`);

  const failed = summary.filter((s) => !s.ok).length;
  await queue.close().catch(() => {});
  await prisma.$disconnect().catch(() => {});
  process.exit(failed ? 1 : 0);
}

main().catch((err) => {
  console.error("Unexpected error:", err);
  process.exit(1);
});
