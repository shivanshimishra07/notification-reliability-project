require("dotenv").config();
const { Worker } = require("bullmq");
const axios = require("axios");
const prisma = require("../app/db");
const { connection, QUEUE_NAME } = require("../queue");

const MOCK_SERVICE_URL = process.env.MOCK_SERVICE_URL || "http://localhost:4000";

function messageFor(type, workshop) {
  if (type === "CANCELLATION") {
    return {
      subject: "Workshop cancelled",
      body: `${workshop.title} has been cancelled`,
    };
  }
  return {
    subject: "You're registered",
    body: `You're registered for ${workshop.title}`,
  };
}

// Step 9: custom backoff = exponential base delay + random jitter.
// attemptsMade is 1 after the first failure, 2 after the second, ...
function backoffStrategy(attemptsMade, type, err, job) {
  const base = job?.opts?.backoff?.delay || 2000;
  const exponential = base * Math.pow(2, attemptsMade - 1); // 2s, 4s, 8s, 16s
  const jitter = Math.floor(Math.random() * 1000); // 0-999 ms
  const delay = exponential + jitter;
  console.log(
    `[backoff] retry #${attemptsMade} in ${delay}ms (base ${exponential} + jitter ${jitter})`
  );
  return delay;
}

const worker = new Worker(
  QUEUE_NAME,
  async (job) => {
    const { notificationLogId, type, recipientId, workshopId } = job.data;
    console.log(`[worker] processing logId=${notificationLogId} type=${type}`);

    // Skip duplicates: never send twice for an already delivered row
    const existing = await prisma.notificationLog.findUnique({
      where: { id: notificationLogId },
    });
    if (!existing) {
      console.warn(`[worker] logId=${notificationLogId} not found, skipping`);
      return;
    }
    if (existing.status === "DELIVERED") {
      console.log(`[worker] Skipping notification ${notificationLogId}, already delivered`);
      return;
    }

    const updated = await prisma.notificationLog.update({
      where: { id: notificationLogId },
      data: { attempts: { increment: 1 }, lastAttemptAt: new Date() },
    });

    const [member, workshop] = await Promise.all([
      prisma.member.findUnique({ where: { id: recipientId } }),
      prisma.workshop.findUnique({ where: { id: workshopId } }),
    ]);
    if (!member || !workshop) {
      throw new Error("Member or workshop not found");
    }

    const { subject, body } = messageFor(type, workshop);
    try {
      await axios.post(
        `${MOCK_SERVICE_URL}/notify`,
        { to: member.email, subject, body },
        {
          timeout: 3000,
          headers: { "Idempotency-Key": String(notificationLogId) },
        }
      );
    } catch (err) {
      const reason = err.response ? `HTTP ${err.response.status}` : err.message;
      console.error(`[worker] logId=${notificationLogId} failed (attempt ${updated.attempts}): ${reason}`);
      throw err; // BullMQ schedules the retry
    }

    await prisma.notificationLog.update({
      where: { id: notificationLogId },
      data: { status: "DELIVERED" },
    });
    console.log(`[worker] delivered logId=${notificationLogId} (attempt ${updated.attempts})`);
  },
  { connection, concurrency: 5, settings: { backoffStrategy } }
);

worker.on("failed", async (job, err) => {
  if (!job) return;
  const notificationLogId = job.data?.notificationLogId;
  const maxAttempts = job.opts.attempts || 1;

  if (job.attemptsMade >= maxAttempts) {
    // Final failure. Never overwrite a DELIVERED row.
    await prisma.notificationLog.updateMany({
      where: { id: notificationLogId, status: { not: "DELIVERED" } },
      data: { status: "FAILED" },
    });
    console.error(
      `[worker] logId=${notificationLogId} permanently FAILED after ${job.attemptsMade} attempts`
    );
  } else {
    console.log(`[worker] logId=${notificationLogId} will retry (${job.attemptsMade}/${maxAttempts})`);
  }
});

worker.on("error", (err) => {
  console.error("[worker] error:", err);
});

console.log("Worker listening for jobs");