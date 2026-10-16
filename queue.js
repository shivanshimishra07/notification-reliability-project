require("dotenv").config();
const { Queue } = require("bullmq");
const IORedis = require("ioredis");

const QUEUE_NAME = "notification-queue";

// Shared Redis connection. maxRetriesPerRequest: null is required by BullMQ workers.
const connection = new IORedis(process.env.REDIS_URL || "redis://localhost:6379", {
  maxRetriesPerRequest: null,
});

const queue = new Queue(QUEUE_NAME, { connection });

connection.on("error", (err) => {
  console.error("Redis connection error:", err);
});

queue.on("error", (err) => {
  console.error("Queue error:", err);
});

// BullMQ rejects integer-like custom ids; prefix keeps a 1:1 map to NotificationLog.id.
function getJobId(notificationLogId) {
  return `notification-${notificationLogId}`;
}

// Retry settings shared by every job (register + cancel).
// type "custom" => the worker's backoffStrategy runs (exponential + jitter).
const RETRY_OPTIONS = {
  attempts: 5,
  backoff: { type: "custom", delay: 2000 },
  removeOnComplete: true,
};

async function enqueueNotification(data) {
  const notificationLogId = data.notificationLogId ?? data.logId;
  return queue.add(
    "send-notification",
    { ...data, notificationLogId },
    { jobId: getJobId(notificationLogId), ...RETRY_OPTIONS }
  );
}

// Add many jobs in one call. One job per recipient, so one failing
// member never blocks another.
async function enqueueBulk(items) {
  if (!items.length) return [];
  return queue.addBulk(
    items.map((data) => ({
      name: "send-notification",
      data,
      opts: { jobId: getJobId(data.notificationLogId), ...RETRY_OPTIONS },
    }))
  );
}

async function closeQueue() {
  await queue.close();
  try {
    await Promise.race([
      connection.quit(),
      new Promise((resolve) => setTimeout(resolve, 500)),
    ]);
  } finally {
    if (connection.status !== "end") connection.disconnect();
  }
}

module.exports = {
  queue,
  connection,
  QUEUE_NAME,
  getJobId,
  RETRY_OPTIONS,
  enqueueNotification,
  enqueueBulk,
  closeQueue,
};