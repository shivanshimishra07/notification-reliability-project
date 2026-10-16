const prisma = require("./db");
const { queue, getJobId, enqueueNotification } = require("../queue");

// Only consider rows older than 10s so we never race a row the
// register/cancel route is about to enqueue itself.
const MIN_AGE_MS = 10000;

async function recoverPendingNotifications() {
  const rows = await prisma.notificationLog.findMany({
    where: {
      status: "PENDING",
      createdAt: { lt: new Date(Date.now() - MIN_AGE_MS) },
    },
    orderBy: { id: "asc" },
  });

  let requeued = 0;
  let skipped = 0;

  for (const row of rows) {
    // One bad row must never stop the rest.
    try {
      const job = await queue.getJob(getJobId(row.id));

      if (job) {
        const state = await job.getState();
        if (state !== "failed") {
          // waiting / active / delayed: the worker owns it.
          skipped++;
          continue;
        }
        // Job failed but the row is still PENDING (worker likely crashed
        // before marking FAILED). Remove the stale job and try again.
        console.warn(`[outbox] logId=${row.id} has a failed job but row is PENDING, re-enqueueing`);
        await job.remove();
      }

      await enqueueNotification({
        notificationLogId: row.id,
        type: row.type,
        recipientId: row.recipientId,
        workshopId: row.workshopId,
      });
      requeued++;
    } catch (err) {
      console.error(`[outbox] logId=${row.id} recovery error:`, err.message);
    }
  }

  console.log(`[outbox] found ${rows.length} pending, re-enqueued ${requeued}, skipped ${skipped}`);
  return { found: rows.length, requeued, skipped };
}

module.exports = { recoverPendingNotifications };