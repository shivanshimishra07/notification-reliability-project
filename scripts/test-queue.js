const { queue, closeQueue, getJobId } = require("../queue");

async function main() {
  // Same jobId on repeat runs: BullMQ keeps a single job (no duplicate).
  try {
    await queue.add("test", { hello: "world" }, { jobId: getJobId("test") });
  } catch (err) {
    if (!String(err.message).includes("already exists")) throw err;
  }

  const counts = await queue.getJobCounts();
  console.log(counts);
  await closeQueue();
  process.exit(0);
}

main().catch(async (err) => {
  console.error(err);
  try {
    await closeQueue();
  } catch (_) {
    /* ignore shutdown errors */
  }
  process.exit(1);
});
