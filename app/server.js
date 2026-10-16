require("dotenv").config();
const express = require("express");
const prisma = require("./db");
const { enqueueNotification, enqueueBulk } = require("../queue");
const { recoverPendingNotifications } = require("./outbox");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

function httpError(status, message) {
  const err = new Error(message);
  err.status = status;
  return err;
}

function parseId(value, name = "id") {
  const id = Number(value);
  if (!Number.isInteger(id)) {
    return { error: `${name} must be a number` };
  }
  return { id };
}

// GET /health
app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

// POST /api/workshops — create a workshop (status defaults to ACTIVE).
app.post("/api/workshops", async (req, res, next) => {
  try {
    const { title, capacity } = req.body || {};
    if (typeof title !== "string" || title.trim() === "") {
      return res.status(400).json({ error: "title must be a non-empty string" });
    }
    if (!Number.isInteger(capacity) || capacity < 1) {
      return res.status(400).json({ error: "capacity must be an integer >= 1" });
    }

    const workshop = await prisma.workshop.create({
      data: { title: title.trim(), capacity },
    });
    res.status(201).json(workshop);
  } catch (err) {
    next(err);
  }
});

// GET /api/workshops/:id
app.get("/api/workshops/:id", async (req, res, next) => {
  try {
    const parsed = parseId(req.params.id);
    if (parsed.error) return res.status(400).json({ error: parsed.error });

    const workshop = await prisma.workshop.findUnique({ where: { id: parsed.id } });
    if (!workshop) {
      return res.status(404).json({ error: "Workshop not found" });
    }
    res.json(workshop);
  } catch (err) {
    next(err);
  }
});

// POST /api/workshops/:id/register — lock workshop row so last-seat races stay safe.
app.post("/api/workshops/:id/register", async (req, res, next) => {
  try {
    const parsed = parseId(req.params.id);
    if (parsed.error) return res.status(400).json({ error: parsed.error });
    const workshopId = parsed.id;

    const { memberId } = req.body || {};
    if (!Number.isInteger(memberId)) {
      return res.status(400).json({ error: "memberId must be an integer" });
    }

    const { registration, log } = await prisma.$transaction(async (tx) => {
      const rows = await tx.$queryRaw`
        SELECT id, title, capacity, status FROM "Workshop" WHERE id = ${workshopId} FOR UPDATE
      `;
      const workshop = rows[0];
      if (!workshop) throw httpError(404, "Workshop not found");
      if (workshop.status === "CANCELLED") throw httpError(409, "Workshop is cancelled");

      const member = await tx.member.findUnique({ where: { id: memberId } });
      if (!member) throw httpError(404, "Member not found");

      const confirmed = await tx.registration.count({
        where: { workshopId, status: "CONFIRMED" },
      });
      if (confirmed >= Number(workshop.capacity)) throw httpError(409, "Workshop is full");

      const registration = await tx.registration.create({
        data: { workshopId, memberId, status: "CONFIRMED" },
      });
      const log = await tx.notificationLog.create({
        data: {
          type: "REGISTRATION_CONFIRMATION",
          recipientId: memberId,
          workshopId,
          status: "PENDING",
        },
      });
      return { registration, log };
    });

    try {
      await enqueueNotification({
        notificationLogId: log.id,
        type: "REGISTRATION_CONFIRMATION",
        recipientId: memberId,
        workshopId,
      });
    } catch (enqueueErr) {
      // Log is already PENDING; outbox recovery will enqueue later.
      console.error("enqueueNotification failed:", enqueueErr);
    }
    res.status(201).json(registration);
  } catch (err) {
    if (err.status) return res.status(err.status).json({ error: err.message });
    if (err.code === "P2002") {
      return res.status(409).json({ error: "Already registered" });
    }
    next(err);
  }
});

// POST /api/workshops/:id/cancel — cancel workshop + confirmed seats, notify each member.
app.post("/api/workshops/:id/cancel", async (req, res, next) => {
  try {
    const parsed = parseId(req.params.id);
    if (parsed.error) return res.status(400).json({ error: parsed.error });
    const workshopId = parsed.id;

    const logs = await prisma.$transaction(async (tx) => {
      const rows = await tx.$queryRaw`
        SELECT id, status FROM "Workshop" WHERE id = ${workshopId} FOR UPDATE
      `;
      const workshop = rows[0];
      if (!workshop) throw httpError(404, "Workshop not found");
      if (workshop.status === "CANCELLED") throw httpError(409, "Workshop already cancelled");

      await tx.workshop.update({
        where: { id: workshopId },
        data: { status: "CANCELLED" },
      });

      const confirmed = await tx.registration.findMany({
        where: { workshopId, status: "CONFIRMED" },
      });
      await tx.registration.updateMany({
        where: { workshopId, status: "CONFIRMED" },
        data: { status: "CANCELLED" },
      });

      const created = [];
      for (const reg of confirmed) {
        created.push(
          await tx.notificationLog.create({
            data: {
              type: "CANCELLATION",
              recipientId: reg.memberId,
              workshopId,
              status: "PENDING",
            },
          })
        );
      }
      return created;
    });

        // After commit: queue one job per member in a single bulk call.
    let queued = 0;
    try {
      const jobs = await enqueueBulk(
        logs.map((log) => ({
          notificationLogId: log.id,
          type: "CANCELLATION",
          recipientId: log.recipientId,
          workshopId,
        }))
      );
      queued = jobs.length;
    } catch (enqueueErr) {
      // Rows are committed as PENDING; outbox recovery (Step 11) re-enqueues them.
      console.error("[cancel] enqueue failed, rows stay PENDING:", enqueueErr);
    }

    console.log(`[cancel] workshop ${workshopId}: ${logs.length} logs, ${queued} jobs queued`);
    res.status(200).json({
      message: `Workshop ${workshopId} cancelled`,
      notificationsCreated: logs.length,
      notificationsQueued: queued,
    });
  } catch (err) {
    if (err.status) return res.status(err.status).json({ error: err.message });
    next(err);
  }
});

// POST /api/members
app.post("/api/members", async (req, res, next) => {
  try {
    const { name, email } = req.body || {};
    if (typeof name !== "string" || name.trim() === "") {
      return res.status(400).json({ error: "name must be a non-empty string" });
    }
    if (typeof email !== "string" || email.trim() === "" || !email.includes("@")) {
      return res.status(400).json({ error: "email must be a non-empty string containing @" });
    }

    const member = await prisma.member.create({
      data: { name: name.trim(), email: email.trim() },
    });
    res.status(201).json(member);
  } catch (err) {
    if (err.code === "P2002") {
      return res.status(409).json({ error: "Email already exists" });
    }
    next(err);
  }
});

// GET /api/members — all members, or CONFIRMED registrants when workshopId is set
app.get("/api/members", async (req, res, next) => {
  try {
    const { workshopId } = req.query;
    if (workshopId !== undefined) {
      const parsed = parseId(workshopId, "workshopId");
      if (parsed.error) return res.status(400).json({ error: parsed.error });

      const members = await prisma.member.findMany({
        where: {
          registrations: { some: { workshopId: parsed.id, status: "CONFIRMED" } },
        },
        orderBy: { id: "asc" },
      });
      return res.json(members);
    }

    const members = await prisma.member.findMany({ orderBy: { id: "asc" } });
    res.json(members);
  } catch (err) {
    next(err);
  }
});

// Basic error-handling middleware
app.use((err, _req, res, _next) => {
  console.error(err);
  res.status(500).json({ error: "Internal server error" });
});

app.listen(PORT, () => {
  console.log(`App server listening on ${PORT}`);

  // Outbox safety net: re-enqueue PENDING rows that have no job (non-blocking).
  recoverPendingNotifications().catch((err) =>
    console.error("[outbox] recovery failed:", err)
  );

  // Periodic sweep for rows missed while Redis was down.
  setInterval(() => {
    recoverPendingNotifications().catch((err) =>
      console.error("[outbox] sweep failed:", err)
    );
  }, 60000);
});
