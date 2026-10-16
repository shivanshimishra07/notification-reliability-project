const { PrismaClient } = require("@prisma/client");

// Single shared Prisma client — reused by the app (and later the worker).
const prisma = new PrismaClient();

module.exports = prisma;
