require("dotenv").config();
const prisma = require("../app/db");

async function main() {
  // Upsert so this can be run repeatedly without duplicates.
  for (let i = 1; i <= 10; i++) {
    const email = `member${i}@example.com`;
    await prisma.member.upsert({
      where: { email },
      update: {},
      create: { name: `Member ${i}`, email },
    });
  }
  console.log("Seeded 10 members");
}

main()
  .catch((err) => {
    console.error(err);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
