import { PrismaD1 } from '@prisma/adapter-d1';
import { smokeTest } from './test'
import { createSQLiteDB } from "@miniflare/shared";
import { D1Database, D1DatabaseAPI } from "@miniflare/d1";

async function main() {

  const sqliteDb = await createSQLiteDB("./prisma/sqlite/dev.db");
  const db = new D1Database(new D1DatabaseAPI(sqliteDb));

  const adapter = new PrismaD1(db as any);

  await smokeTest(adapter)
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
