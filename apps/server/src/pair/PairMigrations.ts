/**
 * Pair Room schema migrations.
 *
 * Pair tables use their own migrator and tracking table instead of joining
 * `persistence/Migrations.ts`. Upstream's migrator runs only ids greater than
 * the latest recorded one, so a fork migration numbered after upstream's head
 * would make upstream's next migration with that id get silently skipped.
 * Keeping a separate `pair_sql_migrations` table leaves upstream's numbering
 * untouched.
 */
import * as Effect from "effect/Effect";
import * as Migrator from "effect/unstable/sql/Migrator";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export const PAIR_MIGRATIONS_TABLE = "pair_sql_migrations";

/**
 * One row per room. The room record is small and bounded (see the limits in
 * `@t3tools/contracts` pairRoom), so it is stored whole as JSON and loaded
 * into memory at startup. Message bodies stay in the participant threads.
 */
const Migration0001PairRooms = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE IF NOT EXISTS pair_rooms (
      room_id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL,
      room_json TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
  `;
});

const pairMigrationEntries = [[1, "PairRooms", Migration0001PairRooms]] as const;

const run = Migrator.make({});

export const runPairMigrations = Effect.fn("runPairMigrations")(function* () {
  const executed = yield* run({
    table: PAIR_MIGRATIONS_TABLE,
    loader: Migrator.fromRecord(
      Object.fromEntries(
        pairMigrationEntries.map(([id, name, migration]) => [`${id}_${name}`, migration]),
      ),
    ),
  });
  if (executed.length > 0) {
    yield* Effect.log("Pair migrations ran successfully").pipe(
      Effect.annotateLogs({ migrations: executed.map(([id, name]) => `${id}_${name}`) }),
    );
  }
  return executed;
});
