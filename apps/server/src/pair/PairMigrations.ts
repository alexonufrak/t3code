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

const Migration0001PairEvents = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE IF NOT EXISTS pair_events (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT NOT NULL UNIQUE,
      room_id TEXT NOT NULL,
      event_type TEXT NOT NULL,
      command_id TEXT NOT NULL UNIQUE,
      occurred_at TEXT NOT NULL,
      payload_json TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_pair_events_room_sequence
    ON pair_events(room_id, sequence)
  `;
});

const pairMigrationEntries = [[1, "PairEvents", Migration0001PairEvents]] as const;

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
