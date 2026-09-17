import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { migrationManifest } from "../persistence/Migrations.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { PAIR_MIGRATIONS_TABLE, runPairMigrations } from "./PairMigrations.ts";

it.layer(SqlitePersistenceMemory)("PairMigrations", (it) => {
  it.effect("tracks pair migrations separately from upstream's migration table", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const upstreamBefore = yield* sql<{ readonly latest: number }>`
        SELECT MAX(migration_id) AS latest FROM effect_sql_migrations
      `;

      const executed = yield* runPairMigrations();
      assert.deepEqual(executed, [[1, "PairEvents"]]);

      const pairRows = yield* sql<{ readonly migrationId: number }>`
        SELECT migration_id AS "migrationId" FROM ${sql(PAIR_MIGRATIONS_TABLE)}
      `;
      assert.deepEqual(
        pairRows.map((row) => row.migrationId),
        [1],
      );

      const upstreamAfter = yield* sql<{ readonly latest: number }>`
        SELECT MAX(migration_id) AS latest FROM effect_sql_migrations
      `;
      assert.strictEqual(upstreamAfter[0]?.latest, upstreamBefore[0]?.latest);
      assert.strictEqual(upstreamAfter[0]?.latest, migrationManifest.at(-1)?.[0]);

      const tables = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'pair_events'
      `;
      assert.strictEqual(tables.length, 1);

      assert.deepEqual(yield* runPairMigrations(), []);
    }),
  );
});
