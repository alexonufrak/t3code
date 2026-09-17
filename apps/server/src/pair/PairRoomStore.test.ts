import {
  PairRoomId,
  ProjectId,
  ThreadId,
  TurnId,
  type PairRoomListEvent,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as PairRoomStore from "./PairRoomStore.ts";

const ROOM_ID = PairRoomId.make("room-1");
const LEAD_THREAD = ThreadId.make("lead-thread");
const AT = "2026-09-17T10:00:00.000Z";

const createRoom = {
  type: "room.create",
  roomId: ROOM_ID,
  projectId: ProjectId.make("project-1"),
  leadThreadId: LEAD_THREAD,
  leadPersona: "astra",
  mode: "pair",
  at: AT,
} as const;

it.layer(SqlitePersistenceMemory)("PairRoomStore", (it) => {
  it.effect("persists rooms, reloads them, and leaves state untouched on a rejection", () =>
    Effect.gen(function* () {
      const store = yield* PairRoomStore.make;
      yield* store.dispatch(createRoom);
      yield* store.dispatch({
        type: "consult.request",
        roomId: ROOM_ID,
        consultId: "consult-1",
        kind: "critique",
        leadTurnId: TurnId.make("turn-1"),
        automatic: false,
        title: "Review the plan",
        at: AT,
      });

      const busy = yield* store
        .dispatch({
          type: "consult.request",
          roomId: ROOM_ID,
          consultId: "consult-2",
          kind: "critique",
          leadTurnId: TurnId.make("turn-1"),
          automatic: false,
          title: "Another question",
          at: AT,
        })
        .pipe(Effect.flip);
      assert.strictEqual(busy._tag === "PairRoomRejectedError" && busy.reason, "peer-busy");

      const reloaded = yield* PairRoomStore.make;
      const room = Option.getOrThrow(yield* reloaded.findByThread(LEAD_THREAD));
      assert.strictEqual(room.roomId, ROOM_ID);
      assert.deepEqual(
        room.consults.map((consult) => consult.consultId),
        ["consult-1"],
      );
      assert.strictEqual(room.participants[0]?.persona, "astra");

      const sql = yield* SqlClient.SqlClient;
      yield* sql`DELETE FROM pair_rooms`;
    }),
  );

  it.effect("keeps a room whose assignment changed a file named only with spaces", () =>
    Effect.gen(function* () {
      const store = yield* PairRoomStore.make;
      yield* store.dispatch(createRoom);
      yield* store.dispatch({
        type: "assignment.create",
        roomId: ROOM_ID,
        assignmentId: "assignment-1",
        title: "Odd files",
        threadId: ThreadId.make("assignee-thread"),
        worktreePath: "/worktrees/pair-odd",
        branch: "pair/odd",
        baseCommit: "abc",
        scopeGlobs: ["src/**"],
        acceptanceCriteria: [],
        expectedArtifact: "patch",
        at: AT,
      });
      yield* store.dispatch({
        type: "assignment.update",
        roomId: ROOM_ID,
        assignmentId: "assignment-1",
        by: "server",
        changedFiles: [" ", "src/a.ts"],
        deviations: [" "],
        at: AT,
      });

      const reloaded = yield* PairRoomStore.make;
      const room = Option.getOrThrow(yield* reloaded.findByThread(LEAD_THREAD));
      assert.deepEqual(room.assignments[0]?.changedFiles, [" ", "src/a.ts"]);

      const sql = yield* SqlClient.SqlClient;
      yield* sql`DELETE FROM pair_rooms`;
    }),
  );

  it.effect("streams every room first, then again after each change", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const store = yield* PairRoomStore.make;
        yield* store.dispatch(createRoom);
        const received = yield* Queue.unbounded<PairRoomListEvent>();
        yield* store.streamRooms.pipe(
          Stream.runForEach((item) => Queue.offer(received, item)),
          Effect.forkScoped,
        );
        // The replace item proves the subscription is live before the update.
        const initial = yield* Queue.take(received);
        yield* store.dispatch({
          type: "room.update",
          roomId: ROOM_ID,
          status: "paused",
          statusReason: "Waiting on the user",
          at: AT,
        });
        const update = yield* Queue.take(received);
        assert.deepEqual(
          initial.map((room) => room.status),
          ["active"],
        );
        assert.deepEqual(
          update.map((room) => room.statusReason),
          ["Waiting on the user"],
        );

        const sql = yield* SqlClient.SqlClient;
        yield* sql`DELETE FROM pair_rooms`;
      }),
    ),
  );
});
