import {
  PairRoom,
  type PairRoomId,
  type PairRoomListEvent,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runPairMigrations } from "./PairMigrations.ts";
import { decidePairRoom, findPairRoomByThread, type PairRoomCommand } from "./pairRoomDecider.ts";

export class PairRoomRejectedError extends Schema.TaggedError<PairRoomRejectedError>()(
  "PairRoomRejectedError",
  { reason: Schema.String, detail: Schema.String },
) {
  override get message(): string {
    return this.detail;
  }
}

export class PairRoomPersistenceError extends Schema.TaggedError<PairRoomPersistenceError>()(
  "PairRoomPersistenceError",
  { operation: Schema.String, cause: Schema.Defect() },
) {
  override get message(): string {
    return `Pair room storage failed during ${this.operation}.`;
  }
}

export interface PairRoomChange {
  readonly sequence: number;
  readonly previous: PairRoom | undefined;
  readonly room: PairRoom;
  readonly command: PairRoomCommand;
}

/**
 * Durable Pair Room records. Every mutation goes through `dispatch`, which
 * runs the pure decider under one lock, writes the whole record, and only
 * then updates memory and publishes the change.
 */
export class PairRoomStore extends Context.Service<
  PairRoomStore,
  {
    readonly list: Effect.Effect<ReadonlyArray<PairRoom>>;
    readonly get: (roomId: PairRoomId) => Effect.Effect<Option.Option<PairRoom>>;
    readonly findByThread: (threadId: ThreadId) => Effect.Effect<Option.Option<PairRoom>>;
    readonly dispatch: (
      command: PairRoomCommand,
    ) => Effect.Effect<PairRoom, PairRoomRejectedError | PairRoomPersistenceError>;
    /** Live changes after subscription. Use `streamRooms` for a replayable client stream. */
    readonly subscribeChanges: Effect.Effect<Stream.Stream<PairRoomChange>, never, Scope.Scope>;
    /** Every room now, then again after each change. */
    readonly streamRooms: Stream.Stream<PairRoomListEvent>;
  }
>()("t3/pair/PairRoomStore") {}

const PairRoomJson = Schema.fromJsonString(PairRoom);
const decodeRoomJson = Schema.decodeUnknownEffect(PairRoomJson);
const encodeRoomJson = Schema.encodeEffect(PairRoomJson);

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* runPairMigrations().pipe(Effect.orDie);

  const rows = yield* sql<{ readonly room_id: string; readonly room_json: string }>`
    SELECT room_id, room_json FROM pair_rooms ORDER BY updated_at ASC
  `.pipe(Effect.orDie);
  const rooms = new Map<PairRoomId, PairRoom>();
  for (const row of rows) {
    const decoded = yield* decodeRoomJson(row.room_json).pipe(Effect.option);
    if (Option.isSome(decoded)) {
      rooms.set(decoded.value.roomId, decoded.value);
    } else {
      yield* Effect.logWarning("Skipping a pair room record that no longer decodes", {
        roomId: row.room_id,
      });
    }
  }

  let sequence = 0;
  const changes = yield* PubSub.unbounded<PairRoomChange>();
  const lock = yield* Semaphore.make(1);

  const persist = (room: PairRoom) =>
    encodeRoomJson(room).pipe(
      Effect.flatMap(
        (json) => sql`
          INSERT INTO pair_rooms (room_id, project_id, room_json, updated_at)
          VALUES (${room.roomId}, ${room.projectId}, ${json}, ${room.updatedAt})
          ON CONFLICT (room_id) DO UPDATE SET
            room_json = excluded.room_json,
            updated_at = excluded.updated_at
        `,
      ),
      Effect.mapError((cause) => new PairRoomPersistenceError({ operation: "persist", cause })),
    );

  const dispatch = (command: PairRoomCommand) =>
    Semaphore.withPermits(
      lock,
      1,
    )(
      Effect.gen(function* () {
        const result = decidePairRoom(rooms, command);
        if (!result.ok) {
          return yield* new PairRoomRejectedError(result.rejection);
        }
        const previous = rooms.get(result.room.roomId);
        if (previous === result.room) return previous;
        yield* persist(result.room);
        rooms.set(result.room.roomId, result.room);
        sequence += 1;
        yield* PubSub.publish(changes, {
          sequence,
          previous,
          room: result.room,
          command,
        });
        return result.room;
      }),
    );

  // One-slot sliding mailbox per subscriber: lists are whole states, so a slow
  // socket skipping intermediates still ends on the latest rooms.
  const streamRooms = Stream.callback<PairRoomListEvent>(
    (mailbox) =>
      Effect.gen(function* () {
        const subscription = yield* PubSub.subscribe(changes);
        Queue.offerUnsafe(mailbox, [...rooms.values()]);
        yield* Stream.fromSubscription(subscription).pipe(
          Stream.runForEach(() =>
            Effect.sync(() => Queue.offerUnsafe(mailbox, [...rooms.values()])),
          ),
          Effect.forkScoped,
        );
      }),
    { bufferSize: 1, strategy: "sliding" },
  );

  return PairRoomStore.of({
    list: Effect.sync(() => [...rooms.values()]),
    get: (roomId) => Effect.sync(() => Option.fromUndefinedOr(rooms.get(roomId))),
    findByThread: (threadId) =>
      Effect.sync(() => Option.fromUndefinedOr(findPairRoomByThread(rooms.values(), threadId))),
    dispatch,
    subscribeChanges: PubSub.subscribe(changes).pipe(Effect.map(Stream.fromSubscription)),
    streamRooms,
  });
});

export const layer = Layer.effect(PairRoomStore, make);
