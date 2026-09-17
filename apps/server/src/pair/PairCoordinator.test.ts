import {
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type OrchestrationCommand,
  type OrchestrationEvent,
  type OrchestrationMessageContext,
  type OrchestrationProjectShell,
  type OrchestrationThread,
  type OrchestrationThreadShell,
  type PairRoom,
  type PairRoomId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import { pairRoomNoteContext, readPairRoomNote } from "@t3tools/shared/pairRoomNote";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { ProviderService } from "../provider/Services/ProviderService.ts";
import * as PairCoordinator from "./PairCoordinator.ts";
import * as PairRoomStore from "./PairRoomStore.ts";
import { PairWorkspace, PairWorkspaceError, type PairIntegrationResult } from "./PairWorkspace.ts";

const PROJECT_ID = ProjectId.make("project-1");
const LEAD = ThreadId.make("lead-thread");
const LEAD_TURN = TurnId.make("lead-turn-1");
const REVIEW_WORKTREE = "/worktrees/repo/pair-review-room";

let randomCalls = 0;
const testCrypto = Crypto.make({
  randomBytes: (size) => {
    randomCalls += 1;
    const bytes = new Uint8Array(size);
    new DataView(bytes.buffer).setUint32(0, randomCalls);
    return bytes;
  },
  digest: (_algorithm, data) => Effect.succeed(data),
});

function makeShell(
  overrides: Partial<OrchestrationThreadShell> & Pick<OrchestrationThreadShell, "id">,
): OrchestrationThreadShell {
  return {
    projectId: PROJECT_ID,
    title: "Retry policy",
    modelSelection: {
      instanceId: ProviderInstanceId.make("claudeAgent"),
      model: "claude-fable-5-1",
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: "main",
    worktreePath: null,
    pullRequests: [],
    latestTurn: null,
    createdAt: "1970-01-01T00:00:00.000Z",
    updatedAt: "1970-01-01T00:00:00.000Z",
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    session: null,
    latestUserMessageAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
    ...overrides,
  };
}

const runningSession = (threadId: ThreadId, activeTurnId: TurnId | null) => ({
  threadId,
  status: activeTurnId ? ("running" as const) : ("ready" as const),
  providerName: "claudeAgent",
  providerInstanceId: ProviderInstanceId.make("claudeAgent"),
  runtimeMode: "full-access" as const,
  activeTurnId,
  lastError: null,
  updatedAt: "1970-01-01T00:00:00.000Z",
});

const threadEvent = (
  type: OrchestrationEvent["type"],
  threadId: ThreadId,
  payload: Record<string, unknown> = {},
) =>
  ({
    type,
    aggregateKind: "thread",
    aggregateId: threadId,
    payload: { threadId, ...payload },
  }) as unknown as OrchestrationEvent;

const makeHarness = Effect.fn("makePairCoordinatorHarness")(function* (options?: {
  readonly seed?: (
    store: PairRoomStore.PairRoomStore["Service"],
  ) => Effect.Effect<
    void,
    PairRoomStore.PairRoomRejectedError | PairRoomStore.PairRoomPersistenceError
  >;
}) {
  const commands = yield* Ref.make<ReadonlyArray<OrchestrationCommand>>([]);
  const shells = yield* Ref.make(
    new Map<ThreadId, OrchestrationThreadShell>([
      [LEAD, makeShell({ id: LEAD, session: runningSession(LEAD, LEAD_TURN) })],
    ]),
  );
  const messages = yield* Ref.make(new Map<ThreadId, OrchestrationThread["messages"]>());
  const domainEvents = yield* PubSub.unbounded<OrchestrationEvent>();
  // Every dispatched command, replayed so a test can wait on one that already landed.
  const dispatched = yield* PubSub.unbounded<OrchestrationCommand>({ replay: 64 });
  // Replay covers the coordinator subscribing to runtime events after a test publishes one.
  const runtimeEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>({ replay: 8 });
  const changedFiles = yield* Ref.make<ReadonlyArray<string>>([]);
  const repository = yield* Ref.make<boolean>(true);
  const integration = yield* Ref.make<PairIntegrationResult>({
    status: "merged",
    commit: "merge1234567890",
  });

  const dependencies = Layer.mergeAll(
    Layer.mock(OrchestrationEngineService)({
      readEvents: () => Stream.empty,
      dispatch: (command) =>
        Effect.gen(function* () {
          yield* Ref.update(commands, (all) => [...all, command]);
          yield* PubSub.publish(dispatched, command);
          if (command.type === "thread.create") {
            yield* Ref.update(shells, (all) =>
              new Map(all).set(
                command.threadId,
                makeShell({
                  id: command.threadId,
                  title: command.title,
                  modelSelection: command.modelSelection,
                  branch: command.branch,
                  worktreePath: command.worktreePath,
                }),
              ),
            );
          }
          return { sequence: 1 };
        }),
      streamDomainEvents: Stream.fromPubSub(domainEvents),
      subscribeDomainEvents: PubSub.subscribe(domainEvents).pipe(
        Effect.map(Stream.fromSubscription),
      ),
      latestSequence: Effect.succeed(0),
    }),
    Layer.mock(ProjectionSnapshotQuery)({
      getThreadShellById: (threadId) =>
        Ref.get(shells).pipe(Effect.map((all) => Option.fromUndefinedOr(all.get(threadId)))),
      getProjectShellById: () =>
        Effect.succeed(Option.some({ workspaceRoot: "/repo" } as OrchestrationProjectShell)),
      getThreadDetailById: (threadId) =>
        Ref.get(messages).pipe(
          Effect.map((all) =>
            all.has(threadId)
              ? Option.some({ messages: all.get(threadId) } as unknown as OrchestrationThread)
              : Option.none(),
          ),
        ),
    }),
    Layer.mock(ProviderService)({ streamEvents: Stream.fromPubSub(runtimeEvents) }),
    Layer.mock(PairWorkspace)({
      assertRepository: ({ cwd }) =>
        Ref.get(repository).pipe(
          Effect.flatMap((isRepository) =>
            isRepository
              ? Effect.void
              : Effect.fail(
                  new PairWorkspaceError({
                    operation: "assertRepository",
                    detail: `Pair rooms need a git repository ... and ${cwd} is not one.`,
                  }),
                ),
          ),
        ),
      resolveCommit: () => Effect.succeed("base1234567890"),
      syncReviewWorktree: () =>
        Effect.succeed({ worktreePath: REVIEW_WORKTREE, snapshotCommit: "snap1234567890" }),
      planAssignmentWorktree: () =>
        Effect.succeed({
          repoRoot: "/repo",
          worktreePath: "/worktrees/repo/pair-retry-tests",
          branch: "pair/retry-tests",
        }),
      createAssignmentWorktree: () => Effect.void,
      changedFiles: () => Ref.get(changedFiles),
      sealAssignment: () => Effect.succeed("approved1234567890"),
      integrate: () => Ref.get(integration),
    }),
    Layer.succeed(Crypto.Crypto, testCrypto),
    PairRoomStore.layer.pipe(Layer.provide(SqlitePersistenceMemory)),
  );

  const dependencyContext = yield* Layer.build(dependencies);
  const store = Context.get(dependencyContext, PairRoomStore.PairRoomStore);
  if (options?.seed) yield* options.seed(store).pipe(Effect.orDie);
  const coordinatorContext = yield* Layer.build(
    PairCoordinator.layer.pipe(Layer.provide(Layer.succeedContext(dependencyContext))),
  );
  const coordinator = Context.get(coordinatorContext, PairCoordinator.PairCoordinator);

  const recorded = <Type extends OrchestrationCommand["type"]>(type: Type) =>
    Ref.get(commands).pipe(
      Effect.map((all) =>
        all.filter(
          (command): command is Extract<OrchestrationCommand, { type: Type }> =>
            command.type === type,
        ),
      ),
    );

  /** Resolves with the first dispatched command matching `predicate`; the receipt tests wait on. */
  const commandWhere = (predicate: (command: OrchestrationCommand) => boolean) =>
    Stream.fromPubSub(dispatched).pipe(
      Stream.filter(predicate),
      Stream.runHead,
      Effect.map(Option.getOrThrow),
    );

  const setShell = (threadId: ThreadId, update: Partial<OrchestrationThreadShell>) =>
    Ref.update(shells, (all) => new Map(all).set(threadId, { ...all.get(threadId)!, ...update }));

  /** Simulates the provider finishing the latest turn on `threadId`, then lands its event. */
  const finishTurn = Effect.fn("finishTurn")(function* (input: {
    readonly threadId: ThreadId;
    readonly state: "completed" | "error";
    readonly answer?: string;
  }) {
    const turnStart = (yield* recorded("thread.turn.start")).findLast(
      (command) => command.threadId === input.threadId,
    )!;
    const turnId = TurnId.make(`turn-for-${turnStart.commandId}`);
    yield* setShell(input.threadId, {
      latestTurn: {
        turnId,
        state: input.state,
        requestedAt: turnStart.createdAt,
        startedAt: turnStart.createdAt,
        completedAt: turnStart.createdAt,
        assistantMessageId: null,
      },
    });
    if (input.answer) {
      yield* Ref.update(messages, (all) =>
        new Map(all).set(input.threadId, [
          { role: "assistant", text: input.answer, turnId, streaming: false },
        ] as unknown as OrchestrationThread["messages"]),
      );
    }
    yield* PubSub.publish(domainEvents, threadEvent("thread.session-set", input.threadId));
  });

  /** Resolves once a room change satisfies `predicate`; the receipt tests wait on. */
  const roomWhere = (predicate: (room: PairRoom) => boolean) =>
    store.streamRooms.pipe(
      Stream.flatMap((rooms) => Stream.fromIterable(rooms)),
      Stream.filter(predicate),
      Stream.runHead,
      Effect.map(Option.getOrThrow),
    );

  let userMessages = 0;
  /** Lands a message the user typed on the Lead's thread, as the turn it starts announces it. */
  const sendUserMessage = Effect.fn("sendUserMessage")(function* (input: {
    readonly text: string;
    readonly createdAt: string;
    readonly context?: OrchestrationMessageContext;
  }) {
    userMessages += 1;
    const messageId = MessageId.make(`user-message-${userMessages}`);
    yield* Ref.update(messages, (all) =>
      new Map(all).set(LEAD, [
        ...(all.get(LEAD) ?? []),
        {
          id: messageId,
          role: "user",
          text: input.text,
          context: input.context,
          turnId: null,
          streaming: false,
          createdAt: input.createdAt,
          updatedAt: input.createdAt,
        },
      ] as unknown as OrchestrationThread["messages"]),
    );
    yield* PubSub.publish(
      domainEvents,
      threadEvent("thread.turn-start-requested", LEAD, { messageId, createdAt: input.createdAt }),
    );
  });

  const createRoom = (mode: "adaptive" | "pair" | "roundtable" = "adaptive") =>
    coordinator
      .dispatchUserCommand({
        type: "room.create",
        projectId: PROJECT_ID,
        leadThreadId: LEAD,
        leadPersona: "fable",
        mode,
      })
      .pipe(Effect.map((result) => result.roomId));

  return {
    coordinator,
    store,
    recorded,
    commandWhere,
    setShell,
    finishTurn,
    roomWhere,
    createRoom,
    sendUserMessage,
    changedFiles,
    repository,
    integration,
    domainEvents,
    runtimeEvents,
  };
});

const peerThreadOf = (store: PairRoomStore.PairRoomStore["Service"], roomId: PairRoomId) =>
  store
    .get(roomId)
    .pipe(
      Effect.map(
        (room) =>
          Option.getOrThrow(room).participants.find((entry) => entry.role === "peer")!.threadId!,
      ),
    );

const MESSAGE_AT = "1970-01-01T00:00:00.000Z";

const turnStartWhere =
  (predicate: (command: Extract<OrchestrationCommand, { type: "thread.turn.start" }>) => boolean) =>
  (command: OrchestrationCommand) =>
    command.type === "thread.turn.start" && predicate(command);

const asTurnStart = (command: OrchestrationCommand) =>
  command as Extract<OrchestrationCommand, { type: "thread.turn.start" }>;

/** The Lead's turn for the message sent at MESSAGE_AT has ended. */
const leadTurnEnded = (state: "completed" | "interrupted") => ({
  session: runningSession(LEAD, null),
  latestTurn: {
    turnId: LEAD_TURN,
    state,
    requestedAt: MESSAGE_AT,
    startedAt: MESSAGE_AT,
    completedAt: MESSAGE_AT,
    assistantMessageId: null,
  },
});

describe("PairCoordinator", () => {
  it("recognizes an @-mention only as its own word", () => {
    expect(PairCoordinator.pairMessageMentions("@Astra, is this safe?", "astra")).toBe(true);
    expect(PairCoordinator.pairMessageMentions("what do you think @astra", "astra")).toBe(true);
    expect(PairCoordinator.pairMessageMentions("mail me@astra.dev", "astra")).toBe(false);
    expect(PairCoordinator.pairMessageMentions("ping @astra-bot", "astra")).toBe(false);
    expect(PairCoordinator.pairMessageMentions("@Fable, is this safe?", "astra")).toBe(false);
  });

  it.effect(
    "sends each roundtable message to the Peer and brings its answer to the Lead after the Lead's turn",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeHarness();
          const roomId = yield* harness.createRoom("roundtable");
          yield* harness.sendUserMessage({
            text: "Should retries use jitter?",
            createdAt: MESSAGE_AT,
          });

          const relay = asTurnStart(
            yield* harness.commandWhere(
              turnStartWhere(
                (command) => readPairRoomNote(command.message.context)?.purpose === "user-relay",
              ),
            ),
          );
          const peerThreadId = yield* peerThreadOf(harness.store, roomId);
          expect(relay.threadId).toBe(peerThreadId);
          expect(relay.message.text).toContain("Pair Room roundtable");
          expect(relay.message.text).toContain("The user's message:\n\nShould retries use jitter?");

          yield* harness.finishTurn({
            threadId: peerThreadId,
            state: "completed",
            answer: "Yes, full jitter, capped at 30 seconds.",
          });
          // A second message lands after the Peer's event is fully handled, so it marks that point.
          yield* harness.sendUserMessage({ text: "@Astra and the cap?", createdAt: MESSAGE_AT });
          const both = yield* harness.roomWhere((room) => room.consults.length === 2);
          expect(both.consults.map((entry) => [entry.kind, entry.status])).toEqual([
            ["roundtable", "answered"],
            ["question", "running"],
          ]);
          const leadTurns = (yield* harness.recorded("thread.turn.start")).filter(
            (command) => command.threadId === LEAD,
          );
          expect(leadTurns).toEqual([]);

          yield* harness.setShell(LEAD, leadTurnEnded("completed"));
          yield* PubSub.publish(harness.domainEvents, threadEvent("thread.session-set", LEAD));
          const answerTurn = asTurnStart(
            yield* harness.commandWhere(turnStartWhere((command) => command.threadId === LEAD)),
          );
          expect(answerTurn.message.text).toContain(
            "Astra (Peer) answered the user's last message independently",
          );
          expect(answerTurn.message.text).toContain("Yes, full jitter, capped at 30 seconds.");
          expect(readPairRoomNote(answerTurn.message.context)).toEqual({
            purpose: "peer-answer",
            from: "astra",
            to: "fable",
          });
          yield* harness.roomWhere((room) => room.consults[0]?.answerDeliveredAt !== null);
        }),
      ),
  );

  it.effect(
    "sends an @-mention to the Peer in adaptive mode and brings nothing back when the Peer fails",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeHarness();
          const roomId = yield* harness.createRoom();
          // A turn the room starts itself is never relayed, even when its text names the Peer.
          yield* harness.sendUserMessage({
            text: "Handoff notes for @astra",
            createdAt: MESSAGE_AT,
            context: pairRoomNoteContext("note-1", {
              purpose: "handoff",
              from: "fable",
              to: "astra",
            }),
          });
          yield* harness.sendUserMessage({ text: "What would astra say?", createdAt: MESSAGE_AT });
          yield* harness.sendUserMessage({
            text: "@astra is retrying a 401 safe?",
            createdAt: MESSAGE_AT,
          });
          const room = yield* harness.roomWhere((candidate) => candidate.consults.length > 0);
          expect(room.consults).toMatchObject([
            {
              title: "@astra is retrying a 401 safe?",
              kind: "question",
              automatic: true,
              answerTo: "lead-turn",
            },
          ]);
          const relay = asTurnStart(
            yield* harness.commandWhere(
              turnStartWhere(
                (command) => readPairRoomNote(command.message.context)?.purpose === "user-relay",
              ),
            ),
          );
          expect(relay.message.text).toContain("the user addressed you directly");

          const peerThreadId = yield* peerThreadOf(harness.store, roomId);
          yield* harness.finishTurn({ threadId: peerThreadId, state: "error" });
          yield* harness.setShell(LEAD, leadTurnEnded("completed"));
          yield* PubSub.publish(harness.domainEvents, threadEvent("thread.session-set", LEAD));
          yield* harness.roomWhere(
            (candidate) => candidate.consults[0]?.answerDeliveredAt !== null,
          );
          const leadTurns = (yield* harness.recorded("thread.turn.start")).filter(
            (command) => command.threadId === LEAD,
          );
          expect(leadTurns).toEqual([]);
        }),
      ),
  );

  it.effect(
    "tells the user when the Peer is still busy instead of dropping the message silently",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeHarness();
          yield* harness.createRoom("roundtable");
          yield* harness.coordinator.consult(LEAD, {
            kind: "roundtable",
            question: "Jitter or not?",
            leadProposal: "Full jitter.",
            waitSeconds: 0,
          });
          yield* harness.sendUserMessage({ text: "Also check the cap.", createdAt: MESSAGE_AT });
          const skipped = yield* harness.commandWhere(
            (command) =>
              command.type === "thread.activity.append" &&
              (command.activity.payload as { status?: string }).status === "failed",
          );
          expect(skipped).toMatchObject({
            threadId: LEAD,
            activity: {
              kind: "task.completed",
              payload: { title: "Astra: Also check the cap." },
            },
          });
          expect(
            (skipped as Extract<OrchestrationCommand, { type: "thread.activity.append" }>).activity
              .payload,
          ).toMatchObject({ summary: expect.stringContaining("only Fable got this message") });
        }),
      ),
  );

  it.effect("consults Astra in a review worktree and returns its answer through pair_wait", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const roomId = yield* harness.createRoom();

        const pending = yield* harness.coordinator.consult(LEAD, {
          question: "Is retrying 401 responses safe?",
          waitSeconds: 0,
        });
        expect(pending).toMatchObject({ status: "pending", retryAfterSeconds: 0 });

        const peerThreadId = yield* peerThreadOf(harness.store, roomId);
        const [peerCreate] = yield* harness.recorded("thread.create");
        expect(peerCreate).toMatchObject({
          threadId: peerThreadId,
          modelSelection: { instanceId: "codex", model: "gpt-6-astra" },
          worktreePath: REVIEW_WORKTREE,
          branch: null,
        });
        const [turnStart] = yield* harness.recorded("thread.turn.start");
        expect(turnStart?.threadId).toBe(peerThreadId);
        expect(turnStart?.modelSelection).toMatchObject({ model: "gpt-6-astra" });
        expect(turnStart?.message.text).toContain(
          "Pair Room consult from Fable (Lead). You are Astra (Peer).",
        );
        expect(turnStart?.message.text).toContain("Is retrying 401 responses safe?");
        expect(readPairRoomNote(turnStart?.message.context)).toEqual({
          purpose: "consult",
          from: "fable",
          to: "astra",
        });

        const [started] = yield* harness.recorded("thread.activity.append");
        expect(started).toMatchObject({
          threadId: LEAD,
          activity: {
            kind: "task.started",
            turnId: LEAD_TURN,
            payload: {
              taskId: pending.handle,
              role: "Peer",
              model: "gpt-6-astra",
              pairMirror: true,
            },
          },
        });

        const peerWait = yield* harness.coordinator.wait(peerThreadId, {
          handle: pending.handle!,
          waitSeconds: 0,
        });
        expect(peerWait).toMatchObject({ status: "rejected" });

        yield* harness.finishTurn({
          threadId: peerThreadId,
          state: "completed",
          answer: "No. A 401 means the token is bad; refresh it instead.",
        });
        const answered = yield* harness.coordinator.wait(LEAD, {
          handle: pending.handle!,
          waitSeconds: 30,
        });
        expect(answered).toMatchObject({
          status: "answered",
          answer: "No. A 401 means the token is bad; refresh it instead.",
        });
        const mirrored = yield* harness.recorded("thread.activity.append");
        expect(mirrored.at(-1)?.activity).toMatchObject({
          kind: "task.completed",
          payload: { taskId: pending.handle, status: "completed" },
        });
      }),
    ),
  );

  it.effect(
    "keeps roles and round limits: Peer cannot consult, the Lead gets two rounds per turn",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeHarness();
          const roomId = yield* harness.createRoom();

          const outsider = yield* harness.coordinator
            .status(ThreadId.make("unrelated-thread"))
            .pipe(Effect.flip);
          expect(outsider._tag).toBe("PairToolUnavailableError");

          for (const round of [1, 2]) {
            const pending = yield* harness.coordinator.consult(LEAD, {
              question: `Round ${round}`,
              waitSeconds: 0,
            });
            if (round === 1) {
              const busy = yield* harness.coordinator.consult(LEAD, { question: "Too soon" });
              expect(busy).toMatchObject({ status: "rejected", reason: "peer-busy" });
            }
            const peerThreadId = yield* peerThreadOf(harness.store, roomId);
            yield* harness.finishTurn({ threadId: peerThreadId, state: "completed", answer: "ok" });
            yield* harness.coordinator.wait(LEAD, { handle: pending.handle!, waitSeconds: 30 });
          }

          const limited = yield* harness.coordinator.consult(LEAD, { question: "Round 3" });
          expect(limited).toMatchObject({ status: "rejected", reason: "round-limit" });

          const peerThreadId = yield* peerThreadOf(harness.store, roomId);
          const peerTries = yield* harness.coordinator.consult(peerThreadId, {
            question: "Me too",
          });
          expect(peerTries).toMatchObject({ status: "rejected", reason: "not-allowed" });

          const status = yield* harness.coordinator.status(peerThreadId);
          expect(status).toMatchObject({
            you: { persona: "astra", role: "peer" },
            other: { persona: "fable" },
            rounds: null,
          });
        }),
      ),
  );

  it.effect(
    "runs an assignment from brief to merge, holding approval while it strays out of scope",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeHarness();
          const roomId = yield* harness.createRoom();

          const assigned = yield* harness.coordinator.assign(LEAD, {
            title: "Retry tests",
            brief: "Add tests for the retry policy.",
            scopeGlobs: ["src/retry/**"],
            acceptanceCriteria: ["401 is never retried"],
          });
          expect(assigned).toMatchObject({ status: "assigned", branch: "pair/retry-tests" });
          const assigneeThread = ThreadId.make(assigned.threadId!);
          const brief = (yield* harness.recorded("thread.turn.start")).at(-1)!;
          expect(brief.threadId).toBe(assigneeThread);
          expect(brief.message.text).toContain("Only change files matching:\n- src/retry/**");

          const overlapping = yield* harness.coordinator.assign(LEAD, {
            title: "Also retry",
            brief: "Overlap",
            scopeGlobs: ["src/**"],
            acceptanceCriteria: [],
          });
          expect(overlapping).toMatchObject({ status: "rejected", reason: "scope-overlap" });

          yield* Ref.set(harness.changedFiles, ["src/retry/policy.test.ts", "package.json"]);
          const submitted = yield* harness.coordinator.submit(assigneeThread, {
            summary: "Added tests.",
            criteriaResults: [{ criterion: "401 is never retried", met: true }],
            testsRun: ["vp test run src/retry"],
            knownLimitations: [],
          });
          expect(submitted.assignment).toMatchObject({
            state: "submitted",
            deviations: ["package.json"],
          });

          const waited = yield* harness.coordinator.wait(LEAD, {
            handle: assigned.assignmentId!,
            waitSeconds: 30,
          });
          expect(waited).toMatchObject({ status: "updated", assignment: { state: "submitted" } });

          const refused = yield* harness.coordinator.review(LEAD, {
            assignmentId: assigned.assignmentId!,
            verdict: "approve",
            notes: "Looks good",
          });
          expect(refused).toMatchObject({ status: "rejected", reason: "scope-deviation" });

          yield* harness.coordinator.dispatchUserCommand({
            type: "assignment.set-scope",
            roomId,
            assignmentId: assigned.assignmentId!,
            scopeGlobs: ["src/retry/**", "package.json"],
          });
          const approved = yield* harness.coordinator.review(LEAD, {
            assignmentId: assigned.assignmentId!,
            verdict: "approve",
            notes: "Looks good",
          });
          expect(approved.assignment?.state).toBe("awaiting-user");

          const whileRunning = yield* harness.coordinator
            .dispatchUserCommand({
              type: "assignment.integrate",
              roomId,
              assignmentId: assigned.assignmentId!,
            })
            .pipe(Effect.flip);
          expect(whileRunning).toMatchObject({ reason: "conflict" });

          yield* harness.setShell(LEAD, { session: runningSession(LEAD, null) });
          yield* harness.coordinator.dispatchUserCommand({
            type: "assignment.integrate",
            roomId,
            assignmentId: assigned.assignmentId!,
          });
          const room = Option.getOrThrow(yield* harness.store.get(roomId));
          expect(room.assignments[0]).toMatchObject({
            state: "integrated",
            integrationCommit: "merge1234567890",
          });
        }),
      ),
  );

  it.effect("leaves a conflicting merge unmerged and waiting for the user", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const roomId = yield* harness.createRoom();
        yield* harness.setShell(LEAD, { session: runningSession(LEAD, null) });
        const assigned = yield* harness.coordinator.assign(LEAD, {
          title: "Retry tests",
          brief: "Add tests.",
          scopeGlobs: ["src/retry/**"],
          acceptanceCriteria: [],
        });
        yield* harness.coordinator.submit(ThreadId.make(assigned.threadId!), {
          summary: "Done",
          criteriaResults: [],
          testsRun: [],
          knownLimitations: [],
        });
        yield* harness.coordinator.review(LEAD, {
          assignmentId: assigned.assignmentId!,
          verdict: "approve",
          notes: "ok",
        });
        yield* Ref.set(harness.integration, { status: "conflict", detail: "CONFLICT in retry.ts" });
        const error = yield* harness.coordinator
          .dispatchUserCommand({
            type: "assignment.integrate",
            roomId,
            assignmentId: assigned.assignmentId!,
          })
          .pipe(Effect.flip);
        expect(error.reason).toBe("conflict");
        const room = Option.getOrThrow(yield* harness.store.get(roomId));
        expect(room.assignments[0]?.state).toBe("awaiting-user");
        expect(room.assignments[0]?.note).toContain("CONFLICT in retry.ts");
      }),
    ),
  );

  it.effect("starts an automatic review when the Lead changes files in pair mode without one", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const roomId = yield* harness.createRoom("pair");
        yield* PubSub.publish(
          harness.domainEvents,
          threadEvent("thread.turn-diff-completed", LEAD, {
            turnId: LEAD_TURN,
            files: [{ path: "src/retry.ts", kind: "modified", additions: 3, deletions: 1 }],
          }),
        );
        const room = yield* harness.roomWhere(
          (candidate) => candidate.roomId === roomId && candidate.consults.length > 0,
        );
        expect(room.consults[0]).toMatchObject({
          kind: "review",
          automatic: true,
          leadTurnId: LEAD_TURN,
        });
        yield* harness.roomWhere((candidate) => candidate.participants[1]?.threadId !== null);
        const reviewTurn = (yield* harness.recorded("thread.turn.start")).at(-1);
        expect(reviewTurn?.message.text).toContain("- src/retry.ts");
      }),
    ),
  );

  it.effect("pauses the room when a participant is rerouted to another model", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const roomId = yield* harness.createRoom();
        yield* PubSub.publish(harness.runtimeEvents, {
          type: "model.rerouted",
          threadId: LEAD,
          payload: {
            fromModel: "claude-fable-5-1",
            toModel: "claude-sonnet-5",
            reason: "capacity",
          },
        } as unknown as ProviderRuntimeEvent);
        const paused = yield* harness.roomWhere(
          (room) => room.roomId === roomId && room.status === "paused",
        );
        expect(paused.statusReason).toContain("claude-sonnet-5");
        const refused = yield* harness.coordinator.consult(LEAD, { question: "Still there?" });
        expect(refused).toMatchObject({ status: "rejected", reason: "room-paused" });
      }),
    ),
  );

  it.effect("mirrors a Peer approval wait onto the consult card in the Lead's thread", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const roomId = yield* harness.createRoom();
        const pending = yield* harness.coordinator.consult(LEAD, {
          question: "Can you run the migration check?",
          waitSeconds: 0,
        });
        const peerThreadId = yield* peerThreadOf(harness.store, roomId);
        const approvalEvent = (kind: string) =>
          threadEvent("thread.activity-appended", peerThreadId, {
            activity: { kind, payload: {} },
          });
        const progressFor = (status: string) => (command: OrchestrationCommand) =>
          command.type === "thread.activity.append" &&
          command.threadId === LEAD &&
          command.activity.kind === "task.progress" &&
          (command.activity.payload as { status?: string }).status === status;

        yield* harness.setShell(peerThreadId, { hasPendingApprovals: true });
        yield* PubSub.publish(harness.domainEvents, approvalEvent("approval.requested"));
        const waiting = yield* harness.commandWhere(progressFor("waiting"));
        expect(waiting).toMatchObject({
          activity: {
            payload: {
              taskId: pending.handle,
              summary: "Waiting for your approval in Astra's thread.",
            },
          },
        });

        yield* harness.setShell(peerThreadId, { hasPendingApprovals: false });
        yield* PubSub.publish(harness.domainEvents, approvalEvent("approval.resolved"));
        const resumed = yield* harness.commandWhere(progressFor("running"));
        expect(resumed).toMatchObject({ activity: { payload: { taskId: pending.handle } } });
      }),
    ),
  );

  it.effect("switches the Lead through a handoff the user confirms", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const roomId = yield* harness.createRoom();
        const busy = yield* Effect.flip(
          harness.coordinator.dispatchUserCommand({ type: "lead.switch-start", roomId }),
        );
        expect(busy).toMatchObject({ reason: "conflict" });

        yield* harness.setShell(LEAD, {
          session: runningSession(LEAD, null),
          worktreePath: "/worktrees/repo/lead",
          branch: "feature/retries",
        });
        yield* harness.coordinator.dispatchUserCommand({ type: "lead.switch-start", roomId });
        const request = (yield* harness.recorded("thread.turn.start")).at(-1);
        expect(request?.threadId).toBe(LEAD);
        expect(request?.message.text).toContain("handing the Lead role from Fable to Astra");
        expect(readPairRoomNote(request?.message.context)?.purpose).toBe("handoff-request");
        const refused = yield* harness.coordinator.consult(LEAD, { question: "Still there?" });
        expect(refused).toMatchObject({ status: "rejected", reason: "lead-switching" });

        yield* harness.finishTurn({
          threadId: LEAD,
          state: "completed",
          answer: "Objective: retry 429s with backoff. Next: add jitter.",
        });
        const ready = yield* harness.roomWhere(
          (room) => room.roomId === roomId && room.leadSwitch?.phase === "ready",
        );
        expect(ready.leadSwitch?.handoff).toContain("add jitter");

        const confirmed = yield* harness.coordinator.dispatchUserCommand({
          type: "lead.switch-confirm",
          roomId,
        });
        const newLead = confirmed.threadId!;
        const create = (yield* harness.recorded("thread.create")).at(-1);
        expect(create).toMatchObject({
          threadId: newLead,
          modelSelection: { instanceId: "codex", model: "gpt-6-astra" },
          worktreePath: "/worktrees/repo/lead",
          branch: "feature/retries",
        });
        const room = Option.getOrThrow(yield* harness.store.get(roomId));
        expect(room.participants).toEqual([
          { persona: "astra", role: "lead", threadId: newLead },
          { persona: "fable", role: "peer", threadId: null },
        ]);
        expect(room.formerParticipants).toMatchObject([{ persona: "fable", threadId: LEAD }]);
        const handoffTurn = (yield* harness.recorded("thread.turn.start")).at(-1);
        expect(handoffTurn?.threadId).toBe(newLead);
        expect(handoffTurn?.message.text).toContain("you are Astra, and you are now the Lead");
        expect(handoffTurn?.message.text).toContain("add jitter");
        expect(readPairRoomNote(handoffTurn?.message.context)).toEqual({
          purpose: "handoff",
          from: "fable",
          to: "astra",
          fromThreadId: LEAD,
        });
      }),
    ),
  );

  it.effect("refuses a room in a folder git does not track, before the first turn", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        yield* Ref.set(harness.repository, false);

        // The common path: the room is created with the first message, before
        // the Lead's thread exists, so the project is all there is to check.
        const draft = yield* Effect.flip(
          harness.coordinator.dispatchUserCommand({
            type: "room.create",
            projectId: PROJECT_ID,
            leadThreadId: ThreadId.make("draft-thread"),
            leadPersona: "fable",
            mode: "roundtable",
          }),
        );
        expect(draft).toMatchObject({ reason: "invalid" });
        expect(draft.detail).toContain("/repo is not one");

        const existing = yield* Effect.flip(harness.createRoom("roundtable"));
        expect(existing).toMatchObject({ reason: "invalid" });
        expect(yield* harness.store.list).toEqual([]);
      }),
    ),
  );

  it.effect("closes the room when the Lead's thread is deleted", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const roomId = yield* harness.createRoom();
        yield* PubSub.publish(
          harness.domainEvents,
          threadEvent("thread.deleted", LEAD, { deletedAt: "1970-01-01T00:00:01.000Z" }),
        );
        const closed = yield* harness.roomWhere(
          (room) => room.roomId === roomId && room.status === "closed",
        );
        expect(closed.statusReason).toBe("The Lead's thread was deleted.");
      }),
    ),
  );

  it.effect("fails in-flight consults and interrupts running assignments after a restart", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const at = "1970-01-01T00:00:00.000Z";
        let roomId: PairRoomId | undefined;
        const harness = yield* makeHarness({
          seed: (store) =>
            Effect.gen(function* () {
              const room = yield* store.dispatch({
                type: "room.create",
                roomId: "room-restart" as PairRoomId,
                projectId: PROJECT_ID,
                leadThreadId: LEAD,
                leadPersona: "fable",
                mode: "adaptive",
                at,
              });
              roomId = room.roomId;
              yield* store.dispatch({
                type: "consult.request",
                roomId: room.roomId,
                consultId: "consult-inflight",
                kind: "critique",
                leadTurnId: LEAD_TURN,
                automatic: false,
                title: "In flight",
                at,
              });
              yield* store.dispatch({
                type: "assignment.create",
                roomId: room.roomId,
                assignmentId: "assignment-inflight",
                title: "In flight",
                threadId: ThreadId.make("assignment-thread"),
                worktreePath: "/worktrees/repo/pair-inflight",
                branch: "pair/inflight",
                baseCommit: "base",
                scopeGlobs: ["src/**"],
                acceptanceCriteria: [],
                expectedArtifact: "patch",
                at,
              });
            }),
        });
        const room = Option.getOrThrow(yield* harness.store.get(roomId!));
        expect(room.consults[0]).toMatchObject({ status: "failed" });
        expect(room.consults[0]?.error).toContain("server restarted");
        expect(room.assignments[0]?.state).toBe("interrupted");
      }),
    ),
  );
});
