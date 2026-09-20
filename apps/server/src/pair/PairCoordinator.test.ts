import {
  MessageId,
  PAIR_CONVERSATION_MAX_EXCHANGES,
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
import * as Fiber from "effect/Fiber";
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
import { pairTranscriptKey } from "./PairTranscript.ts";
import { PairWorkspace, PairWorkspaceError, type PairIntegrationResult } from "./PairWorkspace.ts";

const PROJECT_ID = ProjectId.make("project-1");
const MESSAGE_AT = "1970-01-01T00:00:00.000Z";
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
  /** Thread messages already on disk when the coordinator starts. */
  readonly messages?: ReadonlyMap<ThreadId, OrchestrationThread["messages"]>;
  /** Thread shells when the coordinator starts; by default the Lead is mid-turn. */
  readonly shells?: ReadonlyMap<ThreadId, OrchestrationThreadShell>;
  /** What the workspace answers when asked whether a branch already holds an approved commit. */
  readonly mergedElsewhere?: string | null;
}) {
  const commands = yield* Ref.make<ReadonlyArray<OrchestrationCommand>>([]);
  // The engine answers a repeated command id with its first receipt; so does this.
  const receipts = new Set<string>();
  const shells = yield* Ref.make(
    new Map<ThreadId, OrchestrationThreadShell>(
      options?.shells ?? [
        [LEAD, makeShell({ id: LEAD, session: runningSession(LEAD, LEAD_TURN) })],
      ],
    ),
  );
  const messages = yield* Ref.make(
    new Map<ThreadId, OrchestrationThread["messages"]>(options?.messages ?? []),
  );
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
  /** What `containsCommit` answers: the branch head once the approved commit is on the branch. */
  const mergedElsewhere = yield* Ref.make<string | null>(options?.mergedElsewhere ?? null);
  /** The checkout the last review snapshot was taken from, and the worktree the last merge ran in. */
  const syncedFrom = yield* Ref.make<string | null>(null);
  const integratedInto = yield* Ref.make<string | null>(null);
  /** Worktrees of the project's repository, with the branch each has checked out. */
  const worktrees = yield* Ref.make<ReadonlyMap<string, string | null>>(
    new Map([
      ["/repo", "main"],
      ["/repo-ux", "dev/ux"],
    ]),
  );

  const dependencies = Layer.mergeAll(
    Layer.mock(OrchestrationEngineService)({
      readEvents: () => Stream.empty,
      dispatch: (command) =>
        Effect.gen(function* () {
          if (receipts.has(command.commandId)) return { sequence: 1 };
          receipts.add(command.commandId);
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
          if (command.type === "thread.message.user.append") {
            yield* Ref.update(messages, (all) =>
              new Map(all).set(command.threadId, [
                ...(all.get(command.threadId) ?? []),
                {
                  id: command.message.messageId,
                  role: "user",
                  text: command.message.text,
                  attachments: command.message.attachments,
                  context: command.message.context,
                  turnId: null,
                  streaming: false,
                  createdAt: command.createdAt,
                  updatedAt: command.createdAt,
                },
              ] as unknown as OrchestrationThread["messages"]),
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
      currentBranch: ({ cwd }) =>
        Ref.get(worktrees).pipe(Effect.map((all) => all.get(cwd) ?? null)),
      describeCheckout: ({ cwd }) =>
        Ref.get(worktrees).pipe(
          Effect.flatMap((all) =>
            all.has(cwd)
              ? Effect.succeed({ path: cwd, branch: all.get(cwd) ?? null })
              : Effect.fail(
                  new PairWorkspaceError({
                    operation: "describeCheckout",
                    detail: `${cwd} is not a worktree of this project's repository (/repo), so the room cannot follow it.`,
                  }),
                ),
          ),
        ),
      resolveBase: ({ cwd, ref }) =>
        Ref.get(worktrees).pipe(
          Effect.flatMap((all) =>
            ref !== undefined && !ref.startsWith("dev/") && ref !== "main"
              ? Effect.fail(
                  new PairWorkspaceError({
                    operation: "resolveBase",
                    detail: `"${ref}" is not a local branch.`,
                  }),
                )
              : Effect.succeed({ commit: "base1234567890", branch: ref ?? all.get(cwd) ?? null }),
          ),
        ),
      findBranchWorktree: ({ branch }) =>
        Ref.get(worktrees).pipe(
          Effect.map((all) => [...all.entries()].find(([, name]) => name === branch)?.[0] ?? null),
        ),
      containsCommit: () => Ref.get(mergedElsewhere),
      syncReviewWorktree: ({ leadCwd }) =>
        Ref.set(syncedFrom, leadCwd).pipe(
          Effect.as({ worktreePath: REVIEW_WORKTREE, snapshotCommit: "snap1234567890" }),
        ),
      planAssignmentWorktree: () =>
        Effect.succeed({
          repoRoot: "/repo",
          worktreePath: "/worktrees/repo/pair-retry-tests",
          branch: "pair/retry-tests",
        }),
      createAssignmentWorktree: () => Effect.void,
      changedFiles: () => Ref.get(changedFiles),
      sealAssignment: () => Effect.succeed("approved1234567890"),
      integrate: ({ targetCwd }) =>
        Ref.set(integratedInto, targetCwd).pipe(Effect.andThen(Ref.get(integration))),
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
    readonly state: "completed" | "error" | "interrupted";
    readonly answer?: string;
    /** Defaults to the turn the latest `thread.turn.start` on the thread began. */
    readonly turnId?: TurnId;
    /** Defaults to the moment the turn was requested. */
    readonly completedAt?: string;
  }) {
    const turnStart = (yield* recorded("thread.turn.start")).findLast(
      (command) => command.threadId === input.threadId,
    );
    const turnId = input.turnId ?? TurnId.make(`turn-for-${turnStart!.commandId}`);
    const requestedAt = turnStart?.createdAt ?? MESSAGE_AT;
    yield* setShell(input.threadId, {
      session: runningSession(input.threadId, null),
      latestTurn: {
        turnId,
        state: input.state,
        requestedAt,
        startedAt: requestedAt,
        completedAt: input.completedAt ?? requestedAt,
        assistantMessageId: null,
      },
    });
    if (input.answer) {
      yield* Ref.update(messages, (all) =>
        new Map(all).set(input.threadId, [
          ...(all.get(input.threadId) ?? []),
          {
            id: MessageId.make(`answer-${turnId}`),
            role: "assistant",
            text: input.answer,
            turnId,
            streaming: false,
            createdAt: requestedAt,
            updatedAt: requestedAt,
          },
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
  /** Lands a message the user typed on a participant's thread (the Lead's by default), as the turn it starts announces it. */
  const sendUserMessage = Effect.fn("sendUserMessage")(function* (input: {
    readonly text: string;
    readonly createdAt: string;
    readonly context?: OrchestrationMessageContext;
    readonly threadId?: ThreadId;
  }) {
    userMessages += 1;
    const threadId = input.threadId ?? LEAD;
    const messageId = MessageId.make(`user-message-${userMessages}`);
    yield* Ref.update(messages, (all) =>
      new Map(all).set(threadId, [
        ...(all.get(threadId) ?? []),
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
      threadEvent("thread.turn-start-requested", threadId, {
        messageId,
        createdAt: input.createdAt,
      }),
    );
    return messageId;
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
    mergedElsewhere,
    worktrees,
    syncedFrom,
    integratedInto,
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

const appendsTo = (harness: Effect.Success<ReturnType<typeof makeHarness>>, threadId: ThreadId) =>
  harness.recorded("thread.message.user.append").pipe(
    Effect.map((commands) =>
      commands
        .filter((command) => command.threadId === threadId)
        .map((command) => ({
          text: command.message.text,
          note: readPairRoomNote(command.message.context),
          createdAt: command.createdAt,
        })),
    ),
  );

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
          ).toMatchObject({ summary: expect.stringContaining("Fable answers this one alone") });
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
          "Pair Room: Fable (Lead) is asking you. You are Astra (Peer).",
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

  it.effect("holds a user's decision open until they answer, then hands the answer back", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const roomId = yield* harness.createRoom();

        const routine = yield* harness.coordinator.recordDecision(LEAD, {
          category: "routine",
          title: "Name the helper",
          position: "Call it retryPolicy.",
          resolution: "Called it retryPolicy.",
          waitSeconds: 0,
        });
        expect(routine).toMatchObject({ status: "recorded", handle: null });

        const pending = yield* harness.coordinator.recordDecision(LEAD, {
          category: "security",
          title: "Retry on 401",
          position: "Retrying a 401 risks locking the account.",
          leadRecommendation: "Refresh the token instead.",
          waitSeconds: 0,
        });
        expect(pending).toMatchObject({ status: "pending", retryAfterSeconds: 0 });
        expect(pending.detail).toContain("belongs to the user");
        const handle = pending.handle!;

        // The Lead is mid-turn, waiting on the call it just recorded.
        const answered = yield* harness.coordinator
          .wait(LEAD, { handle, waitSeconds: 30 })
          .pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* harness.coordinator.dispatchUserCommand({
          type: "decision.resolve",
          roomId,
          decisionId: handle,
          resolution: "Refresh the token; never retry a 401.",
        });
        const result = yield* Fiber.join(answered);
        expect(result).toMatchObject({
          status: "answered",
          answer: "Refresh the token; never retry a 401.",
          decision: { resolvedBy: "user" },
        });

        // The waiting call took the answer, so no turn repeats it.
        const leadTurns = (yield* harness.recorded("thread.turn.start")).filter(
          (command) => command.threadId === LEAD,
        );
        expect(leadTurns).toEqual([]);
        const room = Option.getOrThrow(yield* harness.store.get(roomId));
        expect(room.decisions.at(-1)?.resolutionDeliveredAt).not.toBeNull();
      }),
    ),
  );

  it.effect("brings a decision the user answered late to the Lead as a turn", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const roomId = yield* harness.createRoom();
        const pending = yield* harness.coordinator.recordDecision(LEAD, {
          category: "scope",
          title: "Rewrite the retry module",
          position: "It needs a rewrite to fix this properly.",
          waitSeconds: 0,
        });
        // The Lead gave up waiting and its turn ended.
        yield* harness.setShell(LEAD, leadTurnEnded("completed"));

        yield* harness.coordinator.dispatchUserCommand({
          type: "decision.resolve",
          roomId,
          decisionId: pending.handle!,
          resolution: "Patch it now, rewrite next sprint.",
        });

        const turn = asTurnStart(
          yield* harness.commandWhere(turnStartWhere((command) => command.threadId === LEAD)),
        );
        expect(turn.message.text).toContain("Patch it now, rewrite next sprint.");
        expect(turn.message.text).toContain("Rewrite the retry module");
        expect(readPairRoomNote(turn.message.context)).toEqual({
          purpose: "decision",
          from: "fable",
          to: "fable",
        });
        const status = yield* harness.coordinator.status(LEAD);
        expect(status.decisions).toMatchObject([
          { resolution: "Patch it now, rewrite next sprint.", resolvedBy: "user" },
        ]);
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
                targetBranch: null,
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

  describe("transcript", () => {
    it.effect(
      "catches a new Peer up on the Lead's thread, then copies what each side says without starting turns",
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const harness = yield* makeHarness();
            const roomId = yield* harness.createRoom();
            // No Peer thread yet, so this message has nowhere to go until the Peer exists.
            const first = yield* harness.sendUserMessage({
              text: "Let's add retries",
              createdAt: "1970-01-01T00:00:01.000Z",
            });
            expect(yield* harness.recorded("thread.message.user.append")).toEqual([]);

            const consult = yield* harness.coordinator.consult(LEAD, {
              question: "Is exponential backoff enough?",
              waitSeconds: 0,
            });
            expect(consult.status).toBe("pending");
            const peerThreadId = yield* peerThreadOf(harness.store, roomId);
            const bootstrap = yield* appendsTo(harness, peerThreadId);
            expect(bootstrap).toEqual([
              {
                text: "You → Fable: Let's add retries",
                note: {
                  purpose: "transcript",
                  from: "fable",
                  to: "astra",
                  source: {
                    speaker: "user",
                    threadId: LEAD,
                    messageId: first,
                    createdAt: "1970-01-01T00:00:01.000Z",
                  },
                },
                createdAt: expect.any(String),
              },
            ]);
            // The consult's prompt sorts after the catch-up it follows.
            const consultTurn = (yield* harness.recorded("thread.turn.start")).at(-1)!;
            expect(consultTurn.threadId).toBe(peerThreadId);
            expect(consultTurn.createdAt > bootstrap[0]!.createdAt).toBe(true);

            yield* harness.finishTurn({
              threadId: peerThreadId,
              state: "completed",
              answer: "Add jitter too.",
            });
            yield* harness.roomWhere((room) => room.consults[0]?.status === "answered");
            // The Lead takes the reply here, the way a waiting Lead does.
            const read = yield* harness.coordinator.wait(LEAD, { handle: consult.handle! });
            expect(read).toMatchObject({ status: "answered", answer: "Add jitter too." });

            // The Lead's final answer and its changed files reach the Peer.
            yield* harness.finishTurn({
              threadId: LEAD,
              state: "completed",
              answer: "Retries with full jitter are in.",
              turnId: LEAD_TURN,
            });
            yield* harness.commandWhere(
              (command) =>
                command.type === "thread.message.user.append" &&
                command.message.text.includes("Retries with full jitter"),
            );
            yield* PubSub.publish(
              harness.domainEvents,
              threadEvent("thread.turn-diff-completed", LEAD, {
                turnId: LEAD_TURN,
                files: [{ path: "src/retry.ts" }, { path: "src/retry.test.ts" }],
              }),
            );
            yield* harness.commandWhere(
              (command) =>
                command.type === "thread.message.user.append" &&
                command.message.text.startsWith("Fable changed"),
            );
            // Events are handled in order, so the Peer's turn end was fully handled by now:
            // its answer returned through the Lead's tool call and was not copied.
            expect(yield* appendsTo(harness, LEAD)).toEqual([]);
            const copied = yield* appendsTo(harness, peerThreadId);
            expect(copied.map((entry) => entry.text)).toEqual([
              "You → Fable: Let's add retries",
              "Fable (Lead) → you: Retries with full jitter are in.",
              "Fable changed: src/retry.ts, src/retry.test.ts",
            ]);
            expect(copied[1]!.note?.source?.speaker).toBe("agent");
            expect(copied[2]!.note).toEqual({ purpose: "transcript", from: "fable", to: "astra" });
            // Every copy lands strictly after the one before, whatever the clock says.
            expect(copied.map((entry) => entry.createdAt)).toEqual(
              copied.map((entry) => entry.createdAt).toSorted(),
            );
            expect(new Set(copied.map((entry) => entry.createdAt)).size).toBe(copied.length);
            const leadTurns = (yield* harness.recorded("thread.turn.start")).filter(
              (command) => command.threadId === LEAD,
            );
            expect(leadTurns).toEqual([]);

            // pair_read_thread pages the Lead's thread back from the newest line.
            const newest = yield* harness.coordinator.readThread(peerThreadId, { limit: 1 });
            expect(newest).toMatchObject({
              persona: "fable",
              hasMore: true,
              lines: [{ speaker: "agent", text: "Retries with full jitter are in." }],
            });
            const earlier = yield* harness.coordinator.readThread(peerThreadId, {
              beforeMessageId: newest.lines[0]!.messageId,
            });
            expect(earlier).toMatchObject({
              hasMore: false,
              lines: [{ speaker: "user", messageId: first, text: "Let's add retries" }],
            });
          }),
        ),
    );

    it.effect(
      "copies a message typed in the Peer's thread, and the Peer's answer to it, to the Lead",
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const harness = yield* makeHarness();
            const roomId = yield* harness.createRoom();
            yield* harness.coordinator.consult(LEAD, { question: "Thoughts?", waitSeconds: 0 });
            const peerThreadId = yield* peerThreadOf(harness.store, roomId);
            yield* harness.finishTurn({
              threadId: peerThreadId,
              state: "completed",
              answer: "Fine.",
            });
            yield* harness.roomWhere((room) => room.consults[0]?.status === "answered");

            yield* harness.sendUserMessage({
              text: "Astra, what about timeouts?",
              createdAt: "1970-01-01T00:00:02.000Z",
              threadId: peerThreadId,
            });
            yield* harness.commandWhere(
              (command) =>
                command.type === "thread.message.user.append" && command.threadId === LEAD,
            );
            // The Peer's own turn for that message has no consult behind it, so its answer is copied too.
            yield* harness.finishTurn({
              threadId: peerThreadId,
              state: "completed",
              answer: "Cap them at 30s.",
              turnId: TurnId.make("peer-own-turn"),
            });
            yield* harness.commandWhere(
              (command) =>
                command.type === "thread.message.user.append" &&
                command.message.text.includes("Cap them"),
            );
            expect((yield* appendsTo(harness, LEAD)).map((entry) => entry.text)).toEqual([
              "You → Astra: Astra, what about timeouts?",
              "Astra (Peer) → you: Cap them at 30s.",
            ]);
            expect(yield* appendsTo(harness, peerThreadId)).toEqual([]);
          }),
        ),
    );

    it.effect(
      "does not copy a relayed roundtable message, but does copy one the busy Peer missed",
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const harness = yield* makeHarness();
            const roomId = yield* harness.createRoom("roundtable");
            yield* harness.sendUserMessage({
              text: "Should retries use jitter?",
              createdAt: "1970-01-01T00:00:01.000Z",
            });
            yield* harness.commandWhere(
              turnStartWhere(
                (command) => readPairRoomNote(command.message.context)?.purpose === "user-relay",
              ),
            );
            const peerThreadId = yield* peerThreadOf(harness.store, roomId);
            // Relayed as the Peer's prompt, so the catch-up on creation leaves it out.
            expect(yield* appendsTo(harness, peerThreadId)).toEqual([]);

            yield* harness.sendUserMessage({
              text: "And the cap?",
              createdAt: "1970-01-01T00:00:02.000Z",
            });
            const missed = yield* harness.commandWhere(
              (command) => command.type === "thread.message.user.append",
            );
            expect(missed).toMatchObject({
              threadId: peerThreadId,
              message: { text: "You → Fable: And the cap?" },
            });
            expect((yield* harness.recorded("thread.turn.start")).length).toBe(1);
          }),
        ),
    );

    it.effect("tells the other side when a turn stopped without answering", () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeHarness();
          const roomId = yield* harness.createRoom();
          yield* harness.coordinator.consult(LEAD, { question: "Thoughts?", waitSeconds: 0 });
          const peerThreadId = yield* peerThreadOf(harness.store, roomId);
          yield* harness.finishTurn({ threadId: peerThreadId, state: "completed", answer: "Ok." });
          yield* harness.roomWhere((room) => room.consults[0]?.status === "answered");

          yield* harness.finishTurn({ threadId: LEAD, state: "interrupted", turnId: LEAD_TURN });
          const stopped = yield* harness.commandWhere(
            (command) => command.type === "thread.message.user.append",
          );
          expect(stopped).toMatchObject({
            threadId: peerThreadId,
            message: { text: "Fable's turn was stopped before it answered." },
          });
        }),
      ),
    );

    it.effect("re-copies recent lines after a restart, which existing copies absorb", () =>
      Effect.scoped(
        Effect.gen(function* () {
          const peerThreadId = ThreadId.make("peer-thread");
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
                  at: MESSAGE_AT,
                });
                yield* store.dispatch({
                  type: "peer.attach",
                  roomId: room.roomId,
                  threadId: peerThreadId,
                  reviewWorktreePath: REVIEW_WORKTREE,
                  at: MESSAGE_AT,
                });
              }),
            messages: new Map([
              [
                LEAD,
                [
                  {
                    id: MessageId.make("lead-user-1"),
                    role: "user",
                    text: "Add retries",
                    turnId: null,
                    streaming: false,
                    createdAt: MESSAGE_AT,
                    updatedAt: MESSAGE_AT,
                  },
                  {
                    id: MessageId.make("lead-answer-1"),
                    role: "assistant",
                    text: "Added.",
                    turnId: LEAD_TURN,
                    streaming: false,
                    createdAt: MESSAGE_AT,
                    updatedAt: MESSAGE_AT,
                  },
                ] as unknown as OrchestrationThread["messages"],
              ],
            ]),
          });
          const copied = yield* appendsTo(harness, peerThreadId);
          expect(copied.map((entry) => entry.text)).toEqual([
            "You → Fable: Add retries",
            "Fable (Lead) → you: Added.",
          ]);
          const ids = (yield* harness.recorded("thread.message.user.append")).map((command) => [
            command.commandId,
            command.message.messageId,
          ]);
          expect(ids).toEqual(
            ["lead-user-1", "lead-answer-1"].map((source) => {
              const key = pairTranscriptKey(peerThreadId, source);
              return [`pair:transcript:${key}`, `pair-transcript-${key}`];
            }),
          );
        }),
      ),
    );
  });

  describe("conversation", () => {
    const noteOf = (command: OrchestrationCommand) =>
      command.type === "thread.turn.start" ? readPairRoomNote(command.message.context) : null;

    /** Opens a consult, has the Peer answer it, and lets the Lead read the answer through pair_wait. */
    const converse = Effect.fn("converse")(function* (
      harness: Effect.Success<ReturnType<typeof makeHarness>>,
      roomId: PairRoomId,
    ) {
      const opened = yield* harness.coordinator.consult(LEAD, {
        question: "Is exponential backoff enough?",
        waitSeconds: 0,
      });
      const peerThreadId = yield* peerThreadOf(harness.store, roomId);
      yield* harness.finishTurn({
        threadId: peerThreadId,
        state: "completed",
        answer: "Add jitter, or retries stampede.",
      });
      yield* harness.roomWhere((room) => room.consults[0]?.status === "answered");
      const read = yield* harness.coordinator.wait(LEAD, { handle: opened.handle! });
      expect(read).toMatchObject({ status: "answered", exchange: 1 });
      return { handle: opened.handle!, peerThreadId };
    });

    it.effect(
      "lets the Peer ask before answering, and the Lead reply to keep the conversation going",
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const harness = yield* makeHarness();
            const roomId = yield* harness.createRoom();
            const opened = yield* harness.coordinator.consult(LEAD, {
              question: "Should retries back off?",
              waitSeconds: 0,
            });
            const peerThreadId = yield* peerThreadOf(harness.store, roomId);

            const asked = yield* harness.coordinator.ask(peerThreadId, {
              question: "Client retries or the worker's?",
            });
            expect(asked).toMatchObject({ status: "recorded", handle: opened.handle });
            yield* harness.finishTurn({
              threadId: peerThreadId,
              state: "completed",
              answer: "Depends which retries you mean.",
            });
            const question = yield* harness.coordinator.wait(LEAD, { handle: opened.handle! });
            expect(question).toMatchObject({
              status: "question",
              question: "Client retries or the worker's?",
              answer: "Depends which retries you mean.",
              exchange: 1,
              exchangeLimit: PAIR_CONVERSATION_MAX_EXCHANGES,
            });

            const replied = yield* harness.coordinator.reply(LEAD, {
              handle: opened.handle!,
              message: "The worker's.",
              waitSeconds: 0,
            });
            expect(replied).toMatchObject({ status: "pending", exchange: 2 });
            expect(replied.handle).not.toBe(opened.handle);
            const replyTurn = (yield* harness.recorded("thread.turn.start")).at(-1)!;
            expect(replyTurn.threadId).toBe(peerThreadId);
            expect(noteOf(replyTurn)).toEqual({ purpose: "reply", from: "fable", to: "astra" });
            expect(replyTurn.message.text).toContain(
              "Fable (Lead) replies in your critique conversation",
            );
            expect(replyTurn.message.text).toContain("Exchange 2 of");
            expect(replyTurn.message.text).toContain("The worker's.");

            yield* harness.finishTurn({
              threadId: peerThreadId,
              state: "completed",
              answer: "Then cap at five with jitter.",
            });
            const answered = yield* harness.coordinator.wait(LEAD, { handle: replied.handle! });
            expect(answered).toMatchObject({
              status: "answered",
              answer: "Then cap at five with jitter.",
              exchange: 2,
            });
            // A reply continues a conversation; it is not a new round.
            const status = yield* harness.coordinator.status(LEAD);
            expect(status.rounds).toEqual({ used: 1, limit: 2 });
            expect(status.consults.map((entry) => [entry.exchange, entry.peerAsk])).toEqual([
              [1, "Client retries or the worker's?"],
              [2, null],
            ]);
          }),
        ),
    );

    it.effect(
      "brings a reply the Lead never read to it as a turn, then lets the Peer check the Lead's answer",
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const harness = yield* makeHarness();
            const roomId = yield* harness.createRoom();
            const opened = yield* harness.coordinator.consult(LEAD, {
              question: "Is exponential backoff enough?",
              waitSeconds: 0,
            });
            const peerThreadId = yield* peerThreadOf(harness.store, roomId);
            yield* harness.finishTurn({
              threadId: peerThreadId,
              state: "completed",
              answer: "Add jitter, or retries stampede.",
            });
            yield* harness.roomWhere((room) => room.consults[0]?.status === "answered");

            // The Lead moved on without pair_wait, so the reply comes as a turn once it is idle.
            yield* harness.finishTurn({
              threadId: LEAD,
              state: "completed",
              answer: "Backoff is in.",
              turnId: LEAD_TURN,
            });
            const delivered = asTurnStart(
              yield* harness.commandWhere(turnStartWhere((command) => command.threadId === LEAD)),
            );
            expect(noteOf(delivered)).toEqual({
              purpose: "peer-answer",
              from: "astra",
              to: "fable",
            });
            expect(delivered.message.text).toContain("after your turn had ended");
            expect(delivered.message.text).toContain(`pair_reply (handle ${opened.handle})`);
            expect(delivered.message.text).toContain("Add jitter, or retries stampede.");
            const room = Option.getOrThrow(yield* harness.store.get(roomId));
            expect(room.consults[0]?.answerDeliveredAt).toBe(delivered.createdAt);

            // That turn took the reply, so when it ends the Peer gets the Lead's answer to check.
            yield* harness.finishTurn({
              threadId: LEAD,
              state: "completed",
              answer: "Jitter added, capped at 30s.",
            });
            const signOff = asTurnStart(
              yield* harness.commandWhere((command) => noteOf(command)?.purpose === "sign-off"),
            );
            expect(signOff.threadId).toBe(peerThreadId);
            expect(signOff.message.text).toContain("Fable's (Lead) turn has ended");
            expect(signOff.message.text).toContain("- Is exponential backoff enough?");
            expect(signOff.message.text).toContain("Jitter added, capped at 30s.");
            expect(noteOf(signOff)?.source).toMatchObject({ speaker: "agent", threadId: LEAD });
            const checked = Option.getOrThrow(yield* harness.store.get(roomId));
            expect(checked.consults.at(-1)).toMatchObject({
              answerTo: "sign-off",
              continues: opened.handle,
              automatic: true,
              status: "running",
            });

            // A quiet sign-off becomes a line in the Lead's thread, not a Lead turn.
            yield* harness.finishTurn({
              threadId: peerThreadId,
              state: "completed",
              answer: "Aligned; the cap is right.",
            });
            const line = yield* harness.commandWhere(
              (command) =>
                command.type === "thread.message.user.append" &&
                command.threadId === LEAD &&
                command.message.text.includes("Aligned; the cap is right."),
            );
            expect(line).toMatchObject({
              message: { text: "Astra (Peer) → you: Aligned; the cap is right." },
            });
            // The Lead going idle again starts nothing more: each Lead turn is checked once.
            yield* PubSub.publish(harness.domainEvents, threadEvent("thread.session-set", LEAD));
            yield* harness.roomWhere((room) => room.consults.at(-1)?.status === "answered");
            const leadTurns = (yield* harness.recorded("thread.turn.start")).filter(
              (command) => command.threadId === LEAD,
            );
            expect(leadTurns).toHaveLength(1);
            const signOffs = () =>
              harness
                .recorded("thread.turn.start")
                .pipe(
                  Effect.map((commands) =>
                    commands.filter((command) => noteOf(command)?.purpose === "sign-off"),
                  ),
                );
            expect(yield* signOffs()).toHaveLength(1);
            // A later Lead turn that took no reply is not checked either.
            yield* harness.finishTurn({
              threadId: LEAD,
              state: "completed",
              answer: "Also tidied the client.",
              turnId: TurnId.make("lead-turn-3"),
            });
            yield* harness.commandWhere(
              (command) =>
                command.type === "thread.message.user.append" &&
                command.message.text.includes("Also tidied the client."),
            );
            expect(yield* signOffs()).toHaveLength(1);
          }),
        ),
    );

    it.effect("checks every conversation the Lead read in a turn with one sign-off", () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeHarness();
          const roomId = yield* harness.createRoom();
          const handles: Array<string> = [];
          for (const [index, question] of [
            "Is exponential backoff enough?",
            "Should retries be capped?",
          ].entries()) {
            const opened = yield* harness.coordinator.consult(LEAD, { question, waitSeconds: 0 });
            yield* harness.finishTurn({
              threadId: yield* peerThreadOf(harness.store, roomId),
              state: "completed",
              answer: `Answer ${index + 1}`,
            });
            yield* harness.roomWhere((room) => room.consults[index]?.status === "answered");
            yield* harness.coordinator.wait(LEAD, { handle: opened.handle! });
            handles.push(opened.handle!);
          }
          const peerThreadId = yield* peerThreadOf(harness.store, roomId);
          yield* harness.finishTurn({
            threadId: LEAD,
            state: "completed",
            answer: "Both are in.",
            turnId: LEAD_TURN,
          });
          const signOff = asTurnStart(
            yield* harness.commandWhere((command) => noteOf(command)?.purpose === "sign-off"),
          );
          expect(signOff.message.text).toContain("- Is exponential backoff enough?");
          expect(signOff.message.text).toContain("- Should retries be capped?");
          const room = Option.getOrThrow(yield* harness.store.get(roomId));
          expect(room.consults.at(-1)).toMatchObject({
            answerTo: "sign-off",
            continues: handles[1],
          });
          yield* harness.finishTurn({
            threadId: peerThreadId,
            state: "completed",
            answer: "Both hold.",
          });
          yield* harness.roomWhere((r) => r.consults.at(-1)?.status === "answered");
          // The first conversation was covered by that sign-off; a later turn does not revisit it.
          yield* harness.finishTurn({
            threadId: LEAD,
            state: "completed",
            answer: "Docs too.",
            turnId: TurnId.make("lead-turn-3"),
          });
          yield* harness.commandWhere(
            (command) =>
              command.type === "thread.message.user.append" &&
              command.message.text.includes("Docs too."),
          );
          const signOffs = (yield* harness.recorded("thread.turn.start")).filter(
            (command) => noteOf(command)?.purpose === "sign-off",
          );
          expect(signOffs).toHaveLength(1);
        }),
      ),
    );

    it.effect(
      "leaves a turn that ended before the reply it brought to the turn that follows, across a restart",
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const peerThreadId = ThreadId.make("peer-thread");
            const idleLead = makeShell({
              id: LEAD,
              session: runningSession(LEAD, null),
              latestTurn: {
                turnId: LEAD_TURN,
                state: "completed",
                requestedAt: MESSAGE_AT,
                startedAt: MESSAGE_AT,
                completedAt: MESSAGE_AT,
                assistantMessageId: null,
              },
            });
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
                    at: MESSAGE_AT,
                  });
                  yield* store.dispatch({
                    type: "peer.attach",
                    roomId: room.roomId,
                    threadId: peerThreadId,
                    reviewWorktreePath: REVIEW_WORKTREE,
                    at: MESSAGE_AT,
                  });
                  yield* store.dispatch({
                    type: "consult.request",
                    roomId: room.roomId,
                    consultId: "c1",
                    kind: "critique",
                    leadTurnId: LEAD_TURN,
                    automatic: false,
                    title: "Is exponential backoff enough?",
                    at: MESSAGE_AT,
                  });
                  yield* store.dispatch({
                    type: "consult.settle",
                    roomId: room.roomId,
                    consultId: "c1",
                    status: "answered",
                    peerTurnId: null,
                    error: null,
                    at: MESSAGE_AT,
                  });
                  // The room brought the reply as a Lead turn the server stopped before seeing start.
                  yield* store.dispatch({
                    type: "consult.answer-delivered",
                    roomId: room.roomId,
                    consultId: "c1",
                    at: "1970-01-01T00:00:01.000Z",
                  });
                }),
              shells: new Map([
                [LEAD, idleLead],
                [peerThreadId, makeShell({ id: peerThreadId })],
              ]),
              messages: new Map([
                [
                  LEAD,
                  [
                    {
                      id: MessageId.make("lead-answer-1"),
                      role: "assistant",
                      text: "Backoff is in.",
                      turnId: LEAD_TURN,
                      streaming: false,
                      createdAt: MESSAGE_AT,
                      updatedAt: MESSAGE_AT,
                    },
                  ] as unknown as OrchestrationThread["messages"],
                ],
              ]),
            });
            const signOffs = () =>
              harness
                .recorded("thread.turn.start")
                .pipe(
                  Effect.map((commands) =>
                    commands.filter((command) => noteOf(command)?.purpose === "sign-off"),
                  ),
                );
            // Reconciliation ran at boot; the turn that ended before the reply is not checked.
            expect(yield* signOffs()).toEqual([]);
            yield* harness.finishTurn({
              threadId: LEAD,
              state: "completed",
              answer: "Jitter added.",
              turnId: TurnId.make("lead-turn-2"),
              completedAt: "1970-01-01T00:00:02.000Z",
            });
            const signOff = asTurnStart(
              yield* harness.commandWhere((command) => noteOf(command)?.purpose === "sign-off"),
            );
            expect(signOff.message.text).toContain("Jitter added.");
            expect(yield* signOffs()).toHaveLength(1);
          }),
        ),
    );

    it.effect("brings a sign-off the Peer wants a word about back to the Lead", () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeHarness();
          const roomId = yield* harness.createRoom();
          const { handle, peerThreadId } = yield* converse(harness, roomId);
          yield* harness.finishTurn({
            threadId: LEAD,
            state: "completed",
            answer: "Backoff without jitter, keeping it simple.",
            turnId: LEAD_TURN,
          });
          yield* harness.commandWhere((command) => noteOf(command)?.purpose === "sign-off");
          yield* harness.coordinator.ask(peerThreadId, {
            question: "Without jitter the stampede is still there; was that on purpose?",
          });
          yield* harness.finishTurn({
            threadId: peerThreadId,
            state: "completed",
            answer: "Checked the loop: all workers wake together.",
          });
          const objection = asTurnStart(
            yield* harness.commandWhere(
              turnStartWhere(
                (command) =>
                  command.threadId === LEAD &&
                  readPairRoomNote(command.message.context)?.purpose === "peer-answer",
              ),
            ),
          );
          expect(objection.message.text).toContain("has something you should hear");
          expect(objection.message.text).toContain("Astra asks: Without jitter the stampede");
          expect(objection.message.text).toContain("all workers wake together");
          expect(objection.message.text).not.toContain(`handle ${handle})`);
          expect(yield* appendsTo(harness, LEAD)).toEqual([]);
        }),
      ),
    );

    it.effect("stops a conversation at its exchange limit", () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeHarness();
          const roomId = yield* harness.createRoom();
          const opened = yield* converse(harness, roomId);
          let handle = opened.handle;
          for (let exchange = 2; exchange <= PAIR_CONVERSATION_MAX_EXCHANGES; exchange += 1) {
            const replied = yield* harness.coordinator.reply(LEAD, {
              handle,
              message: `Point ${exchange}`,
              waitSeconds: 0,
            });
            expect(replied).toMatchObject({ status: "pending", exchange });
            handle = replied.handle!;
            const turn = (yield* harness.recorded("thread.turn.start")).at(-1)!;
            if (exchange === PAIR_CONVERSATION_MAX_EXCHANGES) {
              expect(turn.message.text).toContain("this is the last one in this conversation");
            }
            yield* harness.finishTurn({
              threadId: opened.peerThreadId,
              state: "completed",
              answer: `Counter ${exchange}`,
            });
            yield* harness.coordinator.wait(LEAD, { handle });
          }
          const refused = yield* harness.coordinator.reply(LEAD, {
            handle,
            message: "One more",
            waitSeconds: 0,
          });
          expect(refused).toMatchObject({ status: "rejected", reason: "conversation-limit" });
          // Nor does the room start a sign-off for a conversation that has run its course.
          yield* harness.finishTurn({
            threadId: LEAD,
            state: "completed",
            answer: "Settled.",
            turnId: LEAD_TURN,
          });
          yield* harness.commandWhere(
            (command) =>
              command.type === "thread.message.user.append" &&
              command.message.text.includes("Settled."),
          );
          const signOffs = (yield* harness.recorded("thread.turn.start")).filter(
            (command) => noteOf(command)?.purpose === "sign-off",
          );
          expect(signOffs).toEqual([]);
        }),
      ),
    );

    it.effect("brings a blocked assignee's question to the Lead and its answer back", () =>
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
          const assigneeThread = ThreadId.make(assigned.threadId!);
          const asked = yield* harness.coordinator.ask(assigneeThread, {
            question: "Which base branch do the tests target?",
          });
          expect(asked).toMatchObject({ status: "recorded", assignment: { state: "blocked" } });
          // The Lead is mid-turn, so the blocker waits.
          expect(
            (yield* harness.recorded("thread.turn.start")).filter((c) => c.threadId === LEAD),
          ).toEqual([]);

          yield* harness.finishTurn({
            threadId: LEAD,
            state: "completed",
            answer: "Assigned the tests.",
            turnId: LEAD_TURN,
          });
          const blocked = asTurnStart(
            yield* harness.commandWhere(turnStartWhere((command) => command.threadId === LEAD)),
          );
          expect(noteOf(blocked)).toEqual({ purpose: "blocked", from: "astra", to: "fable" });
          expect(blocked.message.text).toContain("Which base branch do the tests target?");
          expect(blocked.message.text).toContain(`pair_reply (handle ${assigned.assignmentId})`);
          const room = Option.getOrThrow(yield* harness.store.get(roomId));
          expect(room.assignments[0]?.blockedDeliveredAt).toBe(blocked.createdAt);

          const replied = yield* harness.coordinator.reply(LEAD, {
            handle: assigned.assignmentId!,
            message: "main, as of this morning.",
          });
          expect(replied).toMatchObject({ status: "updated", assignment: { state: "running" } });
          const resumed = (yield* harness.recorded("thread.turn.start")).at(-1)!;
          expect(resumed.threadId).toBe(assigneeThread);
          expect(noteOf(resumed)).toEqual({ purpose: "reply", from: "fable", to: "astra" });
          expect(resumed.message.text).toContain("main, as of this morning.");
          expect(
            Option.getOrThrow(yield* harness.store.get(roomId)).assignments[0]?.blockedDeliveredAt,
          ).toBeNull();
        }),
      ),
    );
  });

  describe("checkout", () => {
    /** Assigns, submits and approves one assignment; the Lead may be running or idle. */
    const approveAssignment = Effect.fn("approveAssignment")(function* (
      harness: Effect.Success<ReturnType<typeof makeHarness>>,
      input: { readonly baseRef?: string } = {},
    ) {
      const assigned = yield* harness.coordinator.assign(LEAD, {
        title: "Retry tests",
        brief: "Add tests for the retry policy.",
        scopeGlobs: ["src/retry/**"],
        acceptanceCriteria: [],
        ...input,
      });
      expect(assigned.status).toBe("assigned");
      yield* harness.coordinator.submit(ThreadId.make(assigned.threadId!), {
        summary: "Done",
        criteriaResults: [],
        testsRun: [],
        knownLimitations: [],
      });
      const approved = yield* harness.coordinator.review(LEAD, {
        assignmentId: assigned.assignmentId!,
        verdict: "approve",
        notes: "ok",
      });
      expect(approved.assignment?.state).toBe("awaiting-user");
      return assigned.assignmentId!;
    });

    it.effect("follows the checkout the Lead points it at: snapshots, assignments, status", () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeHarness();
          const roomId = yield* harness.createRoom();
          expect((yield* harness.coordinator.status(LEAD)).checkout).toEqual({
            path: "/repo",
            branch: "main",
            setBy: "thread",
          });
          const moved = yield* harness.coordinator.checkout(LEAD, { path: "/repo-ux" });
          expect(moved).toMatchObject({ status: "recorded", path: "/repo-ux", branch: "dev/ux" });
          expect(moved.detail).toContain("on dev/ux");
          expect((yield* harness.coordinator.status(LEAD)).checkout).toEqual({
            path: "/repo-ux",
            branch: "dev/ux",
            setBy: "lead",
          });

          yield* harness.coordinator.consult(LEAD, { question: "Look at this?", waitSeconds: 0 });
          expect(yield* Ref.get(harness.syncedFrom)).toBe("/repo-ux");

          const assigned = yield* harness.coordinator.assign(LEAD, {
            title: "UX tests",
            brief: "Add tests.",
            scopeGlobs: ["src/ux/**"],
            acceptanceCriteria: [],
          });
          const room = Option.getOrThrow(yield* harness.store.get(roomId));
          expect(room.assignments[0]).toMatchObject({
            assignmentId: assigned.assignmentId,
            baseCommit: "base1234567890",
            targetBranch: "dev/ux",
          });
          const brief = (yield* harness.recorded("thread.turn.start")).at(-1)!;
          expect(brief.message.text).toContain("of dev/ux, which your work merges back into");
        }),
      ),
    );

    it.effect(
      "refuses a checkout outside the project's repository, and lets the user set one",
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const harness = yield* makeHarness();
            const roomId = yield* harness.createRoom();
            const refused = yield* harness.coordinator.checkout(LEAD, { path: "/elsewhere" });
            expect(refused).toMatchObject({ status: "rejected", reason: "workspace" });
            expect(refused.detail).toContain("not a worktree of this project's repository");
            expect(Option.getOrThrow(yield* harness.store.get(roomId)).checkout).toBeNull();

            yield* harness.coordinator.consult(LEAD, { question: "Look?", waitSeconds: 0 });
            const peerThreadId = yield* peerThreadOf(harness.store, roomId);
            const notLead = yield* harness.coordinator.checkout(peerThreadId, { path: "/repo-ux" });
            expect(notLead.status).toBe("rejected");
            expect(Option.getOrThrow(yield* harness.store.get(roomId)).checkout).toBeNull();

            yield* harness.coordinator.dispatchUserCommand({
              type: "room.checkout",
              roomId,
              path: "/repo-ux",
            });
            expect(Option.getOrThrow(yield* harness.store.get(roomId)).checkout).toMatchObject({
              path: "/repo-ux",
              branch: "dev/ux",
              by: "user",
            });
          }),
        ),
    );

    it.effect("merges an assignment into the worktree that has its base branch checked out", () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeHarness();
          const roomId = yield* harness.createRoom();
          yield* harness.setShell(LEAD, { session: runningSession(LEAD, null) });
          const assignmentId = yield* approveAssignment(harness, { baseRef: "dev/ux" });
          expect(Option.getOrThrow(yield* harness.store.get(roomId)).assignments[0]).toMatchObject({
            targetBranch: "dev/ux",
          });
          yield* harness.coordinator.dispatchUserCommand({
            type: "assignment.integrate",
            roomId,
            assignmentId,
          });
          expect(yield* Ref.get(harness.integratedInto)).toBe("/repo-ux");
          expect(Option.getOrThrow(yield* harness.store.get(roomId)).assignments[0]).toMatchObject({
            state: "integrated",
            integrationCommit: "merge1234567890",
            note: "Merged into dev/ux as merge1234567.",
          });
        }),
      ),
    );

    it.effect("refuses to merge when the base branch is checked out nowhere", () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeHarness();
          const roomId = yield* harness.createRoom();
          yield* harness.setShell(LEAD, { session: runningSession(LEAD, null) });
          const assignmentId = yield* approveAssignment(harness, { baseRef: "dev/other" });
          const error = yield* harness.coordinator
            .dispatchUserCommand({ type: "assignment.integrate", roomId, assignmentId })
            .pipe(Effect.flip);
          expect(error.reason).toBe("conflict");
          expect(error.detail).toContain("dev/other is not checked out in any worktree");
          expect(yield* Ref.get(harness.integratedInto)).toBeNull();
          expect(Option.getOrThrow(yield* harness.store.get(roomId)).assignments[0]?.state).toBe(
            "awaiting-user",
          );
        }),
      ),
    );

    it.effect("reports uncommitted files the merge would overwrite, and merges nothing", () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeHarness();
          const roomId = yield* harness.createRoom();
          yield* harness.setShell(LEAD, { session: runningSession(LEAD, null) });
          const assignmentId = yield* approveAssignment(harness);
          yield* Ref.set(harness.integration, {
            status: "dirty",
            files: ["src/retry.ts", "README.md"],
            detail:
              "2 uncommitted files in /repo would be overwritten by the merge: src/retry.ts, README.md. Commit or stash them, then merge again.",
          });
          const error = yield* harness.coordinator
            .dispatchUserCommand({ type: "assignment.integrate", roomId, assignmentId })
            .pipe(Effect.flip);
          expect(error.detail).toContain("2 uncommitted files in /repo would be overwritten");
          const assignment = Option.getOrThrow(yield* harness.store.get(roomId)).assignments[0];
          expect(assignment?.state).toBe("awaiting-user");
          expect(assignment?.note).toContain("Nothing was merged. 2 uncommitted files");
        }),
      ),
    );

    it.effect("records a merge the Lead made on the user's word once its turn ends", () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeHarness();
          yield* harness.createRoom();
          yield* approveAssignment(harness);
          // The Lead merged with git itself; main now holds the approved commit.
          yield* Ref.set(harness.mergedElsewhere, "head1234567890");
          yield* harness.finishTurn({
            threadId: LEAD,
            state: "completed",
            answer: "Merged Astra's tests.",
            turnId: LEAD_TURN,
          });
          const room = yield* harness.roomWhere(
            (candidate) => candidate.assignments[0]?.state === "integrated",
          );
          expect(room.assignments[0]).toMatchObject({
            integrationCommit: "head1234567890",
            note: "Merged into main outside the room, as head12345678.",
          });
          const card = (yield* harness.recorded("thread.activity.append")).findLast(
            (command) =>
              command.threadId === LEAD && command.activity.summary === "Assignment merged",
          );
          expect(card).toBeDefined();
          expect(yield* Ref.get(harness.integratedInto)).toBeNull();
        }),
      ),
    );

    it.effect(
      "records a merge done by hand instead of merging twice when the user clicks Merge",
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const harness = yield* makeHarness();
            const roomId = yield* harness.createRoom();
            yield* harness.setShell(LEAD, { session: runningSession(LEAD, null) });
            const assignmentId = yield* approveAssignment(harness);
            yield* Ref.set(harness.mergedElsewhere, "head1234567890");
            yield* harness.coordinator.dispatchUserCommand({
              type: "assignment.integrate",
              roomId,
              assignmentId,
            });
            expect(
              Option.getOrThrow(yield* harness.store.get(roomId)).assignments[0],
            ).toMatchObject({
              state: "integrated",
              integrationCommit: "head1234567890",
              note: "Already merged into main outside the room as head12345678.",
            });
            expect(yield* Ref.get(harness.integratedInto)).toBeNull();
          }),
        ),
    );

    it.effect("notices on boot a merge that happened while the server was down", () =>
      Effect.scoped(
        Effect.gen(function* () {
          const peerThreadId = ThreadId.make("peer-thread");
          const harness = yield* makeHarness({
            mergedElsewhere: "head1234567890",
            shells: new Map([
              [LEAD, makeShell({ id: LEAD, session: runningSession(LEAD, null) })],
              [peerThreadId, makeShell({ id: peerThreadId })],
            ]),
            seed: (store) =>
              Effect.gen(function* () {
                const room = yield* store.dispatch({
                  type: "room.create",
                  roomId: "room-restart" as PairRoomId,
                  projectId: PROJECT_ID,
                  leadThreadId: LEAD,
                  leadPersona: "fable",
                  mode: "adaptive",
                  at: MESSAGE_AT,
                });
                yield* store.dispatch({
                  type: "peer.attach",
                  roomId: room.roomId,
                  threadId: peerThreadId,
                  reviewWorktreePath: REVIEW_WORKTREE,
                  at: MESSAGE_AT,
                });
                yield* store.dispatch({
                  type: "assignment.create",
                  roomId: room.roomId,
                  assignmentId: "assignment-down",
                  title: "Retry tests",
                  threadId: ThreadId.make("assignment-thread"),
                  worktreePath: "/worktrees/repo/pair-retry-tests",
                  branch: "pair/retry-tests",
                  baseCommit: "base1234567890",
                  targetBranch: "main",
                  scopeGlobs: ["src/retry/**"],
                  acceptanceCriteria: [],
                  expectedArtifact: "patch",
                  at: MESSAGE_AT,
                });
                yield* store.dispatch({
                  type: "assignment.update",
                  roomId: room.roomId,
                  assignmentId: "assignment-down",
                  by: "server",
                  state: "submitted",
                  at: MESSAGE_AT,
                });
                yield* store.dispatch({
                  type: "assignment.update",
                  roomId: room.roomId,
                  assignmentId: "assignment-down",
                  by: "server",
                  state: "awaiting-user",
                  approvedCommit: "approved1234567890",
                  at: MESSAGE_AT,
                });
              }),
          });
          const room = Option.getOrThrow(yield* harness.store.get("room-restart" as PairRoomId));
          expect(room.assignments[0]).toMatchObject({
            state: "integrated",
            integrationCommit: "head1234567890",
          });
        }),
      ),
    );
  });
});
