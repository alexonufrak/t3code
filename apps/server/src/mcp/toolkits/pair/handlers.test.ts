import {
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type OrchestrationCommand,
  type OrchestrationEvent,
  type OrchestrationThread,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import type { Tool } from "effect/unstable/ai";

import {
  OrchestrationEngineService,
  type OrchestrationEngineShape,
} from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { PairSpikeToolkitHandlersLive, peerPersonaFor } from "./handlers.ts";
import { PairSpikeToolkit } from "./tools.ts";

const PROJECT_ID = ProjectId.make("project-1");
const LEAD_THREAD_ID = ThreadId.make("lead-thread");
const LEAD_TURN_ID = TurnId.make("lead-turn-1");

let randomCalls = 0;
const testCrypto = Crypto.make({
  // Distinct bytes per call so generated thread, command and consult ids differ.
  randomBytes: (size) => new Uint8Array(size).fill((randomCalls += 1) % 256),
  digest: (_algorithm, data) => Effect.succeed(data),
});

const invocation = (
  capabilities: ReadonlyArray<McpInvocationContext.McpCapability>,
): McpInvocationContext.McpInvocationScope => ({
  environmentId: EnvironmentId.make("environment-1"),
  threadId: LEAD_THREAD_ID,
  providerSessionId: "provider-session-1",
  providerInstanceId: ProviderInstanceId.make("codex"),
  capabilities: new Set(capabilities),
  issuedAt: 1,
});

function makeShell(
  overrides: Partial<OrchestrationThreadShell> & Pick<OrchestrationThreadShell, "id">,
): OrchestrationThreadShell {
  return {
    projectId: PROJECT_ID,
    title: "Lead thread",
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6-astra" },
    runtimeMode: "approval-required",
    interactionMode: "default",
    branch: "feature/pair",
    worktreePath: "/workspace/project",
    pullRequests: [],
    latestTurn: null,
    createdAt: "2026-09-17T00:00:00.000Z",
    updatedAt: "2026-09-17T00:00:00.000Z",
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

const leadShell = makeShell({
  id: LEAD_THREAD_ID,
  session: {
    threadId: LEAD_THREAD_ID,
    status: "running",
    providerName: "codex",
    providerInstanceId: ProviderInstanceId.make("codex"),
    runtimeMode: "approval-required",
    activeTurnId: LEAD_TURN_ID,
    lastError: null,
    updatedAt: "2026-09-17T00:00:00.000Z",
  },
});

interface PeerState {
  readonly shell: OrchestrationThreadShell | null;
  readonly messages: OrchestrationThread["messages"];
}

const makeHarness = Effect.fn("makePairSpikeToolkitHarness")(function* () {
  const commands = yield* Ref.make<ReadonlyArray<OrchestrationCommand>>([]);
  const peer = yield* Ref.make<PeerState>({ shell: null, messages: [] });
  const events = yield* PubSub.unbounded<OrchestrationEvent>();

  const dispatch: OrchestrationEngineShape["dispatch"] = (command) =>
    Ref.update(commands, (recorded) => [...recorded, command]).pipe(Effect.as({ sequence: 1 }));

  const dependencies = Layer.mergeAll(
    Layer.mock(ProjectionSnapshotQuery)({
      getThreadShellById: (threadId) =>
        threadId === LEAD_THREAD_ID
          ? Effect.succeed(Option.some(leadShell))
          : Ref.get(peer).pipe(
              Effect.map((state) =>
                state.shell?.id === threadId ? Option.some(state.shell) : Option.none(),
              ),
            ),
      getThreadDetailById: (threadId) =>
        Ref.get(peer).pipe(
          Effect.map((state) =>
            state.shell?.id === threadId
              ? Option.some({ messages: state.messages } as unknown as OrchestrationThread)
              : Option.none(),
          ),
        ),
    }),
    Layer.mock(OrchestrationEngineService)({
      readEvents: () => Stream.empty,
      dispatch,
      streamDomainEvents: Stream.fromPubSub(events),
      subscribeDomainEvents: PubSub.subscribe(events).pipe(Effect.map(Stream.fromSubscription)),
      latestSequence: Effect.succeed(0),
    }),
    Layer.succeed(Crypto.Crypto, testCrypto),
  );

  // Built in the test's scope so the handler's watcher fibers outlive each call.
  const handlers = yield* Layer.build(
    PairSpikeToolkitHandlersLive.pipe(Layer.provide(dependencies)),
  );
  const toolkit = yield* PairSpikeToolkit.pipe(Effect.provide(handlers));

  const call = <Name extends keyof typeof PairSpikeToolkit.tools>(
    name: Name,
    params: Parameters<typeof toolkit.handle<Name>>[1],
    capabilities: ReadonlyArray<McpInvocationContext.McpCapability> = ["pair"],
  ) =>
    toolkit.handle(name, params).pipe(
      Stream.unwrap,
      Stream.runCollect,
      Effect.map(
        (chunk) => chunk.at(-1)!.result as Tool.Success<(typeof PairSpikeToolkit.tools)[Name]>,
      ),
      Effect.provideService(McpInvocationContext.McpInvocationContext, invocation(capabilities)),
      Effect.provide(dependencies),
    );

  const recorded = <Type extends OrchestrationCommand["type"]>(type: Type) =>
    Ref.get(commands).pipe(
      Effect.map((all) =>
        all.filter(
          (command): command is Extract<OrchestrationCommand, { type: Type }> =>
            command.type === type,
        ),
      ),
    );

  /** Simulates the provider finishing the peer turn, then publishes the event that lands it. */
  const settlePeerTurn = (input: {
    readonly state: "completed" | "error";
    readonly answer?: string;
  }) =>
    Effect.gen(function* () {
      const created = (yield* recorded("thread.create"))[0]!;
      const turnStart = (yield* recorded("thread.turn.start"))[0]!;
      const turnId = TurnId.make("peer-turn-1");
      yield* Ref.set(peer, {
        shell: makeShell({
          id: created.threadId,
          modelSelection: created.modelSelection,
          latestTurn: {
            turnId,
            state: input.state,
            requestedAt: turnStart.createdAt,
            startedAt: turnStart.createdAt,
            completedAt: turnStart.createdAt,
            assistantMessageId: null,
          },
        }),
        messages: input.answer
          ? ([
              {
                role: "assistant",
                text: input.answer,
                turnId,
                streaming: false,
              },
            ] as unknown as OrchestrationThread["messages"])
          : [],
      });
      yield* PubSub.publish(events, {
        aggregateId: created.threadId,
      } as unknown as OrchestrationEvent);
    });

  return { call, recorded, settlePeerTurn };
});

describe("pair spike toolkit handlers", () => {
  it("pairs Astra callers with Fable and every other caller with Astra", () => {
    expect(
      peerPersonaFor({
        modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6-astra" },
      }).peer.model,
    ).toBe("claude-fable-5-1");
    expect(
      peerPersonaFor({
        modelSelection: {
          instanceId: ProviderInstanceId.make("claudeAgent"),
          model: "claude-fable-5-1",
        },
      }).peer.model,
    ).toBe("gpt-6-astra");
  });

  it.effect("refuses a credential without the pair capability", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const error = yield* harness.call("pair_ping", {}, ["pull-requests"]).pipe(Effect.flip);
        expect(error).toMatchObject({
          _tag: "McpCapabilityUnavailableError",
          capability: "pair",
          threadId: LEAD_THREAD_ID,
        });
      }),
    ),
  );

  it.effect("reports the calling thread and instance the credential resolves to", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        expect(yield* harness.call("pair_ping", {})).toEqual({
          threadId: LEAD_THREAD_ID,
          providerInstanceId: "codex",
          capabilities: ["pair"],
          sleptSeconds: 0,
        });
      }),
    ),
  );

  it.live(
    "starts a Fable peer turn, mirrors a Peer card, and answers through pair_spike_wait",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeHarness();

          const pending = yield* harness.call("pair_spike_consult", {
            question: "Is the retry backoff safe for auth failures?",
            waitSeconds: 0,
          });
          expect(pending).toMatchObject({ status: "pending", peerModel: "claude-fable-5-1" });

          const [created] = yield* harness.recorded("thread.create");
          expect(created).toMatchObject({
            projectId: PROJECT_ID,
            modelSelection: { instanceId: "claudeAgent", model: "claude-fable-5-1" },
            branch: "feature/pair",
            worktreePath: "/workspace/project",
          });
          const [turnStart] = yield* harness.recorded("thread.turn.start");
          expect(turnStart?.threadId).toBe(created?.threadId);
          expect(turnStart?.message.text).toContain("Is the retry backoff safe for auth failures?");

          const [started] = yield* harness.recorded("thread.activity.append");
          expect(started).toMatchObject({
            threadId: LEAD_THREAD_ID,
            activity: {
              kind: "task.started",
              turnId: LEAD_TURN_ID,
              payload: {
                taskId: pending.consultId,
                agentKind: "agent",
                role: "Peer",
                model: "claude-fable-5-1",
                status: "running",
                pairMirror: true,
              },
            },
          });

          yield* harness.settlePeerTurn({ state: "completed", answer: "No: do not retry 401s." });
          const answered = yield* harness.call("pair_spike_wait", {
            consultId: pending.consultId,
            waitSeconds: 45,
          });
          expect(answered).toMatchObject({
            status: "answered",
            consultId: pending.consultId,
            answer: "No: do not retry 401s.",
          });

          const mirrored = yield* harness.recorded("thread.activity.append");
          expect(mirrored.at(-1)).toMatchObject({
            threadId: LEAD_THREAD_ID,
            activity: {
              kind: "task.completed",
              payload: {
                taskId: pending.consultId,
                status: "completed",
                summary: "No: do not retry 401s.",
              },
            },
          });
        }),
      ),
  );

  it.live("reports a failed peer turn and marks the Peer card failed", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const pending = yield* harness.call("pair_spike_consult", {
          question: "Review the migration.",
          waitSeconds: 0,
        });
        yield* harness.settlePeerTurn({ state: "error" });
        const failed = yield* harness.call("pair_spike_wait", {
          consultId: pending.consultId,
          waitSeconds: 45,
        });
        expect(failed).toMatchObject({ status: "failed", error: "Peer turn ended as error." });
        const mirrored = yield* harness.recorded("thread.activity.append");
        expect(mirrored.at(-1)?.activity.payload).toMatchObject({ status: "failed" });
      }),
    ),
  );

  it.effect("rejects a consult id the server does not know", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const error = yield* harness
          .call("pair_spike_wait", { consultId: "consult-missing" })
          .pipe(Effect.flip);
        expect(error).toMatchObject({
          _tag: "PairConsultNotFoundError",
          consultId: "consult-missing",
        });
      }),
    ),
  );
});
