import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  MessageId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import {
  type PairMirrorCard,
  type PairMirrorOutcome,
  pairMirrorCompletedPayload,
  pairMirrorStartedPayload,
} from "@t3tools/shared/pairMirror";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FiberSet from "effect/FiberSet";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import * as OrchestrationEngine from "../../../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import {
  PAIR_MAX_WAIT_SECONDS,
  PairConsultNotFoundError,
  type PairConsultResult,
  PairDispatchFailedError,
  PairSpikeToolkit,
  PairThreadNotFoundError,
} from "./tools.ts";

/** A peer turn that runs longer than this is reported as failed. */
const PEER_TURN_DEADLINE = Duration.minutes(20);
const DEFAULT_WAIT_SECONDS = 30;

interface PeerPersona {
  readonly name: "Fable" | "Astra";
  readonly instanceId: ProviderInstanceId;
  readonly model: string;
}

const FABLE: PeerPersona = {
  name: "Fable",
  instanceId: ProviderInstanceId.make("claudeAgent"),
  model: "claude-fable-5-1",
};
const ASTRA: PeerPersona = {
  name: "Astra",
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-6-astra",
};

/** The spike pairs a Codex caller with Fable and every other caller with Astra. */
export function peerPersonaFor(caller: Pick<OrchestrationThreadShell, "modelSelection">): {
  readonly caller: PeerPersona;
  readonly peer: PeerPersona;
} {
  return caller.modelSelection.instanceId === ASTRA.instanceId
    ? { caller: ASTRA, peer: FABLE }
    : { caller: FABLE, peer: ASTRA };
}

type ConsultOutcome = PairMirrorOutcome;

interface ConsultRecord {
  readonly consultId: string;
  readonly peerThreadId: ThreadId;
  readonly peer: PeerPersona;
  readonly outcome: Deferred.Deferred<ConsultOutcome>;
}

const make = Effect.gen(function* () {
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const crypto = yield* Crypto.Crypto;
  // Watchers outlive the MCP request that started them and stop with the server.
  const watchers = yield* FiberSet.make<void>();
  // Spike state is in memory only; Phase 1 moves it to the pair event log.
  const peerThreadByCaller = new Map<ThreadId, ThreadId>();
  const consults = new Map<string, ConsultRecord>();

  const uuid = crypto.randomUUIDv4.pipe(Effect.orDie);
  const nowIso = DateTime.now.pipe(Effect.map(DateTime.formatIso));
  const dispatch = (command: Parameters<typeof engine.dispatch>[0]) =>
    engine
      .dispatch(command)
      .pipe(Effect.mapError((cause) => new PairDispatchFailedError({ cause })));

  const requireCallerThread = Effect.gen(function* () {
    const scope = yield* McpInvocationContext.requireMcpCapability("pair");
    const thread = yield* snapshots
      .getThreadShellById(scope.threadId)
      .pipe(Effect.mapError((cause) => new PairDispatchFailedError({ cause })));
    if (Option.isNone(thread)) {
      return yield* new PairThreadNotFoundError({ threadId: scope.threadId });
    }
    return thread.value;
  });

  const ensurePeerThread = Effect.fn("PairSpikeToolkit.ensurePeerThread")(function* (
    caller: OrchestrationThreadShell,
    persona: { readonly caller: PeerPersona; readonly peer: PeerPersona },
  ) {
    const existing = peerThreadByCaller.get(caller.id);
    if (existing) return existing;
    const peerThreadId = ThreadId.make(yield* uuid);
    yield* dispatch({
      type: "thread.create",
      commandId: CommandId.make(`server:pair-spike-peer:${caller.id}:${yield* uuid}`),
      threadId: peerThreadId,
      projectId: caller.projectId,
      title: `${persona.peer.name} (Peer) for ${caller.title}`,
      modelSelection: { instanceId: persona.peer.instanceId, model: persona.peer.model },
      runtimeMode: caller.runtimeMode,
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      branch: caller.branch,
      worktreePath: caller.worktreePath,
      createdAt: yield* nowIso,
    });
    peerThreadByCaller.set(caller.id, peerThreadId);
    return peerThreadId;
  });

  const mirrorActivity = Effect.fn("PairSpikeToolkit.mirrorActivity")(function* (input: {
    readonly callerThreadId: ThreadId;
    readonly kind: "task.started" | "task.completed";
    readonly summary: string;
    readonly payload: Readonly<Record<string, unknown>>;
  }) {
    const caller = yield* snapshots
      .getThreadShellById(input.callerThreadId)
      .pipe(Effect.mapError((cause) => new PairDispatchFailedError({ cause })));
    const createdAt = yield* nowIso;
    yield* dispatch({
      type: "thread.activity.append",
      commandId: CommandId.make(`server:pair-spike-mirror:${input.callerThreadId}:${yield* uuid}`),
      threadId: input.callerThreadId,
      activity: {
        id: EventId.make(yield* uuid),
        tone: "info",
        kind: input.kind,
        summary: input.summary,
        payload: input.payload,
        turnId: Option.getOrUndefined(caller)?.session?.activeTurnId ?? null,
        createdAt,
      },
      createdAt,
    });
  });

  /** The peer's outcome for a turn requested at or after `requestedAfter`, if it has settled. */
  const readPeerOutcome = Effect.fn("PairSpikeToolkit.readPeerOutcome")(function* (
    peerThreadId: ThreadId,
    requestedAfter: string,
  ) {
    const shell = Option.getOrUndefined(yield* snapshots.getThreadShellById(peerThreadId));
    const turn = shell?.latestTurn;
    if (turn && turn.requestedAt >= requestedAfter && turn.state !== "running") {
      if (turn.state !== "completed") {
        return Option.some<ConsultOutcome>({
          status: "failed",
          error: `Peer turn ended as ${turn.state}.`,
        });
      }
      const detail = Option.getOrUndefined(yield* snapshots.getThreadDetailById(peerThreadId));
      const answer = detail?.messages.findLast(
        (message) =>
          message.role === "assistant" && message.turnId === turn.turnId && !message.streaming,
      )?.text;
      return Option.some<ConsultOutcome>(
        answer
          ? { status: "answered", answer }
          : { status: "failed", error: "Peer finished without a reply." },
      );
    }
    const session = shell?.session;
    if (session?.status === "error" && session.updatedAt >= requestedAfter) {
      return Option.some<ConsultOutcome>({
        status: "failed",
        error: session.lastError ?? "Peer session failed.",
      });
    }
    return Option.none<ConsultOutcome>();
  });

  /**
   * Subscribes to domain events, signals `ready`, then settles on the first
   * peer-thread event after which the projection shows the turn finished.
   * Subscribing before the turn is dispatched means no event can slip past.
   */
  const watchPeerTurn = (input: {
    readonly peerThreadId: ThreadId;
    readonly requestedAfter: string;
    readonly ready: Deferred.Deferred<void>;
  }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const events = yield* engine.subscribeDomainEvents;
        yield* Deferred.succeed(input.ready, undefined);
        const settled = yield* events.pipe(
          Stream.filter((event) => event.aggregateId === input.peerThreadId),
          Stream.mapEffect(() => readPeerOutcome(input.peerThreadId, input.requestedAfter)),
          Stream.filter(Option.isSome),
          Stream.runHead,
        );
        return Option.flatten(settled).pipe(
          Option.getOrElse((): ConsultOutcome => ({
            status: "failed",
            error: "Peer event stream ended.",
          })),
        );
      }),
    );

  const resultOf = (
    record: ConsultRecord,
    outcome: Option.Option<ConsultOutcome>,
  ): PairConsultResult => {
    const base = {
      consultId: record.consultId,
      peerThreadId: record.peerThreadId,
      peerModel: record.peer.model,
    };
    if (Option.isNone(outcome)) {
      return {
        ...base,
        status: "pending",
        answer: null,
        error: null,
        retryAfterSeconds: 0,
      };
    }
    return outcome.value.status === "answered"
      ? {
          ...base,
          status: "answered",
          answer: outcome.value.answer,
          error: null,
          retryAfterSeconds: null,
        }
      : {
          ...base,
          status: "failed",
          answer: null,
          error: outcome.value.error,
          retryAfterSeconds: null,
        };
  };

  const waitFor = (record: ConsultRecord, waitSeconds: number | undefined) =>
    Deferred.await(record.outcome).pipe(
      Effect.timeoutOption(
        Duration.seconds(Math.min(waitSeconds ?? DEFAULT_WAIT_SECONDS, PAIR_MAX_WAIT_SECONDS)),
      ),
      Effect.map((outcome) => resultOf(record, outcome)),
    );

  return PairSpikeToolkit.of({
    pair_ping: (input) =>
      Effect.gen(function* () {
        const scope = yield* McpInvocationContext.requireMcpCapability("pair");
        const sleptSeconds = input.sleepSeconds ?? 0;
        if (sleptSeconds > 0) yield* Effect.sleep(Duration.seconds(sleptSeconds));
        return {
          threadId: scope.threadId,
          providerInstanceId: scope.providerInstanceId,
          capabilities: [...scope.capabilities].toSorted(),
          sleptSeconds,
        };
      }),
    pair_spike_consult: (input) =>
      Effect.gen(function* () {
        const caller = yield* requireCallerThread;
        const persona = peerPersonaFor(caller);
        const peerThreadId = yield* ensurePeerThread(caller, persona);
        const record: ConsultRecord = {
          consultId: `consult-${yield* uuid}`,
          peerThreadId,
          peer: persona.peer,
          outcome: yield* Deferred.make<ConsultOutcome>(),
        };
        consults.set(record.consultId, record);

        const card: PairMirrorCard = {
          taskId: record.consultId,
          title: `Consult ${persona.peer.name}`,
          persona: persona.peer.name,
          model: persona.peer.model,
        };
        const requestedAt = yield* nowIso;
        const ready = yield* Deferred.make<void>();
        yield* FiberSet.run(
          watchers,
          watchPeerTurn({ peerThreadId, requestedAfter: requestedAt, ready }).pipe(
            Effect.timeoutOption(PEER_TURN_DEADLINE),
            Effect.map(
              Option.getOrElse((): ConsultOutcome => ({
                status: "failed",
                error: "Peer did not finish in time.",
              })),
            ),
            Effect.catch((cause) =>
              Effect.succeed<ConsultOutcome>({ status: "failed", error: String(cause) }),
            ),
            // Update the Lead's card before handing the answer back, so the card
            // never lags behind what the Lead already knows.
            Effect.tap((outcome) =>
              mirrorActivity({
                callerThreadId: caller.id,
                kind: "task.completed",
                summary: `${persona.peer.name} ${outcome.status === "answered" ? "answered" : "failed"} a consult`,
                payload: pairMirrorCompletedPayload(card, outcome),
              }).pipe(
                Effect.catch((cause) => Effect.logWarning("pair spike mirror failed", { cause })),
              ),
            ),
            Effect.flatMap((outcome) => Deferred.succeed(record.outcome, outcome)),
            Effect.asVoid,
          ),
        );
        yield* Deferred.await(ready);

        yield* mirrorActivity({
          callerThreadId: caller.id,
          kind: "task.started",
          summary: `${persona.peer.name} is reviewing a consult`,
          payload: pairMirrorStartedPayload(card, input.question.slice(0, 200)),
        });
        yield* dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make(`server:pair-spike-consult:${peerThreadId}:${yield* uuid}`),
          threadId: peerThreadId,
          message: {
            messageId: MessageId.make(yield* uuid),
            role: "user",
            text: [
              `Pair Room consult from ${persona.caller.name} (Lead). You are ${persona.peer.name} (Peer).`,
              "Answer with critique, alternatives, or findings. Do not modify files for this consult.",
              "",
              input.question,
            ].join("\n"),
            attachments: [],
          },
          runtimeMode: caller.runtimeMode,
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          createdAt: requestedAt,
        });

        return yield* waitFor(record, input.waitSeconds);
      }),
    pair_spike_wait: (input) =>
      Effect.gen(function* () {
        yield* McpInvocationContext.requireMcpCapability("pair");
        const record = consults.get(input.consultId);
        if (!record) {
          return yield* new PairConsultNotFoundError({ consultId: input.consultId });
        }
        return yield* waitFor(record, input.waitSeconds);
      }),
  });
});

export const PairSpikeToolkitHandlersLive = PairSpikeToolkit.toLayer(make);
