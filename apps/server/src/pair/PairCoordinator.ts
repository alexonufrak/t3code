import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  MessageId,
  PAIR_ASSIGNMENT_ACTIVE_STATES,
  PAIR_PERSONAS,
  PairRoomCommandError,
  PairRoomId,
  ThreadId,
  otherPairPersona,
  pairDecisionLeadMayResolve,
  pairRoomParticipant,
  type OrchestrationCommand,
  type OrchestrationEvent,
  type OrchestrationThreadShell,
  type PairAssignment,
  type PairConsult,
  type PairConsultKind,
  type PairDecision,
  type PairPersona,
  type PairRoom,
  type PairRoomDispatchResult,
  type PairRoomNote,
  type PairRoomUserCommand,
  type TurnId,
} from "@t3tools/contracts";
import {
  type PairMirrorCard,
  type PairMirrorOutcome,
  pairMirrorCompletedPayload,
  pairMirrorProgressPayload,
  pairMirrorStartedPayload,
} from "@t3tools/shared/pairMirror";
import { pairRoomNoteContext, readPairRoomNote } from "@t3tools/shared/pairRoomNote";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderService from "../provider/Services/ProviderService.ts";
import {
  assignmentBrief,
  consultPrompt,
  handoffRequest,
  leadHandoff,
  peerAnswerPrompt,
  resumeRequest,
  revisionRequest,
  roleGuidance,
} from "./PairPrompts.ts";
import { PairRoomRejectedError, PairRoomStore } from "./PairRoomStore.ts";
import {
  PAIR_DEFAULT_WAIT_SECONDS,
  PAIR_MAX_WAIT_SECONDS,
  type PairAckResult,
  type PairAssignResult,
  type PairAssignmentView,
  type PairCallerRole,
  type PairDecisionView,
  type PairHandleResult,
  type PairStatusResult,
} from "./PairToolSchemas.ts";
import { PairWorkspace, type PairWorkspaceError } from "./PairWorkspace.ts";
import {
  clampPairText,
  pairConsultAnswerOwed,
  pairRoundLimit,
  pairRoundsUsed,
  pairScopeDeviations,
} from "./pairRoomDecider.ts";

/** A Peer answer brought back to the Lead is cut at this length; the Peer's thread keeps all of it. */
const PAIR_PEER_ANSWER_MAX_LENGTH = 40_000;

/** A consult the Peer has not answered in this long is failed the next time anyone looks at it. */
export const PAIR_CONSULT_DEADLINE = Duration.minutes(20);

export class PairToolUnavailableError extends Schema.TaggedError<PairToolUnavailableError>()(
  "PairToolUnavailableError",
  { detail: Schema.String },
) {
  override get message(): string {
    return this.detail;
  }
}

type ToolEffect<A> = Effect.Effect<A, PairToolUnavailableError>;

/**
 * Runs Pair Rooms: turns `pair_*` tool calls and user commands into room
 * changes plus ordinary orchestration commands on participant and assignment
 * threads, and reacts to those threads' events (Peer answers, turn diffs,
 * model reroutes).
 */
export class PairCoordinator extends Context.Service<
  PairCoordinator,
  {
    readonly dispatchUserCommand: (
      command: PairRoomUserCommand,
    ) => Effect.Effect<PairRoomDispatchResult, PairRoomCommandError>;
    readonly status: (threadId: ThreadId) => ToolEffect<PairStatusResult>;
    readonly consult: (
      threadId: ThreadId,
      input: {
        readonly kind?: PairConsultKind | undefined;
        readonly question: string;
        readonly focusPaths?: ReadonlyArray<string> | undefined;
        readonly leadProposal?: string | undefined;
        readonly waitSeconds?: number | undefined;
      },
    ) => ToolEffect<PairHandleResult>;
    readonly wait: (
      threadId: ThreadId,
      input: { readonly handle: string; readonly waitSeconds?: number | undefined },
    ) => ToolEffect<PairHandleResult>;
    readonly assign: (
      threadId: ThreadId,
      input: {
        readonly title: string;
        readonly brief: string;
        readonly scopeGlobs: ReadonlyArray<string>;
        readonly acceptanceCriteria: ReadonlyArray<string>;
        readonly expectedArtifact?: PairAssignment["expectedArtifact"] | undefined;
        readonly baseRef?: string | undefined;
      },
    ) => ToolEffect<PairAssignResult>;
    readonly reportProgress: (
      threadId: ThreadId,
      input: {
        readonly note: string;
        readonly blocked?: boolean | undefined;
        readonly question?: string | undefined;
      },
    ) => ToolEffect<PairAckResult>;
    readonly submit: (
      threadId: ThreadId,
      input: {
        readonly summary: string;
        readonly criteriaResults: ReadonlyArray<{
          readonly criterion: string;
          readonly met: boolean;
          readonly evidence?: string | undefined;
        }>;
        readonly testsRun: ReadonlyArray<string>;
        readonly knownLimitations: ReadonlyArray<string>;
      },
    ) => ToolEffect<PairAckResult>;
    readonly review: (
      threadId: ThreadId,
      input: {
        readonly assignmentId: string;
        readonly verdict: "approve" | "request-changes" | "reject";
        readonly notes: string;
      },
    ) => ToolEffect<PairAckResult>;
    readonly recordDecision: (
      threadId: ThreadId,
      input: {
        readonly decisionId?: string | undefined;
        readonly kind?: PairDecision["kind"] | undefined;
        readonly category?: PairDecision["category"] | undefined;
        readonly title?: string | undefined;
        readonly position: string;
        readonly evidence?: string | undefined;
        readonly leadRecommendation?: string | undefined;
        readonly consequenceOfDeferring?: string | undefined;
        readonly resolution?: string | undefined;
      },
    ) => ToolEffect<PairAckResult>;
  }
>()("t3/pair/PairCoordinator") {}

/** Anything that is not a room rule: storage, orchestration or projection failures. */
class PairInternalError extends Schema.TaggedError<PairInternalError>()("PairInternalError", {
  operation: Schema.String,
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return `Pair room ${this.operation} failed.`;
  }
}

type PairFailure = PairRoomRejectedError | PairInternalError;

interface CallerBase {
  readonly room: PairRoom;
  readonly persona: PairPersona;
  readonly threadId: ThreadId;
}

type Caller =
  | (CallerBase & { readonly role: "lead" })
  | (CallerBase & { readonly role: "peer" })
  | (CallerBase & { readonly role: "assignee"; readonly assignment: PairAssignment });

interface PeerOutcome {
  readonly status: "answered" | "failed";
  readonly peerTurnId: TurnId | null;
  readonly error: string | null;
}

interface LeadContext {
  readonly threadId: ThreadId;
  readonly persona: PairPersona;
  readonly shell: OrchestrationThreadShell;
  readonly cwd: string;
  readonly activeTurnId: TurnId | null;
}

const rejected = (reason: string, detail: string) =>
  Effect.fail(new PairRoomRejectedError({ reason, detail }));

const personaName = (persona: PairPersona) => PAIR_PERSONAS[persona].displayName;

const modelSelectionFor = (persona: PairPersona) => ({
  instanceId: PAIR_PERSONAS[persona].instanceId,
  model: PAIR_PERSONAS[persona].model,
});

const isRunningTurn = (shell: OrchestrationThreadShell) =>
  shell.session?.activeTurnId != null || shell.latestTurn?.state === "running";

const consultCard = (consult: PairConsult, peer: PairPersona): PairMirrorCard => ({
  taskId: consult.consultId,
  title: `${personaName(peer)}: ${consult.title}`,
  persona: personaName(peer),
  model: PAIR_PERSONAS[peer].model,
});

const assignmentCard = (assignment: PairAssignment): PairMirrorCard => ({
  taskId: assignment.assignmentId,
  title: `${personaName(assignment.owner)}: ${assignment.title}`,
  persona: personaName(assignment.owner),
  model: PAIR_PERSONAS[assignment.owner].model,
});

export const pairAssignmentView = (assignment: PairAssignment): PairAssignmentView => ({
  assignmentId: assignment.assignmentId,
  title: assignment.title,
  owner: assignment.owner,
  state: assignment.state,
  branch: assignment.branch,
  worktreePath: assignment.worktreePath,
  baseCommit: assignment.baseCommit,
  scopeGlobs: assignment.scopeGlobs,
  acceptanceCriteria: assignment.acceptanceCriteria,
  expectedArtifact: assignment.expectedArtifact,
  note: assignment.note,
  changedFiles: assignment.changedFiles,
  deviations: assignment.deviations,
  report: assignment.report,
  integrationCommit: assignment.integrationCommit,
});

export const pairDecisionView = (decision: PairDecision): PairDecisionView => ({
  decisionId: decision.decisionId,
  kind: decision.kind,
  category: decision.category,
  title: decision.title,
  positions: decision.positions,
  resolution: decision.resolution,
  resolvedBy: decision.resolvedBy,
  waitingOn:
    decision.resolution !== null
      ? null
      : pairDecisionLeadMayResolve(decision.category)
        ? "lead"
        : "user",
});

const titleFrom = (text: string) => clampPairText(text.split("\n")[0] ?? text, 120);

/** "@Astra" as its own word, in any case. "me@astra.dev" and "@astra-bot" do not count. */
export const pairMessageMentions = (text: string, persona: PairPersona) =>
  new RegExp(`(?<![\\w@.])@${PAIR_PERSONAS[persona].displayName}(?![\\w-])`, "i").test(text);

/** What a consult card calls the request: the user's own message, or the Lead's consult. */
const consultNoun = (consult: PairConsult) =>
  consult.answerTo === "lead-turn" ? "your message" : "a consult";

const emptyHandleResult = {
  handle: null,
  answer: null,
  leadProposal: null,
  error: null,
  reason: null,
  retryAfterSeconds: null,
  assignment: null,
} satisfies Omit<PairHandleResult, "status">;

const userCommandError = (error: PairFailure): PairRoomCommandError => {
  if (error._tag === "PairInternalError") {
    return new PairRoomCommandError({ reason: "failed", detail: error.message });
  }
  switch (error.reason) {
    case "not-found":
      return new PairRoomCommandError({ reason: "not-found", detail: error.detail });
    case "invalid":
    case "workspace":
      return new PairRoomCommandError({ reason: "invalid", detail: error.detail });
    case "room-paused":
    case "room-closed":
      return new PairRoomCommandError({ reason: "unavailable", detail: error.detail });
    default:
      return new PairRoomCommandError({ reason: "conflict", detail: error.detail });
  }
};

export const make = Effect.gen(function* () {
  const store = yield* PairRoomStore;
  const workspace = yield* PairWorkspace;
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const providers = yield* ProviderService.ProviderService;
  const crypto = yield* Crypto.Crypto;

  /** Roundtable proposals stay in memory: they only matter until the Peer answers. */
  const leadProposals = new Map<string, string>();

  const uuid = crypto.randomUUIDv4.pipe(Effect.orDie);
  const nowIso = DateTime.now.pipe(Effect.map(DateTime.formatIso));
  const internal =
    (operation: string) =>
    (cause: unknown): PairInternalError =>
      new PairInternalError({ operation, cause });

  const orchestrate = (command: OrchestrationCommand) =>
    engine.dispatch(command).pipe(Effect.mapError(internal(command.type)));

  const fromWorkspace = <A>(effect: Effect.Effect<A, PairWorkspaceError>) =>
    effect.pipe(
      Effect.mapError(
        (error) => new PairRoomRejectedError({ reason: "workspace", detail: error.detail }),
      ),
    );

  const threadShell = (threadId: ThreadId) =>
    snapshots.getThreadShellById(threadId).pipe(Effect.mapError(internal("read thread")));

  const apply = (command: Parameters<typeof store.dispatch>[0]) =>
    store
      .dispatch(command)
      .pipe(
        Effect.catchTag("PairRoomPersistenceError", (error) =>
          Effect.fail(internal("store")(error)),
        ),
      );

  // ── Context ─────────────────────────────────────────────────────────

  const resolveCaller = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const room = yield* store.findByThread(threadId);
      if (Option.isNone(room)) {
        return yield* new PairToolUnavailableError({
          detail: "This thread is not part of a pair room, so pair tools do not apply here.",
        });
      }
      const participant = room.value.participants.find((entry) => entry.threadId === threadId);
      if (participant) {
        const base = { room: room.value, persona: participant.persona, threadId };
        return (participant.role === "lead"
          ? { ...base, role: "lead" }
          : { ...base, role: "peer" }) satisfies Caller as Caller;
      }
      const assignment = room.value.assignments.find((entry) => entry.threadId === threadId)!;
      return {
        role: "assignee",
        room: room.value,
        persona: assignment.owner,
        threadId,
        assignment,
      } satisfies Caller as Caller;
    });

  const leadContext = (room: PairRoom) =>
    Effect.gen(function* () {
      const lead = pairRoomParticipant(room, "lead");
      if (!lead?.threadId) return yield* rejected("invalid", "The pair room has no Lead thread.");
      const shell = yield* threadShell(lead.threadId);
      if (Option.isNone(shell)) {
        return yield* rejected("not-found", "The Lead's thread no longer exists.");
      }
      const expected = PAIR_PERSONAS[lead.persona].model;
      if (shell.value.modelSelection.model !== expected) {
        return yield* rejected(
          "room-paused",
          `${personaName(lead.persona)} leads this room on ${expected}, but its thread now uses ${shell.value.modelSelection.model}. Switch the thread back to ${expected} to continue.`,
        );
      }
      const project = yield* snapshots
        .getProjectShellById(shell.value.projectId)
        .pipe(Effect.mapError(internal("read project")));
      const cwd = shell.value.worktreePath ?? Option.getOrUndefined(project)?.workspaceRoot;
      if (!cwd) return yield* rejected("not-found", "The Lead's project no longer exists.");
      const activeTurnId =
        shell.value.session?.activeTurnId ??
        (shell.value.latestTurn?.state === "running" ? shell.value.latestTurn.turnId : null);
      return {
        threadId: lead.threadId,
        persona: lead.persona,
        shell: shell.value,
        cwd,
        activeTurnId,
      } satisfies LeadContext;
    });

  const mirror = (
    room: PairRoom,
    input: {
      readonly kind: "task.started" | "task.progress" | "task.completed";
      readonly summary: string;
      readonly payload: Readonly<Record<string, unknown>>;
      readonly turnId: TurnId | null;
    },
  ) =>
    Effect.gen(function* () {
      const lead = pairRoomParticipant(room, "lead");
      if (!lead?.threadId) return;
      const createdAt = yield* nowIso;
      yield* orchestrate({
        type: "thread.activity.append",
        commandId: CommandId.make(`pair:${room.roomId}:mirror:${yield* uuid}`),
        threadId: lead.threadId,
        activity: {
          id: EventId.make(yield* uuid),
          tone: "info",
          kind: input.kind,
          summary: clampPairText(input.summary, 200),
          payload: input.payload,
          turnId: input.turnId,
          createdAt,
        },
        createdAt,
      });
    }).pipe(
      // A card that fails to render must never fail the room change behind it.
      Effect.catchCause((cause) => Effect.logWarning("pair room mirror failed", { cause })),
    );

  const leadTurnNow = (room: PairRoom) =>
    leadContext(room).pipe(
      Effect.map((lead) => lead.activeTurnId),
      Effect.orElseSucceed(() => null),
    );

  const startTurn = (input: {
    readonly commandKey: string;
    readonly threadId: ThreadId;
    readonly persona: PairPersona;
    readonly runtimeMode: OrchestrationThreadShell["runtimeMode"];
    readonly text: string;
    readonly note: PairRoomNote;
    readonly createdAt: string;
  }) =>
    Effect.gen(function* () {
      yield* orchestrate({
        type: "thread.turn.start",
        commandId: CommandId.make(input.commandKey),
        threadId: input.threadId,
        message: {
          messageId: MessageId.make(yield* uuid),
          role: "user",
          text: input.text,
          attachments: [],
          context: pairRoomNoteContext(`pair-room-note-${yield* uuid}`, input.note),
        },
        modelSelection: modelSelectionFor(input.persona),
        runtimeMode: input.runtimeMode,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        createdAt: input.createdAt,
      });
    });

  const interruptIfRunning = (threadId: ThreadId, key: string) =>
    Effect.gen(function* () {
      const shell = yield* threadShell(threadId);
      if (Option.isNone(shell) || !isRunningTurn(shell.value)) return;
      yield* orchestrate({
        type: "thread.turn.interrupt",
        commandId: CommandId.make(key),
        threadId,
        createdAt: yield* nowIso,
      });
    });

  // ── Consults ────────────────────────────────────────────────────────

  /**
   * How the latest turn requested at or after `since` ended, once it has.
   * `speaker` names whose turn it is in the error text.
   */
  const readTurnOutcome = (
    threadId: ThreadId,
    since: string,
    speaker: string,
  ): Effect.Effect<Option.Option<PeerOutcome>, PairInternalError> =>
    Effect.gen(function* () {
      const shell = Option.getOrUndefined(yield* threadShell(threadId));
      const turn = shell?.latestTurn;
      if (turn && turn.requestedAt >= since && turn.state !== "running") {
        if (turn.state !== "completed") {
          return Option.some<PeerOutcome>({
            status: "failed",
            peerTurnId: turn.turnId,
            error: `${speaker}'s turn ended as ${turn.state}.`,
          });
        }
        return Option.some<PeerOutcome>({
          status: "answered",
          peerTurnId: turn.turnId,
          error: null,
        });
      }
      const session = shell?.session;
      if (session?.status === "error" && session.updatedAt >= since) {
        return Option.some<PeerOutcome>({
          status: "failed",
          peerTurnId: null,
          error: session.lastError ?? `${speaker}'s session failed.`,
        });
      }
      return Option.none<PeerOutcome>();
    });

  /** The Peer's outcome for a consult, once its turn has settled. */
  const readPeerOutcome = (peerThreadId: ThreadId, consult: PairConsult) =>
    readTurnOutcome(peerThreadId, consult.requestedAt, "The Peer");

  /** The final assistant message of a finished turn. */
  const readTurnText = (threadId: ThreadId, turnId: TurnId) =>
    Effect.gen(function* () {
      const detail = yield* snapshots
        .getThreadDetailById(threadId)
        .pipe(Effect.mapError(internal("read answer")));
      return (
        Option.getOrUndefined(detail)?.messages.findLast(
          (message) =>
            message.role === "assistant" && message.turnId === turnId && !message.streaming,
        )?.text ?? null
      );
    });

  const readAnswer = (room: PairRoom, consult: PairConsult) =>
    Effect.gen(function* () {
      const peerThreadId = pairRoomParticipant(room, "peer")?.threadId;
      if (!peerThreadId || !consult.peerTurnId) return null;
      return yield* readTurnText(peerThreadId, consult.peerTurnId);
    });

  const settleConsult = (
    room: PairRoom,
    consult: PairConsult,
    outcome: {
      readonly status: "answered" | "failed" | "cancelled";
      readonly peerTurnId: TurnId | null;
      readonly error: string | null;
    },
  ) =>
    Effect.gen(function* () {
      const peer = pairRoomParticipant(room, "peer")!.persona;
      const settledAt = yield* nowIso;
      const answer =
        outcome.status === "answered"
          ? yield* readAnswer(room, { ...consult, peerTurnId: outcome.peerTurnId })
          : null;
      const mirrorOutcome: PairMirrorOutcome =
        outcome.status === "answered"
          ? { status: "answered", answer: answer ?? "Answered." }
          : outcome.status === "cancelled"
            ? { status: "stopped", reason: outcome.error ?? "Cancelled." }
            : { status: "failed", error: outcome.error ?? "The Peer did not answer." };
      // The card updates before the room does, so a Lead reading the answer never sees a stale card.
      yield* mirror(room, {
        kind: "task.completed",
        summary: `${personaName(peer)} ${outcome.status === "answered" ? "answered" : outcome.status} ${consultNoun(consult)}`,
        payload: pairMirrorCompletedPayload(consultCard(consult, peer), mirrorOutcome),
        turnId: consult.leadTurnId,
      });
      return yield* apply({
        type: "consult.settle",
        roomId: room.roomId,
        consultId: consult.consultId,
        status: outcome.status,
        peerTurnId: outcome.peerTurnId,
        error: outcome.error,
        at: settledAt,
      });
    });

  const expireStaleConsult = (room: PairRoom) =>
    Effect.gen(function* () {
      const running = room.consults.find((consult) => consult.status === "running");
      if (!running) return room;
      const now = yield* DateTime.now;
      const requested = DateTime.makeUnsafe(running.requestedAt);
      if (
        Duration.toMillis(DateTime.distance(requested, now)) <
        Duration.toMillis(PAIR_CONSULT_DEADLINE)
      ) {
        return room;
      }
      const peerThreadId = pairRoomParticipant(room, "peer")?.threadId;
      if (peerThreadId) {
        yield* interruptIfRunning(peerThreadId, `pair:${room.roomId}:expire:${running.consultId}`);
      }
      return yield* settleConsult(room, running, {
        status: "failed",
        peerTurnId: null,
        error: "The Peer did not answer within 20 minutes.",
      });
    });

  const ensurePeer = (room: PairRoom, lead: LeadContext) =>
    Effect.gen(function* () {
      const peer = pairRoomParticipant(room, "peer")!;
      const review = yield* fromWorkspace(
        workspace.syncReviewWorktree({ roomId: room.roomId, leadCwd: lead.cwd }),
      );
      if (peer.threadId) {
        const existing = yield* threadShell(peer.threadId);
        if (Option.isSome(existing)) {
          return { room, peerThreadId: peer.threadId, snapshotCommit: review.snapshotCommit };
        }
      }
      const peerThreadId = ThreadId.make(yield* uuid);
      const createdAt = yield* nowIso;
      yield* orchestrate({
        type: "thread.create",
        commandId: CommandId.make(`pair:${room.roomId}:peer-thread:${peerThreadId}`),
        threadId: peerThreadId,
        projectId: room.projectId,
        title: clampPairText(`${personaName(peer.persona)} (Peer): ${lead.shell.title}`, 120),
        modelSelection: modelSelectionFor(peer.persona),
        runtimeMode: lead.shell.runtimeMode,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        branch: null,
        worktreePath: review.worktreePath,
        createdAt,
      });
      const attached = yield* apply({
        type: "peer.attach",
        roomId: room.roomId,
        threadId: peerThreadId,
        reviewWorktreePath: review.worktreePath,
        at: createdAt,
      });
      return { room: attached, peerThreadId, snapshotCommit: review.snapshotCommit };
    });

  /** Sends a recorded consult to the Peer. Any failure settles the consult as failed. */
  const launchConsult = (
    room: PairRoom,
    consult: PairConsult,
    lead: LeadContext,
    request: { readonly question: string; readonly focusPaths: ReadonlyArray<string> },
  ) =>
    Effect.gen(function* () {
      const peerPersona = pairRoomParticipant(room, "peer")!.persona;
      const peer = yield* ensurePeer(room, lead);
      yield* mirror(peer.room, {
        kind: "task.started",
        summary: `${personaName(peerPersona)} is answering ${consultNoun(consult)}`,
        payload: pairMirrorStartedPayload(consultCard(consult, peerPersona), request.question),
        turnId: consult.leadTurnId,
      });
      yield* startTurn({
        commandKey: `pair:${room.roomId}:consult:${consult.consultId}`,
        threadId: peer.peerThreadId,
        persona: peerPersona,
        runtimeMode: lead.shell.runtimeMode,
        text: consultPrompt({
          lead: lead.persona,
          peer: peerPersona,
          kind: consult.kind,
          source: consult.answerTo === "lead-turn" ? "user" : consult.automatic ? "review" : "lead",
          round: consult.round,
          roundLimit: consult.automatic ? null : pairRoundLimit(room, consult.leadTurnId),
          snapshotCommit: peer.snapshotCommit,
          question: request.question,
          focusPaths: request.focusPaths,
        }),
        note: {
          purpose: consult.answerTo === "lead-turn" ? "user-relay" : "consult",
          from: lead.persona,
          to: peerPersona,
        },
        createdAt: consult.requestedAt,
      });
    }).pipe(
      Effect.catch((error: PairFailure) =>
        Effect.gen(function* () {
          const current = Option.getOrUndefined(yield* store.get(room.roomId)) ?? room;
          yield* settleConsult(current, consult, {
            status: "failed",
            peerTurnId: null,
            error: error._tag === "PairRoomRejectedError" ? error.detail : error.message,
          });
        }),
      ),
    );

  /**
   * Waits for a room change that makes `ready` true, subscribing before the
   * first check so a change landing in between is not missed.
   */
  const waitForRoom = (
    roomId: PairRoomId,
    ready: (room: PairRoom) => boolean,
    waitSeconds: number | undefined,
  ) =>
    Effect.scoped(
      Effect.gen(function* () {
        const changes = yield* store.subscribeChanges;
        const current = yield* store.get(roomId);
        if (Option.isSome(current) && ready(current.value)) return current;
        const seconds = Math.min(waitSeconds ?? PAIR_DEFAULT_WAIT_SECONDS, PAIR_MAX_WAIT_SECONDS);
        if (seconds === 0) return current;
        const next = yield* changes.pipe(
          Stream.filter((change) => change.room.roomId === roomId && ready(change.room)),
          Stream.runHead,
          Effect.timeoutOption(Duration.seconds(seconds)),
        );
        return Option.isSome(next) && Option.isSome(next.value)
          ? Option.some(next.value.value.room)
          : yield* store.get(roomId);
      }),
    );

  const consultResult = (room: PairRoom, consult: PairConsult) =>
    Effect.gen(function* () {
      const base = {
        ...emptyHandleResult,
        handle: consult.consultId,
        leadProposal: leadProposals.get(consult.consultId) ?? null,
      };
      switch (consult.status) {
        case "running":
          return { ...base, status: "pending", retryAfterSeconds: 0 } satisfies PairHandleResult;
        case "answered":
          return {
            ...base,
            status: "answered",
            answer:
              (yield* readAnswer(room, consult)) ??
              "The Peer answered, but the reply is no longer available. Open the Peer's thread.",
          } satisfies PairHandleResult;
        case "failed":
        case "cancelled":
          return {
            ...base,
            status: consult.status,
            error: consult.error,
          } satisfies PairHandleResult;
      }
    });

  const consultDone = (consultId: string) => (room: PairRoom) =>
    room.consults.find((entry) => entry.consultId === consultId)?.status !== "running";

  const assignmentSettled = (assignmentId: string) => (room: PairRoom) =>
    room.assignments.find((entry) => entry.assignmentId === assignmentId)?.state !== "running";

  const rejectedHandle = (error: PairRoomRejectedError): PairHandleResult => ({
    ...emptyHandleResult,
    status: "rejected",
    reason: error.reason,
    error: error.detail,
  });

  // ── Tool edges ──────────────────────────────────────────────────────

  const toolEdge = <A>(
    effect: Effect.Effect<A, PairFailure | PairToolUnavailableError>,
    onRejected: (error: PairRoomRejectedError) => A | Effect.Effect<A, PairToolUnavailableError>,
  ): ToolEffect<A> =>
    effect.pipe(
      Effect.catchTags({
        PairRoomRejectedError: (error) => {
          const handled = onRejected(error);
          return Effect.isEffect(handled) ? handled : Effect.succeed(handled);
        },
        PairInternalError: (error) =>
          Effect.logWarning("pair tool failed", {
            operation: error.operation,
            cause: error.cause,
          }).pipe(
            Effect.andThen(Effect.fail(new PairToolUnavailableError({ detail: error.message }))),
          ),
      }),
    );

  const ackRejected = (error: PairRoomRejectedError): PairAckResult => ({
    status: "rejected",
    reason: error.reason,
    detail: error.detail,
    assignment: null,
    decision: null,
  });

  const requireRole = <R extends PairCallerRole>(caller: Caller, roles: ReadonlyArray<R>) =>
    (roles as ReadonlyArray<string>).includes(caller.role)
      ? Effect.succeed(caller as Extract<Caller, { role: R }>)
      : rejected(
          "not-allowed",
          `Only the ${roles.join(" or ")} can use this tool; you are the ${caller.role}.`,
        );

  const status: PairCoordinator["Service"]["status"] = (threadId) =>
    toolEdge<PairStatusResult>(
      Effect.gen(function* () {
        const caller = yield* resolveCaller(threadId);
        const room = caller.room;
        const other = otherPairPersona(caller.persona);
        const otherThread = room.participants.find((entry) => entry.persona === other)?.threadId;
        const otherShell = otherThread
          ? Option.getOrUndefined(yield* threadShell(otherThread))
          : undefined;
        const leadTurn = caller.role === "lead" ? yield* leadTurnNow(room) : null;
        return {
          roomId: room.roomId,
          mode: room.mode,
          status: room.status,
          statusReason: room.statusReason,
          you: {
            persona: caller.persona,
            name: personaName(caller.persona),
            model: PAIR_PERSONAS[caller.persona].model,
            role: caller.role,
          },
          other: {
            persona: other,
            name: personaName(other),
            model: PAIR_PERSONAS[other].model,
            busy:
              room.consults.some((consult) => consult.status === "running") ||
              (otherShell !== undefined && isRunningTurn(otherShell)),
          },
          guidance: roleGuidance({ mode: room.mode, role: caller.role }),
          rounds:
            caller.role === "lead"
              ? { used: pairRoundsUsed(room, leadTurn), limit: pairRoundLimit(room, leadTurn) }
              : null,
          consults: room.consults.slice(-5).map((consult) => ({
            consultId: consult.consultId,
            kind: consult.kind,
            status: consult.status,
            title: consult.title,
            automatic: consult.automatic,
            error: consult.error,
          })),
          assignments: room.assignments.map(pairAssignmentView),
          decisions: room.decisions
            .filter((decision) => decision.resolution === null)
            .map(pairDecisionView),
          assignment: caller.role === "assignee" ? pairAssignmentView(caller.assignment) : null,
          leadSwitch: room.leadSwitch
            ? { to: personaName(room.leadSwitch.toPersona), phase: room.leadSwitch.phase }
            : null,
        } satisfies PairStatusResult;
      }),
      (error) => {
        // pair_status never rejects; a rejection here is a bug, surfaced as unavailable.
        return Effect.fail(new PairToolUnavailableError({ detail: error.detail }));
      },
    );

  const consult: PairCoordinator["Service"]["consult"] = (threadId, input) =>
    toolEdge<PairHandleResult>(
      Effect.gen(function* () {
        const caller = yield* requireRole(yield* resolveCaller(threadId), ["lead"]);
        const kind = input.kind ?? "critique";
        if (kind === "roundtable" && !input.leadProposal) {
          return yield* rejected(
            "invalid",
            "A roundtable consult needs leadProposal: record your own proposal first so the Peer's is independent.",
          );
        }
        const lead = yield* leadContext(caller.room);
        const room = yield* expireStaleConsult(caller.room);
        const consultId = `consult-${yield* uuid}`;
        const recorded = yield* apply({
          type: "consult.request",
          roomId: room.roomId,
          consultId,
          kind,
          leadTurnId: lead.activeTurnId,
          automatic: false,
          title: titleFrom(input.question),
          at: yield* nowIso,
        });
        leadProposals.clear();
        if (input.leadProposal) leadProposals.set(consultId, input.leadProposal);
        const recordedConsult = recorded.consults.find((entry) => entry.consultId === consultId)!;
        yield* launchConsult(recorded, recordedConsult, lead, {
          question: input.question,
          focusPaths: input.focusPaths ?? [],
        });
        return yield* waitHandle(recorded, consultId, input.waitSeconds);
      }),
      rejectedHandle,
    );

  /** Handles resolve only inside the Lead's own room, so no other thread reads a consult or its proposal. */
  const waitHandle = (callerRoom: PairRoom, handle: string, waitSeconds: number | undefined) =>
    Effect.gen(function* () {
      if (callerRoom.consults.some((entry) => entry.consultId === handle)) {
        const fresh = yield* expireStaleConsult(callerRoom);
        const settled = yield* waitForRoom(fresh.roomId, consultDone(handle), waitSeconds);
        const room = Option.getOrElse(settled, () => fresh);
        const consultNow = room.consults.find((entry) => entry.consultId === handle);
        if (!consultNow) {
          return yield* rejected("not-found", `Consult ${handle} is too old to read back.`);
        }
        const result = yield* consultResult(room, consultNow);
        if (result.status === "answered" && pairConsultAnswerOwed(consultNow)) {
          // The Lead read the answer here, so it needs no turn to bring it.
          yield* apply({
            type: "consult.answer-delivered",
            roomId: room.roomId,
            consultId: handle,
            at: yield* nowIso,
          });
        }
        return result;
      }
      if (callerRoom.assignments.some((entry) => entry.assignmentId === handle)) {
        const settled = yield* waitForRoom(
          callerRoom.roomId,
          assignmentSettled(handle),
          waitSeconds,
        );
        const room = Option.getOrElse(settled, () => callerRoom);
        const assignment = room.assignments.find((entry) => entry.assignmentId === handle)!;
        return {
          ...emptyHandleResult,
          status: assignment.state === "running" ? "pending" : "updated",
          handle,
          retryAfterSeconds: assignment.state === "running" ? 0 : null,
          assignment: pairAssignmentView(assignment),
        } satisfies PairHandleResult;
      }
      return yield* rejected("not-found", `No consult or assignment has the handle ${handle}.`);
    });

  const wait: PairCoordinator["Service"]["wait"] = (threadId, input) =>
    toolEdge<PairHandleResult>(
      Effect.gen(function* () {
        const caller = yield* requireRole(yield* resolveCaller(threadId), ["lead"]);
        return yield* waitHandle(caller.room, input.handle, input.waitSeconds);
      }),
      rejectedHandle,
    );

  // ── Assignments ─────────────────────────────────────────────────────

  const assign: PairCoordinator["Service"]["assign"] = (threadId, input) =>
    toolEdge<PairAssignResult>(
      Effect.gen(function* () {
        const caller = yield* requireRole(yield* resolveCaller(threadId), ["lead"]);
        const lead = yield* leadContext(caller.room);
        const baseCommit = yield* fromWorkspace(
          workspace.resolveCommit({ cwd: lead.cwd, ref: input.baseRef }),
        );
        const assignmentId = `assignment-${yield* uuid}`;
        const plan = yield* fromWorkspace(
          workspace.planAssignmentWorktree({ leadCwd: lead.cwd, assignmentId, title: input.title }),
        );
        const assignmentThreadId = ThreadId.make(yield* uuid);
        const at = yield* nowIso;
        const room = yield* apply({
          type: "assignment.create",
          roomId: caller.room.roomId,
          assignmentId,
          title: input.title,
          threadId: assignmentThreadId,
          worktreePath: plan.worktreePath,
          branch: plan.branch,
          baseCommit,
          scopeGlobs: input.scopeGlobs,
          acceptanceCriteria: input.acceptanceCriteria,
          expectedArtifact: input.expectedArtifact ?? "patch",
          at,
        });
        const assignment = room.assignments.find((entry) => entry.assignmentId === assignmentId)!;

        yield* Effect.gen(function* () {
          yield* fromWorkspace(workspace.createAssignmentWorktree({ plan, baseCommit }));
          yield* orchestrate({
            type: "thread.create",
            commandId: CommandId.make(`pair:${room.roomId}:assignment-thread:${assignmentId}`),
            threadId: assignmentThreadId,
            projectId: room.projectId,
            title: clampPairText(`${personaName(assignment.owner)}: ${assignment.title}`, 120),
            modelSelection: modelSelectionFor(assignment.owner),
            runtimeMode: lead.shell.runtimeMode,
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            branch: plan.branch,
            worktreePath: plan.worktreePath,
            createdAt: at,
          });
          yield* mirror(room, {
            kind: "task.started",
            summary: `${personaName(assignment.owner)} started an assignment`,
            payload: pairMirrorStartedPayload(assignmentCard(assignment), assignment.title),
            turnId: lead.activeTurnId,
          });
          yield* startTurn({
            commandKey: `pair:${room.roomId}:assignment:${assignmentId}:brief`,
            threadId: assignmentThreadId,
            persona: assignment.owner,
            runtimeMode: lead.shell.runtimeMode,
            text: assignmentBrief({ lead: lead.persona, assignment, brief: input.brief }),
            note: { purpose: "assignment-brief", from: lead.persona, to: assignment.owner },
            createdAt: at,
          });
        }).pipe(
          Effect.catch((error: PairFailure) =>
            Effect.gen(function* () {
              const detail = error._tag === "PairRoomRejectedError" ? error.detail : error.message;
              yield* apply({
                type: "assignment.update",
                roomId: room.roomId,
                assignmentId,
                by: "server",
                state: "failed",
                note: `Could not start: ${detail}`,
                at: yield* nowIso,
              });
              return yield* rejected("workspace", detail);
            }),
          ),
        );

        return {
          status: "assigned",
          assignmentId,
          threadId: assignmentThreadId,
          branch: plan.branch,
          reason: null,
          detail: `${personaName(assignment.owner)} is working in ${plan.worktreePath}. Wait for it with pair_wait using the assignmentId; it returns when the assignment is submitted or blocked.`,
        } satisfies PairAssignResult;
      }),
      (error) => ({
        status: "rejected",
        assignmentId: null,
        threadId: null,
        branch: null,
        reason: error.reason,
        detail: error.detail,
      }),
    );

  const updateAssignment = (
    room: PairRoom,
    assignment: PairAssignment,
    update: Omit<
      Extract<Parameters<typeof store.dispatch>[0], { type: "assignment.update" }>,
      "type" | "roomId" | "assignmentId" | "at"
    >,
  ) =>
    Effect.gen(function* () {
      const next = yield* apply({
        type: "assignment.update",
        roomId: room.roomId,
        assignmentId: assignment.assignmentId,
        ...update,
        at: yield* nowIso,
      });
      return {
        room: next,
        assignment: next.assignments.find(
          (entry) => entry.assignmentId === assignment.assignmentId,
        )!,
      };
    });

  const reportProgress: PairCoordinator["Service"]["reportProgress"] = (threadId, input) =>
    toolEdge<PairAckResult>(
      Effect.gen(function* () {
        const caller = yield* requireRole(yield* resolveCaller(threadId), ["assignee"]);
        const note =
          input.blocked && input.question ? `${input.note}\nNeeds: ${input.question}` : input.note;
        const next = yield* updateAssignment(caller.room, caller.assignment, {
          by: "agent",
          state: input.blocked ? "blocked" : "running",
          note,
        });
        yield* mirror(next.room, {
          kind: "task.progress",
          summary: input.blocked ? "Assignment blocked" : "Assignment progress",
          payload: pairMirrorProgressPayload(assignmentCard(next.assignment), {
            status: input.blocked ? "waiting" : "running",
            summary: note,
          }),
          turnId: yield* leadTurnNow(next.room),
        });
        return {
          status: "recorded",
          reason: null,
          detail: input.blocked
            ? "Recorded. The Lead sees that you are blocked; stop here and wait for instructions."
            : "Progress recorded.",
          assignment: pairAssignmentView(next.assignment),
          decision: null,
        } satisfies PairAckResult;
      }),
      ackRejected,
    );

  const refreshChanges = (assignment: PairAssignment) =>
    fromWorkspace(
      workspace.changedFiles({
        worktreePath: assignment.worktreePath,
        baseCommit: assignment.baseCommit,
      }),
    ).pipe(
      Effect.map((changedFiles) => ({
        changedFiles,
        // A findings assignment should not change anything, so every change is a deviation.
        deviations:
          assignment.expectedArtifact === "findings"
            ? changedFiles
            : pairScopeDeviations(changedFiles, assignment.scopeGlobs),
      })),
    );

  const submit: PairCoordinator["Service"]["submit"] = (threadId, input) =>
    toolEdge<PairAckResult>(
      Effect.gen(function* () {
        const caller = yield* requireRole(yield* resolveCaller(threadId), ["assignee"]);
        const changes = yield* refreshChanges(caller.assignment);
        const next = yield* updateAssignment(caller.room, caller.assignment, {
          by: "agent",
          state: "submitted",
          note: input.summary,
          report: {
            summary: clampPairText(input.summary),
            criteriaResults: input.criteriaResults.map((result) => ({
              criterion: clampPairText(result.criterion),
              met: result.met,
              evidence: result.evidence ? clampPairText(result.evidence) : null,
            })),
            testsRun: input.testsRun.map((entry) => clampPairText(entry)),
            knownLimitations: input.knownLimitations.map((entry) => clampPairText(entry)),
          },
          ...changes,
        });
        const deviationNote =
          changes.deviations.length > 0
            ? `\n\nChanged outside scope: ${changes.deviations.join(", ")}`
            : "";
        yield* mirror(next.room, {
          kind: "task.completed",
          summary: "Assignment submitted",
          payload: pairMirrorCompletedPayload(assignmentCard(next.assignment), {
            status: "answered",
            answer: `${input.summary}${deviationNote}`,
          }),
          turnId: yield* leadTurnNow(next.room),
        });
        return {
          status: "recorded",
          reason: null,
          detail:
            changes.deviations.length > 0
              ? `Submitted, but these files are outside your scope and will block approval: ${changes.deviations.join(", ")}. Stop here; the Lead reviews next.`
              : "Submitted. Stop here; the Lead reviews next.",
          assignment: pairAssignmentView(next.assignment),
          decision: null,
        } satisfies PairAckResult;
      }),
      ackRejected,
    );

  const review: PairCoordinator["Service"]["review"] = (threadId, input) =>
    toolEdge<PairAckResult>(
      Effect.gen(function* () {
        const caller = yield* requireRole(yield* resolveCaller(threadId), ["lead"]);
        const assignment = caller.room.assignments.find(
          (entry) => entry.assignmentId === input.assignmentId,
        );
        if (!assignment) {
          return yield* rejected(
            "not-found",
            `Assignment ${input.assignmentId} is not in this room.`,
          );
        }
        const card = assignmentCard(assignment);
        const turnId = yield* leadTurnNow(caller.room);
        switch (input.verdict) {
          case "approve": {
            if (assignment.state !== "submitted") {
              return yield* rejected(
                "invalid",
                `Only a submitted assignment can be approved; this one is ${assignment.state}.`,
              );
            }
            const assignmentShell = yield* threadShell(assignment.threadId);
            if (Option.isSome(assignmentShell) && isRunningTurn(assignmentShell.value)) {
              return yield* rejected(
                "conflict",
                `${personaName(assignment.owner)} is still working in the assignment thread. Wait for its turn to end, then approve.`,
              );
            }
            // Approval covers one commit: leftovers are committed now, and merging takes
            // exactly that commit, so later edits in the worktree never ride along.
            const approvedCommit =
              assignment.expectedArtifact === "findings"
                ? undefined
                : yield* fromWorkspace(
                    workspace.sealAssignment({
                      worktreePath: assignment.worktreePath,
                      branch: assignment.branch,
                      message: `Pair assignment: ${assignment.title}`,
                    }),
                  );
            const changes = yield* refreshChanges(assignment);
            const approved = yield* updateAssignment(caller.room, assignment, {
              by: "agent",
              ...changes,
              ...(approvedCommit ? { approvedCommit } : {}),
              state: assignment.expectedArtifact === "findings" ? "completed" : "awaiting-user",
              note: input.notes,
            });
            if (approved.assignment.state === "awaiting-user") {
              yield* mirror(approved.room, {
                kind: "task.progress",
                summary: "Assignment approved by the Lead",
                payload: pairMirrorProgressPayload(card, {
                  status: "waiting",
                  summary: `Approved by ${personaName(caller.persona)}. Waiting for you to merge ${assignment.branch}.`,
                }),
                turnId,
              });
            }
            return {
              status: "recorded",
              reason: null,
              detail:
                approved.assignment.state === "awaiting-user"
                  ? "Approved. The user merges it from the room controls; tell them it is ready."
                  : "Approved and completed.",
              assignment: pairAssignmentView(approved.assignment),
              decision: null,
            } satisfies PairAckResult;
          }
          case "request-changes": {
            if (assignment.state !== "submitted" && assignment.state !== "blocked") {
              return yield* rejected(
                "invalid",
                `Changes can be requested on a submitted or blocked assignment; this one is ${assignment.state}.`,
              );
            }
            const next = yield* updateAssignment(caller.room, assignment, {
              by: "agent",
              state: "running",
              note: `Changes requested: ${input.notes}`,
            });
            yield* startTurn({
              commandKey: `pair:${caller.room.roomId}:assignment:${assignment.assignmentId}:revision:${yield* uuid}`,
              threadId: assignment.threadId,
              persona: assignment.owner,
              runtimeMode: (yield* leadContext(caller.room)).shell.runtimeMode,
              text: revisionRequest({
                lead: caller.persona,
                notes: input.notes,
                deviations: assignment.deviations,
              }),
              note: { purpose: "revision", from: caller.persona, to: assignment.owner },
              createdAt: yield* nowIso,
            });
            yield* mirror(next.room, {
              kind: "task.progress",
              summary: "Changes requested",
              payload: pairMirrorProgressPayload(card, {
                status: "running",
                summary: `Changes requested: ${input.notes}`,
              }),
              turnId,
            });
            return {
              status: "recorded",
              reason: null,
              detail: "Sent back for changes. Wait for the resubmission with pair_wait.",
              assignment: pairAssignmentView(next.assignment),
              decision: null,
            } satisfies PairAckResult;
          }
          case "reject": {
            const next = yield* updateAssignment(caller.room, assignment, {
              by: "agent",
              state: "rejected",
              note: `Rejected: ${input.notes}`,
            });
            yield* interruptIfRunning(
              assignment.threadId,
              `pair:${caller.room.roomId}:assignment:${assignment.assignmentId}:reject`,
            );
            yield* mirror(next.room, {
              kind: "task.progress",
              summary: "Assignment rejected",
              payload: pairMirrorProgressPayload(card, {
                status: "waiting",
                summary: `Rejected by ${personaName(caller.persona)}: ${input.notes}`,
              }),
              turnId,
            });
            return {
              status: "recorded",
              reason: null,
              detail: "Rejected. Nothing will be merged.",
              assignment: pairAssignmentView(next.assignment),
              decision: null,
            } satisfies PairAckResult;
          }
        }
      }),
      ackRejected,
    );

  // ── Decisions ───────────────────────────────────────────────────────

  const recordDecision: PairCoordinator["Service"]["recordDecision"] = (threadId, input) =>
    toolEdge<PairAckResult>(
      Effect.gen(function* () {
        const caller = yield* requireRole(yield* resolveCaller(threadId), ["lead", "peer"]);
        const actor = { persona: caller.persona, role: caller.role };
        const position = { summary: input.position, evidence: input.evidence ?? null };
        const at = yield* nowIso;
        let decisionId = input.decisionId;
        let room: PairRoom;
        if (decisionId) {
          room = yield* apply({
            type: "decision.add-position",
            roomId: caller.room.roomId,
            decisionId,
            actor,
            position,
            resolution: input.resolution ?? null,
            at,
          });
        } else {
          if (!input.title || !input.category) {
            return yield* rejected("invalid", "A new decision needs a title and a category.");
          }
          decisionId = `decision-${yield* uuid}`;
          room = yield* apply({
            type: "decision.record",
            roomId: caller.room.roomId,
            decisionId,
            actor,
            kind: input.kind ?? "decision",
            category: input.category,
            title: input.title,
            position,
            leadRecommendation: input.leadRecommendation ?? null,
            consequenceOfDeferring: input.consequenceOfDeferring ?? null,
            resolution: input.resolution ?? null,
            at,
          });
        }
        const decision = room.decisions.find((entry) => entry.decisionId === decisionId)!;
        const view = pairDecisionView(decision);
        return {
          status: "recorded",
          reason: null,
          detail:
            view.waitingOn === "user"
              ? "Recorded and left open: this call belongs to the user. Tell them what you recommend and why."
              : view.resolution
                ? `Recorded and resolved by the ${decision.resolvedBy}.`
                : "Recorded.",
          assignment: null,
          decision: view,
        } satisfies PairAckResult;
      }),
      ackRejected,
    );

  // ── User commands ───────────────────────────────────────────────────

  const requireRoom = (roomId: PairRoomId) =>
    store
      .get(roomId)
      .pipe(
        Effect.flatMap((room) =>
          Option.isSome(room)
            ? Effect.succeed(room.value)
            : rejected("not-found", `Pair room ${roomId} was not found.`),
        ),
      );

  const requireAssignment = (room: PairRoom, assignmentId: string) => {
    const assignment = room.assignments.find((entry) => entry.assignmentId === assignmentId);
    return assignment
      ? Effect.succeed(assignment)
      : rejected("not-found", `Assignment ${assignmentId} was not found.`);
  };

  const runUserCommand = (command: PairRoomUserCommand) =>
    Effect.gen(function* () {
      const at = yield* nowIso;
      switch (command.type) {
        case "room.create": {
          const existing = yield* store.findByThread(command.leadThreadId);
          if (Option.isSome(existing)) {
            const lead = pairRoomParticipant(existing.value, "lead");
            // Retrying the same create returns the room instead of failing.
            if (lead?.threadId === command.leadThreadId && lead.persona === command.leadPersona) {
              return existing.value.roomId;
            }
          }
          const shell = yield* threadShell(command.leadThreadId);
          if (Option.isSome(shell) && shell.value.projectId !== command.projectId) {
            return yield* rejected("invalid", "The Lead thread belongs to a different project.");
          }
          if (
            Option.isSome(shell) &&
            shell.value.modelSelection.model !== PAIR_PERSONAS[command.leadPersona].model
          ) {
            return yield* rejected(
              "invalid",
              `The Lead thread uses ${shell.value.modelSelection.model}, not ${PAIR_PERSONAS[command.leadPersona].model}.`,
            );
          }
          const room = yield* apply({
            type: "room.create",
            roomId: PairRoomId.make(yield* uuid),
            projectId: command.projectId,
            leadThreadId: command.leadThreadId,
            leadPersona: command.leadPersona,
            mode: command.mode,
            maxRoundsPerTurn: command.maxRoundsPerTurn,
            at,
          });
          return room.roomId;
        }
        case "room.update": {
          const room = yield* apply({
            type: "room.update",
            roomId: command.roomId,
            mode: command.mode,
            maxRoundsPerTurn: command.maxRoundsPerTurn,
            status: command.status,
            ...(command.status === "paused" ? { statusReason: "Paused by the user." } : {}),
            ...(command.status === "closed" ? { statusReason: "Closed by the user." } : {}),
            at,
          });
          return room.roomId;
        }
        case "room.grant-rounds": {
          const room = yield* requireRoom(command.roomId);
          const lead = yield* leadContext(room);
          if (!lead.activeTurnId) {
            return yield* rejected(
              "invalid",
              "The Lead is not in a turn. Its next turn starts with a fresh round budget.",
            );
          }
          yield* apply({
            type: "room.grant-rounds",
            roomId: room.roomId,
            leadTurnId: lead.activeTurnId,
            count: command.count,
            at,
          });
          return room.roomId;
        }
        case "consult.cancel": {
          const room = yield* requireRoom(command.roomId);
          const consult = room.consults.find((entry) => entry.consultId === command.consultId);
          if (!consult) return yield* rejected("not-found", "That consult was not found.");
          if (consult.status !== "running") return room.roomId;
          const peerThreadId = pairRoomParticipant(room, "peer")?.threadId;
          if (peerThreadId) {
            yield* interruptIfRunning(
              peerThreadId,
              `pair:${room.roomId}:cancel:${consult.consultId}`,
            );
          }
          yield* settleConsult(room, consult, {
            status: "cancelled",
            peerTurnId: null,
            error: "Cancelled by the user.",
          });
          return room.roomId;
        }
        case "assignment.integrate": {
          const room = yield* requireRoom(command.roomId);
          const assignment = yield* requireAssignment(room, command.assignmentId);
          if (assignment.state !== "awaiting-user") {
            return yield* rejected(
              "invalid",
              `Only an approved assignment can be merged; this one is ${assignment.state}.`,
            );
          }
          const lead = yield* leadContext(room);
          if (lead.activeTurnId || isRunningTurn(lead.shell)) {
            return yield* rejected(
              "conflict",
              `Wait for ${personaName(lead.persona)} to finish its turn before merging.`,
            );
          }
          const assignmentShell = yield* threadShell(assignment.threadId);
          if (Option.isSome(assignmentShell) && isRunningTurn(assignmentShell.value)) {
            return yield* rejected("conflict", "The assignment thread is still running.");
          }
          const sendBackForReview = (detail: string) =>
            Effect.gen(function* () {
              yield* updateAssignment(room, assignment, {
                by: "user",
                state: "submitted",
                note: `${detail} Nothing was merged; the Lead needs to review it again.`,
              });
              return yield* rejected(
                "conflict",
                `${detail} Nothing was merged. Ask ${personaName(lead.persona)} to review it again.`,
              );
            });
          if (assignment.approvedCommit === null) {
            return yield* sendBackForReview(
              "This assignment was approved without a pinned commit.",
            );
          }
          const result = yield* fromWorkspace(
            workspace.integrate({
              leadCwd: lead.cwd,
              worktreePath: assignment.worktreePath,
              branch: assignment.branch,
              commit: assignment.approvedCommit,
              message: `Pair assignment: ${assignment.title}`,
            }),
          );
          if (result.status === "changed") {
            return yield* sendBackForReview(result.detail);
          }
          const card = assignmentCard(assignment);
          if (result.status === "conflict") {
            yield* updateAssignment(room, assignment, {
              by: "user",
              note: `Merge conflict, nothing was merged:\n${result.detail}`,
            });
            return yield* rejected(
              "conflict",
              `Merging ${assignment.branch} conflicts with your checkout, so nothing was merged. The worktree is untouched.`,
            );
          }
          const merged = yield* updateAssignment(room, assignment, {
            by: "user",
            state: "integrated",
            integrationCommit: result.commit,
            note: `Merged as ${result.commit.slice(0, 12)}.`,
          });
          yield* mirror(merged.room, {
            kind: "task.progress",
            summary: "Assignment merged",
            payload: pairMirrorProgressPayload(card, {
              status: "waiting",
              summary: `Merged into your checkout as ${result.commit.slice(0, 12)}.`,
            }),
            turnId: null,
          });
          return room.roomId;
        }
        case "assignment.cancel": {
          const room = yield* requireRoom(command.roomId);
          const assignment = yield* requireAssignment(room, command.assignmentId);
          const next = yield* updateAssignment(room, assignment, {
            by: "user",
            state: "cancelled",
            note: "Cancelled by the user.",
          });
          yield* interruptIfRunning(
            assignment.threadId,
            `pair:${room.roomId}:assignment:${assignment.assignmentId}:cancel:${yield* uuid}`,
          );
          yield* mirror(next.room, {
            kind: "task.progress",
            summary: "Assignment cancelled",
            payload: pairMirrorProgressPayload(assignmentCard(assignment), {
              status: "waiting",
              summary: "Cancelled by you. Resume it from the room controls.",
            }),
            turnId: null,
          });
          return room.roomId;
        }
        case "assignment.resume": {
          const room = yield* requireRoom(command.roomId);
          const assignment = yield* requireAssignment(room, command.assignmentId);
          const lead = yield* leadContext(room);
          const next = yield* updateAssignment(room, assignment, {
            by: "user",
            state: "running",
            note: "Resumed by the user.",
          });
          yield* startTurn({
            commandKey: `pair:${room.roomId}:assignment:${assignment.assignmentId}:resume:${yield* uuid}`,
            threadId: assignment.threadId,
            persona: assignment.owner,
            runtimeMode: lead.shell.runtimeMode,
            text: resumeRequest(assignment),
            note: { purpose: "resume", from: lead.persona, to: assignment.owner },
            createdAt: at,
          });
          yield* mirror(next.room, {
            kind: "task.progress",
            summary: "Assignment resumed",
            payload: pairMirrorProgressPayload(assignmentCard(assignment), {
              status: "running",
              summary: "Resumed.",
            }),
            turnId: null,
          });
          return room.roomId;
        }
        case "assignment.set-scope": {
          const room = yield* requireRoom(command.roomId);
          const assignment = yield* requireAssignment(room, command.assignmentId);
          yield* updateAssignment(room, assignment, { by: "user", scopeGlobs: command.scopeGlobs });
          return room.roomId;
        }
        case "decision.resolve": {
          const room = yield* apply({
            type: "decision.resolve",
            roomId: command.roomId,
            decisionId: command.decisionId,
            resolution: command.resolution,
            resolvedBy: "user",
            at,
          });
          return room.roomId;
        }
        case "lead.switch-start": {
          const room = yield* requireRoom(command.roomId);
          const lead = yield* leadContext(room);
          if (lead.activeTurnId) {
            return yield* rejected(
              "conflict",
              `${personaName(lead.persona)} is still working. Wait for its turn to end or stop it, then switch.`,
            );
          }
          const next = yield* apply({ type: "lead.switch-start", roomId: room.roomId, at });
          yield* startTurn({
            commandKey: `pair:${room.roomId}:handoff-request:${yield* uuid}`,
            threadId: lead.threadId,
            persona: lead.persona,
            runtimeMode: lead.shell.runtimeMode,
            text: handoffRequest({ from: lead.persona, to: next.leadSwitch!.toPersona }),
            note: {
              purpose: "handoff-request",
              from: lead.persona,
              to: next.leadSwitch!.toPersona,
            },
            createdAt: at,
          }).pipe(
            Effect.catch((error: PairFailure) =>
              apply({
                type: "lead.switch-draft",
                roomId: room.roomId,
                handoff: null,
                error: `Could not ask for a handoff: ${error._tag === "PairRoomRejectedError" ? error.detail : error.message}`,
                at,
              }),
            ),
          );
          return room.roomId;
        }
        case "lead.switch-confirm": {
          const room = yield* requireRoom(command.roomId);
          const handoff = room.leadSwitch?.phase === "ready" ? room.leadSwitch.handoff : null;
          if (!room.leadSwitch || handoff === null) {
            return yield* rejected("invalid", "The handoff is not ready to confirm yet.");
          }
          const lead = yield* leadContext(room);
          if (lead.activeTurnId) {
            return yield* rejected(
              "conflict",
              `${personaName(lead.persona)} started another turn. Wait for it to end, then confirm.`,
            );
          }
          const toPersona = room.leadSwitch.toPersona;
          const newLeadThreadId = ThreadId.make(yield* uuid);
          // The new Lead gets its own thread in the same checkout. The old
          // threads stay as history; resuming a provider session in another
          // directory would lose its context anyway, and the handoff carries it.
          yield* orchestrate({
            type: "thread.create",
            commandId: CommandId.make(`pair:${room.roomId}:lead-thread:${newLeadThreadId}`),
            threadId: newLeadThreadId,
            projectId: room.projectId,
            title: lead.shell.title,
            modelSelection: modelSelectionFor(toPersona),
            runtimeMode: lead.shell.runtimeMode,
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            branch: lead.shell.branch,
            worktreePath: lead.shell.worktreePath,
            createdAt: at,
          });
          const next = yield* apply({
            type: "lead.switch-confirm",
            roomId: room.roomId,
            newLeadThreadId,
            at,
          });
          yield* startTurn({
            commandKey: `pair:${room.roomId}:handoff:${newLeadThreadId}`,
            threadId: newLeadThreadId,
            persona: toPersona,
            runtimeMode: lead.shell.runtimeMode,
            text: leadHandoff({
              from: lead.persona,
              to: toPersona,
              handoff,
              facts: {
                assignments: next.assignments.filter(
                  (assignment) =>
                    PAIR_ASSIGNMENT_ACTIVE_STATES.has(assignment.state) ||
                    assignment.state === "interrupted",
                ),
                openDecisions: next.decisions.filter((decision) => decision.resolution === null),
                cwd: lead.cwd,
                branch: lead.shell.branch,
              },
            }),
            note: {
              purpose: "handoff",
              from: lead.persona,
              to: toPersona,
              fromThreadId: lead.threadId,
            },
            createdAt: at,
          });
          return { roomId: room.roomId, threadId: newLeadThreadId };
        }
        case "lead.switch-cancel": {
          const room = yield* requireRoom(command.roomId);
          const leadThreadId = pairRoomParticipant(room, "lead")?.threadId;
          if (room.leadSwitch?.phase === "drafting" && leadThreadId) {
            yield* interruptIfRunning(
              leadThreadId,
              `pair:${room.roomId}:handoff-cancel:${yield* uuid}`,
            );
          }
          yield* apply({ type: "lead.switch-cancel", roomId: room.roomId, at });
          return room.roomId;
        }
      }
    });

  const dispatchUserCommand: PairCoordinator["Service"]["dispatchUserCommand"] = (command) =>
    runUserCommand(command).pipe(
      Effect.map((result): PairRoomDispatchResult =>
        typeof result === "string" ? { roomId: result } : result,
      ),
      Effect.mapError(userCommandError),
    );

  // ── Reactions ───────────────────────────────────────────────────────

  /** Stores the outgoing Lead's handoff once its handoff turn ends. */
  const settleHandoffDraft = (room: PairRoom, threadId: ThreadId) =>
    Effect.gen(function* () {
      const lead = pairRoomParticipant(room, "lead");
      if (room.leadSwitch?.phase !== "drafting" || lead?.threadId !== threadId) return false;
      const outcome = yield* readTurnOutcome(
        threadId,
        room.leadSwitch.requestedAt,
        personaName(lead.persona),
      );
      if (Option.isNone(outcome)) return false;
      const turnId = outcome.value.peerTurnId;
      const handoff =
        outcome.value.status === "answered" && turnId
          ? yield* readTurnText(threadId, turnId)
          : null;
      yield* apply({
        type: "lead.switch-draft",
        roomId: room.roomId,
        handoff,
        error: outcome.value.error,
        at: yield* nowIso,
      });
      return true;
    });

  const settleIfAnswered = (room: PairRoom, threadId: ThreadId) =>
    Effect.gen(function* () {
      if (pairRoomParticipant(room, "peer")?.threadId !== threadId) return;
      const running = room.consults.find((consult) => consult.status === "running");
      if (!running) return;
      const outcome = yield* readPeerOutcome(threadId, running);
      if (Option.isSome(outcome)) yield* settleConsult(room, running, outcome.value);
    });

  /**
   * Sends a user message on the Lead's thread to the Peer as well: every
   * message in roundtable mode, and any message that @-mentions the Peer.
   * The server does it rather than the Lead, so it happens every time.
   */
  const relayUserMessage = (
    room: PairRoom,
    event: Extract<OrchestrationEvent, { type: "thread.turn-start-requested" }>,
  ) =>
    Effect.gen(function* () {
      const lead = pairRoomParticipant(room, "lead");
      const peer = pairRoomParticipant(room, "peer");
      if (!lead || !peer || lead.threadId !== event.payload.threadId) return;
      if (room.status !== "active" || room.leadSwitch) return;
      const detail = yield* snapshots
        .getThreadDetailById(lead.threadId)
        .pipe(Effect.mapError(internal("read user message")));
      const message = Option.getOrUndefined(detail)?.messages.find(
        (entry) => entry.id === event.payload.messageId,
      );
      // Turns the room starts itself (answers, handoffs) carry a note and are never relayed.
      if (message?.role !== "user" || readPairRoomNote(message.context) !== null) return;
      const mentioned = pairMessageMentions(message.text, peer.persona);
      if (!mentioned && room.mode !== "roundtable") return;
      const leadNow = yield* leadContext(room);
      const title = titleFrom(message.text);
      if (room.consults.some((consult) => consult.status === "running")) {
        const card: PairMirrorCard = {
          taskId: `relay-skipped-${yield* uuid}`,
          title: `${personaName(peer.persona)}: ${title}`,
          persona: personaName(peer.persona),
          model: PAIR_PERSONAS[peer.persona].model,
        };
        yield* mirror(room, {
          kind: "task.started",
          summary: `${personaName(peer.persona)} is still busy`,
          payload: pairMirrorStartedPayload(card, message.text),
          turnId: null,
        });
        yield* mirror(room, {
          kind: "task.completed",
          summary: `${personaName(peer.persona)} is still busy`,
          payload: pairMirrorCompletedPayload(card, {
            status: "failed",
            error: `${personaName(peer.persona)} was still answering an earlier request, so only ${personaName(lead.persona)} got this message.`,
          }),
          turnId: null,
        });
        return;
      }
      const consultId = `consult-${yield* uuid}`;
      const recorded = yield* apply({
        type: "consult.request",
        roomId: room.roomId,
        consultId,
        kind: mentioned ? "question" : "roundtable",
        leadTurnId: null,
        automatic: true,
        answerTo: "lead-turn",
        title,
        // The Lead turn for this message was requested at this instant, which is how delivery finds it.
        at: event.payload.createdAt,
      });
      yield* launchConsult(
        recorded,
        recorded.consults.find((entry) => entry.consultId === consultId)!,
        leadNow,
        { question: message.text, focusPaths: [] },
      );
    });

  /**
   * Starts a Lead turn with the Peer's answer to a relayed user message once
   * the Lead's own turn for that message has ended. An answer that can no
   * longer help (the Peer failed, the user stopped the Lead, the room closed
   * or is changing Lead) is marked delivered, and its card is all that shows.
   */
  const deliverPeerAnswer = (room: PairRoom) =>
    Effect.gen(function* () {
      const owed = room.consults.find(pairConsultAnswerOwed);
      if (!owed) return;
      const markDelivered = Effect.gen(function* () {
        yield* apply({
          type: "consult.answer-delivered",
          roomId: room.roomId,
          consultId: owed.consultId,
          at: yield* nowIso,
        });
      });
      const lead = pairRoomParticipant(room, "lead");
      const peer = pairRoomParticipant(room, "peer");
      const shell = lead?.threadId
        ? Option.getOrUndefined(yield* threadShell(lead.threadId))
        : undefined;
      if (
        !lead?.threadId ||
        !peer ||
        !shell ||
        owed.status !== "answered" ||
        room.status !== "active" ||
        room.leadSwitch
      ) {
        return yield* markDelivered;
      }
      const turn = shell.latestTurn;
      if (isRunningTurn(shell) || !turn || turn.requestedAt < owed.requestedAt) return;
      if (turn.state === "interrupted") return yield* markDelivered;
      const answer = yield* readAnswer(room, owed);
      // Marked first: a failed start must not bring the same answer twice.
      yield* markDelivered;
      if (!answer) return;
      yield* startTurn({
        commandKey: `pair:${room.roomId}:peer-answer:${owed.consultId}`,
        threadId: lead.threadId,
        persona: lead.persona,
        runtimeMode: shell.runtimeMode,
        text: peerAnswerPrompt({
          lead: lead.persona,
          peer: peer.persona,
          kind: owed.kind,
          answer: clampPairText(answer, PAIR_PEER_ANSWER_MAX_LENGTH),
        }),
        note: { purpose: "peer-answer", from: peer.persona, to: lead.persona },
        createdAt: yield* nowIso,
      });
    });

  const reviewGuardrail = (
    room: PairRoom,
    event: Extract<OrchestrationEvent, { type: "thread.turn-diff-completed" }>,
  ) =>
    Effect.gen(function* () {
      if (room.mode !== "pair" || room.status !== "active") return;
      if (pairRoomParticipant(room, "lead")?.threadId !== event.payload.threadId) return;
      if (event.payload.files.length === 0) return;
      const turnId = event.payload.turnId;
      if (
        room.consults.some((consult) => consult.leadTurnId === turnId && consult.kind === "review")
      ) {
        return;
      }
      if (room.consults.some((consult) => consult.status === "running")) {
        yield* Effect.logInfo("pair review guardrail skipped: the Peer is busy", {
          roomId: room.roomId,
        });
        return;
      }
      const lead = yield* leadContext(room);
      const consultId = `consult-${yield* uuid}`;
      const files = event.payload.files.map((file) => file.path);
      const recorded = yield* apply({
        type: "consult.request",
        roomId: room.roomId,
        consultId,
        kind: "review",
        leadTurnId: turnId,
        automatic: true,
        title: `Review of ${personaName(lead.persona)}'s changes`,
        at: yield* nowIso,
      });
      yield* launchConsult(
        recorded,
        recorded.consults.find((entry) => entry.consultId === consultId)!,
        lead,
        {
          question: `${personaName(lead.persona)} finished a turn that changed these files without a review:\n${files
            .slice(0, 50)
            .map((file) => `- ${file}`)
            .join("\n")}\n\nReview those changes.`,
          focusPaths: files.slice(0, 20),
        },
      );
    });

  const scopeCheck = (room: PairRoom, threadId: ThreadId) =>
    Effect.gen(function* () {
      const assignment = room.assignments.find((entry) => entry.threadId === threadId);
      if (!assignment || !(assignment.state === "running" || assignment.state === "blocked"))
        return;
      const changes = yield* refreshChanges(assignment);
      const before = new Set(assignment.deviations);
      const next = yield* updateAssignment(room, assignment, { by: "server", ...changes });
      const added = changes.deviations.filter((file) => !before.has(file));
      if (added.length > 0) {
        yield* mirror(next.room, {
          kind: "task.progress",
          summary: "Assignment changed files outside its scope",
          payload: pairMirrorProgressPayload(assignmentCard(assignment), {
            status: "running",
            summary: `Changed outside scope: ${added.join(", ")}`,
          }),
          turnId: null,
        });
      }
    });

  /** An assignee that ends its turn without submitting would leave the Lead waiting forever. */
  const flagUnsubmittedTurn = (room: PairRoom, threadId: ThreadId) =>
    Effect.gen(function* () {
      const assignment = room.assignments.find((entry) => entry.threadId === threadId);
      if (assignment?.state !== "running") return;
      const shell = Option.getOrUndefined(yield* threadShell(threadId));
      const turn = shell?.latestTurn;
      if (!turn || turn.state === "running" || !turn.completedAt) return;
      if (turn.completedAt < assignment.updatedAt) return;
      const next = yield* updateAssignment(room, assignment, {
        by: "server",
        state: "blocked",
        note:
          turn.state === "completed"
            ? `${personaName(assignment.owner)} ended its turn without calling pair_submit.`
            : `${personaName(assignment.owner)}'s turn ended as ${turn.state}.`,
      });
      yield* mirror(next.room, {
        kind: "task.progress",
        summary: "Assignment stopped without submitting",
        payload: pairMirrorProgressPayload(assignmentCard(assignment), {
          status: "waiting",
          summary: next.assignment.note ?? "Stopped.",
        }),
        turnId: null,
      });
    });

  const waitingOnUser = yield* Ref.make(new Set<ThreadId>());
  const WAITING_ACTIVITY_KINDS = new Set([
    "approval.requested",
    "approval.resolved",
    "user-input.requested",
    "user-input.resolved",
  ]);

  /**
   * Peer and assignment threads run out of the user's sight, so an approval
   * or question there is mirrored onto its card in the Lead's thread.
   */
  const mirrorWaitingOnUser = (room: PairRoom, threadId: ThreadId) =>
    Effect.gen(function* () {
      const peer = pairRoomParticipant(room, "peer");
      const consult =
        peer?.threadId === threadId
          ? room.consults.find((entry) => entry.status === "running")
          : undefined;
      const assignment = room.assignments.find(
        (entry) => entry.threadId === threadId && PAIR_ASSIGNMENT_ACTIVE_STATES.has(entry.state),
      );
      const card =
        consult && peer
          ? consultCard(consult, peer.persona)
          : assignment
            ? assignmentCard(assignment)
            : null;
      if (!card) return;
      const shell = Option.getOrUndefined(yield* threadShell(threadId));
      if (!shell) return;
      const waiting = shell.hasPendingApprovals || shell.hasPendingUserInput;
      const changed = yield* Ref.modify(waitingOnUser, (current) => {
        if (current.has(threadId) === waiting) return [false, current] as const;
        const next = new Set(current);
        if (waiting) next.add(threadId);
        else next.delete(threadId);
        return [true, next] as const;
      });
      if (!changed) return;
      const what = shell.hasPendingApprovals ? "approval" : "answer";
      yield* mirror(room, {
        kind: "task.progress",
        summary: waiting ? `${card.persona} is waiting for you` : `${card.persona} is continuing`,
        payload: pairMirrorProgressPayload(card, {
          status: waiting ? "waiting" : "running",
          summary: waiting
            ? `Waiting for your ${what} in ${card.persona}'s thread.`
            : "Continuing.",
        }),
        turnId: null,
      });
    });

  /**
   * Deleting the Lead's thread ends the room, since nothing can speak for it.
   * Deleting an assignment thread cancels that assignment.
   */
  const handleThreadDeleted = (room: PairRoom, threadId: ThreadId) =>
    Effect.gen(function* () {
      if (pairRoomParticipant(room, "lead")?.threadId === threadId) {
        if (room.status === "closed") return;
        yield* apply({
          type: "room.update",
          roomId: room.roomId,
          status: "closed",
          statusReason: "The Lead's thread was deleted.",
          at: yield* nowIso,
        });
        return;
      }
      const assignment = room.assignments.find((entry) => entry.threadId === threadId);
      if (assignment && PAIR_ASSIGNMENT_ACTIVE_STATES.has(assignment.state)) {
        yield* updateAssignment(room, assignment, {
          by: "server",
          state: "cancelled",
          note: "The assignment's thread was deleted.",
        });
      }
    });

  const handleEvent = (event: OrchestrationEvent) =>
    Effect.gen(function* () {
      if (event.aggregateKind !== "thread") return;
      const threadId = ThreadId.make(event.aggregateId);
      const found = yield* store.findByThread(threadId);
      if (Option.isNone(found)) return;
      if (event.type === "thread.deleted") {
        yield* handleThreadDeleted(found.value, threadId);
        return;
      }
      if (
        event.type === "thread.activity-appended" &&
        WAITING_ACTIVITY_KINDS.has(event.payload.activity.kind)
      ) {
        yield* mirrorWaitingOnUser(found.value, threadId);
        return;
      }
      if (event.type === "thread.turn-start-requested") {
        yield* relayUserMessage(found.value, event);
        return;
      }
      yield* settleIfAnswered(found.value, threadId);
      yield* settleHandoffDraft(found.value, threadId);
      if (event.type === "thread.turn-diff-completed") {
        const room = Option.getOrElse(yield* store.get(found.value.roomId), () => found.value);
        yield* reviewGuardrail(room, event);
        yield* scopeCheck(room, threadId);
      }
      if (event.type === "thread.session-set" || event.type === "thread.turn-diff-completed") {
        const room = Option.getOrElse(yield* store.get(found.value.roomId), () => found.value);
        yield* flagUnsubmittedTurn(room, threadId);
        yield* deliverPeerAnswer(
          Option.getOrElse(yield* store.get(found.value.roomId), () => room),
        );
      }
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("pair room event handling failed", { eventType: event.type, cause }),
      ),
    );

  /** No silent model substitution: a reroute pauses the room until the user decides. */
  const pauseOnReroute = providers.streamEvents.pipe(
    Stream.filter((event) => event.type === "model.rerouted"),
    Stream.runForEach((event) =>
      Effect.gen(function* () {
        if (event.type !== "model.rerouted") return;
        const room = yield* store.findByThread(event.threadId);
        if (Option.isNone(room) || room.value.status !== "active") return;
        yield* apply({
          type: "room.update",
          roomId: room.value.roomId,
          status: "paused",
          statusReason: `A participant was rerouted from ${event.payload.fromModel} to ${event.payload.toModel} (${event.payload.reason}). Resume the room to continue anyway.`,
          at: yield* nowIso,
        });
      }).pipe(
        Effect.catchCause((cause) => Effect.logWarning("pair reroute pause failed", { cause })),
      ),
    ),
  );

  /** Domain events are live-only, so anything in flight when the server stopped is settled here. */
  const reconcile = Effect.gen(function* () {
    for (const room of yield* store.list) {
      const leadThreadId = pairRoomParticipant(room, "lead")?.threadId;
      if (room.leadSwitch?.phase === "drafting" && leadThreadId) {
        const settled = yield* settleHandoffDraft(room, leadThreadId);
        const shell = Option.getOrUndefined(yield* threadShell(leadThreadId));
        if (!settled && !(shell && isRunningTurn(shell))) {
          yield* apply({
            type: "lead.switch-draft",
            roomId: room.roomId,
            handoff: null,
            error: "The server restarted before the handoff was written. Start the switch again.",
            at: yield* nowIso,
          });
        }
      }
      const running = room.consults.find((consult) => consult.status === "running");
      const peerThreadId = pairRoomParticipant(room, "peer")?.threadId;
      if (running) {
        const outcome = peerThreadId
          ? yield* readPeerOutcome(peerThreadId, running)
          : Option.none<PeerOutcome>();
        yield* settleConsult(
          room,
          running,
          Option.getOrElse(outcome, (): PeerOutcome => ({
            status: "failed",
            peerTurnId: null,
            error: "The server restarted before the Peer answered. Ask again if you still need it.",
          })),
        );
      }
      yield* deliverPeerAnswer(Option.getOrElse(yield* store.get(room.roomId), () => room));
      for (const assignment of room.assignments) {
        if (assignment.state !== "running") continue;
        const current = Option.getOrElse(yield* store.get(room.roomId), () => room);
        const next = yield* updateAssignment(current, assignment, {
          by: "server",
          state: "interrupted",
          note: "The server restarted while this assignment was running. Resume or cancel it.",
        });
        yield* mirror(next.room, {
          kind: "task.progress",
          summary: "Assignment interrupted",
          payload: pairMirrorProgressPayload(assignmentCard(assignment), {
            status: "waiting",
            summary: "Interrupted by a server restart. Resume or cancel it from the room controls.",
          }),
          turnId: null,
        });
      }
    }
  }).pipe(
    Effect.catchCause((cause) => Effect.logWarning("pair room reconciliation failed", { cause })),
  );

  const events = yield* engine.subscribeDomainEvents;
  yield* reconcile;
  yield* events.pipe(Stream.runForEach(handleEvent), Effect.forkScoped);
  yield* pauseOnReroute.pipe(Effect.forkScoped);

  return PairCoordinator.of({
    dispatchUserCommand,
    status,
    consult,
    wait,
    assign,
    reportProgress,
    submit,
    review,
    recordDecision,
  });
});

export const layer = Layer.effect(PairCoordinator, make);
