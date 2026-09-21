import {
  ApprovalRequestId,
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  MessageId,
  PAIR_ASSIGNMENT_ACTIVE_STATES,
  PAIR_CONVERSATION_MAX_EXCHANGES,
  PAIR_PERSONAS,
  PairRoomCommandError,
  PairRoomId,
  ThreadId,
  otherPairPersona,
  pairDecisionLeadMayResolve,
  pairRoomParticipant,
  type OrchestrationCommand,
  type OrchestrationEvent,
  type OrchestrationMessage,
  type OrchestrationThreadShell,
  type PairAssignment,
  type PairConsult,
  type PairConsultKind,
  type PairDecision,
  type PairParticipant,
  type PairPersona,
  type PairRoom,
  type PairRoomDispatchResult,
  type PairRoomNote,
  type PairRoomUserCommand,
  type ProjectId,
  type TurnId,
} from "@t3tools/contracts";
import {
  type PairMirrorCard,
  type PairMirrorOutcome,
  pairMirrorCompletedPayload,
  pairMirrorProgressPayload,
  pairMirrorStartedPayload,
} from "@t3tools/shared/pairMirror";
import { pairMessageMentions } from "@t3tools/shared/pairMentions";
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
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderService from "../provider/Services/ProviderService.ts";
import {
  assigneeBlockedPrompt,
  assigneeReplyPrompt,
  assignmentBrief,
  consultPrompt,
  handoffRequest,
  decisionResolved,
  leadHandoff,
  peerReplyPrompt,
  replyPrompt,
  resumeRequest,
  revisionRequest,
  roleGuidance,
  signOffPrompt,
  type PeerReplyDelivery,
} from "./PairPrompts.ts";
import { PairRoomRejectedError, PairRoomStore } from "./PairRoomStore.ts";
import {
  PAIR_DEFAULT_WAIT_SECONDS,
  PAIR_MAX_WAIT_SECONDS,
  PAIR_READ_THREAD_DEFAULT_LIMIT,
  type PairAckResult,
  type PairAssignResult,
  type PairCheckoutResult,
  type PairIntegrateResult,
  type PairAssignmentView,
  type PairCallerRole,
  type PairDecisionView,
  type PairHandleResult,
  type PairReadThreadResult,
  type PairStatusResult,
} from "./PairToolSchemas.ts";
import {
  PAIR_TRANSCRIPT_BOOTSTRAP_MESSAGES,
  PAIR_TRANSCRIPT_LINE_MAX_LENGTH,
  pairFinalAnswer,
  pairTranscriptFilesLine,
  pairTranscriptKey,
  pairTranscriptLine,
  pairTranscriptSources,
  pairTranscriptStoppedLine,
} from "./PairTranscript.ts";
import { PairWorkspace, type PairWorkspaceError } from "./PairWorkspace.ts";
import {
  clampPairText,
  pairConsultAnswerOwed,
  pairConversation,
  pairDecisionAnswerOwed,
  pairExchangeIndex,
  pairRoundLimit,
  pairRoundsUsed,
  pairScopeDeviations,
} from "./pairRoomDecider.ts";

/** How many settled decisions pair_status carries alongside the open ones. */
const PAIR_STATUS_SETTLED_DECISIONS = 5;

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
    readonly reply: (
      threadId: ThreadId,
      input: {
        readonly handle: string;
        readonly message: string;
        readonly waitSeconds?: number | undefined;
      },
    ) => ToolEffect<PairHandleResult>;
    readonly ask: (
      threadId: ThreadId,
      input: { readonly question: string },
    ) => ToolEffect<PairAckResult>;
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
    readonly checkout: (
      threadId: ThreadId,
      input: { readonly path: string },
    ) => ToolEffect<PairCheckoutResult>;
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
    readonly integrate: (
      threadId: ThreadId,
      input: { readonly assignmentId: string; readonly userWords: string },
    ) => ToolEffect<PairIntegrateResult>;
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
        readonly waitSeconds?: number | undefined;
      },
    ) => ToolEffect<PairAckResult>;
    readonly readThread: (
      threadId: ThreadId,
      input: {
        readonly persona?: PairPersona | undefined;
        readonly beforeMessageId?: string | undefined;
        readonly limit?: number | undefined;
      },
    ) => ToolEffect<PairReadThreadResult>;
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
  /** The room's checkout: where the Lead works, which may not be its thread's directory. */
  readonly cwd: string;
  readonly branch: string | null;
  readonly activeTurnId: TurnId | null;
}

const rejected = (reason: string, detail: string) =>
  Effect.fail(new PairRoomRejectedError({ reason, detail }));

const personaName = (persona: PairPersona) => PAIR_PERSONAS[persona].displayName;

/** The request id and the user's answer (null once dismissed) in a `user-input.resolved` payload. */
const readRoomAnswer = (
  payload: unknown,
): { readonly requestId: string; readonly answer: string | null } | null => {
  if (typeof payload !== "object" || payload === null) return null;
  const record = payload as Record<string, unknown>;
  if (typeof record.requestId !== "string") return null;
  const answers = record.answers;
  const answer =
    typeof answers === "object" && answers !== null
      ? (answers as Record<string, unknown>)["answer"]
      : undefined;
  return { requestId: record.requestId, answer: typeof answer === "string" ? answer.trim() : null };
};

const modelSelectionFor = (persona: PairPersona) => ({
  instanceId: PAIR_PERSONAS[persona].instanceId,
  model: PAIR_PERSONAS[persona].model,
});

/** How far back each participant thread is re-copied after a restart. */
const PAIR_TRANSCRIPT_RECONCILE_MESSAGES = 6;

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
  targetBranch: assignment.targetBranch,
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
/** What a consult card calls the exchange: the user's own message, the Lead's consult, a reply in it, or the sign-off. */
const consultNoun = (consult: PairConsult) =>
  consult.answerTo === "lead-turn"
    ? "your message"
    : consult.answerTo === "sign-off"
      ? "the sign-off"
      : consult.continues !== null
        ? "the reply"
        : "a consult";

const emptyHandleResult = {
  handle: null,
  answer: null,
  question: null,
  exchange: null,
  exchangeLimit: null,
  leadProposal: null,
  error: null,
  reason: null,
  retryAfterSeconds: null,
  assignment: null,
  decision: null,
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

  const projectCwd = (projectId: ProjectId) =>
    Effect.gen(function* () {
      const project = yield* snapshots
        .getProjectShellById(projectId)
        .pipe(Effect.mapError(internal("read project")));
      const cwd = Option.getOrUndefined(project)?.workspaceRoot;
      if (!cwd) return yield* rejected("not-found", "The Lead's project no longer exists.");
      return cwd;
    });

  /** Where a participant works: its own worktree, or the project the thread belongs to. */
  const threadCwd = (shell: OrchestrationThreadShell) =>
    shell.worktreePath ? Effect.succeed(shell.worktreePath) : projectCwd(shell.projectId);

  /** Every room change goes through here, so the questions in the Lead's thread follow the room. */
  const apply = (command: Parameters<typeof store.dispatch>[0]) =>
    Effect.gen(function* () {
      const before = Option.getOrNull(yield* store.get(command.roomId));
      const after = yield* store
        .dispatch(command)
        .pipe(
          Effect.catchTag("PairRoomPersistenceError", (error) =>
            Effect.fail(internal("store")(error)),
          ),
        );
      yield* syncQuestions(before, after);
      return after;
    });

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
      const cwd = room.checkout?.path ?? (yield* threadCwd(shell.value));
      const branch = room.checkout ? room.checkout.branch : shell.value.branch;
      const activeTurnId =
        shell.value.session?.activeTurnId ??
        (shell.value.latestTurn?.state === "running" ? shell.value.latestTurn.turnId : null);
      return {
        threadId: lead.threadId,
        persona: lead.persona,
        shell: shell.value,
        cwd,
        branch,
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

  // ── Questions in the Lead's thread ──────────────────────────────────

  /**
   * The room asks the user where they are looking: an async question in the
   * Lead's thread, which every client renders, mobile included. The answer
   * comes back as a `user-input.resolved` activity carrying the room's request
   * id, and the answer turn the server starts carries the user's words to the
   * Lead, so the room never starts a turn of its own for it.
   */
  const askUser = (
    room: PairRoom,
    input: {
      readonly requestId: string;
      readonly header: string;
      readonly question: string;
      readonly options: ReadonlyArray<{ readonly label: string; readonly description: string }>;
    },
  ) =>
    Effect.gen(function* () {
      const lead = pairRoomParticipant(room, "lead");
      if (!lead?.threadId) return;
      const createdAt = yield* nowIso;
      yield* orchestrate({
        type: "thread.activity.append",
        commandId: CommandId.make(`pair:${room.roomId}:ask:${input.requestId}`),
        threadId: lead.threadId,
        activity: {
          id: EventId.make(`pair-question:${input.requestId}`),
          tone: "info",
          kind: "user-input.requested",
          summary: "User input requested",
          payload: {
            requestId: input.requestId,
            responseMode: "message",
            questions: [
              {
                id: "answer",
                header: input.header,
                question: input.question,
                options: input.options,
                allowCustomAnswer: true,
                multiSelect: false,
              },
            ],
          },
          turnId: null,
          createdAt,
        },
        createdAt,
      });
    }).pipe(
      Effect.catchCause((cause) => Effect.logWarning("pair room question failed", { cause })),
    );

  /** Closes a question the room asked; one already answered or dismissed is refused, which is fine. */
  const withdrawQuestion = (room: PairRoom, requestId: string) =>
    Effect.gen(function* () {
      const lead = pairRoomParticipant(room, "lead");
      if (!lead?.threadId) return;
      const createdAt = yield* nowIso;
      yield* orchestrate({
        type: "thread.user-input.dismiss",
        commandId: CommandId.make(`pair:${room.roomId}:withdraw:${requestId}:${yield* uuid}`),
        threadId: lead.threadId,
        requestId: ApprovalRequestId.make(requestId),
        createdAt,
      });
    }).pipe(Effect.catchCause(() => Effect.void));

  // One question per approval: the commit pins it, so a re-approval asks again.
  const mergeQuestionId = (room: PairRoom, assignment: PairAssignment) =>
    `pair:${room.roomId}:merge:${assignment.assignmentId}:${assignment.approvedCommit ?? "none"}`;
  // One question per set of positions: a new position withdraws the old question and asks again.
  const decisionQuestionId = (room: PairRoom, decision: PairDecision) =>
    `pair:${room.roomId}:decision:${decision.decisionId}:${decision.positions.length}`;
  const positionLabel = (persona: PairPersona) => `${personaName(persona)}'s position`;

  const mergeQuestion = (room: PairRoom, assignment: PairAssignment) => {
    const lead = pairRoomParticipant(room, "lead");
    const leadName = lead ? personaName(lead.persona) : "the Lead";
    const target = assignment.targetBranch ?? "your checkout";
    return {
      requestId: mergeQuestionId(room, assignment),
      header: "Merge",
      question: `${leadName} approved ${personaName(assignment.owner)}'s assignment "${assignment.title}". Merge it into ${target}?`,
      options: [
        {
          label: "Merge",
          description: `The room merges the approved commit into ${target} and closes the card.`,
        },
        {
          label: "Send back",
          description: `Keep it open; ${leadName} asks ${personaName(assignment.owner)} for changes.`,
        },
      ],
    };
  };

  const decisionQuestion = (room: PairRoom, decision: PairDecision) => {
    const lead = pairRoomParticipant(room, "lead");
    const parts = [`${decision.title} (${decision.category}: your call).`];
    if (decision.leadRecommendation && lead) {
      parts.push(
        `${personaName(lead.persona)} recommends: ${clampPairText(decision.leadRecommendation, 300)}`,
      );
    }
    if (decision.consequenceOfDeferring) {
      parts.push(`If deferred: ${clampPairText(decision.consequenceOfDeferring, 200)}`);
    }
    return {
      requestId: decisionQuestionId(room, decision),
      header: "Decision",
      question: parts.join(" "),
      options: decision.positions.map((position) => ({
        label: positionLabel(position.persona),
        description: clampPairText(position.summary, 300),
      })),
    };
  };

  const userOwnedOpen = (decision: PairDecision) =>
    decision.resolution === null && !pairDecisionLeadMayResolve(decision.category);

  /** Asks for what newly needs the user and withdraws what no longer does, from one room change to the next. */
  const syncQuestions = (before: PairRoom | null, after: PairRoom) =>
    Effect.gen(function* () {
      const wasClosed = before?.status === "closed";
      const closing = after.status === "closed";
      for (const assignment of after.assignments) {
        const prev = before?.assignments.find(
          (entry) => entry.assignmentId === assignment.assignmentId,
        );
        const wasOpen = prev?.state === "awaiting-user" && !wasClosed;
        const isOpen = assignment.state === "awaiting-user" && !closing;
        if (isOpen && !wasOpen) yield* askUser(after, mergeQuestion(after, assignment));
        if (wasOpen && !isOpen) yield* withdrawQuestion(after, mergeQuestionId(after, prev!));
      }
      for (const decision of after.decisions) {
        const prev = before?.decisions.find((entry) => entry.decisionId === decision.decisionId);
        const wasOpen = prev !== undefined && userOwnedOpen(prev) && !wasClosed;
        const isOpen = userOwnedOpen(decision) && !closing;
        if (isOpen && (!wasOpen || prev!.positions.length !== decision.positions.length)) {
          if (wasOpen) yield* withdrawQuestion(after, decisionQuestionId(after, prev!));
          yield* askUser(after, decisionQuestion(after, decision));
        } else if (wasOpen && !isOpen) {
          yield* withdrawQuestion(after, decisionQuestionId(after, prev!));
        }
      }
    });

  /** The user answered, or dismissed, a question the room put in the Lead's thread. */
  const answerReceived = (room: PairRoom, requestId: string, answer: string | null) =>
    Effect.gen(function* () {
      const match = /^pair:[^:]+:(merge|decision):([^:]+):/.exec(requestId);
      if (!match || answer === null) return;
      const kind = match[1];
      const id = match[2]!;
      if (kind === "merge") {
        // "Send back" and anything typed reach the Lead through the answer turn; only a merge is the room's to do.
        if (answer !== "Merge") return;
        const assignment = yield* requireApproved(room, id);
        const lead = yield* leadContext(room);
        // The answer turn is only on its way to the Lead, so the merge lands before it can edit.
        yield* integrateAssignment(room, assignment, lead, { by: "answer" });
        return;
      }
      const decision = room.decisions.find((entry) => entry.decisionId === id);
      if (!decision || decision.resolution !== null) return;
      const chosen = decision.positions.find(
        (position) => positionLabel(position.persona) === answer,
      );
      const resolution = chosen ? `${positionLabel(chosen.persona)}: ${chosen.summary}` : answer;
      yield* apply({
        type: "decision.resolve",
        roomId: room.roomId,
        decisionId: decision.decisionId,
        resolution: clampPairText(resolution),
        resolvedBy: "user",
        at: yield* nowIso,
      });
      // The answer turn the server started carries the user's words to the Lead; the room starts no second one.
      yield* apply({
        type: "decision.resolution-delivered",
        roomId: room.roomId,
        decisionId: decision.decisionId,
        at: yield* nowIso,
      });
    }).pipe(
      Effect.catchTag("PairRoomRejectedError", (error) =>
        Effect.logInfo("pair room answer not applied", {
          requestId,
          reason: error.reason,
          detail: error.detail,
        }),
      ),
    );

  const leadTurnNow = (room: PairRoom) =>
    leadContext(room).pipe(
      Effect.map((lead) => lead.activeTurnId),
      Effect.orElseSucceed(() => null),
    );

  /**
   * Turns the room started that their thread does not show yet: the shell
   * moves only once the provider picks a turn up, so until then it still
   * reads as idle at the turn that ended before. Keyed by thread, holding the
   * turn the thread was at when the room started one.
   */
  const startedTurns = new Map<ThreadId, TurnId | null>();

  /** Whether a thread can take a turn from the room: nothing running, and nothing the room started still on its way. */
  const isIdle = (threadId: ThreadId, shell: OrchestrationThreadShell) => {
    if (isRunningTurn(shell)) return false;
    const before = shell.latestTurn?.turnId ?? null;
    if (startedTurns.has(threadId) && startedTurns.get(threadId) === before) return false;
    startedTurns.delete(threadId);
    return true;
  };

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
      const shell = Option.getOrUndefined(yield* threadShell(input.threadId));
      startedTurns.set(input.threadId, shell?.latestTurn?.turnId ?? null);
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

  // ── Transcript ──────────────────────────────────────────────────────

  /**
   * Strictly increasing timestamps for the lines the room appends: threads
   * order messages by createdAt first, and several lines can land within one
   * millisecond. Turns the room starts after appending use it too, so they
   * sort after the catch-up they follow.
   */
  let lastAppendedMs = 0;
  const nextIso = DateTime.now.pipe(
    Effect.map((now) => {
      lastAppendedMs = Math.max(DateTime.toEpochMillis(now), lastAppendedMs + 1);
      return DateTime.formatIso(DateTime.makeUnsafe(lastAppendedMs));
    }),
  );

  const otherParticipant = (room: PairRoom, participant: PairParticipant) =>
    room.participants.find((entry) => entry.persona !== participant.persona);

  const readMessages = (threadId: ThreadId) =>
    snapshots.getThreadDetailById(threadId).pipe(
      Effect.map((detail) => Option.getOrUndefined(detail)?.messages ?? []),
      Effect.mapError(internal("read transcript")),
    );

  /**
   * Lines the other participant already received another way: a user message
   * the room relayed as the Peer's prompt, and a Peer reply in a conversation,
   * which the consult delivers itself (tool call, Lead turn, or a transcript
   * line at settle for a sign-off).
   */
  const deliveredInBand = (
    room: PairRoom,
    source: PairParticipant,
    message: OrchestrationMessage,
  ) =>
    message.role === "user"
      ? source.role === "lead" &&
        room.consults.some(
          (consult) =>
            consult.answerTo === "lead-turn" && consult.requestedAt === message.createdAt,
        )
      : source.role === "peer" &&
        room.consults.some((consult) => consult.peerTurnId === message.turnId);

  /**
   * Appends one transcript line to `target`'s thread without starting a turn.
   * The ids derive from the line, so a replay after a restart is a no-op, and
   * a line that fails to land never fails the turn that produced it.
   */
  const appendLine = (input: {
    readonly room: PairRoom;
    readonly from: PairParticipant;
    readonly target: PairParticipant;
    readonly key: string;
    readonly text: string;
    readonly source: PairRoomNote["source"];
  }) =>
    Effect.gen(function* () {
      if (!input.target.threadId) return;
      const key = pairTranscriptKey(input.target.threadId, input.key);
      yield* orchestrate({
        type: "thread.message.user.append",
        commandId: CommandId.make(`pair:transcript:${key}`),
        threadId: input.target.threadId,
        message: {
          messageId: MessageId.make(`pair-transcript-${key}`),
          text: input.text,
          attachments: [],
          context: pairRoomNoteContext(`pair-room-note-transcript-${key}`, {
            purpose: "transcript",
            from: input.from.persona,
            to: input.target.persona,
            ...(input.source ? { source: input.source } : {}),
          }),
        },
        createdAt: yield* nextIso,
      });
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("pair transcript line failed", { key: input.key, cause }),
      ),
    );

  const copyMessage = (
    room: PairRoom,
    from: PairParticipant,
    target: PairParticipant,
    line: { readonly speaker: "user" | "agent"; readonly message: OrchestrationMessage },
  ) =>
    appendLine({
      room,
      from,
      target,
      key: line.message.id,
      text: pairTranscriptLine({
        speaker: line.speaker,
        persona: from.persona,
        role: from.role,
        text: line.message.text,
        attachments: line.message.attachments,
      }),
      source: {
        speaker: line.speaker,
        threadId: from.threadId!,
        messageId: line.message.id,
        createdAt: line.message.createdAt,
      },
    });

  /** Copies the last `limit` lines of `from`'s thread that the other participant has not seen. */
  const copyRecent = (
    room: PairRoom,
    from: PairParticipant,
    target: PairParticipant,
    limit: number,
  ) =>
    Effect.gen(function* () {
      if (!from.threadId || !target.threadId) return;
      const sources = pairTranscriptSources(yield* readMessages(from.threadId)).filter(
        (line) => !deliveredInBand(room, from, line.message),
      );
      for (const line of sources.slice(-limit)) {
        yield* copyMessage(room, from, target, line);
      }
    }).pipe(
      Effect.catchCause((cause) => Effect.logWarning("pair transcript catch-up failed", { cause })),
    );

  /** A message the user typed in a participant's thread goes to the other participant, unless it was relayed. */
  const copyUserMessage = (room: PairRoom, threadId: ThreadId, messageId: MessageId) =>
    Effect.gen(function* () {
      const from = room.participants.find((entry) => entry.threadId === threadId);
      const target = from && otherParticipant(room, from);
      if (!from || !target?.threadId) return;
      const message = (yield* readMessages(threadId)).find((entry) => entry.id === messageId);
      if (
        message?.role !== "user" ||
        readPairRoomNote(message.context) !== null ||
        deliveredInBand(room, from, message)
      ) {
        return;
      }
      yield* copyMessage(room, from, target, { speaker: "user", message });
    }).pipe(
      Effect.catchCause((cause) => Effect.logWarning("pair transcript copy failed", { cause })),
    );

  /** Each thread's latest turn is copied once when it ends; command ids catch the rest. */
  const copiedTurns = new Map<ThreadId, TurnId>();

  /** A participant's final answer, or the fact that its turn stopped short, goes to the other participant. */
  const copyTurnEnd = (room: PairRoom, threadId: ThreadId) =>
    Effect.gen(function* () {
      const from = room.participants.find((entry) => entry.threadId === threadId);
      const target = from && otherParticipant(room, from);
      if (!from || !target?.threadId) return;
      const shell = Option.getOrUndefined(yield* threadShell(threadId));
      const turn = shell?.latestTurn;
      if (!shell || !turn || isRunningTurn(shell) || copiedTurns.get(threadId) === turn.turnId) {
        return;
      }
      copiedTurns.set(threadId, turn.turnId);
      const inBand = room.consults.some((consult) => consult.peerTurnId === turn.turnId);
      const answer = pairFinalAnswer(yield* readMessages(threadId), turn.turnId);
      if (answer) {
        if (deliveredInBand(room, from, answer)) return;
        yield* copyMessage(room, from, target, { speaker: "agent", message: answer });
      } else if (turn.state !== "completed" && !(from.role === "peer" && inBand)) {
        yield* appendLine({
          room,
          from,
          target,
          key: `stopped-${turn.turnId}`,
          text: pairTranscriptStoppedLine(from.persona),
          source: undefined,
        });
      }
    }).pipe(
      Effect.catchCause((cause) => Effect.logWarning("pair transcript copy failed", { cause })),
    );

  const copyTurnFiles = (
    room: PairRoom,
    event: Extract<OrchestrationEvent, { type: "thread.turn-diff-completed" }>,
  ) =>
    Effect.gen(function* () {
      const from = room.participants.find((entry) => entry.threadId === event.payload.threadId);
      const target = from && otherParticipant(room, from);
      if (!from || !target?.threadId || event.payload.files.length === 0) return;
      // A consult's edits are discarded with the snapshot, so they are not worth a line.
      if (room.consults.some((consult) => consult.peerTurnId === event.payload.turnId)) return;
      yield* appendLine({
        room,
        from,
        target,
        key: `files-${event.payload.turnId}`,
        text: pairTranscriptFilesLine(
          from.persona,
          from.role,
          event.payload.files.map((file) => file.path),
        ),
        source: undefined,
      });
    });

  const readThread: PairCoordinator["Service"]["readThread"] = (threadId, input) =>
    toolEdge(
      Effect.gen(function* () {
        const caller = yield* resolveCaller(threadId);
        const persona = input.persona ?? otherPairPersona(caller.persona);
        const target = caller.room.participants.find((entry) => entry.persona === persona);
        const empty = (detail: string): PairReadThreadResult => ({
          persona,
          detail,
          lines: [],
          hasMore: false,
        });
        if (!target?.threadId) {
          return empty(`${personaName(persona)} has no thread in this room yet.`);
        }
        const sources = pairTranscriptSources(yield* readMessages(target.threadId));
        const before = input.beforeMessageId
          ? sources.findIndex((line) => line.message.id === input.beforeMessageId)
          : -1;
        const end = before === -1 ? sources.length : before;
        const start = Math.max(0, end - (input.limit ?? PAIR_READ_THREAD_DEFAULT_LIMIT));
        const lines = sources.slice(start, end).map((line) => ({
          messageId: line.message.id,
          at: line.message.createdAt,
          speaker: line.speaker,
          text: clampPairText(line.message.text, PAIR_TRANSCRIPT_LINE_MAX_LENGTH),
        }));
        return {
          persona,
          detail:
            lines.length === 0
              ? `Nothing ${before === -1 ? "said" : "earlier"} in ${personaName(persona)}'s thread.`
              : `${lines.length} line${lines.length === 1 ? "" : "s"} from ${personaName(persona)}'s thread, newest last.`,
          lines,
          hasMore: start > 0,
        } satisfies PairReadThreadResult;
      }),
      (error) => Effect.fail(new PairToolUnavailableError({ detail: error.detail })),
    );

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
  const readTurnAnswer = (threadId: ThreadId, turnId: TurnId) =>
    Effect.gen(function* () {
      const detail = yield* snapshots
        .getThreadDetailById(threadId)
        .pipe(Effect.mapError(internal("read answer")));
      return pairFinalAnswer(Option.getOrUndefined(detail)?.messages ?? [], turnId);
    });

  const readAnswerMessage = (room: PairRoom, consult: PairConsult) =>
    Effect.gen(function* () {
      const peerThreadId = pairRoomParticipant(room, "peer")?.threadId;
      if (!peerThreadId || !consult.peerTurnId) return null;
      return yield* readTurnAnswer(peerThreadId, consult.peerTurnId);
    });

  const readAnswer = (room: PairRoom, consult: PairConsult) =>
    readAnswerMessage(room, consult).pipe(Effect.map((message) => message?.text ?? null));

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
      const peerParticipant = pairRoomParticipant(room, "peer")!;
      const peer = peerParticipant.persona;
      const settledAt = yield* nowIso;
      const answerMessage =
        outcome.status === "answered"
          ? yield* readAnswerMessage(room, { ...consult, peerTurnId: outcome.peerTurnId })
          : null;
      const answer = answerMessage?.text ?? null;
      const mirrorOutcome: PairMirrorOutcome =
        outcome.status === "answered"
          ? { status: "answered", answer: answer ?? "Answered." }
          : outcome.status === "cancelled"
            ? { status: "stopped", reason: outcome.error ?? "Cancelled." }
            : { status: "failed", error: outcome.error ?? "The Peer did not answer." };
      const verb =
        outcome.status !== "answered"
          ? outcome.status
          : consult.peerAsk !== null
            ? "has a question about"
            : consult.answerTo === "sign-off"
              ? "checked"
              : "answered";
      // The card updates before the room does, so a Lead reading the answer never sees a stale card.
      yield* mirror(room, {
        kind: "task.completed",
        summary: `${personaName(peer)} ${verb} ${consultNoun(consult)}`,
        payload: pairMirrorCompletedPayload(consultCard(consult, peer), mirrorOutcome),
        turnId: consult.leadTurnId,
      });
      const settled = yield* apply({
        type: "consult.settle",
        roomId: room.roomId,
        consultId: consult.consultId,
        status: outcome.status,
        peerTurnId: outcome.peerTurnId,
        error: outcome.error,
        at: settledAt,
      });
      // A sign-off that asks nothing starts no Lead turn; the Lead reads it at its next one.
      const lead = pairRoomParticipant(settled, "lead");
      if (consult.answerTo === "sign-off" && consult.peerAsk === null && answerMessage && lead) {
        yield* copyMessage(settled, peerParticipant, lead, {
          speaker: "agent",
          message: answerMessage,
        });
      }
      return settled;
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
      // A fresh Peer starts from where the Lead's conversation already is.
      yield* copyRecent(
        attached,
        pairRoomParticipant(attached, "lead")!,
        pairRoomParticipant(attached, "peer")!,
        PAIR_TRANSCRIPT_BOOTSTRAP_MESSAGES,
      );
      return { room: attached, peerThreadId, snapshotCommit: review.snapshotCommit };
    });

  /** What a Peer turn in a conversation carries: the opening ask, the Lead's reply, or the Lead's answer to check. */
  type ConsultBody =
    | {
        readonly kind: "ask";
        readonly question: string;
        readonly focusPaths: ReadonlyArray<string>;
      }
    | { readonly kind: "reply"; readonly message: string }
    | {
        readonly kind: "sign-off";
        readonly answer: OrchestrationMessage;
        readonly topics: ReadonlyArray<string>;
      };

  /** Sends a recorded consult to the Peer. Any failure settles the consult as failed. */
  const launchConsult = (
    room: PairRoom,
    consult: PairConsult,
    lead: LeadContext,
    body: ConsultBody,
  ) =>
    Effect.gen(function* () {
      const peerPersona = pairRoomParticipant(room, "peer")!.persona;
      const peer = yield* ensurePeer(room, lead);
      const exchange = pairExchangeIndex(room, consult.consultId);
      const common = { lead: lead.persona, peer: peerPersona, snapshotCommit: peer.snapshotCommit };
      const turn = (() => {
        switch (body.kind) {
          case "ask":
            return {
              summary: `${personaName(peerPersona)} is answering ${consultNoun(consult)}`,
              request: body.question,
              text: consultPrompt({
                ...common,
                kind: consult.kind,
                source:
                  consult.answerTo === "lead-turn" ? "user" : consult.automatic ? "review" : "lead",
                round: consult.round,
                roundLimit: consult.automatic ? null : pairRoundLimit(room, consult.leadTurnId),
                question: body.question,
                focusPaths: body.focusPaths,
              }),
              note: {
                purpose: consult.answerTo === "lead-turn" ? "user-relay" : "consult",
                from: lead.persona,
                to: peerPersona,
              } satisfies PairRoomNote,
            };
          case "reply":
            return {
              summary: `${personaName(peerPersona)} is reading ${personaName(lead.persona)}'s reply`,
              request: body.message,
              text: replyPrompt({ ...common, kind: consult.kind, exchange, message: body.message }),
              note: {
                purpose: "reply",
                from: lead.persona,
                to: peerPersona,
              } satisfies PairRoomNote,
            };
          case "sign-off":
            return {
              summary: `${personaName(peerPersona)} is checking ${personaName(lead.persona)}'s answer`,
              request: body.answer.text,
              text: signOffPrompt({
                ...common,
                exchange,
                topics: body.topics,
                answer: clampPairText(body.answer.text, PAIR_PEER_ANSWER_MAX_LENGTH),
              }),
              // The note names the answer it carries, so the catch-up does not repeat it.
              note: {
                purpose: "sign-off",
                from: lead.persona,
                to: peerPersona,
                source: {
                  speaker: "agent",
                  threadId: lead.threadId,
                  messageId: body.answer.id,
                  createdAt: body.answer.createdAt,
                },
              } satisfies PairRoomNote,
            };
        }
      })();
      yield* mirror(peer.room, {
        kind: "task.started",
        summary: turn.summary,
        payload: pairMirrorStartedPayload(consultCard(consult, peerPersona), turn.request),
        turnId: consult.leadTurnId,
      });
      yield* startTurn({
        commandKey: `pair:${room.roomId}:consult:${consult.consultId}`,
        threadId: peer.peerThreadId,
        persona: peerPersona,
        runtimeMode: lead.shell.runtimeMode,
        text: turn.text,
        note: turn.note,
        createdAt: yield* nextIso,
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
        exchange: pairExchangeIndex(room, consult.consultId),
        exchangeLimit: PAIR_CONVERSATION_MAX_EXCHANGES,
        leadProposal: leadProposals.get(consult.consultId) ?? null,
      };
      switch (consult.status) {
        case "running":
          return { ...base, status: "pending", retryAfterSeconds: 0 } satisfies PairHandleResult;
        case "answered":
          return {
            ...base,
            status: consult.peerAsk !== null ? "question" : "answered",
            question: consult.peerAsk,
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
    handle: null,
    retryAfterSeconds: null,
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
        const checkout = yield* roomCheckout(room);
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
          checkout,
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
            exchange: pairExchangeIndex(room, consult.consultId),
            peerAsk: consult.peerAsk,
            error: consult.error,
          })),
          assignments: room.assignments.map(pairAssignmentView),
          // Open calls, plus the last few answers so a settled call can be
          // re-read instead of asked again.
          decisions: room.decisions
            .filter(
              (decision, index) =>
                decision.resolution === null ||
                index >= room.decisions.length - PAIR_STATUS_SETTLED_DECISIONS,
            )
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
          kind: "ask",
          question: input.question,
          focusPaths: input.focusPaths ?? [],
        });
        return yield* waitHandle(recorded, consultId, input.waitSeconds);
      }),
      rejectedHandle,
    );

  /**
   * The Lead's next word in a conversation: an answer to the Peer's question
   * or an argument, sent as the Peer's next turn. Or the answer a blocked
   * assignee is waiting for.
   */
  const reply: PairCoordinator["Service"]["reply"] = (threadId, input) =>
    toolEdge<PairHandleResult>(
      Effect.gen(function* () {
        const caller = yield* requireRole(yield* resolveCaller(threadId), ["lead"]);
        const previous = caller.room.consults.find((entry) => entry.consultId === input.handle);
        if (previous) {
          if (previous.status === "running") {
            return yield* rejected(
              "peer-busy",
              "The Peer has not replied to that consult yet. Wait for it with pair_wait.",
            );
          }
          const lead = yield* leadContext(caller.room);
          const room = yield* expireStaleConsult(caller.room);
          if (pairConsultAnswerOwed(previous)) {
            // Answering it is reading it, so no turn needs to bring it.
            yield* apply({
              type: "consult.answer-delivered",
              roomId: room.roomId,
              consultId: previous.consultId,
              at: yield* nowIso,
            });
          }
          const consultId = `consult-${yield* uuid}`;
          const recorded = yield* apply({
            type: "consult.request",
            roomId: room.roomId,
            consultId,
            kind: previous.kind,
            leadTurnId: lead.activeTurnId,
            automatic: false,
            continues: previous.consultId,
            title: previous.title,
            at: yield* nowIso,
          });
          const recordedConsult = recorded.consults.find((entry) => entry.consultId === consultId)!;
          yield* launchConsult(recorded, recordedConsult, lead, {
            kind: "reply",
            message: input.message,
          });
          return yield* waitHandle(recorded, consultId, input.waitSeconds);
        }
        const assignment = caller.room.assignments.find(
          (entry) => entry.assignmentId === input.handle,
        );
        if (assignment) {
          if (assignment.state !== "blocked") {
            return yield* rejected(
              "invalid",
              `Assignment "${assignment.title}" is ${assignment.state}, not blocked. Review a submitted one with pair_review.`,
            );
          }
          const shell = yield* threadShell(assignment.threadId);
          if (Option.isNone(shell)) {
            return yield* rejected("not-found", "The assignment's thread no longer exists.");
          }
          const next = yield* updateAssignment(caller.room, assignment, {
            by: "agent",
            state: "running",
            note: `${personaName(caller.persona)} answered: ${clampPairText(input.message, 200)}`,
          });
          yield* startTurn({
            commandKey: `pair:${caller.room.roomId}:reply:${assignment.assignmentId}:${yield* uuid}`,
            threadId: assignment.threadId,
            persona: assignment.owner,
            runtimeMode: shell.value.runtimeMode,
            text: assigneeReplyPrompt({
              lead: caller.persona,
              assignment: next.assignment,
              message: input.message,
            }),
            note: { purpose: "reply", from: caller.persona, to: assignment.owner },
            createdAt: yield* nowIso,
          });
          yield* mirror(next.room, {
            kind: "task.progress",
            summary: "Assignment continuing",
            payload: pairMirrorProgressPayload(assignmentCard(next.assignment), {
              status: "running",
              summary: next.assignment.note ?? "Continuing.",
            }),
            turnId: yield* leadTurnNow(next.room),
          });
          return {
            ...emptyHandleResult,
            status: "updated",
            handle: assignment.assignmentId,
            assignment: pairAssignmentView(next.assignment),
          } satisfies PairHandleResult;
        }
        return yield* rejected(
          "not-found",
          `No consult or blocked assignment has the handle ${input.handle}.`,
        );
      }),
      rejectedHandle,
    );

  /** The Peer, mid-consult, or an assignee, needs the Lead's answer before it can finish. */
  const ask: PairCoordinator["Service"]["ask"] = (threadId, input) =>
    toolEdge<PairAckResult>(
      Effect.gen(function* () {
        const caller = yield* resolveCaller(threadId);
        const leadName = personaName(otherPairPersona(caller.persona));
        switch (caller.role) {
          case "peer": {
            const running = caller.room.consults.find((entry) => entry.status === "running");
            if (!running) {
              return yield* rejected(
                "invalid",
                `You are not in a conversation with ${leadName} right now. Put the question in your reply; ${leadName} sees it at its next turn.`,
              );
            }
            yield* apply({
              type: "consult.ask",
              roomId: caller.room.roomId,
              consultId: running.consultId,
              question: input.question,
              at: yield* nowIso,
            });
            return {
              status: "recorded",
              reason: null,
              detail: `Recorded. Finish your reply and stop; ${leadName}'s answer starts your next turn.`,
              assignment: null,
              decision: null,
              handle: running.consultId,
              retryAfterSeconds: null,
            } satisfies PairAckResult;
          }
          case "assignee":
            return yield* blockAssignment(caller, `Needs: ${input.question}`);
          case "lead":
            return yield* rejected(
              "not-allowed",
              "The Lead answers questions here; ask the user directly, or the Peer with pair_consult.",
            );
        }
      }),
      ackRejected,
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
        if (
          (result.status === "answered" || result.status === "question") &&
          pairConsultAnswerOwed(consultNow)
        ) {
          // The Lead read the reply here, so it needs no turn to bring it.
          yield* apply({
            type: "consult.answer-delivered",
            roomId: room.roomId,
            consultId: handle,
            at: yield* nowIso,
          });
        }
        return result;
      }
      if (callerRoom.decisions.some((entry) => entry.decisionId === handle)) {
        const decision = yield* waitForDecision(callerRoom, handle, waitSeconds);
        return {
          ...emptyHandleResult,
          status: decision.resolution === null ? "pending" : "answered",
          handle,
          answer: decision.resolution,
          retryAfterSeconds: decision.resolution === null ? 0 : null,
          decision: pairDecisionView(decision),
        } satisfies PairHandleResult;
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
      return yield* rejected(
        "not-found",
        `No consult, assignment or decision has the handle ${handle}.`,
      );
    });

  const wait: PairCoordinator["Service"]["wait"] = (threadId, input) =>
    toolEdge<PairHandleResult>(
      Effect.gen(function* () {
        const caller = yield* resolveCaller(threadId);
        // Decisions are room-wide, so whoever recorded one may wait on it.
        // Consults and assignments stay the Lead's to read.
        if (!caller.room.decisions.some((entry) => entry.decisionId === input.handle)) {
          yield* requireRole(caller, ["lead"]);
        }
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
        const base = yield* fromWorkspace(
          workspace.resolveBase({ cwd: lead.cwd, ref: input.baseRef }),
        );
        const baseCommit = base.commit;
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
          targetBranch: base.branch,
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

  /** Marks an assignment blocked on the Lead; the blocker reaches the Lead as a turn once it is idle. */
  const blockAssignment = (caller: Extract<Caller, { role: "assignee" }>, note: string) =>
    Effect.gen(function* () {
      const next = yield* updateAssignment(caller.room, caller.assignment, {
        by: "agent",
        state: "blocked",
        note,
      });
      yield* mirror(next.room, {
        kind: "task.progress",
        summary: "Assignment blocked",
        payload: pairMirrorProgressPayload(assignmentCard(next.assignment), {
          status: "waiting",
          summary: note,
        }),
        turnId: yield* leadTurnNow(next.room),
      });
      yield* deliverBlocked(next.room);
      return {
        status: "recorded",
        reason: null,
        detail: `Recorded. ${personaName(otherPairPersona(caller.persona))} gets your question; stop here, and its answer starts your next turn.`,
        assignment: pairAssignmentView(next.assignment),
        decision: null,
        handle: caller.assignment.assignmentId,
        retryAfterSeconds: null,
      } satisfies PairAckResult;
    });

  /** Points the room at a worktree of the project's repository; the Lead thread's own directory anchors the check. */
  const recordCheckout = (room: PairRoom, path: string, by: "lead" | "user") =>
    Effect.gen(function* () {
      const lead = pairRoomParticipant(room, "lead");
      const shell = lead?.threadId
        ? Option.getOrUndefined(yield* threadShell(lead.threadId))
        : undefined;
      const projectPath = shell ? yield* threadCwd(shell) : yield* projectCwd(room.projectId);
      const info = yield* fromWorkspace(
        workspace.describeCheckout({ cwd: path, projectCwd: projectPath }),
      );
      yield* apply({
        type: "room.checkout",
        roomId: room.roomId,
        path: info.path,
        branch: info.branch,
        by,
        at: yield* nowIso,
      });
      return info;
    });

  const checkout: PairCoordinator["Service"]["checkout"] = (threadId, input) =>
    toolEdge<PairCheckoutResult>(
      Effect.gen(function* () {
        const caller = yield* requireRole(yield* resolveCaller(threadId), ["lead"]);
        const info = yield* recordCheckout(caller.room, input.path, "lead");
        return {
          status: "recorded",
          path: info.path,
          branch: info.branch,
          reason: null,
          detail: `The room follows ${info.path}${info.branch ? ` on ${info.branch}` : " (detached HEAD)"} now: the Peer's snapshots, new assignments and merges use it.`,
        } satisfies PairCheckoutResult;
      }),
      (error) => ({
        status: "rejected",
        path: null,
        branch: null,
        reason: error.reason,
        detail: error.detail,
      }),
    );

  const integrate: PairCoordinator["Service"]["integrate"] = (threadId, input) =>
    toolEdge<PairIntegrateResult>(
      Effect.gen(function* () {
        const caller = yield* requireRole(yield* resolveCaller(threadId), ["lead"]);
        const assignment = yield* requireApproved(caller.room, input.assignmentId);
        const lead = yield* leadContext(caller.room);
        if (lead.activeTurnId && (yield* turnStartedByRoom(lead.threadId, lead.activeTurnId))) {
          return yield* rejected(
            "not-asked",
            "The room started this turn, so the user has not asked for a merge in it. Say the assignment is ready and stop; merge only in the turn where they ask.",
          );
        }
        const merged = yield* integrateAssignment(caller.room, assignment, lead, {
          by: "lead",
          userWords: input.userWords,
        });
        return {
          status: "merged",
          assignmentId: assignment.assignmentId,
          commit: merged.commit,
          target: merged.target,
          reason: null,
          detail: `Merged "${assignment.title}" into ${merged.target} as ${merged.commit.slice(0, 12)}. Tell the user.`,
        } satisfies PairIntegrateResult;
      }),
      (error) => ({
        status: "rejected",
        assignmentId: input.assignmentId,
        commit: null,
        target: null,
        reason: error.reason,
        detail: error.detail,
      }),
    );

  const reportProgress: PairCoordinator["Service"]["reportProgress"] = (threadId, input) =>
    toolEdge<PairAckResult>(
      Effect.gen(function* () {
        const caller = yield* requireRole(yield* resolveCaller(threadId), ["assignee"]);
        if (input.blocked) {
          return yield* blockAssignment(
            caller,
            input.question ? `${input.note}\nNeeds: ${input.question}` : input.note,
          );
        }
        const next = yield* updateAssignment(caller.room, caller.assignment, {
          by: "agent",
          state: "running",
          note: input.note,
        });
        yield* mirror(next.room, {
          kind: "task.progress",
          summary: "Assignment progress",
          payload: pairMirrorProgressPayload(assignmentCard(next.assignment), {
            status: "running",
            summary: input.note,
          }),
          turnId: yield* leadTurnNow(next.room),
        });
        return {
          status: "recorded",
          reason: null,
          detail: "Progress recorded.",
          assignment: pairAssignmentView(next.assignment),
          decision: null,
          handle: null,
          retryAfterSeconds: null,
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
          handle: null,
          retryAfterSeconds: null,
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
                  summary: `Approved by ${personaName(caller.persona)}. Waiting for you to merge ${assignment.branch} into ${assignment.targetBranch ?? "your checkout"}.`,
                }),
                turnId,
              });
            }
            return {
              status: "recorded",
              reason: null,
              detail:
                approved.assignment.state === "awaiting-user"
                  ? `Approved. It merges into ${assignment.targetBranch ?? "the room's checkout"} when the user says so: from the room controls, or by telling you, in which case you call pair_integrate. Say it is ready.`
                  : "Approved and completed.",
              assignment: pairAssignmentView(approved.assignment),
              decision: null,
              handle: null,
              retryAfterSeconds: null,
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
              handle: null,
              retryAfterSeconds: null,
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
              handle: null,
              retryAfterSeconds: null,
            } satisfies PairAckResult;
          }
        }
      }),
      ackRejected,
    );

  // ── Decisions ───────────────────────────────────────────────────────

  const decisionSettled = (decisionId: string) => (room: PairRoom) =>
    room.decisions.find((entry) => entry.decisionId === decisionId)?.resolution != null;

  /**
   * Waits for the user to settle `decisionId`, and marks their answer read
   * once it is returned, so no turn repeats it.
   */
  const waitForDecision = (room: PairRoom, decisionId: string, waitSeconds: number | undefined) =>
    Effect.gen(function* () {
      const settled = yield* waitForRoom(room.roomId, decisionSettled(decisionId), waitSeconds);
      const current = Option.getOrElse(settled, () => room);
      const decision = current.decisions.find((entry) => entry.decisionId === decisionId);
      if (!decision) {
        return yield* rejected("not-found", `Decision ${decisionId} is too old to read back.`);
      }
      if (pairDecisionAnswerOwed(decision)) {
        yield* apply({
          type: "decision.resolution-delivered",
          roomId: current.roomId,
          decisionId,
          at: yield* nowIso,
        });
      }
      return decision;
    });

  const decisionAck = (decision: PairDecision) =>
    Effect.succeed(
      decision.resolution === null
        ? ({
            status: "pending",
            reason: null,
            detail:
              "This call belongs to the user, who has not answered yet. Tell them what you recommend and why, then keep waiting with pair_wait and this handle. If you stop waiting, their answer reaches you in a later turn.",
            assignment: null,
            decision: pairDecisionView(decision),
            handle: decision.decisionId,
            retryAfterSeconds: 0,
          } satisfies PairAckResult)
        : ({
            status: "settled",
            reason: null,
            detail: `The user decided: ${decision.resolution}`,
            assignment: null,
            decision: pairDecisionView(decision),
            handle: decision.decisionId,
            retryAfterSeconds: null,
          } satisfies PairAckResult),
    );

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
        const recorded = room.decisions.find((entry) => entry.decisionId === decisionId)!;
        if (pairDecisionView(recorded).waitingOn !== "user") {
          const view = pairDecisionView(recorded);
          return {
            status: "recorded",
            reason: null,
            detail: view.resolution
              ? `Recorded and resolved by the ${recorded.resolvedBy}.`
              : "Recorded.",
            assignment: null,
            decision: view,
            handle: null,
            retryAfterSeconds: null,
          } satisfies PairAckResult;
        }
        // The call is the user's, so the tool waits on them the way a consult
        // waits on the Peer: the answer comes back inside this turn if they are
        // quick, and through pair_wait or a later turn if they are not.
        return yield* decisionAck(yield* waitForDecision(room, decisionId, input.waitSeconds));
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

  const requireApproved = (room: PairRoom, assignmentId: string) =>
    Effect.gen(function* () {
      const assignment = yield* requireAssignment(room, assignmentId);
      if (assignment.state !== "awaiting-user") {
        return yield* rejected(
          "invalid",
          assignment.state === "integrated"
            ? "This assignment is already merged."
            : `Only an approved assignment can be merged; this one is ${assignment.state}.`,
        );
      }
      return assignment;
    });

  /** Whether a turn opened with a message the room sent, rather than one the user wrote. */
  const turnStartedByRoom = (threadId: ThreadId, turnId: TurnId) =>
    readMessages(threadId).pipe(
      Effect.map((messages) => {
        const opener = messages.find(
          (message) => message.role === "user" && message.turnId === turnId,
        );
        return opener !== undefined && readPairRoomNote(opener.context) !== null;
      }),
    );

  const integrating = yield* Semaphore.make(1);

  /**
   * Merges an approved assignment into the worktree that has its base branch
   * checked out, or records a merge that already happened there. Shared by
   * the user's Merge control, the Lead's pair_integrate on the user's word
   * and the room's own question; each caller settles first whether the Lead
   * may be mid-turn, and the lock settles who gets there first.
   */
  const integrateAssignment = (
    room: PairRoom,
    assignment: PairAssignment,
    lead: LeadContext,
    actor:
      | { readonly by: "user" }
      | { readonly by: "lead"; readonly userWords: string }
      | { readonly by: "answer" },
  ) =>
    integrating.withPermits(1)(
      Effect.gen(function* () {
        // Re-read under the lock: two of the callers can race for the same assignment.
        const current = yield* requireRoom(room.roomId);
        const live = yield* requireApproved(current, assignment.assignmentId);
        const assignmentShell = yield* threadShell(live.threadId);
        if (Option.isSome(assignmentShell) && isRunningTurn(assignmentShell.value)) {
          return yield* rejected("conflict", "The assignment thread is still running.");
        }
        // Every path acts on the user's word, so the room records them as the user's.
        const sendBackForReview = (detail: string) =>
          Effect.gen(function* () {
            yield* updateAssignment(current, live, {
              by: "user",
              state: "submitted",
              note: `${detail} Nothing was merged; the Lead needs to review it again.`,
            });
            return yield* rejected(
              "conflict",
              `${detail} Nothing was merged. ${
                actor.by === "lead"
                  ? "Review it again with pair_review."
                  : `Ask ${personaName(lead.persona)} to review it again.`
              }`,
            );
          });
        if (live.approvedCommit === null) {
          return yield* sendBackForReview("This assignment was approved without a pinned commit.");
        }
        const approvedCommit = live.approvedCommit;
        // Older assignments carry no target; they merge into the room's checkout.
        const target = live.targetBranch ?? lead.branch;
        const targetName = target ?? "the checkout";
        const card = assignmentCard(live);
        const settleMerged = (commit: string, how: string) =>
          Effect.gen(function* () {
            const merged = yield* updateAssignment(current, live, {
              by: "user",
              state: "integrated",
              integrationCommit: commit,
              note: `${how} as ${commit.slice(0, 12)}.`,
            });
            yield* mirror(merged.room, {
              kind: "task.progress",
              summary: "Assignment merged",
              payload: pairMirrorProgressPayload(card, {
                status: "waiting",
                summary: `${how} as ${commit.slice(0, 12)}.`,
              }),
              turnId: null,
            });
            return { commit, target: targetName };
          });
        // Merged by hand, or by the Lead on the user's word: record it rather than merge twice.
        const already = yield* fromWorkspace(
          workspace.containsCommit({
            cwd: lead.cwd,
            ref: target ? `refs/heads/${target}` : "HEAD",
            commit: approvedCommit,
          }),
        );
        if (already) {
          return yield* settleMerged(already, `Already merged into ${targetName} outside the room`);
        }
        const targetCwd =
          target === null || target === lead.branch
            ? lead.cwd
            : yield* fromWorkspace(workspace.findBranchWorktree({ cwd: lead.cwd, branch: target }));
        if (targetCwd === null) {
          return yield* rejected(
            "conflict",
            `${target} is not checked out in any worktree, so there is nowhere to merge into. Check it out, or point the room at it with pair_checkout, then merge again.`,
          );
        }
        const result = yield* fromWorkspace(
          workspace.integrate({
            targetCwd,
            worktreePath: live.worktreePath,
            branch: live.branch,
            commit: approvedCommit,
            message: `Pair assignment: ${live.title}`,
          }),
        );
        if (result.status === "changed") {
          return yield* sendBackForReview(result.detail);
        }
        if (result.status === "dirty" || result.status === "conflict") {
          yield* updateAssignment(current, live, {
            by: "user",
            note: `Nothing was merged. ${result.detail}`,
          });
          return yield* rejected(
            "conflict",
            result.status === "dirty"
              ? result.detail
              : `Merging ${live.branch} into ${targetName} hit conflicts, so nothing was merged. ${result.detail}`,
          );
        }
        const how =
          actor.by === "lead"
            ? `Merged into ${targetName} by ${personaName(lead.persona)} on the user's word ("${actor.userWords}")`
            : `Merged into ${targetName}`;
        return yield* settleMerged(result.commit, how);
      }),
    );

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
          // Without a repository the Peer has nowhere to work, which would only
          // surface as a failed consult in the middle of the Lead's first turn.
          // A room created with the first message has no Lead thread yet, so
          // this falls back to the project the room is being created in.
          const cwd = Option.isSome(shell)
            ? yield* threadCwd(shell.value)
            : yield* projectCwd(command.projectId);
          yield* fromWorkspace(workspace.assertRepository({ cwd })).pipe(
            Effect.catch((error: PairRoomRejectedError) =>
              rejected(
                "workspace",
                `${error.detail} Open the repository itself as the project, or run git init there, and start the room again.`,
              ),
            ),
          );
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
        case "room.checkout": {
          const room = yield* requireRoom(command.roomId);
          yield* recordCheckout(room, command.path, "user");
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
          const assignment = yield* requireApproved(room, command.assignmentId);
          const lead = yield* leadContext(room);
          if (lead.activeTurnId || isRunningTurn(lead.shell)) {
            return yield* rejected(
              "conflict",
              `Wait for ${personaName(lead.persona)} to finish its turn before merging.`,
            );
          }
          yield* integrateAssignment(room, assignment, lead, { by: "user" });
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
          yield* deliverDecision(room);
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
                branch: lead.branch,
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
          ? ((yield* readTurnAnswer(threadId, turnId))?.text ?? null)
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
      // An answer to the room's own question is for the room and the Lead; the Peer gets it as a transcript line.
      if (event.payload.messageId.startsWith("async-answer:pair:")) return;
      const detail = yield* snapshots
        .getThreadDetailById(lead.threadId)
        .pipe(Effect.mapError(internal("read user message")));
      const message = Option.getOrUndefined(detail)?.messages.find(
        (entry) => entry.id === event.payload.messageId,
      );
      // Turns the room starts itself (answers, handoffs) carry a note and are never relayed.
      if (message?.role !== "user" || readPairRoomNote(message.context) !== null) return;
      const mentioned = pairMessageMentions(message.text, { persona: peer.persona, role: "peer" });
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
            error: `${personaName(peer.persona)} was still answering an earlier request, so ${personaName(lead.persona)} answers this one alone. ${personaName(peer.persona)} sees it at its next turn.`,
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
        { kind: "ask", question: message.text, focusPaths: [] },
      );
    });

  /**
   * Starts a Lead turn with a Peer reply no tool call took: the answer to a
   * relayed user message once the Lead's own turn for it has ended, a consult
   * reply that arrived after the Lead's turn, a question the Peer asked, or a
   * sign-off the Peer wants a word about. A reply that can no longer help (the
   * Peer failed, the user stopped the Lead, the room closed or is changing
   * Lead) is marked delivered, and its card is all that shows.
   */
  const deliverPeerAnswer = (room: PairRoom) =>
    Effect.gen(function* () {
      const owed = room.consults.find(pairConsultAnswerOwed);
      if (!owed) return;
      const markDelivered = (at: string) =>
        apply({
          type: "consult.answer-delivered",
          roomId: room.roomId,
          consultId: owed.consultId,
          at,
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
        yield* markDelivered(yield* nowIso);
        return;
      }
      if (!isIdle(lead.threadId, shell)) return;
      const turn = shell.latestTurn;
      if (owed.answerTo === "lead-turn") {
        // The Lead answers the user's message first, then hears the Peer.
        if (!turn || turn.requestedAt < owed.requestedAt) return;
        if (turn.state === "interrupted") {
          yield* markDelivered(yield* nowIso);
          return;
        }
      }
      const answer = yield* readAnswer(room, owed);
      const exchange = pairExchangeIndex(room, owed.consultId);
      const delivery: PeerReplyDelivery =
        owed.peerAsk !== null
          ? { kind: "ask", ask: owed.peerAsk, exchange, signOff: owed.answerTo === "sign-off" }
          : owed.answerTo === "lead-turn"
            ? { kind: owed.kind === "roundtable" ? "roundtable" : "relay" }
            : { kind: "consult", consultKind: owed.kind, exchange };
      // Marked first, so a failed start never brings it twice.
      const at = yield* nowIso;
      yield* markDelivered(at);
      if (!answer) return;
      yield* startTurn({
        commandKey: `pair:${room.roomId}:peer-answer:${owed.consultId}`,
        threadId: lead.threadId,
        persona: lead.persona,
        runtimeMode: shell.runtimeMode,
        text: peerReplyPrompt({
          lead: lead.persona,
          peer: peer.persona,
          delivery,
          handle: owed.consultId,
          answer: clampPairText(answer, PAIR_PEER_ANSWER_MAX_LENGTH),
        }),
        note: { purpose: "peer-answer", from: peer.persona, to: lead.persona },
        createdAt: at,
      });
    });

  /**
   * Closes the loop on a conversation: once the Lead's turn has ended, the
   * Peer gets the Lead's final answer to check against what it raised. A
   * reply counts as taken by the first Lead turn to complete after it was
   * delivered, whether the Lead read it through pair_wait or the room brought
   * it as that turn. Every reply before the latest sign-off was covered by
   * it (a sign-off waits for anything running or owed), so consults are only
   * looked at past that point, and one Lead turn gets one sign-off. A relay,
   * a reply still owed, or a conversation at its exchange limit does not
   * trigger it. The Peer's reply is a card and a transcript line unless it
   * asks for more.
   */
  const signOff = (room: PairRoom) =>
    Effect.gen(function* () {
      if (room.status !== "active" || room.leadSwitch) return;
      if (room.consults.some((c) => c.status === "running" || pairConsultAnswerOwed(c))) return;
      const lead = pairRoomParticipant(room, "lead");
      const peer = pairRoomParticipant(room, "peer");
      if (!lead?.threadId || !peer) return;
      const shell = Option.getOrUndefined(yield* threadShell(lead.threadId));
      const turn = shell?.latestTurn;
      if (!shell || !turn || !isIdle(lead.threadId, shell) || turn.state !== "completed") return;
      const covered = room.consults.findLastIndex((consult) => consult.answerTo === "sign-off");
      const taken = room.consults.filter(
        (consult, index) =>
          index > covered &&
          consult.answerTo === "tool" &&
          consult.status === "answered" &&
          consult.kind !== "roundtable" &&
          consult.answerDeliveredAt !== null &&
          // A reply delivered after this turn ended belongs to the turn it starts.
          (turn.completedAt === null || consult.answerDeliveredAt <= turn.completedAt),
      );
      if (taken.length === 0) return;
      const latest = taken.at(-1)!;
      if (pairConversation(room, latest.consultId).length >= PAIR_CONVERSATION_MAX_EXCHANGES) {
        return;
      }
      const answer = yield* readTurnAnswer(lead.threadId, turn.turnId);
      if (!answer) return;
      const leadNow = yield* leadContext(room);
      const consultId = `consult-${yield* uuid}`;
      const recorded = yield* apply({
        type: "consult.request",
        roomId: room.roomId,
        consultId,
        kind: latest.kind,
        leadTurnId: turn.turnId,
        automatic: true,
        answerTo: "sign-off",
        continues: latest.consultId,
        title: latest.title,
        at: yield* nowIso,
      });
      yield* launchConsult(
        recorded,
        recorded.consults.find((entry) => entry.consultId === consultId)!,
        leadNow,
        {
          kind: "sign-off",
          answer,
          topics: [...new Set(taken.map((consult) => consult.title))],
        },
      );
    }).pipe(Effect.catchCause((cause) => Effect.logWarning("pair sign-off failed", { cause })));

  /** Where the Lead works: a recorded move, else its thread's own directory. */
  const roomCheckout = (room: PairRoom) =>
    Effect.gen(function* () {
      if (room.checkout) {
        return {
          path: room.checkout.path,
          branch: room.checkout.branch,
          setBy: room.checkout.by,
        } as const;
      }
      const lead = pairRoomParticipant(room, "lead");
      const shell = lead?.threadId
        ? Option.getOrUndefined(yield* threadShell(lead.threadId))
        : undefined;
      return {
        path: shell ? yield* threadCwd(shell) : yield* projectCwd(room.projectId),
        branch: shell?.branch ?? null,
        setBy: "thread" as const,
      };
    });

  /** A recorded checkout can change branches under the room; a Lead turn ending is when it is re-read. */
  const refreshCheckout = (room: PairRoom) =>
    Effect.gen(function* () {
      if (!room.checkout) return room;
      const branch = yield* fromWorkspace(workspace.currentBranch({ cwd: room.checkout.path }));
      if (branch === room.checkout.branch) return room;
      return yield* apply({
        type: "room.checkout",
        roomId: room.roomId,
        path: room.checkout.path,
        branch,
        by: room.checkout.by,
        at: yield* nowIso,
      });
    }).pipe(Effect.orElseSucceed(() => room));

  /**
   * An approved assignment merged outside the room, by hand or by the Lead on
   * the user's word, is recorded once its target branch contains the approved
   * commit, so the card stops asking for a merge that already happened.
   */
  const noticeMerges = (room: PairRoom) =>
    Effect.gen(function* () {
      const waiting = room.assignments.filter(
        (assignment) => assignment.state === "awaiting-user" && assignment.approvedCommit !== null,
      );
      if (waiting.length === 0) return;
      const checkout = yield* roomCheckout(room);
      for (const assignment of waiting) {
        const target = assignment.targetBranch ?? checkout.branch;
        const head = yield* fromWorkspace(
          workspace.containsCommit({
            cwd: checkout.path,
            ref: target ? `refs/heads/${target}` : "HEAD",
            commit: assignment.approvedCommit!,
          }),
        );
        if (!head) continue;
        const current = Option.getOrElse(yield* store.get(room.roomId), () => room);
        const live = current.assignments.find(
          (entry) => entry.assignmentId === assignment.assignmentId,
        );
        if (!live || live.state !== "awaiting-user") continue;
        const targetName = target ?? "the checkout";
        const next = yield* updateAssignment(current, live, {
          by: "server",
          state: "integrated",
          integrationCommit: head,
          note: `Merged into ${targetName} outside the room, as ${head.slice(0, 12)}.`,
        });
        yield* mirror(next.room, {
          kind: "task.progress",
          summary: "Assignment merged",
          payload: pairMirrorProgressPayload(assignmentCard(live), {
            status: "waiting",
            summary: `Merged into ${targetName} outside the room.`,
          }),
          turnId: null,
        });
      }
    }).pipe(Effect.catchCause((cause) => Effect.logWarning("pair merge check failed", { cause })));

  /** Brings an assignee's blocker to the Lead as a turn, once, when the Lead is idle. */
  const deliverBlocked = (room: PairRoom) =>
    Effect.gen(function* () {
      if (room.status !== "active" || room.leadSwitch) return;
      const blocked = room.assignments.find(
        (assignment) => assignment.state === "blocked" && assignment.blockedDeliveredAt === null,
      );
      if (!blocked) return;
      const lead = pairRoomParticipant(room, "lead");
      const shell = lead?.threadId
        ? Option.getOrUndefined(yield* threadShell(lead.threadId))
        : undefined;
      if (!lead?.threadId || !shell || !isIdle(lead.threadId, shell)) return;
      const at = yield* nowIso;
      const next = yield* updateAssignment(room, blocked, { by: "server", blockedDeliveredAt: at });
      yield* startTurn({
        commandKey: `pair:${room.roomId}:blocked:${blocked.assignmentId}:${at}`,
        threadId: lead.threadId,
        persona: lead.persona,
        runtimeMode: shell.runtimeMode,
        text: assigneeBlockedPrompt({ assignment: next.assignment }),
        note: { purpose: "blocked", from: blocked.owner, to: lead.persona },
        createdAt: at,
      });
    }).pipe(
      Effect.catchCause((cause) => Effect.logWarning("pair blocked delivery failed", { cause })),
    );

  /**
   * Starts a Lead turn with a decision the user settled, when no tool call
   * took the answer. The Lead owns the reply to the user, so the answer goes
   * there even when the Peer recorded the decision.
   */
  const deliverDecision = (room: PairRoom) =>
    Effect.gen(function* () {
      const owed = room.decisions.find(pairDecisionAnswerOwed);
      if (!owed?.resolution) return;
      const markDelivered = Effect.gen(function* () {
        yield* apply({
          type: "decision.resolution-delivered",
          roomId: room.roomId,
          decisionId: owed.decisionId,
          at: yield* nowIso,
        });
      });
      const lead = pairRoomParticipant(room, "lead");
      const shell = lead?.threadId
        ? Option.getOrUndefined(yield* threadShell(lead.threadId))
        : undefined;
      if (!lead?.threadId || !shell || room.status !== "active" || room.leadSwitch) {
        return yield* markDelivered;
      }
      // A running turn may still be waiting in pair_wait; it settles on its own,
      // and an idle moment later delivers whatever no call picked up.
      if (!isIdle(lead.threadId, shell)) return;
      yield* markDelivered;
      yield* startTurn({
        commandKey: `pair:${room.roomId}:decision:${owed.decisionId}`,
        threadId: lead.threadId,
        persona: lead.persona,
        runtimeMode: shell.runtimeMode,
        text: decisionResolved({
          title: owed.title,
          category: owed.category,
          resolution: owed.resolution,
        }),
        note: { purpose: "decision", from: lead.persona, to: lead.persona },
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
          kind: "ask",
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
      if (event.type === "thread.activity-appended") {
        const activity = event.payload.activity;
        if (activity.kind === "user-input.resolved") {
          const answer = readRoomAnswer(activity.payload);
          if (answer?.requestId.startsWith(`pair:${found.value.roomId}:`)) {
            yield* answerReceived(found.value, answer.requestId, answer.answer);
            return;
          }
        }
        if (WAITING_ACTIVITY_KINDS.has(activity.kind)) {
          yield* mirrorWaitingOnUser(found.value, threadId);
          return;
        }
      }
      if (event.type === "thread.turn-start-requested") {
        yield* relayUserMessage(found.value, event);
        // Relayed messages reach the Peer as its prompt; the rest are copied.
        const room = Option.getOrElse(yield* store.get(found.value.roomId), () => found.value);
        yield* copyUserMessage(room, threadId, event.payload.messageId);
        return;
      }
      yield* settleIfAnswered(found.value, threadId);
      yield* settleHandoffDraft(found.value, threadId);
      if (event.type === "thread.turn-diff-completed") {
        const room = Option.getOrElse(yield* store.get(found.value.roomId), () => found.value);
        yield* reviewGuardrail(room, event);
        yield* scopeCheck(room, threadId);
        yield* copyTurnFiles(room, event);
      }
      if (event.type === "thread.session-set" || event.type === "thread.turn-diff-completed") {
        const latest = () =>
          store.get(found.value.roomId).pipe(Effect.map(Option.getOrElse(() => found.value)));
        const room = yield* latest();
        yield* copyTurnEnd(room, threadId);
        if (
          event.type === "thread.session-set" &&
          pairRoomParticipant(room, "lead")?.threadId === threadId
        ) {
          const shell = Option.getOrUndefined(yield* threadShell(threadId));
          if (shell && !isRunningTurn(shell)) {
            yield* noticeMerges(yield* refreshCheckout(room));
          }
        }
        yield* flagUnsubmittedTurn(room, threadId);
        yield* deliverPeerAnswer(yield* latest());
        yield* deliverDecision(yield* latest());
        yield* deliverBlocked(yield* latest());
        yield* signOff(yield* latest());
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
      yield* deliverDecision(Option.getOrElse(yield* store.get(room.roomId), () => room));
      yield* deliverBlocked(Option.getOrElse(yield* store.get(room.roomId), () => room));
      yield* signOff(Option.getOrElse(yield* store.get(room.roomId), () => room));
      yield* noticeMerges(
        yield* refreshCheckout(Option.getOrElse(yield* store.get(room.roomId), () => room)),
      );
      // Lines that ended up between a turn's end and the restart are copied now; the rest replay as no-ops.
      const settled = Option.getOrElse(yield* store.get(room.roomId), () => room);
      if (settled.status !== "closed") {
        for (const participant of settled.participants) {
          const other = otherParticipant(settled, participant);
          if (other)
            yield* copyRecent(settled, participant, other, PAIR_TRANSCRIPT_RECONCILE_MESSAGES);
        }
      }
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
    reply,
    ask,
    wait,
    assign,
    checkout,
    reportProgress,
    submit,
    review,
    integrate,
    recordDecision,
    readThread,
  });
});

export const layer = Layer.effect(PairCoordinator, make);
