import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  IsoDateTime,
  PositiveInt,
  ProjectId,
  ThreadId,
  TrimmedNonEmptyString,
  TurnId,
} from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";

/**
 * Pair Room: one Lead and one Peer (Fable and Astra) working a single user
 * request. Each participant and each assignment is an ordinary thread; the
 * room record ties them together and carries the coordination state that
 * clients render (roles, consult rounds, assignments, decisions).
 *
 * Free text on the wire is clamped to these limits. Full questions, answers
 * and briefs live in the participant and assignment threads themselves.
 */
export const PAIR_ROOM_TITLE_MAX_LENGTH = 200;
export const PAIR_ROOM_TEXT_MAX_LENGTH = 2000;
export const PAIR_ROOM_LIST_MAX_ITEMS = 20;
export const PAIR_ROOM_MAX_ROUNDS_LIMIT = 6;
/** Settled consults older than this many are dropped from the room record. */
export const PAIR_ROOM_SETTLED_CONSULTS_KEPT = 20;
/** The outgoing Lead's handoff, held on the room only until the user confirms the switch. */
export const PAIR_ROOM_HANDOFF_MAX_LENGTH = 16000;
/** Threads a room remembers from before its Lead switches. */
export const PAIR_ROOM_FORMER_PARTICIPANTS_KEPT = 20;

const PairTitle = TrimmedNonEmptyString.check(Schema.isMaxLength(PAIR_ROOM_TITLE_MAX_LENGTH));
const PairText = TrimmedNonEmptyString.check(Schema.isMaxLength(PAIR_ROOM_TEXT_MAX_LENGTH));

export const PairRoomId = TrimmedNonEmptyString.pipe(Schema.brand("PairRoomId"));
export type PairRoomId = typeof PairRoomId.Type;

export const PairPersona = Schema.Literals(["fable", "astra"]);
export type PairPersona = typeof PairPersona.Type;

export const PairRole = Schema.Literals(["lead", "peer"]);
export type PairRole = typeof PairRole.Type;

export interface PairPersonaProfile {
  readonly persona: PairPersona;
  readonly displayName: "Fable" | "Astra";
  readonly instanceId: ProviderInstanceId;
  readonly model: string;
}

/** Pair Room only runs these two pinned models; it never substitutes another. */
export const PAIR_PERSONAS: Readonly<Record<PairPersona, PairPersonaProfile>> = {
  fable: {
    persona: "fable",
    displayName: "Fable",
    instanceId: ProviderInstanceId.make("claudeAgent"),
    model: "claude-fable-5-1",
  },
  astra: {
    persona: "astra",
    displayName: "Astra",
    instanceId: ProviderInstanceId.make("codex"),
    model: "gpt-6-astra",
  },
};

export const otherPairPersona = (persona: PairPersona): PairPersona =>
  persona === "fable" ? "astra" : "fable";

export const PairRoomMode = Schema.Literals(["adaptive", "pair", "roundtable"]);
export type PairRoomMode = typeof PairRoomMode.Type;

export const PAIR_ROOM_DEFAULT_MAX_ROUNDS: Readonly<Record<PairRoomMode, number>> = {
  adaptive: 2,
  pair: 2,
  roundtable: 3,
};

export const PairRoomStatus = Schema.Literals(["active", "paused", "closed"]);
export type PairRoomStatus = typeof PairRoomStatus.Type;

export const PairParticipant = Schema.Struct({
  persona: PairPersona,
  role: PairRole,
  /** Always set for the Lead. The Peer's thread is created the first time it is needed. */
  threadId: Schema.NullOr(ThreadId),
});
export type PairParticipant = typeof PairParticipant.Type;

export const PairConsultKind = Schema.Literals(["critique", "review", "question", "roundtable"]);
export type PairConsultKind = typeof PairConsultKind.Type;

export const PairConsultStatus = Schema.Literals(["running", "answered", "failed", "cancelled"]);
export type PairConsultStatus = typeof PairConsultStatus.Type;

export const PairConsult = Schema.Struct({
  consultId: TrimmedNonEmptyString,
  kind: PairConsultKind,
  /** The Lead turn the consult belongs to; rounds are counted per Lead turn. */
  leadTurnId: Schema.NullOr(TurnId),
  round: PositiveInt,
  /** Started by the server (pair mode review guardrail) rather than the Lead. */
  automatic: Schema.Boolean,
  status: PairConsultStatus,
  /** The Peer turn that answered; set when the consult settles. */
  peerTurnId: Schema.NullOr(TurnId),
  title: PairTitle,
  error: Schema.NullOr(PairText),
  requestedAt: IsoDateTime,
  settledAt: Schema.NullOr(IsoDateTime),
});
export type PairConsult = typeof PairConsult.Type;

export const PairAssignmentArtifact = Schema.Literals(["findings", "patch", "commit"]);
export type PairAssignmentArtifact = typeof PairAssignmentArtifact.Type;

export const PairAssignmentState = Schema.Literals([
  "running",
  "blocked",
  "submitted",
  "awaiting-user",
  "integrated",
  "completed",
  "rejected",
  "cancelled",
  "failed",
  "interrupted",
]);
export type PairAssignmentState = typeof PairAssignmentState.Type;

export const PAIR_ASSIGNMENT_ACTIVE_STATES: ReadonlySet<PairAssignmentState> = new Set([
  "running",
  "blocked",
  "submitted",
  "awaiting-user",
]);

export const PairCriterionResult = Schema.Struct({
  criterion: PairText,
  met: Schema.Boolean,
  evidence: Schema.NullOr(PairText),
});
export type PairCriterionResult = typeof PairCriterionResult.Type;

export const PairAssignmentReport = Schema.Struct({
  summary: PairText,
  criteriaResults: Schema.Array(PairCriterionResult),
  testsRun: Schema.Array(PairText),
  knownLimitations: Schema.Array(PairText),
});
export type PairAssignmentReport = typeof PairAssignmentReport.Type;

export const PairAssignment = Schema.Struct({
  assignmentId: TrimmedNonEmptyString,
  title: PairTitle,
  owner: PairPersona,
  threadId: ThreadId,
  worktreePath: TrimmedNonEmptyString,
  branch: TrimmedNonEmptyString,
  baseCommit: TrimmedNonEmptyString,
  scopeGlobs: Schema.Array(TrimmedNonEmptyString),
  acceptanceCriteria: Schema.Array(PairText),
  expectedArtifact: PairAssignmentArtifact,
  state: PairAssignmentState,
  /** Latest progress, review or integration note, for the card and the Lead. */
  note: Schema.NullOr(PairText),
  report: Schema.NullOr(PairAssignmentReport),
  changedFiles: Schema.Array(TrimmedNonEmptyString),
  /** Changed files outside `scopeGlobs`. Lead approval is refused while any remain. */
  deviations: Schema.Array(TrimmedNonEmptyString),
  integrationCommit: Schema.NullOr(TrimmedNonEmptyString),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type PairAssignment = typeof PairAssignment.Type;

export const PairDecisionKind = Schema.Literals(["decision", "disagreement"]);
export type PairDecisionKind = typeof PairDecisionKind.Type;

export const PairDecisionCategory = Schema.Literals([
  "routine",
  "architecture",
  "product",
  "security",
  "scope",
  "destructive",
]);
export type PairDecisionCategory = typeof PairDecisionCategory.Type;

/** Only routine and architecture calls may be settled by the Lead; the rest wait for the user. */
export const pairDecisionLeadMayResolve = (category: PairDecisionCategory): boolean =>
  category === "routine" || category === "architecture";

export const PairDecisionPosition = Schema.Struct({
  persona: PairPersona,
  summary: PairText,
  evidence: Schema.NullOr(PairText),
});
export type PairDecisionPosition = typeof PairDecisionPosition.Type;

export const PairDecision = Schema.Struct({
  decisionId: TrimmedNonEmptyString,
  kind: PairDecisionKind,
  category: PairDecisionCategory,
  title: PairTitle,
  positions: Schema.Array(PairDecisionPosition),
  leadRecommendation: Schema.NullOr(PairText),
  consequenceOfDeferring: Schema.NullOr(PairText),
  resolution: Schema.NullOr(PairText),
  resolvedBy: Schema.NullOr(Schema.Literals(["lead", "user"])),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type PairDecision = typeof PairDecision.Type;

/**
 * A Lead switch in progress. The outgoing Lead drafts a handoff, the user
 * reads it, and only their confirmation swaps the roles.
 */
export const PairLeadSwitch = Schema.Struct({
  toPersona: PairPersona,
  phase: Schema.Literals(["drafting", "ready", "failed"]),
  requestedAt: IsoDateTime,
  handoff: Schema.NullOr(
    TrimmedNonEmptyString.check(Schema.isMaxLength(PAIR_ROOM_HANDOFF_MAX_LENGTH)),
  ),
  error: Schema.NullOr(PairText),
});
export type PairLeadSwitch = typeof PairLeadSwitch.Type;

/** A participant thread from before a Lead switch; kept as read-only history. */
export const PairFormerParticipant = Schema.Struct({
  persona: PairPersona,
  role: PairRole,
  threadId: ThreadId,
  until: IsoDateTime,
});
export type PairFormerParticipant = typeof PairFormerParticipant.Type;

export const PairRoom = Schema.Struct({
  roomId: PairRoomId,
  projectId: ProjectId,
  mode: PairRoomMode,
  maxRoundsPerTurn: PositiveInt.check(Schema.isLessThanOrEqualTo(PAIR_ROOM_MAX_ROUNDS_LIMIT)),
  status: PairRoomStatus,
  statusReason: Schema.NullOr(PairText),
  participants: Schema.Array(PairParticipant),
  /** Room-owned detached worktree the Peer reviews Lead snapshots in. */
  reviewWorktreePath: Schema.NullOr(TrimmedNonEmptyString),
  /** Extra consult rounds the user granted for one Lead turn. */
  extraRounds: Schema.NullOr(Schema.Struct({ leadTurnId: TurnId, count: PositiveInt })),
  consults: Schema.Array(PairConsult),
  assignments: Schema.Array(PairAssignment),
  decisions: Schema.Array(PairDecision),
  leadSwitch: Schema.NullOr(PairLeadSwitch).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
  formerParticipants: Schema.Array(PairFormerParticipant).pipe(
    Schema.withDecodingDefault(Effect.succeed([])),
  ),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type PairRoom = typeof PairRoom.Type;

export const pairRoomParticipant = (
  room: Pick<PairRoom, "participants">,
  role: PairRole,
): PairParticipant | undefined =>
  room.participants.find((participant) => participant.role === role);

/** Commands only the user can issue. Agents never reach these; they use the `pair_*` MCP tools. */
export const PairRoomUserCommand = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("room.create"),
    projectId: ProjectId,
    /** The Lead's thread. The client creates it with its first turn right after this call. */
    leadThreadId: ThreadId,
    leadPersona: PairPersona,
    mode: PairRoomMode,
    maxRoundsPerTurn: Schema.optional(
      PositiveInt.check(Schema.isLessThanOrEqualTo(PAIR_ROOM_MAX_ROUNDS_LIMIT)),
    ),
  }),
  Schema.Struct({
    type: Schema.Literal("room.update"),
    roomId: PairRoomId,
    mode: Schema.optional(PairRoomMode),
    maxRoundsPerTurn: Schema.optional(
      PositiveInt.check(Schema.isLessThanOrEqualTo(PAIR_ROOM_MAX_ROUNDS_LIMIT)),
    ),
    status: Schema.optional(PairRoomStatus),
  }),
  Schema.Struct({
    type: Schema.Literal("room.grant-rounds"),
    roomId: PairRoomId,
    count: PositiveInt.check(Schema.isLessThanOrEqualTo(PAIR_ROOM_MAX_ROUNDS_LIMIT)),
  }),
  Schema.Struct({
    type: Schema.Literal("consult.cancel"),
    roomId: PairRoomId,
    consultId: TrimmedNonEmptyString,
  }),
  Schema.Struct({
    type: Schema.Literal("assignment.integrate"),
    roomId: PairRoomId,
    assignmentId: TrimmedNonEmptyString,
  }),
  Schema.Struct({
    type: Schema.Literal("assignment.cancel"),
    roomId: PairRoomId,
    assignmentId: TrimmedNonEmptyString,
  }),
  Schema.Struct({
    type: Schema.Literal("assignment.resume"),
    roomId: PairRoomId,
    assignmentId: TrimmedNonEmptyString,
  }),
  Schema.Struct({
    type: Schema.Literal("assignment.set-scope"),
    roomId: PairRoomId,
    assignmentId: TrimmedNonEmptyString,
    scopeGlobs: Schema.Array(TrimmedNonEmptyString).check(Schema.isMinLength(1)),
  }),
  Schema.Struct({
    /** Asks the Lead for a handoff; nothing changes hands until `lead.switch-confirm`. */
    type: Schema.Literal("lead.switch-start"),
    roomId: PairRoomId,
  }),
  Schema.Struct({
    type: Schema.Literal("lead.switch-confirm"),
    roomId: PairRoomId,
  }),
  Schema.Struct({
    type: Schema.Literal("lead.switch-cancel"),
    roomId: PairRoomId,
  }),
  Schema.Struct({
    type: Schema.Literal("decision.resolve"),
    roomId: PairRoomId,
    decisionId: TrimmedNonEmptyString,
    resolution: PairText,
  }),
]);
export type PairRoomUserCommand = typeof PairRoomUserCommand.Type;

export const PairRoomDispatchResult = Schema.Struct({
  roomId: PairRoomId,
  /** The thread the user should look at next, such as the new Lead's after a switch. */
  threadId: Schema.optionalKey(ThreadId),
});
export type PairRoomDispatchResult = typeof PairRoomDispatchResult.Type;

export class PairRoomCommandError extends Schema.TaggedError<PairRoomCommandError>()(
  "PairRoomCommandError",
  {
    reason: Schema.Literals(["not-found", "invalid", "conflict", "unavailable", "failed"]),
    detail: Schema.String,
  },
) {
  override get message(): string {
    return this.detail;
  }
}

export const PairRoomsSubscribeInput = Schema.Struct({});
export type PairRoomsSubscribeInput = typeof PairRoomsSubscribeInput.Type;

/** Every room on the environment. Sent first, then after every change. */
export const PairRoomListEvent = Schema.Array(PairRoom);
export type PairRoomListEvent = typeof PairRoomListEvent.Type;
