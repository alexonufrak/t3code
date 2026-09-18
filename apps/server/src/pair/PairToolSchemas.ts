import {
  PAIR_ROOM_TEXT_MAX_LENGTH,
  PairAssignmentArtifact,
  PairAssignmentState,
  PairConsultKind,
  PairConsultStatus,
  PairDecisionCategory,
  PairDecisionKind,
  PairPersona,
  PairRoomMode,
  PairRoomStatus,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

/**
 * Inputs and results of the `pair_*` MCP tools. Results are flat structs with
 * nullable fields: agents read them as JSON, and a rejection is a normal
 * result (`status: "rejected"` with a reason) rather than a tool error, so the
 * agent can adjust instead of retrying blindly.
 */

/** Both CLIs cut a single MCP call off near 60s, so waits stay well under it. */
export const PAIR_MAX_WAIT_SECONDS = 45;
export const PAIR_DEFAULT_WAIT_SECONDS = 30;

const Text = TrimmedNonEmptyString.check(Schema.isMaxLength(PAIR_ROOM_TEXT_MAX_LENGTH));
const LongText = TrimmedNonEmptyString.check(Schema.isMaxLength(20_000));
const ShortList = <S extends Schema.Top>(item: S) =>
  Schema.Array(item).check(Schema.isMaxLength(20));

const WaitSeconds = Schema.Int.check(
  Schema.isBetween({ minimum: 0, maximum: PAIR_MAX_WAIT_SECONDS }),
).annotate({
  description: `Seconds to wait before returning status "pending" with a handle (0-${PAIR_MAX_WAIT_SECONDS}, default ${PAIR_DEFAULT_WAIT_SECONDS}).`,
});

export const PairRole = Schema.Literals(["lead", "peer", "assignee"]);
export type PairCallerRole = typeof PairRole.Type;

export const PairAssignmentView = Schema.Struct({
  assignmentId: Schema.String,
  title: Schema.String,
  owner: PairPersona,
  state: PairAssignmentState,
  branch: Schema.String,
  worktreePath: Schema.String,
  baseCommit: Schema.String,
  scopeGlobs: Schema.Array(Schema.String),
  acceptanceCriteria: Schema.Array(Schema.String),
  expectedArtifact: PairAssignmentArtifact,
  note: Schema.NullOr(Schema.String),
  changedFiles: Schema.Array(Schema.String),
  deviations: Schema.Array(Schema.String),
  report: Schema.NullOr(
    Schema.Struct({
      summary: Schema.String,
      criteriaResults: Schema.Array(
        Schema.Struct({
          criterion: Schema.String,
          met: Schema.Boolean,
          evidence: Schema.NullOr(Schema.String),
        }),
      ),
      testsRun: Schema.Array(Schema.String),
      knownLimitations: Schema.Array(Schema.String),
    }),
  ),
  integrationCommit: Schema.NullOr(Schema.String),
});
export type PairAssignmentView = typeof PairAssignmentView.Type;

export const PairDecisionView = Schema.Struct({
  decisionId: Schema.String,
  kind: PairDecisionKind,
  category: PairDecisionCategory,
  title: Schema.String,
  positions: Schema.Array(
    Schema.Struct({
      persona: PairPersona,
      summary: Schema.String,
      evidence: Schema.NullOr(Schema.String),
    }),
  ),
  resolution: Schema.NullOr(Schema.String),
  resolvedBy: Schema.NullOr(Schema.Literals(["lead", "user"])),
  /** Who has to act next: the Lead may settle routine and architecture calls, the rest need the user. */
  waitingOn: Schema.NullOr(Schema.Literals(["lead", "user"])),
});
export type PairDecisionView = typeof PairDecisionView.Type;

// ── pair_status ─────────────────────────────────────────────────────────

export const PairStatusResult = Schema.Struct({
  roomId: Schema.String,
  mode: PairRoomMode,
  status: PairRoomStatus,
  statusReason: Schema.NullOr(Schema.String),
  you: Schema.Struct({
    persona: PairPersona,
    name: Schema.String,
    model: Schema.String,
    role: PairRole,
  }),
  other: Schema.Struct({
    persona: PairPersona,
    name: Schema.String,
    model: Schema.String,
    busy: Schema.Boolean,
  }),
  guidance: Schema.String,
  rounds: Schema.NullOr(Schema.Struct({ used: Schema.Int, limit: Schema.Int })),
  consults: Schema.Array(
    Schema.Struct({
      consultId: Schema.String,
      kind: PairConsultKind,
      status: PairConsultStatus,
      title: Schema.String,
      automatic: Schema.Boolean,
      /** Which exchange of its conversation this is; replies and sign-offs continue one. */
      exchange: Schema.Int,
      /** What the Peer asked the Lead to answer, if it did. */
      peerAsk: Schema.NullOr(Schema.String),
      error: Schema.NullOr(Schema.String),
    }),
  ),
  assignments: Schema.Array(PairAssignmentView),
  decisions: Schema.Array(PairDecisionView),
  /** Set when the caller is working an assignment. */
  assignment: Schema.NullOr(PairAssignmentView),
  /** Set while the user switches the Lead; new consults and assignments wait until it ends. */
  leadSwitch: Schema.NullOr(
    Schema.Struct({
      to: Schema.String,
      phase: Schema.Literals(["drafting", "ready", "failed"]),
    }),
  ),
});
export type PairStatusResult = typeof PairStatusResult.Type;

// ── pair_consult / pair_wait ────────────────────────────────────────────

export const PairConsultInput = Schema.Struct({
  kind: Schema.optional(PairConsultKind).annotate({
    description:
      "critique (default): challenge your approach. review: check your changes. question: ask for facts. roundtable: independent proposals; requires leadProposal.",
  }),
  question: LongText.annotate({
    description:
      "What you want from the Peer, with the context it needs. It sees a snapshot of your checkout but not your conversation.",
  }),
  focusPaths: Schema.optional(ShortList(TrimmedNonEmptyString)),
  leadProposal: Schema.optional(LongText).annotate({
    description:
      "Roundtable only: your own proposal, recorded before the Peer answers and never shown to it.",
  }),
  waitSeconds: Schema.optional(WaitSeconds),
});

export const PairWaitInput = Schema.Struct({
  handle: TrimmedNonEmptyString.annotate({
    description:
      "A consultId from pair_consult, an assignmentId from pair_assign, or a decisionId from pair_record_decision.",
  }),
  waitSeconds: Schema.optional(WaitSeconds),
});

export const PairHandleResult = Schema.Struct({
  /** "question": the Peer replied but needs your answer to continue; pair_reply with the handle. */
  status: Schema.Literals([
    "answered",
    "question",
    "failed",
    "cancelled",
    "pending",
    "rejected",
    "updated",
  ]),
  handle: Schema.NullOr(Schema.String),
  answer: Schema.NullOr(Schema.String),
  question: Schema.NullOr(Schema.String),
  /** Which exchange of the conversation this reply is, and how many the room allows. */
  exchange: Schema.NullOr(Schema.Int),
  exchangeLimit: Schema.NullOr(Schema.Int),
  leadProposal: Schema.NullOr(Schema.String),
  error: Schema.NullOr(Schema.String),
  reason: Schema.NullOr(Schema.String),
  retryAfterSeconds: Schema.NullOr(Schema.Int),
  assignment: Schema.NullOr(PairAssignmentView),
  decision: Schema.NullOr(PairDecisionView),
});
export type PairHandleResult = typeof PairHandleResult.Type;

export const PairReplyInput = Schema.Struct({
  handle: TrimmedNonEmptyString.annotate({
    description:
      "The consultId whose reply you are answering, or the assignmentId of a blocked assignment.",
  }),
  message: LongText.annotate({
    description:
      "Your answer or argument. The Peer sees a refreshed snapshot of your checkout, so refer to files freely.",
  }),
  waitSeconds: Schema.optional(WaitSeconds),
});

export const PairAskInput = Schema.Struct({
  question: Text.annotate({
    description: "What you need the Lead to answer before you can finish. One question at a time.",
  }),
});

// ── assignments ─────────────────────────────────────────────────────────

export const PairAssignInput = Schema.Struct({
  title: TrimmedNonEmptyString.check(Schema.isMaxLength(200)),
  brief: LongText.annotate({
    description:
      "Objective, context, constraints and pointers. The assignee starts fresh in its own worktree and cannot see your conversation.",
  }),
  scopeGlobs: ShortList(TrimmedNonEmptyString).check(Schema.isMinLength(1)).annotate({
    description:
      "Repository-relative globs the assignee may change, e.g. apps/server/src/auth/**. Supports *, ** and ? only (no braces or brackets); list alternatives as separate globs. Must not overlap another active assignment.",
  }),
  acceptanceCriteria: ShortList(Text),
  expectedArtifact: Schema.optional(PairAssignmentArtifact).annotate({
    description:
      "patch (default): changes left in the worktree for the user to merge. commit: committed on the branch. findings: no file changes.",
  }),
  baseRef: Schema.optional(TrimmedNonEmptyString).annotate({
    description: "Commit or branch to start from. Defaults to your checkout's HEAD.",
  }),
});

export const PairAssignResult = Schema.Struct({
  status: Schema.Literals(["assigned", "rejected"]),
  assignmentId: Schema.NullOr(Schema.String),
  threadId: Schema.NullOr(Schema.String),
  branch: Schema.NullOr(Schema.String),
  reason: Schema.NullOr(Schema.String),
  detail: Schema.String,
});
export type PairAssignResult = typeof PairAssignResult.Type;

export const PairReportProgressInput = Schema.Struct({
  note: Text,
  blocked: Schema.optional(Schema.Boolean),
  question: Schema.optional(Text).annotate({
    description: "When blocked, what you need from the Lead or the user.",
  }),
});

export const PairSubmitInput = Schema.Struct({
  summary: Text,
  criteriaResults: ShortList(
    Schema.Struct({
      criterion: Text,
      met: Schema.Boolean,
      evidence: Schema.optional(Text),
    }),
  ),
  testsRun: ShortList(Text),
  knownLimitations: ShortList(Text),
});

export const PairReviewInput = Schema.Struct({
  assignmentId: TrimmedNonEmptyString,
  verdict: Schema.Literals(["approve", "request-changes", "reject"]),
  notes: Text,
});

export const PairRecordDecisionInput = Schema.Struct({
  decisionId: Schema.optional(TrimmedNonEmptyString).annotate({
    description: "Add your position to an existing decision instead of recording a new one.",
  }),
  kind: Schema.optional(PairDecisionKind),
  category: Schema.optional(PairDecisionCategory).annotate({
    description:
      "routine and architecture calls may be settled by the Lead. product, security, scope and destructive calls always wait for the user.",
  }),
  title: Schema.optional(TrimmedNonEmptyString.check(Schema.isMaxLength(200))),
  position: Text,
  evidence: Schema.optional(Text),
  leadRecommendation: Schema.optional(Text),
  consequenceOfDeferring: Schema.optional(Text),
  resolution: Schema.optional(Text).annotate({
    description: "Lead only, for routine and architecture calls: how it was settled.",
  }),
  waitSeconds: Schema.optional(WaitSeconds),
});

export const PAIR_READ_THREAD_DEFAULT_LIMIT = 20;

export const PairReadThreadInput = Schema.Struct({
  persona: Schema.optional(PairPersona).annotate({
    description: "Whose thread to read. Defaults to the other participant.",
  }),
  beforeMessageId: Schema.optional(TrimmedNonEmptyString).annotate({
    description: "Page back: return lines before this messageId from an earlier result.",
  }),
  limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 50 }))).annotate({
    description: `Lines to return, newest last (1-50, default ${PAIR_READ_THREAD_DEFAULT_LIMIT}).`,
  }),
});

export const PairReadThreadResult = Schema.Struct({
  persona: PairPersona,
  detail: Schema.String,
  lines: Schema.Array(
    Schema.Struct({
      messageId: Schema.String,
      at: Schema.String,
      speaker: Schema.Literals(["user", "agent"]),
      text: Schema.String,
    }),
  ),
  hasMore: Schema.Boolean,
});
export type PairReadThreadResult = typeof PairReadThreadResult.Type;

export const PairAckResult = Schema.Struct({
  /**
   * "settled" is a decision the user answered while you waited; "pending" is
   * one they have not, and its handle goes to pair_wait.
   */
  status: Schema.Literals(["recorded", "settled", "pending", "rejected"]),
  reason: Schema.NullOr(Schema.String),
  detail: Schema.String,
  assignment: Schema.NullOr(PairAssignmentView),
  decision: Schema.NullOr(PairDecisionView),
  handle: Schema.NullOr(Schema.String),
  retryAfterSeconds: Schema.NullOr(Schema.Int),
});
export type PairAckResult = typeof PairAckResult.Type;
