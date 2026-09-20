import { McpCapabilityUnavailableError } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";

import { PairCoordinator, PairToolUnavailableError } from "../../../pair/PairCoordinator.ts";
import {
  PAIR_MAX_WAIT_SECONDS,
  PairAckResult,
  PairAskInput,
  PairAssignInput,
  PairAssignResult,
  PairCheckoutInput,
  PairCheckoutResult,
  PairConsultInput,
  PairHandleResult,
  PairReadThreadInput,
  PairReadThreadResult,
  PairRecordDecisionInput,
  PairReplyInput,
  PairReportProgressInput,
  PairReviewInput,
  PairStatusResult,
  PairSubmitInput,
  PairWaitInput,
} from "../../../pair/PairToolSchemas.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const dependencies = [McpInvocationContext.McpInvocationContext, PairCoordinator];

export const PairToolError = Schema.Union([
  McpCapabilityUnavailableError,
  PairToolUnavailableError,
]);
export type PairToolError = typeof PairToolError.Type;

const PairStatusTool = Tool.make("pair_status", {
  description:
    "Pair Room: your role (Lead, Peer or assignee), the other participant, the room mode and guidance, consult rounds used, assignments and open decisions. Call it first in a pair room and whenever you resume.",
  success: PairStatusResult,
  failure: PairToolError,
  dependencies,
})
  .annotate(Tool.Title, "Pair room status")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const PairReplyTool = Tool.make("pair_reply", {
  description: `Pair Room, Lead: continue a conversation with the Peer, answering its question or arguing a point, or answer a blocked assignee. Pass the consultId (or assignmentId) as the handle. For a consult it waits up to waitSeconds (max ${PAIR_MAX_WAIT_SECONDS}) like pair_consult and returns the Peer's next reply, "question" if the Peer needs more from you, or "pending" with the handle for pair_wait.`,
  parameters: PairReplyInput,
  success: PairHandleResult,
  failure: PairToolError,
  dependencies,
})
  .annotate(Tool.Title, "Reply to the Peer")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, false);

const PairAskTool = Tool.make("pair_ask", {
  description:
    "Pair Room, Peer or assignee: ask the Lead something you need answered before you can finish, instead of guessing. Call it, then finish your reply and stop; the Lead's answer starts your next turn.",
  parameters: PairAskInput,
  success: PairAckResult,
  failure: PairToolError,
  dependencies,
})
  .annotate(Tool.Title, "Ask the Lead")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, false);

const PairConsultTool = Tool.make("pair_consult", {
  description: `Pair Room, Lead only: open a conversation with the Peer for a critique, review, answer or independent roundtable proposal. The Peer works on a snapshot of your checkout, including uncommitted files. Waits up to waitSeconds (max ${PAIR_MAX_WAIT_SECONDS}); if the Peer is still working the result is status "pending" and you continue with pair_wait. "question" means the Peer needs your answer first: give it with pair_reply. Continue the conversation with pair_reply as long as it earns its keep. A "rejected" result explains why (round limit, Peer busy, room paused).`,
  parameters: PairConsultInput,
  success: PairHandleResult,
  failure: PairToolError,
  dependencies,
})
  .annotate(Tool.Title, "Consult the Peer")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, false);

const PairWaitTool = Tool.make("pair_wait", {
  description: `Pair Room: wait up to waitSeconds (max ${PAIR_MAX_WAIT_SECONDS}) for a handle. Consult and assignment handles are the Lead's; a decision handle belongs to whoever recorded it. Consults return the Peer's answer when done, assignments return once they are no longer running, and decisions return the user's answer once they settle it. Call again while the status is "pending".`,
  parameters: PairWaitInput,
  success: PairHandleResult,
  failure: PairToolError,
  dependencies,
})
  .annotate(Tool.Title, "Wait for pair work")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const PairAssignTool = Tool.make("pair_assign", {
  description:
    "Pair Room, Lead only: delegate bounded work to the Peer in its own git worktree and branch, based on the room checkout's branch (or baseRef), which it merges back into. Give a self-contained brief, the globs it may change, and acceptance criteria. Returns an assignmentId to pass to pair_wait. It is merged after you approve with pair_review and the user says so.",
  parameters: PairAssignInput,
  success: PairAssignResult,
  failure: PairToolError,
  dependencies,
})
  .annotate(Tool.Title, "Assign work to the Peer")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, false);

const PairCheckoutTool = Tool.make("pair_checkout", {
  description:
    "Pair Room, Lead only: tell the room which worktree you work in when it is not your thread's own directory. The Peer's snapshots, assignment bases and merges follow it. The path must be a worktree of the project's repository.",
  parameters: PairCheckoutInput,
  success: PairCheckoutResult,
  failure: PairToolError,
  dependencies,
})
  .annotate(Tool.Title, "Point the room at your checkout")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const PairReportProgressTool = Tool.make("pair_report_progress", {
  description:
    "Pair Room, assignee only: report progress on your assignment, or set blocked with a question when you cannot continue. The Lead sees it on the assignment card.",
  parameters: PairReportProgressInput,
  success: PairAckResult,
  failure: PairToolError,
  dependencies,
})
  .annotate(Tool.Title, "Report assignment progress")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, false);

const PairSubmitTool = Tool.make("pair_submit", {
  description:
    "Pair Room, assignee only: submit your assignment for the Lead's review with a summary, each acceptance criterion's result and evidence, tests run and known limitations. The server attaches the changed files and flags any outside your scope. Stop after submitting.",
  parameters: PairSubmitInput,
  success: PairAckResult,
  failure: PairToolError,
  dependencies,
})
  .annotate(Tool.Title, "Submit assignment")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, false);

const PairReviewTool = Tool.make("pair_review", {
  description:
    "Pair Room, Lead only: review an assignment. approve moves a submitted assignment to the user for merging (refused while files outside its scope changed). request-changes sends your notes back to the assignee. reject closes it without merging.",
  parameters: PairReviewInput,
  success: PairAckResult,
  failure: PairToolError,
  dependencies,
})
  .annotate(Tool.Title, "Review assignment")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, false);

const PairRecordDecisionTool = Tool.make("pair_record_decision", {
  description: `Pair Room, Lead or Peer: record a decision or a disagreement with your position and evidence, or add your position to an existing one by decisionId. The Lead may settle routine and architecture calls itself, and those return "recorded". Product, security, scope and destructive calls belong to the user: the tool waits up to waitSeconds (max ${PAIR_MAX_WAIT_SECONDS}) for their answer and returns "settled" with it, or "pending" with a handle for pair_wait. Tell the user what you recommend and why before you wait.`,
  parameters: PairRecordDecisionInput,
  success: PairAckResult,
  failure: PairToolError,
  dependencies,
})
  .annotate(Tool.Title, "Record decision")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, false);

const PairReadThreadTool = Tool.make("pair_read_thread", {
  description:
    "Pair Room, any member: read what the user and the other participant said in that participant's thread, newest last. Recent lines already open each of your turns as a catch-up; use this for lines the catch-up omitted or to page further back with beforeMessageId.",
  parameters: PairReadThreadInput,
  success: PairReadThreadResult,
  failure: PairToolError,
  dependencies,
})
  .annotate(Tool.Title, "Read the other thread")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

export const PairToolkit = Toolkit.make(
  PairStatusTool,
  PairConsultTool,
  PairReplyTool,
  PairAskTool,
  PairWaitTool,
  PairAssignTool,
  PairCheckoutTool,
  PairReportProgressTool,
  PairSubmitTool,
  PairReviewTool,
  PairRecordDecisionTool,
  PairReadThreadTool,
);
