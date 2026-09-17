import { McpCapabilityUnavailableError } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";

import { PairCoordinator, PairToolUnavailableError } from "../../../pair/PairCoordinator.ts";
import {
  PAIR_MAX_WAIT_SECONDS,
  PairAckResult,
  PairAssignInput,
  PairAssignResult,
  PairConsultInput,
  PairHandleResult,
  PairRecordDecisionInput,
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

const PairConsultTool = Tool.make("pair_consult", {
  description: `Pair Room, Lead only: ask the Peer for a critique, review, answer or independent roundtable proposal. The Peer works on a snapshot of your checkout, including uncommitted files, and cannot see your conversation. Waits up to waitSeconds (max ${PAIR_MAX_WAIT_SECONDS}); if the Peer is still working the result is status "pending" and you continue with pair_wait. A "rejected" result explains why (round limit, Peer busy, room paused).`,
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
  description: `Pair Room: wait up to waitSeconds (max ${PAIR_MAX_WAIT_SECONDS}) for a consult or assignment handle. Consults return the Peer's answer when done; assignments return once they are submitted, blocked or otherwise no longer running. Call again while the status is "pending".`,
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
    "Pair Room, Lead only: delegate bounded work to the Peer in its own git worktree and branch. Give a self-contained brief, the globs it may change, and acceptance criteria. Returns an assignmentId to pass to pair_wait. Nothing is merged until you approve with pair_review and the user merges.",
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
  description:
    "Pair Room, Lead or Peer: record a decision or a disagreement with your position and evidence, or add your position to an existing one by decisionId. The Lead may settle routine and architecture calls; product, security, scope and destructive calls stay open for the user.",
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

export const PairToolkit = Toolkit.make(
  PairStatusTool,
  PairConsultTool,
  PairWaitTool,
  PairAssignTool,
  PairReportProgressTool,
  PairSubmitTool,
  PairReviewTool,
  PairRecordDecisionTool,
);
