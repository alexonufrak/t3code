import { McpCapabilityUnavailableError, TrimmedNonEmptyString } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as OrchestrationEngine from "../../../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";

/**
 * Phase 0 feasibility tools for Pair Room. They prove the seams the real
 * `pair_*` toolkit depends on (caller identity, server-created peer threads,
 * bounded waits with continuation handles, mirrored subagent cards) and are
 * replaced by the room-aware toolkit in Phase 1.
 */

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  OrchestrationEngine.OrchestrationEngineService,
  ProjectionSnapshotQuery.ProjectionSnapshotQuery,
];

/** Both CLIs cut a single MCP call off near 60s, so waits stay well under it. */
export const PAIR_MAX_WAIT_SECONDS = 45;

export class PairThreadNotFoundError extends Schema.TaggedError<PairThreadNotFoundError>()(
  "PairThreadNotFoundError",
  { threadId: Schema.String },
) {
  override get message(): string {
    return `Thread ${this.threadId} was not found.`;
  }
}

export class PairConsultNotFoundError extends Schema.TaggedError<PairConsultNotFoundError>()(
  "PairConsultNotFoundError",
  { consultId: Schema.String },
) {
  override get message(): string {
    return `Consult ${this.consultId} is unknown or was lost when the server restarted.`;
  }
}

export class PairDispatchFailedError extends Schema.TaggedError<PairDispatchFailedError>()(
  "PairDispatchFailedError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Could not reach the peer participant.";
  }
}

export const PairToolError = Schema.Union([
  McpCapabilityUnavailableError,
  PairThreadNotFoundError,
  PairConsultNotFoundError,
  PairDispatchFailedError,
]);
export type PairToolError = typeof PairToolError.Type;

const WaitSeconds = Schema.Int.check(
  Schema.isBetween({ minimum: 0, maximum: PAIR_MAX_WAIT_SECONDS }),
).annotate({
  description: `Seconds to wait for the peer before returning a pending handle (0-${PAIR_MAX_WAIT_SECONDS}).`,
});

export const PairPingInput = Schema.Struct({
  sleepSeconds: Schema.optional(
    Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 120 })).annotate({
      description:
        "Hold the call open this long before answering. Used to probe CLI tool timeouts.",
    }),
  ),
});

export const PairPingResult = Schema.Struct({
  threadId: Schema.String,
  providerInstanceId: Schema.String,
  capabilities: Schema.Array(Schema.String),
  sleptSeconds: Schema.Int,
});
export type PairPingResult = typeof PairPingResult.Type;

export const PairConsultInput = Schema.Struct({
  question: TrimmedNonEmptyString.annotate({
    description:
      "What you want the peer to critique, investigate, or answer. Include the context it needs; it cannot see your conversation.",
  }),
  waitSeconds: Schema.optional(WaitSeconds),
});

export const PairWaitInput = Schema.Struct({
  consultId: TrimmedNonEmptyString.annotate({
    description: "The consultId returned by pair_spike_consult.",
  }),
  waitSeconds: Schema.optional(WaitSeconds),
});

export const PairConsultResult = Schema.Struct({
  status: Schema.Literals(["answered", "pending", "failed"]),
  consultId: Schema.String,
  peerThreadId: Schema.String,
  peerModel: Schema.String,
  answer: Schema.NullOr(Schema.String),
  error: Schema.NullOr(Schema.String),
  retryAfterSeconds: Schema.NullOr(Schema.Int),
});
export type PairConsultResult = typeof PairConsultResult.Type;

const PairPingTool = Tool.make("pair_ping", {
  description:
    "Pair Room feasibility probe. Reports which thread and provider instance the server sees for this call, optionally after holding the call open.",
  parameters: PairPingInput,
  success: PairPingResult,
  failure: PairToolError,
  dependencies,
})
  .annotate(Tool.Title, "Pair ping")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const PairSpikeConsultTool = Tool.make("pair_spike_consult", {
  description: `Ask the other Pair Room participant (Fable if you are Astra, Astra if you are Fable) a question in its own thread. Waits up to waitSeconds (max ${PAIR_MAX_WAIT_SECONDS}); if the peer is still working, returns status "pending" and you call pair_spike_wait with the consultId.`,
  parameters: PairConsultInput,
  success: PairConsultResult,
  failure: PairToolError,
  dependencies,
})
  .annotate(Tool.Title, "Consult pair peer")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, false);

const PairSpikeWaitTool = Tool.make("pair_spike_wait", {
  description: `Wait up to waitSeconds (max ${PAIR_MAX_WAIT_SECONDS}) for a pending pair_spike_consult to finish. Call again while the status stays "pending".`,
  parameters: PairWaitInput,
  success: PairConsultResult,
  failure: PairToolError,
  dependencies,
})
  .annotate(Tool.Title, "Wait for pair peer")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

export const PairSpikeToolkit = Toolkit.make(PairPingTool, PairSpikeConsultTool, PairSpikeWaitTool);
