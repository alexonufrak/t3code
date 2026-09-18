import * as Effect from "effect/Effect";

import { PairCoordinator } from "../../../pair/PairCoordinator.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { PairToolkit } from "./tools.ts";

/**
 * The caller's identity comes only from its MCP credential, never from tool
 * input, so a participant cannot act as the other or as the user.
 */
const make = Effect.gen(function* () {
  const coordinator = yield* PairCoordinator;
  const caller = McpInvocationContext.requireMcpCapability("pair").pipe(
    Effect.map((scope) => scope.threadId),
  );

  return PairToolkit.of({
    pair_status: () => caller.pipe(Effect.flatMap(coordinator.status)),
    pair_consult: (input) =>
      caller.pipe(Effect.flatMap((threadId) => coordinator.consult(threadId, input))),
    pair_reply: (input) =>
      caller.pipe(Effect.flatMap((threadId) => coordinator.reply(threadId, input))),
    pair_ask: (input) =>
      caller.pipe(Effect.flatMap((threadId) => coordinator.ask(threadId, input))),
    pair_wait: (input) =>
      caller.pipe(Effect.flatMap((threadId) => coordinator.wait(threadId, input))),
    pair_assign: (input) =>
      caller.pipe(Effect.flatMap((threadId) => coordinator.assign(threadId, input))),
    pair_report_progress: (input) =>
      caller.pipe(Effect.flatMap((threadId) => coordinator.reportProgress(threadId, input))),
    pair_submit: (input) =>
      caller.pipe(Effect.flatMap((threadId) => coordinator.submit(threadId, input))),
    pair_review: (input) =>
      caller.pipe(Effect.flatMap((threadId) => coordinator.review(threadId, input))),
    pair_record_decision: (input) =>
      caller.pipe(Effect.flatMap((threadId) => coordinator.recordDecision(threadId, input))),
    pair_read_thread: (input) =>
      caller.pipe(Effect.flatMap((threadId) => coordinator.readThread(threadId, input))),
  });
});

export const PairToolkitHandlersLive = PairToolkit.toLayer(make);
