import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import type { Tool } from "effect/unstable/ai";

import { PairCoordinator } from "../../../pair/PairCoordinator.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { PairToolkitHandlersLive } from "./handlers.ts";
import { PairToolkit } from "./tools.ts";

const CALLER_THREAD = ThreadId.make("assignee-thread");

const invocation = (
  capabilities: ReadonlyArray<McpInvocationContext.McpCapability>,
): McpInvocationContext.McpInvocationScope => ({
  environmentId: EnvironmentId.make("environment-1"),
  threadId: CALLER_THREAD,
  providerSessionId: "provider-session-1",
  providerInstanceId: ProviderInstanceId.make("codex"),
  capabilities: new Set(capabilities),
  issuedAt: 1,
});

const ack = {
  status: "recorded",
  reason: null,
  detail: "Submitted.",
  assignment: null,
  decision: null,
  handle: null,
  retryAfterSeconds: null,
} as const;

const makeHarness = Effect.gen(function* () {
  const callers = yield* Ref.make<ReadonlyArray<ThreadId>>([]);
  const coordinator = Layer.mock(PairCoordinator)({
    submit: (threadId) => Ref.update(callers, (all) => [...all, threadId]).pipe(Effect.as(ack)),
  });
  const toolkit = yield* PairToolkit.pipe(
    Effect.provide(PairToolkitHandlersLive.pipe(Layer.provide(coordinator))),
  );
  const submit = (capabilities: ReadonlyArray<McpInvocationContext.McpCapability>) =>
    toolkit
      .handle("pair_submit", {
        summary: "Done",
        criteriaResults: [],
        testsRun: [],
        knownLimitations: [],
      })
      .pipe(
        Stream.unwrap,
        Stream.runCollect,
        Effect.map(
          (chunk) =>
            chunk.at(-1)!.result as Tool.Success<(typeof PairToolkit.tools)["pair_submit"]>,
        ),
        Effect.provideService(McpInvocationContext.McpInvocationContext, invocation(capabilities)),
        Effect.provide(coordinator),
      );
  return { callers, submit };
});

describe("pair toolkit handlers", () => {
  it.effect("acts as the thread the MCP credential belongs to", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      expect(yield* harness.submit(["pair"])).toEqual(ack);
      expect(yield* Ref.get(harness.callers)).toEqual([CALLER_THREAD]);
    }),
  );

  it.effect("refuses a credential without the pair capability before reaching the room", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      const error = yield* harness.submit(["pull-requests"]).pipe(Effect.flip);
      expect(error).toMatchObject({ _tag: "McpCapabilityUnavailableError", capability: "pair" });
      expect(yield* Ref.get(harness.callers)).toEqual([]);
    }),
  );
});
