import { EventId, TurnId, type OrchestrationThreadActivity } from "@t3tools/contracts";
import {
  type PairMirrorCard,
  pairMirrorCompletedPayload,
  pairMirrorStartedPayload,
} from "@t3tools/shared/pairMirror";
import { describe, expect, it } from "vite-plus/test";

import { foldSubagentActivities } from "./subagentRuntime.ts";

const card: PairMirrorCard = {
  taskId: "consult-1",
  title: "Consult Fable",
  persona: "Fable",
  model: "claude-fable-5-1",
};

function mirrored(
  kind: "task.started" | "task.completed",
  payload: Readonly<Record<string, unknown>>,
  second: number,
): OrchestrationThreadActivity {
  return {
    id: EventId.make(`${kind}-${second}`),
    tone: "info",
    kind,
    summary: kind,
    payload,
    turnId: TurnId.make("lead-turn-1"),
    createdAt: `2026-09-17T10:00:0${second}.000Z`,
  };
}

describe("Pair Room mirrored Peer activities", () => {
  it("fold into a Peer subagent with its model and answer", () => {
    const [peer, ...rest] = foldSubagentActivities([
      mirrored("task.started", pairMirrorStartedPayload(card, "Review the retry policy"), 1),
      mirrored(
        "task.completed",
        pairMirrorCompletedPayload(card, { status: "answered", answer: "Do not retry 401s." }),
        2,
      ),
    ]);
    expect(rest).toEqual([]);
    expect(peer).toMatchObject({
      id: "consult-1",
      title: "Consult Fable",
      role: "Peer",
      model: "claude-fable-5-1",
      status: "completed",
      result: "Do not retry 401s.",
    });
  });

  it("fold a failed consult into a failed Peer subagent with the error", () => {
    const [peer] = foldSubagentActivities([
      mirrored("task.started", pairMirrorStartedPayload(card, "Review the migration"), 1),
      mirrored(
        "task.completed",
        pairMirrorCompletedPayload(card, { status: "failed", error: "Peer turn ended as error." }),
        2,
      ),
    ]);
    expect(peer).toMatchObject({ status: "failed", error: "Peer turn ended as error." });
  });

  it("stay running until the consult settles", () => {
    const [peer] = foldSubagentActivities([
      mirrored("task.started", pairMirrorStartedPayload(card, "Review the retry policy"), 1),
    ]);
    expect(peer).toMatchObject({ status: "running", role: "Peer" });
  });
});
