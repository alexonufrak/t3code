import { EventId, TurnId, type OrchestrationThreadActivity } from "@t3tools/contracts";
import {
  type PairMirrorCard,
  pairMirrorCompletedPayload,
  pairMirrorStartedPayload,
} from "@t3tools/shared/pairMirror";
import { describe, expect, it } from "vite-plus/test";

import { buildThreadFeed } from "./threadActivity";

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

describe("buildThreadFeed Pair Room Peer cards", () => {
  it("shows a mirrored consult as one subagent spawn row on mobile", () => {
    const rows = buildThreadFeed({
      messages: [],
      activities: [
        mirrored("task.started", pairMirrorStartedPayload(card, "Review the retry policy"), 1),
        mirrored(
          "task.completed",
          pairMirrorCompletedPayload(card, { status: "answered", answer: "Do not retry 401s." }),
          2,
        ),
      ],
    }).flatMap((entry) => (entry.type === "activity-group" ? entry.activities : []));
    expect(rows).toMatchObject([
      { workEntry: { agentSpawn: { workflowId: null, agentTaskIds: ["consult-1"] } } },
    ]);
  });
});
