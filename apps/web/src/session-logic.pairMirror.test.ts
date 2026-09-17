import { EventId, TurnId, type OrchestrationThreadActivity } from "@t3tools/contracts";
import {
  type PairMirrorCard,
  pairMirrorCompletedPayload,
  pairMirrorStartedPayload,
} from "@t3tools/shared/pairMirror";
import { describe, expect, it } from "vite-plus/test";

import { deriveWorkLogEntries } from "./session-logic";

const card: PairMirrorCard = {
  taskId: "consult-1",
  title: "Consult Astra",
  persona: "Astra",
  model: "gpt-6-astra",
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

describe("deriveWorkLogEntries Pair Room Peer cards", () => {
  it("shows a mirrored consult as one agent spawn row on the Lead's timeline", () => {
    const entries = deriveWorkLogEntries([
      mirrored("task.started", pairMirrorStartedPayload(card, "Review the retry policy"), 1),
      mirrored(
        "task.completed",
        pairMirrorCompletedPayload(card, { status: "answered", answer: "Do not retry 401s." }),
        2,
      ),
    ]);
    const spawnRows = entries.filter((entry) => entry.agentSpawn !== undefined);
    expect(spawnRows).toHaveLength(1);
    expect(spawnRows[0]!.agentSpawn).toEqual({ workflowId: null, agentTaskIds: ["consult-1"] });
  });
});
