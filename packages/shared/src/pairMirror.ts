/**
 * Payloads for Pair Room activities mirrored into the Lead's thread.
 *
 * They use the `task.*` shape that provider-native subagents already produce,
 * so every client (including older and mobile ones) renders Peer work as a
 * subagent card without new timeline code. `agentKind: "agent"` is required:
 * without it the subagent fold treats the row as background work and hides it.
 * `agentId` and `timelineBypass` are deliberately absent, because either one
 * marks the row as agent-internal and keeps it off the Lead's timeline.
 */

export type PairPersonaName = "Fable" | "Astra";

export interface PairMirrorCard {
  /** Stable per consult or assignment; the subagent fold keys on it. */
  readonly taskId: string;
  readonly title: string;
  readonly persona: PairPersonaName;
  readonly model: string;
}

export type PairMirrorOutcome =
  | { readonly status: "answered"; readonly answer: string }
  | { readonly status: "failed"; readonly error: string };

function cardFields(card: PairMirrorCard) {
  return {
    taskId: card.taskId,
    agentKind: "agent",
    title: card.title,
    role: "Peer",
    model: card.model,
    pairMirror: true,
  } as const;
}

export function pairMirrorStartedPayload(card: PairMirrorCard, detail: string) {
  return { ...cardFields(card), status: "running", detail } as const;
}

export function pairMirrorCompletedPayload(card: PairMirrorCard, outcome: PairMirrorOutcome) {
  return outcome.status === "answered"
    ? ({ ...cardFields(card), status: "completed", summary: outcome.answer } as const)
    : ({ ...cardFields(card), status: "failed", summary: outcome.error } as const);
}
