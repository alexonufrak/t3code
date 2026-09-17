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
  | { readonly status: "failed"; readonly error: string }
  | { readonly status: "stopped"; readonly reason: string };

/** Card text rides the Lead's activity stream to every client, so it stays short. */
export const PAIR_MIRROR_SUMMARY_MAX_LENGTH = 2000;

const clampSummary = (value: string) =>
  value.length <= PAIR_MIRROR_SUMMARY_MAX_LENGTH
    ? value
    : `${value.slice(0, PAIR_MIRROR_SUMMARY_MAX_LENGTH - 3)}...`;

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
  return { ...cardFields(card), status: "running", detail: clampSummary(detail) } as const;
}

/** `running` after a terminal state reopens the card, which is how a revision round shows. */
export function pairMirrorProgressPayload(
  card: PairMirrorCard,
  update: { readonly status: "running" | "waiting"; readonly summary: string },
) {
  return {
    ...cardFields(card),
    status: update.status,
    summary: clampSummary(update.summary),
  } as const;
}

export function pairMirrorCompletedPayload(card: PairMirrorCard, outcome: PairMirrorOutcome) {
  switch (outcome.status) {
    case "answered":
      return {
        ...cardFields(card),
        status: "completed",
        summary: clampSummary(outcome.answer),
      } as const;
    case "failed":
      return {
        ...cardFields(card),
        status: "failed",
        summary: clampSummary(outcome.error),
      } as const;
    case "stopped":
      return {
        ...cardFields(card),
        status: "stopped",
        summary: clampSummary(outcome.reason),
      } as const;
  }
}
