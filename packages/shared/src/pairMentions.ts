import { PAIR_PERSONAS, type PairPersona, type PairRole } from "@t3tools/contracts";

/**
 * How a message addresses a Pair Room participant: `@Astra` by name, or
 * `@peer` and `@lead` by role, so nobody has to remember which model holds
 * the role today. A mention is its own word, matched case-insensitively,
 * and the punctuation typed against it ("@peer," or "@Astra?") is ignored.
 */
export interface PairMentionParticipant {
  readonly persona: PairPersona;
  readonly role: PairRole;
}

/** The words that address a participant after an `@`: its name and its role. */
export const pairMentionNames = (participant: PairMentionParticipant): ReadonlyArray<string> => [
  PAIR_PERSONAS[participant.persona].displayName,
  participant.role,
];

const escapeRegExp = (value: string) => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const TRAILING_PUNCTUATION = /[,.:;!?]+$/;

/** Whether `text` mentions the participant as its own word. */
export function pairMessageMentions(text: string, participant: PairMentionParticipant): boolean {
  const names = pairMentionNames(participant).map(escapeRegExp).join("|");
  return new RegExp(`(?<![\\w@.])@(?:${names})(?![\\w-])`, "i").test(text);
}

/** The participant a mention token names ("Astra", "peer,"), or null for a file path. */
export function pairMentionedParticipant(
  token: string,
  participants: ReadonlyArray<PairMentionParticipant>,
): PairMentionParticipant | null {
  const name = token.replace(TRAILING_PUNCTUATION, "").toLowerCase();
  return (
    participants.find((participant) =>
      pairMentionNames(participant).some((candidate) => candidate.toLowerCase() === name),
    ) ?? null
  );
}

/** The punctuation a mention token carries after the name, shown after the chip that replaces it. */
export const pairMentionTrailing = (token: string): string =>
  TRAILING_PUNCTUATION.exec(token)?.[0] ?? "";
