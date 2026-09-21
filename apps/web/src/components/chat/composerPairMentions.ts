import { PAIR_PERSONAS, pairRoomParticipant } from "@t3tools/contracts";
import type { PairThreadMembership } from "@t3tools/client-runtime/state/pair-room-index";
import { pairMentionNames, type PairMentionParticipant } from "@t3tools/shared/pairMentions";

import type { ComposerCommandItem } from "./ComposerCommandMenu";

/**
 * Who `@` can address from this thread: the Peer, and only from the Lead's
 * thread, because that is where the server relays a mention from.
 */
export function pairMentionTargets(
  membership: PairThreadMembership | null,
): ReadonlyArray<PairMentionParticipant> {
  if (!membership || membership.role !== "lead" || membership.room.status === "closed") return [];
  const peer = pairRoomParticipant(membership.room, "peer");
  return peer ? [{ persona: peer.persona, role: "peer" }] : [];
}

/** Both participants, so a typed `@Fable` or `@lead` also reads as who it is. */
export function pairMentionChipParticipants(
  membership: PairThreadMembership | null,
): ReadonlyArray<PairMentionParticipant> {
  return (
    membership?.room.participants.map((entry) => ({ persona: entry.persona, role: entry.role })) ??
    []
  );
}

/** The participants the `@` query matches by name or role word, listed ahead of files. */
export function pairParticipantMenuItems(
  targets: ReadonlyArray<PairMentionParticipant>,
  query: string,
): Array<Extract<ComposerCommandItem, { type: "pair-participant" }>> {
  const needle = query.trim().toLowerCase();
  return targets
    .filter(
      (target) =>
        needle.length === 0 ||
        pairMentionNames(target).some((name) => name.toLowerCase().startsWith(needle)),
    )
    .map((target) => ({
      id: `pair-participant:${target.persona}`,
      type: "pair-participant",
      persona: target.persona,
      role: target.role,
      label: PAIR_PERSONAS[target.persona].displayName,
      description: `The ${target.role} in this Pair Room. @${target.role} works too.`,
    }));
}
