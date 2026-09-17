import {
  PAIR_ASSIGNMENT_ACTIVE_STATES,
  PAIR_PERSONAS,
  pairDecisionLeadMayResolve,
  pairRoomParticipant,
  type PairAssignment,
  type PairDecision,
  type PairPersona,
  type PairRoom,
  type ServerProvider,
  type ThreadId,
} from "@t3tools/contracts";

/**
 * Pure Pair Room helpers shared by web and mobile: whether a room can be
 * started, which room a thread belongs to, and the words a room shows.
 */

export type PairParticipantAvailability =
  | { readonly persona: PairPersona; readonly available: true }
  | { readonly persona: PairPersona; readonly available: false; readonly remedy: string };

export interface PairAvailability {
  readonly available: boolean;
  readonly participants: ReadonlyArray<PairParticipantAvailability>;
}

const PROVIDER_LABELS: Readonly<Record<PairPersona, string>> = {
  fable: "Claude Code",
  astra: "Codex",
};

function personaAvailability(
  providers: ReadonlyArray<ServerProvider>,
  persona: PairPersona,
): PairParticipantAvailability {
  const profile = PAIR_PERSONAS[persona];
  const provider = providers.find((entry) => entry.instanceId === profile.instanceId);
  const label = PROVIDER_LABELS[persona];
  const unavailable = (remedy: string): PairParticipantAvailability => ({
    persona,
    available: false,
    remedy,
  });
  if (!provider || provider.availability === "unavailable") {
    return unavailable(`${label} is not configured on this environment.`);
  }
  if (!provider.enabled) return unavailable(`Enable ${label} in Settings.`);
  if (!provider.installed) return unavailable(`Install ${label} on this environment.`);
  if (provider.auth.status === "unauthenticated") {
    return unavailable(`Sign in to ${label} on this environment.`);
  }
  const hasModel = provider.models.some(
    (model) => model.slug === profile.model || model.aliases?.includes(profile.model),
  );
  if (!hasModel) {
    return unavailable(`${label} does not offer ${profile.model}. Update ${label} and try again.`);
  }
  return { persona, available: true };
}

export function derivePairAvailability(providers: ReadonlyArray<ServerProvider>): PairAvailability {
  const participants = (["fable", "astra"] as const).map((persona) =>
    personaAvailability(providers, persona),
  );
  return { available: participants.every((entry) => entry.available), participants };
}

/** "former" is a participant thread retired by a Lead switch, kept as history. */
export type PairThreadRole = "lead" | "peer" | "assignee" | "former";

export interface PairThreadMembership {
  readonly room: PairRoom;
  readonly role: PairThreadRole;
  readonly persona: PairPersona;
  readonly assignment: PairAssignment | null;
}

export function pairRoomMembership(
  rooms: ReadonlyArray<PairRoom>,
  threadId: ThreadId,
): PairThreadMembership | null {
  for (const room of rooms) {
    const participant = room.participants.find((entry) => entry.threadId === threadId);
    if (participant) {
      return { room, role: participant.role, persona: participant.persona, assignment: null };
    }
    const assignment = room.assignments.find((entry) => entry.threadId === threadId);
    if (assignment) return { room, role: "assignee", persona: assignment.owner, assignment };
    const former = room.formerParticipants.find((entry) => entry.threadId === threadId);
    if (former) return { room, role: "former", persona: former.persona, assignment: null };
  }
  return null;
}

export const pairPersonaName = (persona: PairPersona) => PAIR_PERSONAS[persona].displayName;

const PAIR_THREAD_ROLE_WORDS: Readonly<Record<PairThreadRole, string>> = {
  lead: "Lead",
  peer: "Peer",
  assignee: "Assignment",
  former: "earlier thread",
};

/** Who writes the replies in a room thread, as a heading reads it: "Fable, Lead". */
export function pairThreadAuthorLabel(membership: PairThreadMembership): string {
  return `${pairPersonaName(membership.persona)}, ${PAIR_THREAD_ROLE_WORDS[membership.role]}`;
}

/** Work waiting on the user: merges to approve and decisions only they can make. */
export interface PairRoomAttention {
  readonly merges: ReadonlyArray<PairAssignment>;
  readonly decisions: ReadonlyArray<PairDecision>;
}

export function pairRoomAttention(room: PairRoom): PairRoomAttention {
  return {
    merges: room.assignments.filter((assignment) => assignment.state === "awaiting-user"),
    decisions: room.decisions.filter(
      (decision) => decision.resolution === null && !pairDecisionLeadMayResolve(decision.category),
    ),
  };
}

/** Consults and assignments in flight, for the header chip. */
export function pairRoomRunningCount(room: PairRoom): number {
  return (
    room.consults.filter((consult) => consult.status === "running").length +
    room.assignments.filter(
      (assignment) => assignment.state === "running" || assignment.state === "blocked",
    ).length
  );
}

/** "Fable leads · 2 running · 1 needs you", or the paused or closed state in words. */
export function pairRoomSummary(room: PairRoom): string {
  const lead = pairRoomParticipant(room, "lead");
  const parts = [lead ? `${pairPersonaName(lead.persona)} leads` : "Pair room"];
  if (room.status === "paused") parts.push("paused");
  if (room.status === "closed") parts.push("closed");
  if (room.leadSwitch?.phase === "drafting") parts.push("writing handoff");
  if (room.leadSwitch?.phase === "ready") parts.push("handoff ready");
  const running = pairRoomRunningCount(room);
  if (running > 0) parts.push(`${running} running`);
  const attention = pairRoomAttention(room);
  const needsYou = attention.merges.length + attention.decisions.length;
  if (needsYou > 0) parts.push(`${needsYou} need${needsYou === 1 ? "s" : ""} you`);
  return parts.join(" · ");
}

export const PAIR_ASSIGNMENT_STATE_LABELS: Readonly<Record<PairAssignment["state"], string>> = {
  running: "Working",
  blocked: "Blocked",
  submitted: "Waiting for Lead review",
  "awaiting-user": "Ready for you to merge",
  integrated: "Merged",
  completed: "Completed",
  rejected: "Rejected",
  cancelled: "Cancelled",
  failed: "Failed",
  interrupted: "Interrupted",
};

export const isActivePairAssignment = (assignment: PairAssignment) =>
  PAIR_ASSIGNMENT_ACTIVE_STATES.has(assignment.state);

/**
 * Maps each Peer, assignment and former participant thread to the current
 * Lead thread it nests under in a thread list. Lead threads are not in the
 * map; they stay top-level.
 */
export function pairRoomParentThreads(
  rooms: ReadonlyArray<PairRoom>,
): ReadonlyMap<ThreadId, ThreadId> {
  const parents = new Map<ThreadId, ThreadId>();
  for (const room of rooms) {
    const leadThreadId = pairRoomParticipant(room, "lead")?.threadId;
    if (!leadThreadId) continue;
    const peerThreadId = pairRoomParticipant(room, "peer")?.threadId;
    if (peerThreadId) parents.set(peerThreadId, leadThreadId);
    for (const assignment of room.assignments) parents.set(assignment.threadId, leadThreadId);
    for (const former of room.formerParticipants) parents.set(former.threadId, leadThreadId);
  }
  return parents;
}

/**
 * Reorders a thread list so each room's Peer and assignment threads sit
 * right after their Lead, keeping everything else in its original order.
 * A child whose Lead is not in the list stays where it was. Keys are thread
 * ids, or scoped thread keys for lists that span environments.
 */
export function nestPairRoomItems<T, K>(
  items: ReadonlyArray<T>,
  keyOf: (item: T) => K,
  parents: ReadonlyMap<K, K>,
): ReadonlyArray<{ readonly item: T; readonly depth: 0 | 1 }> {
  if (parents.size === 0) return items.map((item) => ({ item, depth: 0 }));
  const present = new Set(items.map(keyOf));
  const childrenByLead = new Map<K, T[]>();
  for (const item of items) {
    const parent = parents.get(keyOf(item));
    if (parent && present.has(parent)) {
      const children = childrenByLead.get(parent) ?? [];
      children.push(item);
      childrenByLead.set(parent, children);
    }
  }
  const nested: Array<{ readonly item: T; readonly depth: 0 | 1 }> = [];
  for (const item of items) {
    const id = keyOf(item);
    const parent = parents.get(id);
    if (parent && present.has(parent)) continue;
    nested.push({ item, depth: 0 });
    for (const child of childrenByLead.get(id) ?? []) nested.push({ item: child, depth: 1 });
  }
  return nested;
}
