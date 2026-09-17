import { useAtomValue } from "@effect/atom-react";
import { parseScopedThreadKey, scopedThreadKey } from "@t3tools/client-runtime/environment";
import {
  pairRoomMembership,
  type PairThreadMembership,
} from "@t3tools/client-runtime/state/pair-room-index";
import { createPairRoomEnvironmentAtoms } from "@t3tools/client-runtime/state/pair-rooms";
import {
  pairRoomParticipant,
  type EnvironmentId,
  type PairPersona,
  type PairRoom,
  type PairRoomMode,
  type ScopedThreadRef,
  type ThreadId,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import { AsyncResult, Atom } from "effect/unstable/reactivity";
import { create } from "zustand";

import type { DraftId } from "../composerDraftStore";
import { connectionAtomRuntime } from "../connection/runtime";
import { environmentServerConfigsAtom } from "./server";

export const pairRoomEnvironment = createPairRoomEnvironmentAtoms(connectionAtomRuntime);

const EMPTY_ROOMS: ReadonlyArray<PairRoom> = [];
const EMPTY_SUPPORTED_ATOM = Atom.make(false).pipe(Atom.withLabel("web-pair-rooms-supported:none"));
const EMPTY_ROOMS_ATOM = Atom.make(EMPTY_ROOMS).pipe(Atom.withLabel("web-pair-rooms:none"));
const EMPTY_MEMBERSHIP_ATOM = Atom.make<PairThreadMembership | null>(null).pipe(
  Atom.withLabel("web-pair-room-membership:none"),
);

const pairRoomsSupportedAtom = Atom.family((environmentId: EnvironmentId) =>
  Atom.make(
    (get) =>
      get(environmentServerConfigsAtom).get(environmentId)?.environment.capabilities.pairRooms ===
      true,
  ).pipe(Atom.withLabel(`web-pair-rooms-supported:${environmentId}`)),
);

/**
 * Every room on an environment; empty until the stream delivers, and never
 * subscribed on servers without Pair Room support.
 */
const environmentPairRoomsAtom = Atom.family((environmentId: EnvironmentId) =>
  Atom.make((get): ReadonlyArray<PairRoom> => {
    if (!get(pairRoomsSupportedAtom(environmentId))) return EMPTY_ROOMS;
    const result = get(pairRoomEnvironment.rooms({ environmentId, input: {} }));
    return Option.getOrElse(AsyncResult.value(result), () => EMPTY_ROOMS);
  }).pipe(Atom.withLabel(`web-pair-rooms:${environmentId}`)),
);

const pairThreadMembershipAtom = Atom.family((key: string) => {
  const ref = parseScopedThreadKey(key);
  return Atom.make((get): PairThreadMembership | null =>
    ref === null
      ? null
      : pairRoomMembership(get(environmentPairRoomsAtom(ref.environmentId)), ref.threadId),
  ).pipe(Atom.withLabel(`web-pair-room-membership:${key}`));
});

/**
 * Sidebar placement for room threads across every environment: which Lead
 * each Peer and assignment thread nests under, and the word each row shows.
 * Keys are scoped thread keys.
 */
export interface PairRoomSidebarIndex {
  readonly parents: ReadonlyMap<string, string>;
  readonly labels: ReadonlyMap<string, string>;
}

const EMPTY_SIDEBAR_INDEX: PairRoomSidebarIndex = { parents: new Map(), labels: new Map() };

const pairRoomSidebarIndexAtom = Atom.make((get): PairRoomSidebarIndex => {
  const parents = new Map<string, string>();
  const labels = new Map<string, string>();
  for (const environmentId of get(environmentServerConfigsAtom).keys()) {
    for (const room of get(environmentPairRoomsAtom(environmentId))) {
      const keyOf = (threadId: ThreadId) => scopedThreadKey({ environmentId, threadId });
      const leadThreadId = pairRoomParticipant(room, "lead")?.threadId;
      if (!leadThreadId) continue;
      const leadKey = keyOf(leadThreadId);
      labels.set(leadKey, "Pair");
      const peerThreadId = pairRoomParticipant(room, "peer")?.threadId;
      if (peerThreadId) {
        parents.set(keyOf(peerThreadId), leadKey);
        labels.set(keyOf(peerThreadId), "Peer");
      }
      for (const assignment of room.assignments) {
        parents.set(keyOf(assignment.threadId), leadKey);
        labels.set(keyOf(assignment.threadId), "Assignment");
      }
      for (const former of room.formerParticipants) {
        parents.set(keyOf(former.threadId), leadKey);
        labels.set(keyOf(former.threadId), "Earlier");
      }
    }
  }
  return labels.size === 0 ? EMPTY_SIDEBAR_INDEX : { parents, labels };
}).pipe(Atom.withLabel("web-pair-room-sidebar-index"));

export function usePairRoomSidebarIndex(): PairRoomSidebarIndex {
  return useAtomValue(pairRoomSidebarIndexAtom);
}

export function usePairRoomsSupported(environmentId: EnvironmentId | null): boolean {
  return useAtomValue(
    environmentId === null ? EMPTY_SUPPORTED_ATOM : pairRoomsSupportedAtom(environmentId),
  );
}

export function useEnvironmentPairRooms(
  environmentId: EnvironmentId | null,
): ReadonlyArray<PairRoom> {
  return useAtomValue(
    environmentId === null ? EMPTY_ROOMS_ATOM : environmentPairRoomsAtom(environmentId),
  );
}

/** The room a thread belongs to and its role there, or null for ordinary threads. */
export function usePairThreadMembership(ref: ScopedThreadRef | null): PairThreadMembership | null {
  return useAtomValue(
    ref === null ? EMPTY_MEMBERSHIP_ATOM : pairThreadMembershipAtom(scopedThreadKey(ref)),
  );
}

/**
 * A draft's Pair Room choice. It lives only in memory: after a reload the
 * draft is an ordinary thread again, and the composer chip is gone to say so.
 */
export interface PairRoomDraft {
  readonly leadPersona: PairPersona;
  readonly mode: PairRoomMode;
  readonly maxRoundsPerTurn: number;
}

interface PairRoomDraftStoreState {
  readonly byDraftId: Readonly<Record<string, PairRoomDraft>>;
  /** A draft whose composer should open Pair room setup, from "New pair room". */
  readonly setupRequestedDraftId: DraftId | null;
  readonly set: (draftId: DraftId, draft: PairRoomDraft) => void;
  readonly clear: (draftId: DraftId) => void;
  readonly requestSetup: (draftId: DraftId | null) => void;
}

export const usePairRoomDraftStore = create<PairRoomDraftStoreState>()((set) => ({
  byDraftId: {},
  setupRequestedDraftId: null,
  requestSetup: (draftId) =>
    set((state) =>
      state.setupRequestedDraftId === draftId ? state : { setupRequestedDraftId: draftId },
    ),
  set: (draftId, draft) =>
    set((state) => ({ byDraftId: { ...state.byDraftId, [draftId]: draft } })),
  clear: (draftId) =>
    set((state) => {
      if (!(draftId in state.byDraftId)) return state;
      const { [draftId]: _removed, ...rest } = state.byDraftId;
      return { byDraftId: rest };
    }),
}));

export function usePairRoomDraft(draftId: DraftId | null): PairRoomDraft | null {
  return usePairRoomDraftStore((state) =>
    draftId === null ? null : (state.byDraftId[draftId] ?? null),
  );
}
