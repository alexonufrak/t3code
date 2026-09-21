import { PairRoomId, ProjectId, ThreadId, type PairRoom } from "@t3tools/contracts";
import type { PairThreadMembership } from "@t3tools/client-runtime/state/pair-room-index";
import { describe, expect, it } from "vite-plus/test";

import {
  pairMentionChipParticipants,
  pairMentionTargets,
  pairParticipantMenuItems,
} from "./composerPairMentions";

const room = {
  roomId: PairRoomId.make("room-1"),
  projectId: ProjectId.make("project-1"),
  status: "active",
  participants: [
    { persona: "fable", role: "lead", threadId: ThreadId.make("lead") },
    { persona: "astra", role: "peer", threadId: ThreadId.make("peer") },
  ],
} as unknown as PairRoom;

const membership = (role: PairThreadMembership["role"], status = "active"): PairThreadMembership =>
  ({ room: { ...room, status }, role, persona: "fable", assignment: null }) as PairThreadMembership;

describe("pairMentionTargets", () => {
  it("offers the Peer from the Lead's thread and nobody elsewhere", () => {
    expect(pairMentionTargets(membership("lead"))).toEqual([{ persona: "astra", role: "peer" }]);
    expect(pairMentionTargets(membership("peer"))).toEqual([]);
    expect(pairMentionTargets(membership("assignee"))).toEqual([]);
    expect(pairMentionTargets(membership("lead", "closed"))).toEqual([]);
    expect(pairMentionTargets(null)).toEqual([]);
  });
});

describe("pairParticipantMenuItems", () => {
  const targets = pairMentionTargets(membership("lead"));

  it("matches the start of the name or the role word, in either case", () => {
    for (const query of ["", "a", "AST", "p", "peer"]) {
      expect(pairParticipantMenuItems(targets, query).map((item) => item.label)).toEqual(["Astra"]);
    }
    expect(pairParticipantMenuItems(targets, "src/")).toEqual([]);
    expect(pairParticipantMenuItems(targets, "fab")).toEqual([]);
  });

  it("says who the participant is and that the role word works", () => {
    expect(pairParticipantMenuItems(targets, "")[0]).toMatchObject({
      id: "pair-participant:astra",
      type: "pair-participant",
      persona: "astra",
      role: "peer",
      description: "The peer in this Pair Room. @peer works too.",
    });
  });
});

describe("pairMentionChipParticipants", () => {
  it("names both participants so either mention reads as a person", () => {
    expect(pairMentionChipParticipants(membership("peer"))).toEqual([
      { persona: "fable", role: "lead" },
      { persona: "astra", role: "peer" },
    ]);
    expect(pairMentionChipParticipants(null)).toEqual([]);
  });
});
