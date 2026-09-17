import {
  PairRoomId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type PairAssignment,
  type PairDecision,
  type PairRoom,
  type ServerProvider,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  derivePairAvailability,
  nestPairRoomItems,
  pairRoomMembership,
  pairRoomParentThreads,
  pairRoomSummary,
} from "./pairRoomIndex.ts";

const AT = "2026-09-17T10:00:00.000Z";
const LEAD = ThreadId.make("lead");
const PEER = ThreadId.make("peer");
const ASSIGNEE = ThreadId.make("assignee");

function provider(
  instanceId: string,
  model: string,
  overrides: Partial<ServerProvider> = {},
): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make(instanceId),
    driver: instanceId,
    enabled: true,
    installed: true,
    version: "1.0.0",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: AT,
    models: [{ slug: model, name: model, isCustom: false, capabilities: null }],
    slashCommands: [],
    skills: [],
    ...overrides,
  } as ServerProvider;
}

function assignment(overrides: Partial<PairAssignment>): PairAssignment {
  return {
    assignmentId: "assignment-1",
    title: "Retry tests",
    owner: "astra",
    threadId: ASSIGNEE,
    worktreePath: "/worktrees/pair-retry",
    branch: "pair/retry",
    baseCommit: "abc",
    scopeGlobs: ["src/**"],
    acceptanceCriteria: [],
    expectedArtifact: "patch",
    state: "running",
    note: null,
    report: null,
    changedFiles: [],
    deviations: [],
    integrationCommit: null,
    createdAt: AT,
    updatedAt: AT,
    ...overrides,
  };
}

function decision(overrides: Partial<PairDecision>): PairDecision {
  return {
    decisionId: "decision-1",
    kind: "disagreement",
    category: "product",
    title: "Retry on 429?",
    positions: [],
    leadRecommendation: null,
    consequenceOfDeferring: null,
    resolution: null,
    resolvedBy: null,
    createdAt: AT,
    updatedAt: AT,
    ...overrides,
  };
}

function room(overrides: Partial<PairRoom> = {}): PairRoom {
  return {
    roomId: PairRoomId.make("room-1"),
    projectId: ProjectId.make("project-1"),
    mode: "pair",
    maxRoundsPerTurn: 2,
    status: "active",
    statusReason: null,
    participants: [
      { persona: "fable", role: "lead", threadId: LEAD },
      { persona: "astra", role: "peer", threadId: PEER },
    ],
    reviewWorktreePath: null,
    extraRounds: null,
    consults: [],
    assignments: [],
    decisions: [],
    createdAt: AT,
    updatedAt: AT,
    ...overrides,
  };
}

describe("derivePairAvailability", () => {
  it("is available only when both pinned models are offered by signed-in providers", () => {
    expect(
      derivePairAvailability([
        provider("claudeAgent", "claude-fable-5-1"),
        provider("codex", "gpt-6-astra"),
      ]).available,
    ).toBe(true);

    const result = derivePairAvailability([
      provider("claudeAgent", "claude-fable-5-1", { auth: { status: "unauthenticated" } }),
      provider("codex", "gpt-5"),
    ]);
    expect(result.available).toBe(false);
    expect(result.participants).toEqual([
      { persona: "fable", available: false, remedy: "Sign in to Claude Code on this environment." },
      {
        persona: "astra",
        available: false,
        remedy: "Codex does not offer gpt-6-astra. Update Codex and try again.",
      },
    ]);
  });
});

describe("pair room membership and summary", () => {
  it("finds the role of Lead, Peer and assignment threads", () => {
    const rooms = [room({ assignments: [assignment({})] })];
    expect(pairRoomMembership(rooms, LEAD)).toMatchObject({ role: "lead", persona: "fable" });
    expect(pairRoomMembership(rooms, PEER)).toMatchObject({ role: "peer", persona: "astra" });
    expect(pairRoomMembership(rooms, ASSIGNEE)).toMatchObject({
      role: "assignee",
      assignment: { assignmentId: "assignment-1" },
    });
    expect(pairRoomMembership(rooms, ThreadId.make("other"))).toBeNull();
  });

  it("says who leads, what is running and what waits on the user", () => {
    expect(
      pairRoomSummary(
        room({
          status: "paused",
          assignments: [
            assignment({ state: "running" }),
            assignment({ assignmentId: "a2", state: "awaiting-user" }),
          ],
          decisions: [decision({}), decision({ decisionId: "d2", category: "routine" })],
        }),
      ),
    ).toBe("Fable leads · paused · 1 running · 2 need you");
  });
});

describe("nestPairRoomItems", () => {
  it("puts Peer and assignment threads under their Lead and leaves orphans in place", () => {
    const parents = pairRoomParentThreads([room({ assignments: [assignment({})] })]);
    const items = [ASSIGNEE, ThreadId.make("unrelated"), LEAD, PEER];
    expect(
      nestPairRoomItems(items, (id) => id, parents).map(({ item, depth }) => `${item}:${depth}`),
    ).toEqual(["unrelated:0", "lead:0", "assignee:1", "peer:1"]);

    const withoutLead = [PEER, ThreadId.make("unrelated")];
    expect(
      nestPairRoomItems(withoutLead, (id) => id, parents).map(
        ({ item, depth }) => `${item}:${depth}`,
      ),
    ).toEqual(["peer:0", "unrelated:0"]);
  });
});
