import {
  PAIR_ROOM_SETTLED_CONSULTS_KEPT,
  PairRoomId,
  ProjectId,
  ThreadId,
  TurnId,
  type PairRoom,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  decidePairRoom,
  findPairRoomByThread,
  pairScopeDeviations,
  pairScopesOverlap,
  type PairRoomCommand,
} from "./pairRoomDecider.ts";

const ROOM_ID = PairRoomId.make("room-1");
const LEAD_THREAD = ThreadId.make("lead-thread");
const TURN_1 = TurnId.make("turn-1");
const AT = "2026-09-17T10:00:00.000Z";

function apply(rooms: Map<PairRoomId, PairRoom>, command: PairRoomCommand): PairRoom {
  const result = decidePairRoom(rooms, command);
  if (!result.ok) throw new Error(`${result.rejection.reason}: ${result.rejection.detail}`);
  rooms.set(result.room.roomId, result.room);
  return result.room;
}

function rejectionOf(rooms: Map<PairRoomId, PairRoom>, command: PairRoomCommand) {
  const result = decidePairRoom(rooms, command);
  if (result.ok) throw new Error(`expected ${command.type} to be rejected`);
  return result.rejection;
}

function createRoom(mode: "adaptive" | "pair" | "roundtable" = "adaptive") {
  const rooms = new Map<PairRoomId, PairRoom>();
  apply(rooms, {
    type: "room.create",
    roomId: ROOM_ID,
    projectId: ProjectId.make("project-1"),
    leadThreadId: LEAD_THREAD,
    leadPersona: "fable",
    mode,
    at: AT,
  });
  return rooms;
}

const consult = (consultId: string, overrides: Partial<PairRoomCommand> = {}): PairRoomCommand =>
  ({
    type: "consult.request",
    roomId: ROOM_ID,
    consultId,
    kind: "critique",
    leadTurnId: TURN_1,
    automatic: false,
    title: "Check the retry policy",
    at: AT,
    ...overrides,
  }) as PairRoomCommand;

const settle = (consultId: string): PairRoomCommand => ({
  type: "consult.settle",
  roomId: ROOM_ID,
  consultId,
  status: "answered",
  peerTurnId: null,
  error: null,
  at: AT,
});

const assign = (assignmentId: string, scopeGlobs: ReadonlyArray<string>): PairRoomCommand => ({
  type: "assignment.create",
  roomId: ROOM_ID,
  assignmentId,
  title: `Assignment ${assignmentId}`,
  threadId: ThreadId.make(`thread-${assignmentId}`),
  worktreePath: `/worktrees/${assignmentId}`,
  branch: `pair/${assignmentId}`,
  baseCommit: "abc123",
  scopeGlobs,
  acceptanceCriteria: ["Tests pass"],
  expectedArtifact: "patch",
  at: AT,
});

describe("decidePairRoom", () => {
  it("creates a room with Fable leading and Astra as a Peer without a thread yet", () => {
    const room = createRoom("roundtable").get(ROOM_ID)!;
    expect(room.participants).toEqual([
      { persona: "fable", role: "lead", threadId: LEAD_THREAD },
      { persona: "astra", role: "peer", threadId: null },
    ]);
    expect(room.maxRoundsPerTurn).toBe(3);
  });

  it("refuses a second room for a thread that already belongs to one", () => {
    const rooms = createRoom();
    expect(
      rejectionOf(rooms, {
        type: "room.create",
        roomId: PairRoomId.make("room-2"),
        projectId: ProjectId.make("project-1"),
        leadThreadId: LEAD_THREAD,
        leadPersona: "astra",
        mode: "pair",
        at: AT,
      }).reason,
    ).toBe("conflict");
  });

  it("limits consult rounds per Lead turn until the user grants more", () => {
    const rooms = createRoom();
    apply(rooms, consult("c1"));
    apply(rooms, settle("c1"));
    apply(rooms, consult("c2"));
    apply(rooms, settle("c2"));
    expect(rejectionOf(rooms, consult("c3")).reason).toBe("round-limit");

    // A new Lead turn starts counting again.
    expect(
      apply(rooms, consult("c3", { leadTurnId: TurnId.make("turn-2") })).consults.at(-1),
    ).toMatchObject({ round: 1 });
    apply(rooms, settle("c3"));

    apply(rooms, {
      type: "room.grant-rounds",
      roomId: ROOM_ID,
      leadTurnId: TURN_1,
      count: 1,
      at: AT,
    });
    expect(apply(rooms, consult("c4")).consults.at(-1)).toMatchObject({ round: 3 });
  });

  it("lets the pair mode review guardrail run past the round limit but not beside a running consult", () => {
    const rooms = createRoom("pair");
    apply(rooms, consult("c1"));
    expect(rejectionOf(rooms, consult("review", { automatic: true })).reason).toBe("peer-busy");
    apply(rooms, settle("c1"));
    apply(rooms, consult("c2"));
    apply(rooms, settle("c2"));
    expect(
      apply(rooms, consult("review", { automatic: true, kind: "review" })).consults.at(-1),
    ).toMatchObject({ automatic: true, status: "running" });
  });

  it("treats a repeated settle as a no-op so reconciliation and the watcher can both run", () => {
    const rooms = createRoom();
    apply(rooms, consult("c1"));
    apply(rooms, settle("c1"));
    const again = apply(rooms, {
      type: "consult.settle",
      roomId: ROOM_ID,
      consultId: "c1",
      status: "failed",
      peerTurnId: null,
      error: "Server restarted",
      at: "2026-09-17T11:00:00.000Z",
    });
    expect(again.consults[0]).toMatchObject({ status: "answered", error: null, settledAt: AT });
  });

  it("keeps running consults and only the newest settled ones", () => {
    const rooms = createRoom();
    for (let index = 0; index < PAIR_ROOM_SETTLED_CONSULTS_KEPT + 5; index += 1) {
      apply(rooms, consult(`c${index}`, { automatic: true }));
      apply(rooms, settle(`c${index}`));
    }
    const room = apply(rooms, consult("running", { automatic: true }));
    expect(room.consults).toHaveLength(PAIR_ROOM_SETTLED_CONSULTS_KEPT + 1);
    expect(room.consults[0]?.consultId).toBe("c5");
    expect(room.consults.at(-1)?.status).toBe("running");
  });

  it("blocks new work while paused and resumes on the user's word", () => {
    const rooms = createRoom();
    apply(rooms, {
      type: "room.update",
      roomId: ROOM_ID,
      status: "paused",
      statusReason: "Astra was rerouted to another model.",
      at: AT,
    });
    expect(rejectionOf(rooms, consult("c1"))).toMatchObject({
      reason: "room-paused",
      detail: "This pair room is paused: Astra was rerouted to another model.",
    });
    expect(rejectionOf(rooms, assign("a1", ["src/**"])).reason).toBe("room-paused");
    const resumed = apply(rooms, {
      type: "room.update",
      roomId: ROOM_ID,
      status: "active",
      at: AT,
    });
    expect(resumed.statusReason).toBeNull();
    apply(rooms, consult("c1"));
  });

  it("resets the round budget to the new mode's default when the mode changes", () => {
    const rooms = createRoom("adaptive");
    expect(
      apply(rooms, { type: "room.update", roomId: ROOM_ID, mode: "roundtable", at: AT })
        .maxRoundsPerTurn,
    ).toBe(3);
  });

  it("rejects overlapping active assignment scopes and frees them once the assignment ends", () => {
    const rooms = createRoom();
    apply(rooms, assign("a1", ["apps/server/src/auth/**"]));
    expect(rejectionOf(rooms, assign("a2", ["apps/server/**"])).reason).toBe("scope-overlap");
    apply(rooms, assign("a2", ["apps/web/src/**"]));
    apply(rooms, {
      type: "assignment.update",
      roomId: ROOM_ID,
      assignmentId: "a1",
      state: "cancelled",
      at: AT,
    });
    apply(rooms, assign("a3", ["apps/server/**"]));
  });

  it("refuses approval while changes sit outside the scope, and widening the scope clears them", () => {
    const rooms = createRoom();
    apply(rooms, assign("a1", ["src/auth/**"]));
    apply(rooms, {
      type: "assignment.update",
      roomId: ROOM_ID,
      assignmentId: "a1",
      state: "submitted",
      changedFiles: ["src/auth/token.ts", "package.json"],
      deviations: pairScopeDeviations(["src/auth/token.ts", "package.json"], ["src/auth/**"]),
      at: AT,
    });
    expect(
      rejectionOf(rooms, {
        type: "assignment.update",
        roomId: ROOM_ID,
        assignmentId: "a1",
        state: "awaiting-user",
        at: AT,
      }),
    ).toMatchObject({ reason: "scope-deviation" });

    const widened = apply(rooms, {
      type: "assignment.update",
      roomId: ROOM_ID,
      assignmentId: "a1",
      scopeGlobs: ["src/auth/**", "package.json"],
      at: AT,
    });
    expect(widened.assignments[0]?.deviations).toEqual([]);
    expect(
      apply(rooms, {
        type: "assignment.update",
        roomId: ROOM_ID,
        assignmentId: "a1",
        state: "awaiting-user",
        at: AT,
      }).assignments[0]?.state,
    ).toBe("awaiting-user");
  });

  it("does not let an integrated assignment reopen", () => {
    const rooms = createRoom();
    apply(rooms, assign("a1", ["src/**"]));
    for (const state of ["submitted", "awaiting-user", "integrated"] as const) {
      apply(rooms, {
        type: "assignment.update",
        roomId: ROOM_ID,
        assignmentId: "a1",
        state,
        at: AT,
      });
    }
    expect(
      rejectionOf(rooms, {
        type: "assignment.update",
        roomId: ROOM_ID,
        assignmentId: "a1",
        state: "running",
        at: AT,
      }).reason,
    ).toBe("invalid");
  });

  it("lets the Lead settle routine calls but leaves product calls open for the user", () => {
    const rooms = createRoom();
    const lead = { persona: "fable", role: "lead" } as const;
    const peer = { persona: "astra", role: "peer" } as const;
    const record = (decisionId: string, category: "routine" | "product") =>
      apply(rooms, {
        type: "decision.record",
        roomId: ROOM_ID,
        decisionId,
        actor: lead,
        kind: "decision",
        category,
        title: `Decide ${decisionId}`,
        position: { summary: "Use exponential backoff", evidence: null },
        leadRecommendation: null,
        consequenceOfDeferring: null,
        resolution: "Go with backoff",
        at: AT,
      }).decisions.at(-1);

    expect(record("d1", "routine")).toMatchObject({
      resolvedBy: "lead",
      resolution: "Go with backoff",
    });
    expect(record("d2", "product")).toMatchObject({ resolvedBy: null, resolution: null });

    const disputed = apply(rooms, {
      type: "decision.add-position",
      roomId: ROOM_ID,
      decisionId: "d2",
      actor: peer,
      position: { summary: "Ask the user; it changes billing", evidence: "billing.ts:40" },
      resolution: "Peer tries to settle it",
      at: AT,
    }).decisions.at(-1);
    expect(disputed?.positions.map((position) => position.persona)).toEqual(["fable", "astra"]);
    expect(disputed?.resolvedBy).toBeNull();

    expect(
      rejectionOf(rooms, {
        type: "decision.resolve",
        roomId: ROOM_ID,
        decisionId: "d2",
        resolution: "Lead overrides",
        resolvedBy: "lead",
        at: AT,
      }).reason,
    ).toBe("decision-authority");
    apply(rooms, {
      type: "decision.resolve",
      roomId: ROOM_ID,
      decisionId: "d1",
      resolution: "User prefers fixed delays",
      resolvedBy: "user",
      at: AT,
    });
    expect(
      rejectionOf(rooms, {
        type: "decision.resolve",
        roomId: ROOM_ID,
        decisionId: "d1",
        resolution: "Lead reverts the user",
        resolvedBy: "lead",
        at: AT,
      }).reason,
    ).toBe("decision-authority");
  });
});

describe("pair scopes", () => {
  it("compares static prefixes conservatively", () => {
    expect(pairScopesOverlap(["src/api/**"], ["src/web/**"])).toBe(false);
    expect(pairScopesOverlap(["src/api/**"], ["src/**/*.css"])).toBe(true);
    expect(pairScopesOverlap(["**/*.ts"], ["docs/**"])).toBe(true);
    expect(pairScopesOverlap(["src/a.ts"], ["src/b.ts"])).toBe(false);
    expect(pairScopesOverlap(["./src/api/"], ["src/api/client.ts"])).toBe(true);
  });

  it("matches changed files against globs and bare directories", () => {
    expect(
      pairScopeDeviations(
        ["src/api/client.ts", "src/api/nested/deep.ts", "docs/readme.md", "src/web/app.tsx"],
        ["src/api", "docs/*.md"],
      ),
    ).toEqual(["src/web/app.tsx"]);
  });
});

describe("lead switch", () => {
  const PEER_THREAD = ThreadId.make("peer-thread");
  const NEW_LEAD_THREAD = ThreadId.make("new-lead-thread");

  it("holds the room while the handoff is drafted, then swaps roles only on confirm", () => {
    const rooms = createRoom();
    apply(rooms, {
      type: "peer.attach",
      roomId: ROOM_ID,
      threadId: PEER_THREAD,
      reviewWorktreePath: "/worktrees/review",
      at: AT,
    });
    const started = apply(rooms, { type: "lead.switch-start", roomId: ROOM_ID, at: AT });
    expect(started.leadSwitch).toMatchObject({ toPersona: "astra", phase: "drafting" });
    expect(rejectionOf(rooms, consult("during-switch")).reason).toBe("lead-switching");
    expect(
      rejectionOf(rooms, {
        type: "lead.switch-confirm",
        roomId: ROOM_ID,
        newLeadThreadId: NEW_LEAD_THREAD,
        at: AT,
      }).reason,
    ).toBe("invalid");

    const drafted = apply(rooms, {
      type: "lead.switch-draft",
      roomId: ROOM_ID,
      handoff: "Objective: ship retries.",
      error: null,
      at: AT,
    });
    expect(drafted.leadSwitch).toMatchObject({
      phase: "ready",
      handoff: "Objective: ship retries.",
    });

    const switched = apply(rooms, {
      type: "lead.switch-confirm",
      roomId: ROOM_ID,
      newLeadThreadId: NEW_LEAD_THREAD,
      at: AT,
    });
    expect(switched.participants).toEqual([
      { persona: "astra", role: "lead", threadId: NEW_LEAD_THREAD },
      { persona: "fable", role: "peer", threadId: null },
    ]);
    expect(switched.leadSwitch).toBeNull();
    expect(switched.formerParticipants.map((former) => former.threadId)).toEqual([
      LEAD_THREAD,
      PEER_THREAD,
    ]);
    // Retired threads keep no pair tools and cannot start another room.
    expect(findPairRoomByThread(rooms.values(), LEAD_THREAD)).toBeUndefined();
    expect(findPairRoomByThread(rooms.values(), NEW_LEAD_THREAD)?.roomId).toBe(ROOM_ID);
    expect(
      rejectionOf(rooms, {
        type: "room.create",
        roomId: PairRoomId.make("room-2"),
        projectId: ProjectId.make("project-1"),
        leadThreadId: LEAD_THREAD,
        leadPersona: "fable",
        mode: "adaptive",
        at: AT,
      }).reason,
    ).toBe("conflict");
  });

  it("marks a draft without a handoff failed, which frees the room and allows a retry", () => {
    const rooms = createRoom();
    apply(rooms, { type: "lead.switch-start", roomId: ROOM_ID, at: AT });
    const failed = apply(rooms, {
      type: "lead.switch-draft",
      roomId: ROOM_ID,
      handoff: "   ",
      error: "Fable's turn ended as error.",
      at: AT,
    });
    expect(failed.leadSwitch).toMatchObject({
      phase: "failed",
      handoff: null,
      error: "Fable's turn ended as error.",
    });
    apply(rooms, consult("after-failure"));
    apply(rooms, {
      type: "consult.settle",
      roomId: ROOM_ID,
      consultId: "after-failure",
      status: "answered",
      peerTurnId: null,
      error: null,
      at: AT,
    });
    expect(
      apply(rooms, { type: "lead.switch-start", roomId: ROOM_ID, at: AT }).leadSwitch?.phase,
    ).toBe("drafting");
    expect(
      apply(rooms, { type: "lead.switch-cancel", roomId: ROOM_ID, at: AT }).leadSwitch,
    ).toBeNull();
  });
});
