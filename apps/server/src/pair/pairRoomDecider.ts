// @effect-diagnostics nodeBuiltinImport:off
import {
  PAIR_ASSIGNMENT_ACTIVE_STATES,
  PAIR_ROOM_DEFAULT_MAX_ROUNDS,
  PAIR_ROOM_FORMER_PARTICIPANTS_KEPT,
  PAIR_ROOM_HANDOFF_MAX_LENGTH,
  PAIR_ROOM_SETTLED_CONSULTS_KEPT,
  PAIR_ROOM_TEXT_MAX_LENGTH,
  PAIR_ROOM_TITLE_MAX_LENGTH,
  otherPairPersona,
  pairDecisionLeadMayResolve,
  pairRoomParticipant,
  type PairAssignment,
  type PairAssignmentArtifact,
  type PairAssignmentReport,
  type PairAssignmentState,
  type PairConsult,
  type PairConsultKind,
  type PairDecision,
  type PairDecisionCategory,
  type PairDecisionKind,
  type PairPersona,
  type PairRole,
  type PairRoom,
  type PairRoomId,
  type PairRoomMode,
  type PairRoomStatus,
  type ProjectId,
  type ThreadId,
  type TurnId,
} from "@t3tools/contracts";

/**
 * Pure Pair Room rules. Every change to a room goes through `decidePairRoom`,
 * which either returns the next room record or a rejection the caller can
 * show to the agent or user as is. Callers resolve ids, timestamps and the
 * acting participant before calling, so this stays deterministic.
 */

export type PairRejectionReason =
  | "not-found"
  | "invalid"
  | "conflict"
  | "not-lead"
  | "room-paused"
  | "room-closed"
  | "peer-busy"
  | "round-limit"
  | "scope-overlap"
  | "scope-deviation"
  | "decision-authority"
  | "lead-switching";

export interface PairRejection {
  readonly reason: PairRejectionReason;
  readonly detail: string;
}

export interface PairActor {
  readonly persona: PairPersona;
  readonly role: PairRole;
}

export interface PairPositionInput {
  readonly summary: string;
  readonly evidence: string | null;
}

export type PairRoomCommand =
  | {
      readonly type: "room.create";
      readonly roomId: PairRoomId;
      readonly projectId: ProjectId;
      readonly leadThreadId: ThreadId;
      readonly leadPersona: PairPersona;
      readonly mode: PairRoomMode;
      readonly maxRoundsPerTurn?: number | undefined;
      readonly at: string;
    }
  | {
      readonly type: "room.update";
      readonly roomId: PairRoomId;
      readonly mode?: PairRoomMode | undefined;
      readonly maxRoundsPerTurn?: number | undefined;
      readonly status?: PairRoomStatus | undefined;
      readonly statusReason?: string | null | undefined;
      readonly at: string;
    }
  | {
      readonly type: "room.grant-rounds";
      readonly roomId: PairRoomId;
      readonly leadTurnId: TurnId;
      readonly count: number;
      readonly at: string;
    }
  | {
      readonly type: "peer.attach";
      readonly roomId: PairRoomId;
      readonly threadId: ThreadId;
      readonly reviewWorktreePath: string;
      readonly at: string;
    }
  | {
      readonly type: "consult.request";
      readonly roomId: PairRoomId;
      readonly consultId: string;
      readonly kind: PairConsultKind;
      readonly leadTurnId: TurnId | null;
      readonly automatic: boolean;
      readonly title: string;
      readonly at: string;
    }
  | {
      readonly type: "consult.settle";
      readonly roomId: PairRoomId;
      readonly consultId: string;
      readonly status: "answered" | "failed" | "cancelled";
      readonly peerTurnId: TurnId | null;
      readonly error: string | null;
      readonly at: string;
    }
  | {
      readonly type: "assignment.create";
      readonly roomId: PairRoomId;
      readonly assignmentId: string;
      readonly title: string;
      readonly threadId: ThreadId;
      readonly worktreePath: string;
      readonly branch: string;
      readonly baseCommit: string;
      readonly scopeGlobs: ReadonlyArray<string>;
      readonly acceptanceCriteria: ReadonlyArray<string>;
      readonly expectedArtifact: PairAssignmentArtifact;
      readonly at: string;
    }
  | {
      readonly type: "assignment.update";
      readonly roomId: PairRoomId;
      readonly assignmentId: string;
      readonly state?: PairAssignmentState | undefined;
      readonly note?: string | null | undefined;
      readonly report?: PairAssignmentReport | undefined;
      readonly changedFiles?: ReadonlyArray<string> | undefined;
      readonly deviations?: ReadonlyArray<string> | undefined;
      readonly scopeGlobs?: ReadonlyArray<string> | undefined;
      readonly integrationCommit?: string | undefined;
      readonly at: string;
    }
  | {
      readonly type: "decision.record";
      readonly roomId: PairRoomId;
      readonly decisionId: string;
      readonly actor: PairActor;
      readonly kind: PairDecisionKind;
      readonly category: PairDecisionCategory;
      readonly title: string;
      readonly position: PairPositionInput;
      readonly leadRecommendation: string | null;
      readonly consequenceOfDeferring: string | null;
      readonly resolution: string | null;
      readonly at: string;
    }
  | {
      readonly type: "decision.add-position";
      readonly roomId: PairRoomId;
      readonly decisionId: string;
      readonly actor: PairActor;
      readonly position: PairPositionInput;
      readonly resolution: string | null;
      readonly at: string;
    }
  | {
      readonly type: "decision.resolve";
      readonly roomId: PairRoomId;
      readonly decisionId: string;
      readonly resolution: string;
      readonly resolvedBy: "lead" | "user";
      readonly at: string;
    }
  | { readonly type: "lead.switch-start"; readonly roomId: PairRoomId; readonly at: string }
  | {
      readonly type: "lead.switch-draft";
      readonly roomId: PairRoomId;
      readonly handoff: string | null;
      readonly error: string | null;
      readonly at: string;
    }
  | {
      readonly type: "lead.switch-confirm";
      readonly roomId: PairRoomId;
      readonly newLeadThreadId: ThreadId;
      readonly at: string;
    }
  | { readonly type: "lead.switch-cancel"; readonly roomId: PairRoomId; readonly at: string };

export type PairDecideResult =
  | { readonly ok: true; readonly room: PairRoom }
  | { readonly ok: false; readonly rejection: PairRejection };

const reject = (reason: PairRejectionReason, detail: string): PairDecideResult => ({
  ok: false,
  rejection: { reason, detail },
});

const accept = (room: PairRoom): PairDecideResult => ({ ok: true, room });

const ellipsis = "...";

/** Clamps free text to a wire limit so a room record always encodes. */
export const clampPairText = (value: string, max = PAIR_ROOM_TEXT_MAX_LENGTH): string => {
  const trimmed = value.trim();
  return trimmed.length <= max ? trimmed : `${trimmed.slice(0, max - ellipsis.length)}${ellipsis}`;
};

const clampTitle = (value: string) => clampPairText(value, PAIR_ROOM_TITLE_MAX_LENGTH);
const clampNullable = (value: string | null | undefined) =>
  value === null || value === undefined || value.trim().length === 0 ? null : clampPairText(value);

// ── Scopes ──────────────────────────────────────────────────────────────

const GLOB_SEGMENT = /[*?[\]{}!]/;

const normalizeScopePath = (value: string) =>
  value
    .trim()
    .replace(/\\/g, "/")
    .replace(/^\.\/+/, "")
    .replace(/\/+$/, "");

/** Path segments before the first segment that contains glob syntax. */
export const staticGlobPrefix = (glob: string): ReadonlyArray<string> => {
  const segments = normalizeScopePath(glob).split("/").filter(Boolean);
  const firstDynamic = segments.findIndex((segment) => GLOB_SEGMENT.test(segment));
  return firstDynamic === -1 ? segments : segments.slice(0, firstDynamic);
};

const isSegmentPrefix = (prefix: ReadonlyArray<string>, of: ReadonlyArray<string>) =>
  prefix.length <= of.length && prefix.every((segment, index) => segment === of[index]);

/**
 * Conservative overlap test on static prefixes: `src/api/**` and `src/**`
 * overlap, `src/api/**` and `src/web/**` do not. Two globs that share a
 * prefix count as overlapping even if their patterns could never match the
 * same file, because a wrong "no overlap" would let two writers collide.
 */
export const pairScopesOverlap = (
  left: ReadonlyArray<string>,
  right: ReadonlyArray<string>,
): boolean =>
  left.some((leftGlob) =>
    right.some((rightGlob) => {
      const a = staticGlobPrefix(leftGlob);
      const b = staticGlobPrefix(rightGlob);
      return isSegmentPrefix(a, b) || isSegmentPrefix(b, a);
    }),
  );

export const PAIR_SCOPE_GLOBS_MAX = 20;
export const PAIR_SCOPE_GLOB_MAX_LENGTH = 200;
const UNSUPPORTED_GLOB_SYNTAX = /[{}()[\]!]/;

/**
 * Why a set of scope globs is refused, or null. Scopes come from agents, so
 * only `*`, `**` and `?` are supported: the matcher below stays linear in the
 * glob and path lengths, where brace or extglob syntax lets a short pattern
 * stall a backtracking matcher for minutes.
 */
export const pairScopeGlobsProblem = (globs: ReadonlyArray<string>): string | null => {
  if (globs.length === 0) return "An assignment needs at least one scope glob.";
  if (globs.length > PAIR_SCOPE_GLOBS_MAX) {
    return `An assignment takes at most ${PAIR_SCOPE_GLOBS_MAX} scope globs.`;
  }
  for (const glob of globs) {
    if (glob.length > PAIR_SCOPE_GLOB_MAX_LENGTH) {
      return `Scope globs are at most ${PAIR_SCOPE_GLOB_MAX_LENGTH} characters.`;
    }
    if (UNSUPPORTED_GLOB_SYNTAX.test(glob)) {
      return `Scope glob "${glob}" uses {}, (), [] or !. Scopes support *, ** and ?; list alternatives as separate globs.`;
    }
  }
  return null;
};

/** One path segment against literals, `*` and `?`, in O(pattern x segment) with one backtrack point. */
const segmentMatches = (pattern: string, segment: string): boolean => {
  // As in shell globs, a leading dot is only matched by a literal dot.
  if (segment.startsWith(".") && !pattern.startsWith(".")) return false;
  let p = 0;
  let s = 0;
  let star = -1;
  let resume = 0;
  while (s < segment.length) {
    if (p < pattern.length && (pattern[p] === "?" || pattern[p] === segment[s])) {
      p += 1;
      s += 1;
    } else if (p < pattern.length && pattern[p] === "*") {
      star = p;
      p += 1;
      resume = s;
    } else if (star !== -1) {
      p = star + 1;
      resume += 1;
      s = resume;
    } else {
      return false;
    }
  }
  while (pattern[p] === "*") p += 1;
  return p === pattern.length;
};

/** `**` spans any number of segments (never a dot segment); each pair of segments is compared once. */
export const pairGlobMatches = (file: string, glob: string): boolean => {
  const fileSegments = file.split("/");
  const globSegments = glob.split("/");
  const width = fileSegments.length + 1;
  const memo = new Map<number, boolean>();
  const visit = (g: number, f: number): boolean => {
    const key = g * width + f;
    const cached = memo.get(key);
    if (cached !== undefined) return cached;
    const globSegment = globSegments[g];
    const fileSegment = fileSegments[f];
    const result =
      globSegment === undefined
        ? fileSegment === undefined
        : globSegment === "**"
          ? visit(g + 1, f) ||
            (fileSegment !== undefined && !fileSegment.startsWith(".") && visit(g, f + 1))
          : fileSegment !== undefined &&
            segmentMatches(globSegment, fileSegment) &&
            visit(g + 1, f + 1);
    memo.set(key, result);
    return result;
  };
  return visit(0, 0);
};

export const pairScopeMatches = (file: string, scopeGlobs: ReadonlyArray<string>): boolean => {
  const normalizedFile = normalizeScopePath(file);
  return scopeGlobs.some((glob) => {
    const normalizedGlob = normalizeScopePath(glob);
    return (
      pairGlobMatches(normalizedFile, normalizedGlob) ||
      // A bare directory scope such as `src/api` covers everything under it.
      (!GLOB_SEGMENT.test(normalizedGlob) && normalizedFile.startsWith(`${normalizedGlob}/`))
    );
  });
};

export const pairScopeDeviations = (
  changedFiles: ReadonlyArray<string>,
  scopeGlobs: ReadonlyArray<string>,
): ReadonlyArray<string> => changedFiles.filter((file) => !pairScopeMatches(file, scopeGlobs));

// ── Rounds ──────────────────────────────────────────────────────────────

export const pairRoundsUsed = (room: PairRoom, leadTurnId: TurnId | null): number =>
  leadTurnId === null
    ? 0
    : room.consults.filter((consult) => consult.leadTurnId === leadTurnId && !consult.automatic)
        .length;

export const pairRoundLimit = (room: PairRoom, leadTurnId: TurnId | null): number =>
  room.maxRoundsPerTurn +
  (leadTurnId !== null && room.extraRounds?.leadTurnId === leadTurnId ? room.extraRounds.count : 0);

// ── Assignment states ───────────────────────────────────────────────────

const ASSIGNMENT_TRANSITIONS: Readonly<
  Record<PairAssignmentState, ReadonlySet<PairAssignmentState>>
> = {
  running: new Set(["running", "blocked", "submitted", "failed", "interrupted", "cancelled"]),
  blocked: new Set(["running", "blocked", "submitted", "failed", "interrupted", "cancelled"]),
  submitted: new Set([
    "submitted",
    "running",
    "awaiting-user",
    "completed",
    "rejected",
    "cancelled",
  ]),
  "awaiting-user": new Set(["awaiting-user", "running", "integrated", "rejected", "cancelled"]),
  interrupted: new Set(["running", "cancelled"]),
  failed: new Set(["running", "cancelled"]),
  cancelled: new Set(["running"]),
  rejected: new Set(["running"]),
  integrated: new Set(),
  completed: new Set(),
};

// ── Decider ─────────────────────────────────────────────────────────────

/** Whether a thread was ever part of a room, including threads retired by a Lead switch. */
const everInPairRoom = (rooms: Iterable<PairRoom>, threadId: ThreadId): boolean => {
  for (const room of rooms) {
    if (room.formerParticipants.some((former) => former.threadId === threadId)) return true;
  }
  return findPairRoomByThread(rooms, threadId) !== undefined;
};

/** The room a live participant or assignment thread belongs to. Former threads do not count. */
export const findPairRoomByThread = (
  rooms: Iterable<PairRoom>,
  threadId: ThreadId,
): PairRoom | undefined => {
  for (const room of rooms) {
    if (room.participants.some((participant) => participant.threadId === threadId)) return room;
    if (room.assignments.some((assignment) => assignment.threadId === threadId)) return room;
  }
  return undefined;
};

const requireWritable = (room: PairRoom): PairRejection | null => {
  if (room.status === "closed") {
    return { reason: "room-closed", detail: "This pair room is closed." };
  }
  if (room.status === "paused") {
    return {
      reason: "room-paused",
      detail: room.statusReason
        ? `This pair room is paused: ${room.statusReason}`
        : "This pair room is paused until the user resumes it.",
    };
  }
  if (room.leadSwitch !== null && room.leadSwitch.phase !== "failed") {
    return {
      reason: "lead-switching",
      detail:
        "The user is switching this room's Lead. Finish or stop the current step and wait for the switch.",
    };
  }
  return null;
};

const replaceById = <T, K extends keyof T>(items: ReadonlyArray<T>, key: K, next: T) =>
  items.map((item) => (item[key] === next[key] ? next : item));

const trimSettledConsults = (consults: ReadonlyArray<PairConsult>): ReadonlyArray<PairConsult> => {
  let settledSeen = 0;
  const keptNewestFirst = consults.toReversed().filter((consult) => {
    if (consult.status === "running") return true;
    settledSeen += 1;
    return settledSeen <= PAIR_ROOM_SETTLED_CONSULTS_KEPT;
  });
  return keptNewestFirst.toReversed();
};

const upsertPosition = (
  decision: PairDecision,
  actor: PairActor,
  position: PairPositionInput,
): PairDecision["positions"] => [
  ...decision.positions.filter((existing) => existing.persona !== actor.persona),
  {
    persona: actor.persona,
    summary: clampPairText(position.summary),
    evidence: clampNullable(position.evidence),
  },
];

const leadResolution = (
  decision: PairDecision,
  actor: PairActor,
  resolution: string | null,
): Pick<PairDecision, "resolution" | "resolvedBy"> => {
  const text = clampNullable(resolution);
  if (
    text === null ||
    actor.role !== "lead" ||
    !pairDecisionLeadMayResolve(decision.category) ||
    decision.resolvedBy === "user"
  ) {
    return { resolution: decision.resolution, resolvedBy: decision.resolvedBy };
  }
  return { resolution: text, resolvedBy: "lead" };
};

export function decidePairRoom(
  rooms: ReadonlyMap<PairRoomId, PairRoom>,
  command: PairRoomCommand,
): PairDecideResult {
  if (command.type === "room.create") {
    if (rooms.has(command.roomId)) {
      return reject("conflict", `Pair room ${command.roomId} already exists.`);
    }
    if (everInPairRoom([...rooms.values()], command.leadThreadId)) {
      return reject("conflict", "That thread already belongs to a pair room.");
    }
    return accept({
      roomId: command.roomId,
      projectId: command.projectId,
      mode: command.mode,
      maxRoundsPerTurn: command.maxRoundsPerTurn ?? PAIR_ROOM_DEFAULT_MAX_ROUNDS[command.mode],
      status: "active",
      statusReason: null,
      participants: [
        { persona: command.leadPersona, role: "lead", threadId: command.leadThreadId },
        { persona: otherPairPersona(command.leadPersona), role: "peer", threadId: null },
      ],
      reviewWorktreePath: null,
      extraRounds: null,
      consults: [],
      assignments: [],
      decisions: [],
      leadSwitch: null,
      formerParticipants: [],
      createdAt: command.at,
      updatedAt: command.at,
    });
  }

  const room = rooms.get(command.roomId);
  if (!room) return reject("not-found", `Pair room ${command.roomId} was not found.`);
  const touched = { updatedAt: command.at };

  switch (command.type) {
    case "room.update": {
      const mode = command.mode ?? room.mode;
      const status = command.status ?? room.status;
      return accept({
        ...room,
        ...touched,
        mode,
        maxRoundsPerTurn:
          command.maxRoundsPerTurn ??
          (command.mode && command.mode !== room.mode
            ? PAIR_ROOM_DEFAULT_MAX_ROUNDS[command.mode]
            : room.maxRoundsPerTurn),
        status,
        statusReason:
          status === "active"
            ? null
            : command.statusReason !== undefined
              ? clampNullable(command.statusReason)
              : room.statusReason,
      });
    }

    case "room.grant-rounds": {
      const blocked = requireWritable(room);
      if (blocked) return { ok: false, rejection: blocked };
      const existing =
        room.extraRounds?.leadTurnId === command.leadTurnId ? room.extraRounds.count : 0;
      return accept({
        ...room,
        ...touched,
        extraRounds: { leadTurnId: command.leadTurnId, count: existing + command.count },
      });
    }

    case "peer.attach": {
      const peer = pairRoomParticipant(room, "peer");
      if (!peer) return reject("invalid", "The pair room has no Peer.");
      // Replacing is allowed: if the user deleted the Peer's thread, the next consult makes a new one.
      return accept({
        ...room,
        ...touched,
        participants: room.participants.map((participant) =>
          participant.role === "peer"
            ? { ...participant, threadId: command.threadId }
            : participant,
        ),
        reviewWorktreePath: command.reviewWorktreePath,
      });
    }

    case "consult.request": {
      const blocked = requireWritable(room);
      if (blocked) return { ok: false, rejection: blocked };
      if (room.consults.some((consult) => consult.status === "running")) {
        return reject(
          "peer-busy",
          "The Peer is still answering another consult. Wait for it with pair_wait first.",
        );
      }
      const used = pairRoundsUsed(room, command.leadTurnId);
      const limit = pairRoundLimit(room, command.leadTurnId);
      if (!command.automatic && used >= limit) {
        return reject(
          "round-limit",
          `This turn already used ${used} of ${limit} consult rounds. Decide with what you have, or tell the user you want more rounds.`,
        );
      }
      if (room.consults.some((consult) => consult.consultId === command.consultId)) {
        return reject("conflict", `Consult ${command.consultId} already exists.`);
      }
      return accept({
        ...room,
        ...touched,
        consults: trimSettledConsults([
          ...room.consults,
          {
            consultId: command.consultId,
            kind: command.kind,
            leadTurnId: command.leadTurnId,
            round: used + 1,
            automatic: command.automatic,
            status: "running",
            peerTurnId: null,
            title: clampTitle(command.title),
            error: null,
            requestedAt: command.at,
            settledAt: null,
          },
        ]),
      });
    }

    case "consult.settle": {
      const consult = room.consults.find((candidate) => candidate.consultId === command.consultId);
      if (!consult) return reject("not-found", `Consult ${command.consultId} was not found.`);
      // Settling twice is a no-op: the watcher and startup reconciliation may both try.
      if (consult.status !== "running") return accept(room);
      return accept({
        ...room,
        ...touched,
        consults: trimSettledConsults(
          replaceById(room.consults, "consultId", {
            ...consult,
            status: command.status,
            peerTurnId: command.peerTurnId,
            error: command.status === "answered" ? null : clampNullable(command.error),
            settledAt: command.at,
          }),
        ),
      });
    }

    case "assignment.create": {
      const blocked = requireWritable(room);
      if (blocked) return { ok: false, rejection: blocked };
      const scopeProblem = pairScopeGlobsProblem(command.scopeGlobs);
      if (scopeProblem) return reject("invalid", scopeProblem);
      if (room.assignments.some((existing) => existing.assignmentId === command.assignmentId)) {
        return reject("conflict", `Assignment ${command.assignmentId} already exists.`);
      }
      const overlapping = room.assignments.find(
        (existing) =>
          PAIR_ASSIGNMENT_ACTIVE_STATES.has(existing.state) &&
          pairScopesOverlap(existing.scopeGlobs, command.scopeGlobs),
      );
      if (overlapping) {
        return reject(
          "scope-overlap",
          `Scope overlaps active assignment "${overlapping.title}" (${overlapping.scopeGlobs.join(", ")}). Narrow the scope or wait for it to finish.`,
        );
      }
      const peer = pairRoomParticipant(room, "peer");
      if (!peer) return reject("invalid", "The pair room has no Peer.");
      const assignment: PairAssignment = {
        assignmentId: command.assignmentId,
        title: clampTitle(command.title),
        owner: peer.persona,
        threadId: command.threadId,
        worktreePath: command.worktreePath,
        branch: command.branch,
        baseCommit: command.baseCommit,
        scopeGlobs: command.scopeGlobs.map(normalizeScopePath),
        acceptanceCriteria: command.acceptanceCriteria.map((criterion) => clampPairText(criterion)),
        expectedArtifact: command.expectedArtifact,
        state: "running",
        note: null,
        report: null,
        changedFiles: [],
        deviations: [],
        integrationCommit: null,
        createdAt: command.at,
        updatedAt: command.at,
      };
      return accept({ ...room, ...touched, assignments: [...room.assignments, assignment] });
    }

    case "assignment.update": {
      const assignment = room.assignments.find(
        (candidate) => candidate.assignmentId === command.assignmentId,
      );
      if (!assignment) {
        return reject("not-found", `Assignment ${command.assignmentId} was not found.`);
      }
      const state = command.state ?? assignment.state;
      if (!ASSIGNMENT_TRANSITIONS[assignment.state].has(state)) {
        return reject(
          "invalid",
          `Assignment "${assignment.title}" is ${assignment.state} and cannot move to ${state}.`,
        );
      }
      const scopeGlobs = command.scopeGlobs?.map(normalizeScopePath) ?? assignment.scopeGlobs;
      if (command.scopeGlobs) {
        const scopeProblem = pairScopeGlobsProblem(scopeGlobs);
        if (scopeProblem) return reject("invalid", scopeProblem);
        const overlapping = room.assignments.find(
          (existing) =>
            existing.assignmentId !== assignment.assignmentId &&
            PAIR_ASSIGNMENT_ACTIVE_STATES.has(existing.state) &&
            pairScopesOverlap(existing.scopeGlobs, scopeGlobs),
        );
        if (overlapping) {
          return reject(
            "scope-overlap",
            `Scope overlaps active assignment "${overlapping.title}" (${overlapping.scopeGlobs.join(", ")}).`,
          );
        }
      }
      const changedFiles = command.changedFiles ?? assignment.changedFiles;
      // Scope changes re-judge the files already changed, so widening a scope clears deviations.
      const deviations =
        command.deviations ??
        (command.scopeGlobs
          ? pairScopeDeviations(changedFiles, scopeGlobs)
          : assignment.deviations);
      if ((state === "awaiting-user" || state === "completed") && deviations.length > 0) {
        return reject(
          "scope-deviation",
          `Changes outside the assignment scope: ${deviations.slice(0, 10).join(", ")}. Request changes to revert them, or ask the user to widen the scope.`,
        );
      }
      return accept({
        ...room,
        ...touched,
        assignments: replaceById(room.assignments, "assignmentId", {
          ...assignment,
          state,
          note: command.note !== undefined ? clampNullable(command.note) : assignment.note,
          report: command.report ?? assignment.report,
          changedFiles: [...changedFiles],
          deviations: [...deviations],
          scopeGlobs: [...scopeGlobs],
          integrationCommit: command.integrationCommit ?? assignment.integrationCommit,
          updatedAt: command.at,
        }),
      });
    }

    case "decision.record": {
      if (room.status === "closed") return reject("room-closed", "This pair room is closed.");
      if (room.decisions.some((existing) => existing.decisionId === command.decisionId)) {
        return reject("conflict", `Decision ${command.decisionId} already exists.`);
      }
      const base: PairDecision = {
        decisionId: command.decisionId,
        kind: command.kind,
        category: command.category,
        title: clampTitle(command.title),
        positions: [],
        leadRecommendation: clampNullable(command.leadRecommendation),
        consequenceOfDeferring: clampNullable(command.consequenceOfDeferring),
        resolution: null,
        resolvedBy: null,
        createdAt: command.at,
        updatedAt: command.at,
      };
      const decision: PairDecision = {
        ...base,
        positions: upsertPosition(base, command.actor, command.position),
        ...leadResolution(base, command.actor, command.resolution),
      };
      return accept({ ...room, ...touched, decisions: [...room.decisions, decision] });
    }

    case "decision.add-position": {
      if (room.status === "closed") return reject("room-closed", "This pair room is closed.");
      const decision = room.decisions.find(
        (candidate) => candidate.decisionId === command.decisionId,
      );
      if (!decision) return reject("not-found", `Decision ${command.decisionId} was not found.`);
      return accept({
        ...room,
        ...touched,
        decisions: replaceById(room.decisions, "decisionId", {
          ...decision,
          positions: upsertPosition(decision, command.actor, command.position),
          ...leadResolution(decision, command.actor, command.resolution),
          updatedAt: command.at,
        }),
      });
    }

    case "decision.resolve": {
      const decision = room.decisions.find(
        (candidate) => candidate.decisionId === command.decisionId,
      );
      if (!decision) return reject("not-found", `Decision ${command.decisionId} was not found.`);
      if (command.resolvedBy === "lead") {
        if (!pairDecisionLeadMayResolve(decision.category)) {
          return reject(
            "decision-authority",
            `A ${decision.category} decision needs the user. Leave it open with your recommendation.`,
          );
        }
        if (decision.resolvedBy === "user") {
          return reject("decision-authority", "The user already resolved this decision.");
        }
      }
      return accept({
        ...room,
        ...touched,
        decisions: replaceById(room.decisions, "decisionId", {
          ...decision,
          resolution: clampPairText(command.resolution),
          resolvedBy: command.resolvedBy,
          updatedAt: command.at,
        }),
      });
    }

    case "lead.switch-start": {
      const blocked = requireWritable(room);
      if (blocked) return { ok: false, rejection: blocked };
      if (room.consults.some((consult) => consult.status === "running")) {
        return reject(
          "peer-busy",
          "The Peer is answering a consult. Cancel it or wait before switching the Lead.",
        );
      }
      const lead = pairRoomParticipant(room, "lead");
      if (!lead?.threadId) return reject("invalid", "The pair room has no Lead thread yet.");
      return accept({
        ...room,
        ...touched,
        leadSwitch: {
          toPersona: otherPairPersona(lead.persona),
          phase: "drafting",
          requestedAt: command.at,
          handoff: null,
          error: null,
        },
      });
    }

    case "lead.switch-draft": {
      if (room.leadSwitch?.phase !== "drafting") {
        return reject("invalid", "No Lead handoff is being drafted.");
      }
      const handoff = command.handoff?.trim()
        ? clampPairText(command.handoff, PAIR_ROOM_HANDOFF_MAX_LENGTH)
        : null;
      return accept({
        ...room,
        ...touched,
        leadSwitch: {
          ...room.leadSwitch,
          phase: handoff === null ? "failed" : "ready",
          handoff,
          error:
            handoff === null
              ? clampPairText(command.error ?? "The Lead did not write a handoff.")
              : null,
        },
      });
    }

    case "lead.switch-confirm": {
      if (room.leadSwitch?.phase !== "ready") {
        return reject("invalid", "The handoff is not ready to confirm yet.");
      }
      const lead = pairRoomParticipant(room, "lead");
      const peer = pairRoomParticipant(room, "peer");
      if (!lead || !peer) return reject("invalid", "The pair room is missing a participant.");
      const retired = [lead, peer].flatMap((participant) =>
        participant.threadId
          ? [
              {
                persona: participant.persona,
                role: participant.role,
                threadId: participant.threadId,
                until: command.at,
              },
            ]
          : [],
      );
      return accept({
        ...room,
        ...touched,
        participants: [
          { persona: room.leadSwitch.toPersona, role: "lead", threadId: command.newLeadThreadId },
          // The new Peer gets a fresh thread in the review worktree on its first consult.
          { persona: lead.persona, role: "peer", threadId: null },
        ],
        extraRounds: null,
        leadSwitch: null,
        formerParticipants: [...room.formerParticipants, ...retired].slice(
          -PAIR_ROOM_FORMER_PARTICIPANTS_KEPT,
        ),
      });
    }

    case "lead.switch-cancel": {
      if (room.leadSwitch === null) return accept(room);
      return accept({ ...room, ...touched, leadSwitch: null });
    }
  }
}
