import {
  PAIR_CONVERSATION_MAX_EXCHANGES,
  PAIR_PERSONAS,
  type PairAssignment,
  type PairConsultKind,
  type PairPersona,
  type PairRoomMode,
} from "@t3tools/contracts";

/**
 * Messages the server sends into participant and assignment threads. They
 * arrive as ordinary user turns, so they say who is speaking, where the
 * conversation stands, and what the receiving agent may and may not do.
 *
 * The voice is deliberate: the two are a team. The Lead decides, after it
 * understands the Peer; the Peer digs, verifies, asks and concedes.
 */

const name = (persona: PairPersona) => PAIR_PERSONAS[persona].displayName;

const bulletList = (items: ReadonlyArray<string>) =>
  items.length === 0 ? "- (none)" : items.map((item) => `- ${item}`).join("\n");

/** Where a conversation stands, and what the last exchange means for it. */
function exchangeLine(exchange: number, limit = PAIR_CONVERSATION_MAX_EXCHANGES): string {
  if (exchange >= limit) {
    return `Exchange ${exchange} of ${limit}: this is the last one in this conversation. Say where you land; anything still unresolved goes to pair_record_decision for the user.`;
  }
  if (exchange === limit - 1) {
    return `Exchange ${exchange} of ${limit}: one more after this, so get to what matters.`;
  }
  return `Exchange ${exchange} of ${limit}.`;
}

const PEER_STANCE =
  "You are the second pair of eyes, and the sharper one for detail. Find what was missed, then verify it before you claim it: read the code, run the check, cite file and line. Say what is right as plainly as what is wrong. If you need something only the Lead knows, ask instead of guessing.";

const PEER_ASK =
  "To ask the Lead something before this rests, call pair_ask with the question, then finish your reply and stop; the answer starts your next turn. Otherwise just reply.";

const PEER_DISAGREE =
  "If you and the Lead still disagree on something material after hearing each other, record it with pair_record_decision so the user can settle it.";

const CONSULT_ASKS: Readonly<Record<PairConsultKind, string>> = {
  critique:
    "The Lead wants its approach tested. Look for bugs, risks, missing cases and simpler alternatives, and check the ones you find. Say what holds up too.",
  review:
    "The Lead wants its changes checked. Verify correctness, regressions, missing tests and scope creep against the snapshot, and lead with the finding that matters most.",
  question: "The Lead has a question. Answer it from the code, and say how sure you are.",
  roundtable:
    "Give your own independent proposal. The Lead has already recorded theirs and will compare after you answer, so do not guess what it is.",
};

/**
 * Who asked: the Lead through pair_consult, the pair mode review guardrail,
 * or the user, whose message the server sent to the Peer as well.
 */
export type PairConsultSource = "lead" | "review" | "user";

function consultOrigin(input: {
  readonly lead: PairPersona;
  readonly peer: PairPersona;
  readonly kind: PairConsultKind;
  readonly source: PairConsultSource;
}): { readonly origin: string; readonly ask: string } {
  const you = `You are ${name(input.peer)} (Peer).`;
  switch (input.source) {
    case "lead":
      return {
        origin: `Pair Room: ${name(input.lead)} (Lead) is asking you. ${you}`,
        ask: CONSULT_ASKS[input.kind],
      };
    case "review":
      return {
        origin: `Pair Room: automatic review of ${name(input.lead)}'s (Lead) last turn. ${you}`,
        ask: CONSULT_ASKS.review,
      };
    case "user":
      return input.kind === "roundtable"
        ? {
            origin: `Pair Room roundtable: the user's message below went to both of you. ${you} ${name(input.lead)} (Lead) is answering it at the same time without seeing your answer, then compares both and gives the user the final answer.`,
            ask: "Give your own independent answer or proposal.",
          }
        : {
            origin: `Pair Room: the user addressed you directly. ${you} ${name(input.lead)} (Lead) is replying to the same message and responds to your answer once you finish.`,
            ask: "Answer the user's message from the code.",
          };
  }
}

/** The first exchange of a conversation: a consult from the Lead, the review guardrail, or the user. */
export function consultPrompt(input: {
  readonly lead: PairPersona;
  readonly peer: PairPersona;
  readonly kind: PairConsultKind;
  readonly source: PairConsultSource;
  readonly round: number;
  readonly roundLimit: number | null;
  readonly snapshotCommit: string;
  readonly question: string;
  readonly focusPaths: ReadonlyArray<string>;
}): string {
  const { origin, ask } = consultOrigin(input);
  const lines = [
    origin,
    input.roundLimit === null
      ? `Kind: ${input.kind}.`
      : `Kind: ${input.kind}. Conversation ${input.round} of ${input.roundLimit} for this Lead turn; ${exchangeLine(1)}`,
    `Your working directory is a snapshot of the Lead's checkout (${input.snapshotCommit.slice(0, 12)}), including uncommitted files. Read and run what you need. Do not edit files: edits here are thrown away and flagged.`,
    PEER_STANCE,
    ask,
    input.source === "user" ? PEER_DISAGREE : `${PEER_ASK} ${PEER_DISAGREE}`,
    "Reply with conclusions and evidence, not a transcript of your reasoning.",
  ];
  if (input.focusPaths.length > 0) {
    lines.push(`Focus on:\n${bulletList(input.focusPaths)}`);
  }
  lines.push(
    "",
    input.source === "user" ? `The user's message:\n\n${input.question}` : input.question,
  );
  return lines.join("\n");
}

/** The Lead's reply in a running conversation, sent to the Peer. */
export function replyPrompt(input: {
  readonly lead: PairPersona;
  readonly peer: PairPersona;
  readonly kind: PairConsultKind;
  readonly exchange: number;
  readonly snapshotCommit: string;
  readonly message: string;
}): string {
  return [
    `Pair Room: ${name(input.lead)} (Lead) replies in your ${input.kind} conversation. You are ${name(input.peer)} (Peer). ${exchangeLine(input.exchange)}`,
    `Your snapshot of the Lead's checkout is refreshed (${input.snapshotCommit.slice(0, 12)}). Do not edit files.`,
    "Take the reply seriously and check it: if it answers you, say so and move on; if it is wrong or incomplete, show why with evidence; if it changes your mind, say that plainly.",
    `${PEER_ASK} ${PEER_DISAGREE}`,
    "",
    `${name(input.lead)}'s reply:`,
    "",
    input.message,
  ].join("\n");
}

/** The Lead's final answer to the user, sent to the Peer to check before the conversation rests. */
export function signOffPrompt(input: {
  readonly lead: PairPersona;
  readonly peer: PairPersona;
  readonly exchange: number;
  readonly snapshotCommit: string;
  readonly topics: ReadonlyArray<string>;
  readonly answer: string;
}): string {
  return [
    `Pair Room: ${name(input.lead)}'s (Lead) turn has ended, and this is what it told the user. You are ${name(input.peer)} (Peer). You talked during that turn about:\n${bulletList(input.topics)}`,
    exchangeLine(input.exchange),
    `Your snapshot of the Lead's checkout is refreshed (${input.snapshotCommit.slice(0, 12)}), so you can check what actually landed. Do not edit files.`,
    "Check the answer against what you raised and against the code: what was taken, what was dropped and whether the reason holds, and anything new you notice now that the work is done. Verify before you object.",
    `If ${name(input.lead)} should hear something before this rests, call pair_ask with it, then finish your reply and stop; it reaches ${name(input.lead)} as a turn. If you are aligned, a few sentences saying so are enough. ${PEER_DISAGREE}`,
    "",
    `${name(input.lead)}'s answer to the user:`,
    "",
    input.answer,
  ].join("\n");
}

/** Brings the user's answer to a decision back to the Lead, when no tool call is waiting for it. */
export function decisionResolved(input: {
  readonly title: string;
  readonly category: string;
  readonly resolution: string;
}): string {
  return [
    `Pair Room: the user settled the open ${input.category} decision "${input.title}".`,
    "",
    `Their answer: ${input.resolution}`,
    "",
    "Act on it and tell the user what you are doing. It is settled: do not ask again or reopen it.",
  ].join("\n");
}

/** How a Peer reply reached the Lead as a turn rather than through a waiting tool call. */
export type PeerReplyDelivery =
  | { readonly kind: "roundtable" }
  | { readonly kind: "relay" }
  /** An answer to a consult the Lead opened, arriving after the Lead's turn ended. */
  | { readonly kind: "consult"; readonly consultKind: PairConsultKind; readonly exchange: number }
  /** The Peer asked the Lead something, via pair_ask, in a consult or a sign-off. */
  | {
      readonly kind: "ask";
      readonly ask: string;
      readonly exchange: number;
      readonly signOff: boolean;
    };

/** Brings the Peer's reply to the Lead as a turn. */
export function peerReplyPrompt(input: {
  readonly lead: PairPersona;
  readonly peer: PairPersona;
  readonly delivery: PeerReplyDelivery;
  readonly handle: string;
  readonly answer: string;
}): string {
  const peer = name(input.peer);
  const reply = `Reply to ${peer} with pair_reply (handle ${input.handle}) when there is something to answer or argue; it waits for ${peer}'s next reply like pair_consult does.`;
  const { delivery } = input;
  const ask = (() => {
    switch (delivery.kind) {
      case "roundtable":
        return `Pair Room roundtable: ${peer} (Peer) answered the user's last message independently, and the user can see that answer in the room. Compare it with yours: say briefly where you agree, where you differ and why, then give the final recommendation. Do not restate either answer in full. If you disagree on something material that the user should decide, record it with pair_record_decision.`;
      case "relay":
        return `Pair Room: the user asked ${peer} (Peer) directly, and the user can see ${peer}'s answer in the room. Respond to it in a few sentences: whether you agree, what you would add or change, and what to do next. Do not restate it.`;
      case "consult":
        return `Pair Room: ${peer} (Peer) answered the ${delivery.consultKind} you asked for after your turn had ended. ${exchangeLine(delivery.exchange)} Read it as you would a colleague's: take what holds, say why where you disagree, and tell the user what changes as a result. ${reply}`;
      case "ask":
        return delivery.signOff
          ? `Pair Room: ${peer} (Peer) checked your last answer to the user and has something you should hear before it rests. ${exchangeLine(delivery.exchange)} Consider it honestly: fix what it got right, explain what you are keeping and why. ${reply} If it is settled, tell the user where you landed instead.`
          : `Pair Room: ${peer} (Peer) needs an answer from you to continue. ${exchangeLine(delivery.exchange)} ${reply}`;
    }
  })();
  const question = delivery.kind === "ask" ? [`${peer} asks: ${delivery.ask}`, ""] : [];
  return [ask, "", ...question, `${peer}'s reply:`, "", input.answer].join("\n");
}

const ARTIFACT_INSTRUCTIONS: Readonly<Record<PairAssignment["expectedArtifact"], string>> = {
  findings: "Do not change files. Investigate and report findings.",
  patch: "Leave your changes in this worktree. The user merges them after the Lead reviews.",
  commit: "Commit your work on this branch with clear messages.",
};

export function assignmentBrief(input: {
  readonly lead: PairPersona;
  readonly assignment: PairAssignment;
  readonly brief: string;
}): string {
  const { assignment } = input;
  return [
    `Pair Room assignment from ${name(input.lead)} (Lead). You are ${name(assignment.owner)}, working in your own worktree on branch ${assignment.branch}, based on ${assignment.baseCommit.slice(0, 12)}.`,
    "",
    `Objective: ${assignment.title}`,
    "",
    input.brief,
    "",
    `Only change files matching:\n${bulletList(assignment.scopeGlobs)}`,
    "Changes outside that scope are flagged and block approval.",
    "",
    `Acceptance criteria:\n${bulletList(assignment.acceptanceCriteria)}`,
    "",
    ARTIFACT_INSTRUCTIONS[assignment.expectedArtifact],
    "",
    `When you are done, call pair_submit with a summary, each criterion's result with evidence, the tests you ran and known limitations. If the brief is unclear or you hit something only ${name(input.lead)} can decide, call pair_ask with the question and stop; the answer starts your next turn. Call pair_status any time to see the room.`,
  ].join("\n");
}

/** The Lead's answer to a blocked assignee, sent to the assignment thread. */
export function assigneeReplyPrompt(input: {
  readonly lead: PairPersona;
  readonly assignment: PairAssignment;
  readonly message: string;
}): string {
  return [
    `Pair Room: ${name(input.lead)} (Lead) answered your question on "${input.assignment.title}". Continue the assignment with it, and call pair_submit when done.`,
    "",
    `${name(input.lead)}'s answer:`,
    "",
    input.message,
  ].join("\n");
}

/** Brings an assignee's blocker to the Lead as a turn. */
export function assigneeBlockedPrompt(input: { readonly assignment: PairAssignment }): string {
  const owner = name(input.assignment.owner);
  return [
    `Pair Room: ${owner} is blocked on the assignment "${input.assignment.title}" and is waiting for you.`,
    "",
    `${owner} reports: ${input.assignment.note ?? "(no note)"}`,
    "",
    `Answer with pair_reply (handle ${input.assignment.assignmentId}); the answer starts ${owner}'s next turn. If the assignment should change instead, say so to the user.`,
  ].join("\n");
}

export function revisionRequest(input: {
  readonly lead: PairPersona;
  readonly notes: string;
  readonly deviations: ReadonlyArray<string>;
}): string {
  const lines = [
    `${name(input.lead)} (Lead) reviewed your submission and requested changes.`,
    "",
    input.notes,
  ];
  if (input.deviations.length > 0) {
    lines.push("", `Revert these changes outside your scope:\n${bulletList(input.deviations)}`);
  }
  lines.push(
    "",
    "If a requested change is wrong, say why with pair_ask before making it. Call pair_submit again when the changes are done.",
  );
  return lines.join("\n");
}

export const resumeRequest = (assignment: PairAssignment) =>
  `The user resumed this Pair Room assignment ("${assignment.title}"). Check the worktree state, continue where the work stopped, and call pair_submit when done.`;

const MODE_GUIDANCE: Readonly<Record<PairRoomMode, string>> = {
  adaptive:
    "Adaptive: bring the Peer in when a second mind would change the outcome (design choices, risky changes, unclear requirements, debugging dead ends), and skip it for routine work. When in doubt, ask.",
  pair: "Pair: review-first. Consult the Peer before finalizing any code change. If you finish a turn that changed files without a review, the server starts one and brings the findings to you.",
  roundtable:
    "Roundtable: the server sends each user message to the Peer as well, and starts a turn for you with the Peer's independent answer once your own turn ends. Answer the user yourself first, without waiting for the Peer or consulting it about that message. When the Peer's answer arrives, compare and give the final recommendation.",
};

export function roleGuidance(input: {
  readonly mode: PairRoomMode;
  readonly role: "lead" | "peer" | "assignee";
}): string {
  switch (input.role) {
    case "lead":
      return `You are the Lead. You own the reply to the user and have the final say, which you use after you understand what the Peer thinks and why, not instead of it. Ask, reply, change your mind when the evidence says so, and say why when you keep your position. ${MODE_GUIDANCE[input.mode]} Delegate bounded work with pair_assign, review it with pair_review; only the user merges.`;
    case "peer":
      return `You are the Peer. ${PEER_STANCE} You do not edit the Lead's files. When you still disagree after hearing the Lead out, record it with pair_record_decision.`;
    case "assignee":
      return "You are working a bounded assignment. Stay inside its scope, ask the Lead with pair_ask when the brief does not settle something, and finish with pair_submit.";
  }
}

export const handoffRequest = (input: { readonly from: PairPersona; readonly to: PairPersona }) =>
  [
    `Pair Room: the user is handing the Lead role from ${name(input.from)} to ${name(input.to)}.`,
    "Do not start new work, consult or assign. Write a handoff that lets the new Lead continue without this thread's history. Your reply is the handoff, and the user reads it before confirming.",
    "Use these sections, with file paths, commands and branch names where they apply:",
    "- Objective",
    "- Plan",
    "- Done",
    "- In progress",
    "- Changed files",
    "- Tests run and their results",
    "- Decisions and disagreements",
    "- Risks",
    "- Open questions for the user",
    "- Next actions",
  ].join("\n");

export interface HandoffFacts {
  readonly assignments: ReadonlyArray<PairAssignment>;
  readonly openDecisions: ReadonlyArray<{ readonly title: string; readonly category: string }>;
  readonly cwd: string;
  readonly branch: string | null;
}

export function leadHandoff(input: {
  readonly from: PairPersona;
  readonly to: PairPersona;
  readonly handoff: string;
  readonly facts: HandoffFacts;
}): string {
  const assignments = input.facts.assignments.map(
    (assignment) =>
      `${assignment.title} (${name(assignment.owner)}, ${assignment.state}, branch ${assignment.branch})`,
  );
  const decisions = input.facts.openDecisions.map(
    (decision) => `${decision.title} (${decision.category})`,
  );
  return [
    `Pair Room: you are ${name(input.to)}, and you are now the Lead. ${name(input.from)} led until now and is your Peer from here on. The user confirmed this handoff.`,
    `Your working directory is the room's checkout (${input.facts.cwd}${input.facts.branch ? `, branch ${input.facts.branch}` : ""}). Call pair_status first.`,
    "",
    `Handoff from ${name(input.from)}:`,
    "",
    input.handoff,
    "",
    `Assignments, as the server records them:\n${bulletList(assignments)}`,
    "",
    `Open decisions:\n${bulletList(decisions)}`,
    "",
    "Assignments keep their owners. Confirm your understanding with the user in a short reply before you change anything.",
  ].join("\n");
}
