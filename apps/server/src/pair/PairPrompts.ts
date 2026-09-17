import {
  PAIR_PERSONAS,
  type PairAssignment,
  type PairConsultKind,
  type PairPersona,
  type PairRoomMode,
} from "@t3tools/contracts";

/**
 * Messages the server sends into participant and assignment threads. They
 * arrive as ordinary user turns, so they say who is speaking and what the
 * receiving agent may and may not do.
 */

const name = (persona: PairPersona) => PAIR_PERSONAS[persona].displayName;

const bulletList = (items: ReadonlyArray<string>) =>
  items.length === 0 ? "- (none)" : items.map((item) => `- ${item}`).join("\n");

const CONSULT_ASKS: Readonly<Record<PairConsultKind, string>> = {
  critique:
    "Critique the Lead's approach: point out bugs, risks, missing cases and simpler alternatives. Say plainly when you agree.",
  review:
    "Review the Lead's changes in this snapshot for correctness, regressions, missing tests and scope creep. Lead with the most important finding.",
  question: "Answer the Lead's question with evidence from the code.",
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
        origin: `Pair Room consult from ${name(input.lead)} (Lead). ${you}`,
        ask: CONSULT_ASKS[input.kind],
      };
    case "review":
      return {
        origin: `Pair Room automatic review of ${name(input.lead)}'s (Lead) last turn. ${you}`,
        ask: CONSULT_ASKS.review,
      };
    case "user":
      return input.kind === "roundtable"
        ? {
            origin: `Pair Room roundtable: the user's message below went to both participants. ${you} ${name(input.lead)} (Lead) is answering it at the same time without seeing your answer, then compares both and gives the user the final answer.`,
            ask: "Give your own independent answer or proposal.",
          }
        : {
            origin: `Pair Room: the user addressed you directly. ${you} ${name(input.lead)} (Lead) is replying to the same message and responds to your answer once you finish.`,
            ask: "Answer the user's message with evidence from the code.",
          };
  }
}

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
      : `Kind: ${input.kind}. Round ${input.round} of ${input.roundLimit} for this Lead turn.`,
    `Your working directory is a snapshot of the Lead's checkout (${input.snapshotCommit.slice(0, 12)}), including uncommitted files. Read anything you need. Do not edit files: edits here are thrown away and flagged.`,
    ask,
    "Reply concisely with conclusions and evidence (file paths, line numbers, commands you ran). If you and the Lead disagree on something material, say so and call pair_record_decision.",
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

/** Brings the Peer's answer to a relayed user message back to the Lead. */
export function peerAnswerPrompt(input: {
  readonly lead: PairPersona;
  readonly peer: PairPersona;
  readonly kind: PairConsultKind;
  readonly answer: string;
}): string {
  const peer = name(input.peer);
  const ask =
    input.kind === "roundtable"
      ? `Pair Room roundtable: ${peer} (Peer) answered the user's last message independently, and the user can see that answer in the room. Compare it with yours: say briefly where you agree, where you differ and why, then give the final recommendation. Do not restate either answer in full. If you disagree on something material that the user should decide, record it with pair_record_decision.`
      : `Pair Room: the user asked ${peer} (Peer) directly, and the user can see ${peer}'s answer in the room. Respond to it in a few sentences: whether you agree, what you would add or change, and what to do next. Do not restate it.`;
  return [ask, "", `${peer}'s answer:`, "", input.answer].join("\n");
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
    "When you are done, call pair_submit with a summary, each criterion's result with evidence, the tests you ran and known limitations. If you are blocked, call pair_report_progress with blocked set and your question, then stop. Call pair_status any time to see the room.",
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
  lines.push("", "Call pair_submit again when the changes are done.");
  return lines.join("\n");
}

export const resumeRequest = (assignment: PairAssignment) =>
  `The user resumed this Pair Room assignment ("${assignment.title}"). Check the worktree state, continue where the work stopped, and call pair_submit when done.`;

const MODE_GUIDANCE: Readonly<Record<PairRoomMode, string>> = {
  adaptive:
    "Adaptive: consult the Peer when a second opinion would change the outcome (risky changes, unclear requirements, debugging dead ends). Skip it for routine work.",
  pair: "Pair: review-first. Consult the Peer before finalizing any code change. If you finish a turn that changed files without a review consult, the server starts one automatically.",
  roundtable:
    "Roundtable: the server sends each user message to the Peer as well, and starts a turn for you with the Peer's independent answer once your own turn ends. Answer the user's message yourself first, without waiting for the Peer or consulting it about that message. When the Peer's answer arrives, compare and give the final recommendation.",
};

export function roleGuidance(input: {
  readonly mode: PairRoomMode;
  readonly role: "lead" | "peer" | "assignee";
}): string {
  switch (input.role) {
    case "lead":
      return `You are the Lead. You own the reply to the user and either implement or delegate with pair_assign. ${MODE_GUIDANCE[input.mode]} Assignments are reviewed with pair_review; only the user merges them.`;
    case "peer":
      return "You are the Peer. Answer consults with critique and evidence. Do not edit the Lead's files. Record material disagreements with pair_record_decision.";
    case "assignee":
      return "You are working a bounded assignment. Stay inside its scope, report blockers with pair_report_progress, and finish with pair_submit.";
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
