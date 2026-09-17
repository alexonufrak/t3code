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

export function consultPrompt(input: {
  readonly lead: PairPersona;
  readonly peer: PairPersona;
  readonly kind: PairConsultKind;
  readonly round: number;
  readonly roundLimit: number | null;
  readonly automatic: boolean;
  readonly snapshotCommit: string;
  readonly question: string;
  readonly focusPaths: ReadonlyArray<string>;
}): string {
  const origin = input.automatic
    ? `Pair Room automatic review of ${name(input.lead)}'s (Lead) last turn. You are ${name(input.peer)} (Peer).`
    : `Pair Room consult from ${name(input.lead)} (Lead). You are ${name(input.peer)} (Peer).`;
  const lines = [
    origin,
    input.roundLimit === null
      ? `Kind: ${input.kind}.`
      : `Kind: ${input.kind}. Round ${input.round} of ${input.roundLimit} for this Lead turn.`,
    `Your working directory is a snapshot of the Lead's checkout (${input.snapshotCommit.slice(0, 12)}), including uncommitted files. Read anything you need. Do not edit files: edits here are thrown away and flagged.`,
    CONSULT_ASKS[input.kind],
    "Reply concisely with conclusions and evidence (file paths, line numbers, commands you ran). If you and the Lead disagree on something material, say so and call pair_record_decision.",
  ];
  if (input.focusPaths.length > 0) {
    lines.push(`Focus on:\n${bulletList(input.focusPaths)}`);
  }
  lines.push("", input.question);
  return lines.join("\n");
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
    "Roundtable: for design questions, form your own proposal first and pass it as leadProposal to pair_consult with kind roundtable, so both proposals are independent. Then synthesize.",
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
