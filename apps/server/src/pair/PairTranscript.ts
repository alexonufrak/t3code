import {
  PAIR_PERSONAS,
  pairRoomParticipant,
  type ChatAttachment,
  type MessageId,
  type OrchestrationMessage,
  type PairPersona,
  type PairRoomNote,
  type ThreadId,
} from "@t3tools/contracts";
import { readPairRoomNote } from "@t3tools/shared/pairRoomNote";
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { PairRoomStore } from "./PairRoomStore.ts";
import { PairTranscriptPrelude } from "./PairTranscriptPrelude.ts";
import { clampPairText } from "./pairRoomDecider.ts";

/**
 * The room transcript: what the user and one participant said in that
 * participant's thread, copied into the other participant's thread as
 * user-role messages that start no turn. A provider only ever reads the
 * message a turn carries, so the copies reach the model through
 * `PairTranscriptPrelude`, which folds every line the thread has not run yet
 * into the front of its next turn.
 */

/** One copied line is cut here; the source thread keeps all of it. */
export const PAIR_TRANSCRIPT_LINE_MAX_LENGTH = 1500;
/** A fold is cut here, oldest lines first, and says how many it dropped. */
export const PAIR_TRANSCRIPT_FOLD_MAX_LENGTH = 10_000;
/** How far back a fresh participant thread is caught up when it is created. */
export const PAIR_TRANSCRIPT_BOOTSTRAP_MESSAGES = 30;

const name = (persona: PairPersona) => PAIR_PERSONAS[persona].displayName;

/**
 * The id stem of one copied line, fixed by where it goes and what it copies,
 * so a replay lands on the same command receipt. Hashed because ids have a
 * length cap and context ids a character set, and a source id is neither.
 */
export const pairTranscriptKey = (targetThreadId: ThreadId, source: string) =>
  NodeCrypto.createHash("sha256").update(`${targetThreadId}\n${source}`).digest("hex").slice(0, 32);

export type PairTranscriptSpeaker = NonNullable<PairRoomNote["source"]>["speaker"];

/** "You → Astra: …" or "Astra (Lead) → you: …", with attachments named since they do not travel. */
export function pairTranscriptLine(input: {
  readonly speaker: PairTranscriptSpeaker;
  readonly persona: PairPersona;
  readonly role: "lead" | "peer";
  readonly text: string;
  readonly attachments?: ReadonlyArray<ChatAttachment> | undefined;
}): string {
  const who =
    input.speaker === "user"
      ? `You → ${name(input.persona)}`
      : `${name(input.persona)} (${input.role === "lead" ? "Lead" : "Peer"}) → you`;
  const attached =
    input.attachments && input.attachments.length > 0
      ? ` [attached: ${input.attachments.map((attachment) => attachment.name).join(", ")}]`
      : "";
  return `${who}: ${clampPairText(input.text, PAIR_TRANSCRIPT_LINE_MAX_LENGTH)}${attached}`;
}

/** The Peer edits a review worktree, so its changes are named as such rather than as the Lead's files. */
export const pairTranscriptFilesLine = (
  persona: PairPersona,
  role: "lead" | "peer",
  files: ReadonlyArray<string>,
) =>
  `${name(persona)} changed${role === "peer" ? " in its review worktree (not your files)" : ""}: ${files
    .slice(0, 30)
    .join(", ")}${files.length > 30 ? ` and ${files.length - 30} more` : ""}`;

export const pairTranscriptStoppedLine = (persona: PairPersona) =>
  `${name(persona)}'s turn was stopped before it answered.`;

const isTranscriptLine = (message: OrchestrationMessage) =>
  message.role === "user" && readPairRoomNote(message.context)?.purpose === "transcript";

export interface PairTranscriptSource {
  readonly speaker: PairTranscriptSpeaker;
  readonly message: OrchestrationMessage;
}

/**
 * What a thread contributes to the transcript, in thread order: messages the
 * user typed there (room-started turns and copied lines carry a note) and the
 * final answer of each turn.
 */
export function pairTranscriptSources(
  messages: ReadonlyArray<OrchestrationMessage>,
): ReadonlyArray<PairTranscriptSource> {
  const finalByTurn = new Map<string, MessageId>();
  for (const message of messages) {
    if (message.role === "assistant" && message.turnId && !message.streaming) {
      finalByTurn.set(message.turnId, message.id);
    }
  }
  const sources: Array<PairTranscriptSource> = [];
  for (const message of messages) {
    if (message.role === "user" && readPairRoomNote(message.context) === null) {
      sources.push({ speaker: "user", message });
    } else if (
      message.role === "assistant" &&
      message.turnId &&
      finalByTurn.get(message.turnId) === message.id &&
      message.text.trim().length > 0
    ) {
      sources.push({ speaker: "agent", message });
    }
  }
  return sources;
}

/**
 * The transcript lines a thread has not run yet: every copied line since the
 * last message that started a turn there, up to (not including) the message
 * that starts this one. Copies are appended in order, so thread order is the
 * fold order.
 */
export function pairTranscriptPending(
  messages: ReadonlyArray<OrchestrationMessage>,
  turnMessageId: MessageId,
): ReadonlyArray<OrchestrationMessage> {
  const end = messages.findIndex((message) => message.id === turnMessageId);
  const before = end === -1 ? messages : messages.slice(0, end);
  // A turn that carries a line itself (a sign-off with the Lead's answer) does not repeat it.
  const carried =
    end === -1 ? undefined : readPairRoomNote(messages[end]!.context)?.source?.messageId;
  const pending: Array<OrchestrationMessage> = [];
  for (let index = before.length - 1; index >= 0; index -= 1) {
    const message = before[index]!;
    if (isTranscriptLine(message)) {
      if (
        carried === undefined ||
        readPairRoomNote(message.context)?.source?.messageId !== carried
      ) {
        pending.push(message);
      }
    } else if (message.role === "user") {
      break;
    }
  }
  return pending.toReversed();
}

/**
 * The catch-up block for the front of a turn, or none when nothing is pending.
 * Newest lines win the budget; a dropped count keeps the cut honest.
 */
export function pairTranscriptFold(
  pending: ReadonlyArray<OrchestrationMessage>,
  budget = PAIR_TRANSCRIPT_FOLD_MAX_LENGTH,
): Option.Option<string> {
  if (pending.length === 0) return Option.none();
  const kept: Array<string> = [];
  let used = 0;
  let omitted = 0;
  for (let index = pending.length - 1; index >= 0; index -= 1) {
    const line = pending[index]!.text;
    if (kept.length > 0 && used + line.length + 1 > budget) {
      omitted = index + 1;
      break;
    }
    kept.push(line);
    used += line.length + 1;
  }
  const lines = [
    "Catching up on the Pair Room since your last turn. These lines are what the user and the other participant said elsewhere in the room, labeled by speaker. You do not answer them here; the request follows below.",
    "",
    ...(omitted > 0
      ? [
          `(${omitted} earlier line${omitted === 1 ? "" : "s"} omitted; pair_read_thread shows them.)`,
        ]
      : []),
    ...kept.toReversed(),
    "",
    "---",
    "",
    "",
  ];
  return Option.some(lines.join("\n"));
}

/** The final answer of a completed turn: its last finished assistant message. */
export const pairFinalAnswer = (messages: ReadonlyArray<OrchestrationMessage>, turnId: string) =>
  messages.findLast(
    (message) => message.role === "assistant" && message.turnId === turnId && !message.streaming,
  ) ?? null;

export const make = Effect.gen(function* () {
  const rooms = yield* PairRoomStore;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const forTurn: PairTranscriptPrelude["Service"]["forTurn"] = (threadId, messageId) =>
    Effect.gen(function* () {
      const room = yield* rooms.findByThread(threadId);
      // Only participant threads carry transcript lines; assignments get a brief.
      if (Option.isNone(room) || !pairRoomParticipant(room.value, "lead")) return Option.none();
      const isParticipant = room.value.participants.some((entry) => entry.threadId === threadId);
      if (!isParticipant) return Option.none();
      const detail = yield* snapshots.getThreadDetailById(threadId);
      if (Option.isNone(detail)) return Option.none();
      return pairTranscriptFold(pairTranscriptPending(detail.value.messages, messageId));
    }).pipe(
      // A missing prelude costs context, never the turn.
      Effect.catchCause((cause) =>
        Effect.logWarning("pair transcript prelude failed", { threadId, cause }).pipe(
          Effect.as(Option.none<string>()),
        ),
      ),
    );
  return PairTranscriptPrelude.of({ forTurn });
});

export const layer = Layer.effect(PairTranscriptPrelude, make);
export { PairTranscriptPrelude };
