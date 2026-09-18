import {
  ComposerContextId,
  PAIR_PERSONAS,
  PAIR_ROOM_NOTE_CONTEXT_KIND,
  PairRoomNote,
  type OrchestrationMessageContext,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

const decodeNote = Schema.decodeUnknownOption(PairRoomNote);

/** Message context for a turn the room starts on its own; see `PAIR_ROOM_NOTE_CONTEXT_KIND`. */
export function pairRoomNoteContext(
  contextId: string,
  note: PairRoomNote,
): OrchestrationMessageContext {
  return {
    version: 1,
    records: [
      {
        version: 1,
        contextId: ComposerContextId.make(contextId),
        label: "Pair Room",
        kind: PAIR_ROOM_NOTE_CONTEXT_KIND,
        payload: note,
      },
    ],
  };
}

/** The room note a user-role message carries, or null for a message the user wrote. */
export function readPairRoomNote(
  context: OrchestrationMessageContext | undefined,
): PairRoomNote | null {
  const record = context?.records.find((entry) => entry.kind === PAIR_ROOM_NOTE_CONTEXT_KIND);
  if (!record || !("payload" in record)) return null;
  return Option.getOrNull(decodeNote(record.payload));
}

/** One line naming what the room sent, in the words the timeline shows. */
export function pairRoomNoteTitle(note: PairRoomNote): string {
  const from = PAIR_PERSONAS[note.from].displayName;
  const to = PAIR_PERSONAS[note.to].displayName;
  switch (note.purpose) {
    case "consult":
      return `${from} consulted ${to}`;
    case "assignment-brief":
      return `${from} assigned this work to ${to}`;
    case "revision":
      return `${from} asked ${to} for changes`;
    case "resume":
      return `You resumed ${to}'s assignment`;
    case "handoff-request":
      return `You asked ${from} to hand off to ${to}`;
    case "handoff":
      return `${from} handed off to ${to}`;
    case "user-relay":
      return `Your message, sent to ${to} too`;
    case "peer-answer":
      return `${from}'s answer, brought back to ${to}`;
    case "decision":
      return `Your decision, sent to ${to}`;
    case "transcript":
      return note.source?.speaker === "user" ? `You, to ${from}` : `${from}, in its own thread`;
    case "reply":
      return `${from}'s reply to ${to}`;
    case "sign-off":
      return `${from}'s answer, sent to ${to} to check`;
    case "blocked":
      return `${from} is blocked, brought to ${to}`;
  }
}
