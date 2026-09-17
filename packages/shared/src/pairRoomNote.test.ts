import { OrchestrationMessageContext } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "@effect/vitest";

import { projectComposerContextForProvider } from "./composerContextReferences.ts";
import { pairRoomNoteContext, pairRoomNoteTitle, readPairRoomNote } from "./pairRoomNote.ts";

const roundTrip = Schema.decodeUnknownSync(OrchestrationMessageContext);
const encodeContext = Schema.encodeSync(OrchestrationMessageContext);

describe("pairRoomNote", () => {
  it("survives the message context wire schema", () => {
    const note = { purpose: "handoff", from: "fable", to: "astra" } as const;
    const context = roundTrip(encodeContext(pairRoomNoteContext("pair-room-note-1", note)));

    expect(readPairRoomNote(context)).toEqual(note);
    expect(pairRoomNoteTitle(note)).toBe("Fable handed off to Astra");
  });

  it("keeps the note out of the provider prompt", () => {
    const context = pairRoomNoteContext("pair-room-note-2", {
      purpose: "consult",
      from: "astra",
      to: "fable",
    });

    expect(
      projectComposerContextForProvider({ text: "Review this.", records: context.records }),
    ).toBe("Review this.");
  });

  it("reads messages the user wrote as having no note", () => {
    expect(readPairRoomNote(undefined)).toBeNull();
    expect(
      readPairRoomNote(
        roundTrip({
          version: 1,
          records: [
            {
              version: 1,
              contextId: "x",
              label: "Pair Room",
              kind: "pair-room-note",
              payload: { purpose: "gossip" },
            },
          ],
        }),
      ),
    ).toBeNull();
  });
});
