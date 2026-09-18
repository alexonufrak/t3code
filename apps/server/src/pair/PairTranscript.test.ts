import { MessageId, ThreadId, type OrchestrationMessage } from "@t3tools/contracts";
import { pairRoomNoteContext } from "@t3tools/shared/pairRoomNote";
import * as Option from "effect/Option";
import { describe, expect, it } from "vite-plus/test";

import {
  PAIR_TRANSCRIPT_LINE_MAX_LENGTH,
  pairTranscriptFold,
  pairTranscriptLine,
  pairTranscriptPending,
  pairTranscriptSources,
} from "./PairTranscript.ts";

const message = (input: {
  readonly id: string;
  readonly role: OrchestrationMessage["role"];
  readonly text: string;
  readonly turnId?: string;
  readonly streaming?: boolean;
  readonly transcript?: boolean;
  readonly consult?: boolean;
  /** For a transcript line, the copied message; for a sign-off prompt, the answer it carries. */
  readonly sourceMessageId?: string;
}): OrchestrationMessage =>
  ({
    id: MessageId.make(input.id),
    role: input.role,
    text: input.text,
    attachments: [],
    turnId: input.turnId ?? null,
    streaming: input.streaming ?? false,
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    ...(input.transcript
      ? {
          context: pairRoomNoteContext(`note-${input.id}`, {
            purpose: "transcript",
            from: "astra",
            to: "fable",
            source: {
              speaker: "user",
              threadId: ThreadId.make("peer"),
              messageId: MessageId.make(input.sourceMessageId ?? `source-${input.id}`),
              createdAt: "2026-01-01T00:00:00.000Z",
            },
          }),
        }
      : {}),
    ...(input.consult
      ? {
          context: pairRoomNoteContext(`note-${input.id}`, {
            purpose: input.sourceMessageId ? "sign-off" : "consult",
            from: "fable",
            to: "astra",
            ...(input.sourceMessageId
              ? {
                  source: {
                    speaker: "agent" as const,
                    threadId: ThreadId.make("lead"),
                    messageId: MessageId.make(input.sourceMessageId),
                    createdAt: "2026-01-01T00:00:00.000Z",
                  },
                }
              : {}),
          }),
        }
      : {}),
  }) as unknown as OrchestrationMessage;

describe("pairTranscriptLine", () => {
  it("labels who spoke and to whom, naming attachments that do not travel", () => {
    expect(
      pairTranscriptLine({ speaker: "user", persona: "astra", role: "peer", text: "Check this" }),
    ).toBe("You → Astra: Check this");
    expect(
      pairTranscriptLine({
        speaker: "agent",
        persona: "fable",
        role: "lead",
        text: "Done.",
        attachments: [{ name: "diagram.png" }, { name: "notes.txt" }] as never,
      }),
    ).toBe("Fable (Lead) → you: Done. [attached: diagram.png, notes.txt]");
  });

  it("cuts a long line but keeps the label", () => {
    const line = pairTranscriptLine({
      speaker: "agent",
      persona: "astra",
      role: "peer",
      text: "x".repeat(PAIR_TRANSCRIPT_LINE_MAX_LENGTH * 2),
    });
    expect(line.startsWith("Astra (Peer) → you: ")).toBe(true);
    expect(line.length).toBeLessThan(PAIR_TRANSCRIPT_LINE_MAX_LENGTH + 100);
  });
});

describe("pairTranscriptSources", () => {
  it("keeps what the user typed and each turn's final answer, in thread order", () => {
    const sources = pairTranscriptSources([
      message({ id: "u1", role: "user", text: "Add retries" }),
      message({ id: "a1", role: "assistant", text: "Working…", turnId: "t1" }),
      message({ id: "a2", role: "assistant", text: "Added retries.", turnId: "t1" }),
      message({ id: "c1", role: "user", text: "consult prompt", consult: true }),
      message({ id: "x1", role: "user", text: "copied line", transcript: true }),
      message({ id: "a3", role: "assistant", text: "", turnId: "t2" }),
      message({ id: "a4", role: "assistant", text: "still typing", turnId: "t3", streaming: true }),
      message({ id: "r1", role: "reasoning", text: "thinking", turnId: "t3" }),
      message({ id: "u2", role: "user", text: "Now tests" }),
    ]);
    expect(sources.map((line) => [line.speaker, line.message.id])).toEqual([
      ["user", "u1"],
      ["agent", "a2"],
      ["user", "u2"],
    ]);
  });
});

describe("pairTranscriptPending", () => {
  const thread = [
    message({ id: "u1", role: "user", text: "Add retries" }),
    message({ id: "x1", role: "user", text: "old copied line", transcript: true }),
    message({ id: "a1", role: "assistant", text: "Added.", turnId: "t1" }),
    message({ id: "c1", role: "user", text: "consult prompt", consult: true }),
    message({ id: "a2", role: "assistant", text: "Looks fine.", turnId: "t2" }),
    message({ id: "x2", role: "user", text: "You → Astra: and jitter?", transcript: true }),
    message({ id: "x3", role: "user", text: "Fable (Lead) → you: yes", transcript: true }),
    message({ id: "u2", role: "user", text: "next request" }),
  ];

  it("returns the copied lines since the thread last ran, up to the turn's message", () => {
    expect(pairTranscriptPending(thread, MessageId.make("u2")).map((line) => line.id)).toEqual([
      "x2",
      "x3",
    ]);
  });

  it("counts a line that landed during the previous turn, and only until the next turn folds it", () => {
    expect(pairTranscriptPending(thread, MessageId.make("c1")).map((line) => line.id)).toEqual([
      "x1",
    ]);
    expect(pairTranscriptPending(thread, MessageId.make("u2"))).not.toContainEqual(
      expect.objectContaining({ id: "x1" }),
    );
  });

  it("does not repeat a line the turn's own message carries", () => {
    const messages = [
      message({ id: "u1", role: "user", text: "Add retries" }),
      message({ id: "x1", role: "user", text: "You → Astra: and the cap?", transcript: true }),
      message({
        id: "x2",
        role: "user",
        text: "Fable (Lead) → you: Retries are in.",
        transcript: true,
        sourceMessageId: "lead-answer",
      }),
      message({
        id: "s1",
        role: "user",
        text: "sign-off prompt",
        consult: true,
        sourceMessageId: "lead-answer",
      }),
    ];
    expect(pairTranscriptPending(messages, MessageId.make("s1")).map((line) => line.id)).toEqual([
      "x1",
    ]);
  });

  it("treats a message not in the thread yet as the end", () => {
    expect(
      pairTranscriptPending(thread.slice(0, -1), MessageId.make("not-yet")).map((line) => line.id),
    ).toEqual(["x2", "x3"]);
  });
});

describe("pairTranscriptFold", () => {
  it("is empty when nothing is pending", () => {
    expect(pairTranscriptFold([])).toEqual(Option.none());
  });

  it("wraps the lines with an explanation and a separator", () => {
    const fold = Option.getOrThrow(
      pairTranscriptFold([
        message({ id: "x1", role: "user", text: "You → Astra: and jitter?" }),
        message({ id: "x2", role: "user", text: "Astra (Peer) → you: full jitter" }),
      ]),
    );
    expect(fold).toContain("Catching up on the Pair Room");
    expect(fold).toContain("You → Astra: and jitter?\nAstra (Peer) → you: full jitter");
    expect(fold.endsWith("\n---\n\n")).toBe(true);
    expect(fold).not.toContain("omitted");
  });

  it("keeps the newest lines within the budget and says how many it dropped", () => {
    const lines = Array.from({ length: 5 }, (_, index) =>
      message({ id: `x${index}`, role: "user", text: `line ${index} ${"·".repeat(20)}` }),
    );
    const fold = Option.getOrThrow(pairTranscriptFold(lines, 60));
    expect(fold).toContain("(3 earlier lines omitted; pair_read_thread shows them.)");
    expect(fold).toContain("line 3");
    expect(fold).toContain("line 4");
    expect(fold).not.toContain("line 2");
  });

  it("always keeps the newest line even when it alone exceeds the budget", () => {
    const fold = Option.getOrThrow(
      pairTranscriptFold([message({ id: "x1", role: "user", text: "y".repeat(200) })], 10),
    );
    expect(fold).toContain("y".repeat(200));
  });
});
