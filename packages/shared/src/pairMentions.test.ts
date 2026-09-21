import { describe, expect, it } from "vite-plus/test";

import {
  pairMentionedParticipant,
  pairMentionTrailing,
  pairMessageMentions,
} from "./pairMentions.ts";

const peer = { persona: "astra", role: "peer" } as const;
const lead = { persona: "fable", role: "lead" } as const;

describe("pairMessageMentions", () => {
  it("recognizes the name and the role word, each only as its own word", () => {
    expect(pairMessageMentions("@Astra, is this safe?", peer)).toBe(true);
    expect(pairMessageMentions("what do you think @astra", peer)).toBe(true);
    expect(pairMessageMentions("@peer, is this safe?", peer)).toBe(true);
    expect(pairMessageMentions("ask @Peer.", peer)).toBe(true);
    expect(pairMessageMentions("mail me@astra.dev", peer)).toBe(false);
    expect(pairMessageMentions("ping @astra-bot", peer)).toBe(false);
    expect(pairMessageMentions("@peers, listen", peer)).toBe(false);
    expect(pairMessageMentions("peer review this", peer)).toBe(false);
    expect(pairMessageMentions("@Fable, is this safe?", peer)).toBe(false);
    expect(pairMessageMentions("@lead, is this safe?", peer)).toBe(false);
  });
});

describe("pairMentionedParticipant", () => {
  it("maps a mention token to a participant by name or role, ignoring punctuation and case", () => {
    expect(pairMentionedParticipant("Astra", [lead, peer])).toBe(peer);
    expect(pairMentionedParticipant("peer,", [lead, peer])).toBe(peer);
    expect(pairMentionedParticipant("FABLE?", [lead, peer])).toBe(lead);
    expect(pairMentionedParticipant("lead", [lead, peer])).toBe(lead);
    expect(pairMentionedParticipant("src/astra.ts", [lead, peer])).toBeNull();
    expect(pairMentionedParticipant("Astra", [])).toBeNull();
  });

  it("keeps the punctuation typed against the name", () => {
    expect(pairMentionTrailing("Astra,")).toBe(",");
    expect(pairMentionTrailing("peer")).toBe("");
  });
});
