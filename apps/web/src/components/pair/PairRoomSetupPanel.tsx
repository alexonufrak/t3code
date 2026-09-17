import { Radio as RadioPrimitive } from "@base-ui/react/radio";
import {
  derivePairAvailability,
  pairPersonaName,
} from "@t3tools/client-runtime/state/pair-room-index";
import {
  PAIR_PERSONAS,
  PAIR_ROOM_DEFAULT_MAX_ROUNDS,
  PAIR_ROOM_MAX_ROUNDS_LIMIT,
  type PairPersona,
  type PairRoomMode,
  type ServerProvider,
} from "@t3tools/contracts";
import { memo, useId, useMemo, useState } from "react";

import { Button } from "~/components/ui/button";
import { RadioGroup } from "~/components/ui/radio-group";
import { toggleVariants } from "~/components/ui/toggle";
import type { PairRoomDraft } from "~/state/pairRooms";

export const PAIR_ROOM_MODE_COPY: Readonly<
  Record<PairRoomMode, { readonly label: string; readonly description: string }>
> = {
  adaptive: {
    label: "Adaptive",
    description: "The Lead asks the Peer when a second opinion would change the outcome.",
  },
  pair: {
    label: "Pair",
    description: "The Peer reviews every code change before the Lead's turn ends.",
  },
  roundtable: {
    label: "Roundtable",
    description: "Both propose independently on design questions, then the Lead decides.",
  },
};

const PAIR_ROOM_MODES = [
  "adaptive",
  "pair",
  "roundtable",
] as const satisfies ReadonlyArray<PairRoomMode>;

const PROVIDER_NAMES: Readonly<Record<PairPersona, string>> = {
  fable: "Claude Code",
  astra: "Codex",
};

export const defaultPairRoomDraft = (leadPersona: PairPersona = "fable"): PairRoomDraft => ({
  leadPersona,
  mode: "adaptive",
  maxRoundsPerTurn: PAIR_ROOM_DEFAULT_MAX_ROUNDS.adaptive,
});

/**
 * Chooses how a new thread becomes a Pair Room: who leads, how the two
 * collaborate, and how many consult rounds a Lead turn may use.
 */
export const PairRoomSetupPanel = memo(function PairRoomSetupPanel(props: {
  providers: ReadonlyArray<ServerProvider>;
  value: PairRoomDraft | null;
  onChange: (draft: PairRoomDraft | null) => void;
}) {
  const availability = useMemo(() => derivePairAvailability(props.providers), [props.providers]);
  const [draft, setDraft] = useState<PairRoomDraft>(props.value ?? defaultPairRoomDraft());
  const leadLabelId = useId();
  const modeLabelId = useId();
  const roundsLabelId = useId();
  const dirty =
    props.value === null ||
    props.value.leadPersona !== draft.leadPersona ||
    props.value.mode !== draft.mode ||
    props.value.maxRoundsPerTurn !== draft.maxRoundsPerTurn;

  return (
    <section
      aria-labelledby={`${leadLabelId}-title`}
      className="flex min-h-0 flex-1 flex-col overflow-y-auto overscroll-contain p-3 text-sm"
    >
      <h2 id={`${leadLabelId}-title`} className="font-medium text-foreground">
        Pair room
      </h2>
      <p className="mt-1 text-xs text-muted-foreground">
        Fable and Astra share this thread. The Lead answers you and does the work or hands out
        assignments. The Peer critiques and reviews.
      </p>

      <ul className="mt-3 flex flex-col gap-1.5" aria-label="Participants">
        {availability.participants.map((participant) => (
          <li key={participant.persona} className="text-xs">
            <span className="font-medium text-foreground">
              {pairPersonaName(participant.persona)}
            </span>{" "}
            <span className="text-muted-foreground">
              ({PROVIDER_NAMES[participant.persona]}, {PAIR_PERSONAS[participant.persona].model})
            </span>
            <span className="block text-muted-foreground">
              {participant.available ? "Ready" : participant.remedy}
            </span>
          </li>
        ))}
      </ul>

      <div className="mt-3 space-y-1.5">
        <span id={leadLabelId} className="text-xs font-medium text-foreground">
          Lead
        </span>
        <RadioGroup
          className="w-fit flex-row gap-0.5 rounded-lg bg-input/40 p-0.5"
          value={draft.leadPersona}
          aria-labelledby={leadLabelId}
          onValueChange={(persona) => {
            if (persona === "fable" || persona === "astra") {
              setDraft((current) => ({ ...current, leadPersona: persona }));
            }
          }}
        >
          {(["fable", "astra"] as const).map((persona) => (
            <RadioPrimitive.Root
              key={persona}
              value={persona}
              data-pressed={draft.leadPersona === persona ? "" : undefined}
              className={toggleVariants({ variant: "segmented", size: "segmented" })}
            >
              {pairPersonaName(persona)}
            </RadioPrimitive.Root>
          ))}
        </RadioGroup>
      </div>

      <div className="mt-3 space-y-1.5">
        <span id={modeLabelId} className="text-xs font-medium text-foreground">
          Mode
        </span>
        <RadioGroup
          className="gap-1"
          value={draft.mode}
          aria-labelledby={modeLabelId}
          onValueChange={(mode) => {
            const next = PAIR_ROOM_MODES.find((candidate) => candidate === mode);
            if (next) {
              setDraft((current) => ({
                ...current,
                mode: next,
                maxRoundsPerTurn: PAIR_ROOM_DEFAULT_MAX_ROUNDS[next],
              }));
            }
          }}
        >
          {PAIR_ROOM_MODES.map((mode) => (
            <RadioPrimitive.Root
              key={mode}
              value={mode}
              data-pressed={draft.mode === mode ? "" : undefined}
              className="rounded-md border border-transparent px-2 py-1.5 text-left outline-none hover:bg-accent focus-visible:ring-2 focus-visible:ring-ring data-checked:border-input data-checked:bg-accent/60"
            >
              <span className="block text-xs font-medium text-foreground">
                {PAIR_ROOM_MODE_COPY[mode].label}
              </span>
              <span className="block text-xs text-muted-foreground">
                {PAIR_ROOM_MODE_COPY[mode].description}
              </span>
            </RadioPrimitive.Root>
          ))}
        </RadioGroup>
      </div>

      <div className="mt-3 space-y-1.5">
        <span id={roundsLabelId} className="text-xs font-medium text-foreground">
          Consult rounds per Lead turn
        </span>
        <RadioGroup
          className="w-fit flex-row gap-0.5 rounded-lg bg-input/40 p-0.5"
          value={String(draft.maxRoundsPerTurn)}
          aria-labelledby={roundsLabelId}
          onValueChange={(rounds) => {
            const count = Number(rounds);
            if (Number.isInteger(count)) {
              setDraft((current) => ({ ...current, maxRoundsPerTurn: count }));
            }
          }}
        >
          {Array.from({ length: PAIR_ROOM_MAX_ROUNDS_LIMIT }, (_, index) => String(index + 1)).map(
            (rounds) => (
              <RadioPrimitive.Root
                key={rounds}
                value={rounds}
                data-pressed={String(draft.maxRoundsPerTurn) === rounds ? "" : undefined}
                className={toggleVariants({ variant: "segmented", size: "segmented" })}
              >
                {rounds}
              </RadioPrimitive.Root>
            ),
          )}
        </RadioGroup>
      </div>

      <div className="mt-4 flex items-center gap-2">
        <Button
          type="button"
          size="sm"
          disabled={!availability.available || !dirty}
          onClick={() => props.onChange(draft)}
        >
          {props.value === null ? "Start as pair room" : "Update pair room"}
        </Button>
        {props.value !== null ? (
          <Button type="button" size="sm" variant="ghost" onClick={() => props.onChange(null)}>
            Turn off
          </Button>
        ) : null}
      </div>
    </section>
  );
});
