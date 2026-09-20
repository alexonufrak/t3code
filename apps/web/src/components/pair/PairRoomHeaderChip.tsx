import { Radio as RadioPrimitive } from "@base-ui/react/radio";
import {
  isActivePairAssignment,
  PAIR_ASSIGNMENT_STATE_LABELS,
  pairPersonaName,
  pairRoomAttention,
  pairRoomSummary,
} from "@t3tools/client-runtime/state/pair-room-index";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import {
  PAIR_CONVERSATION_MAX_EXCHANGES,
  PAIR_ROOM_CHANGED_FILES_KEPT,
  pairRoomParticipant,
  type PairAssignment,
  type PairDecision,
  type PairRoom,
  type PairRoomDispatchResult,
  type PairRoomUserCommand,
  type ScopedThreadRef,
  type ThreadId,
} from "@t3tools/contracts";
import { Link, useNavigate } from "@tanstack/react-router";
import { UsersIcon } from "lucide-react";
import { memo, useCallback, useId, useState, type ReactNode } from "react";

import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Popover, PopoverPopup, PopoverTrigger } from "~/components/ui/popover";
import { RadioGroup } from "~/components/ui/radio-group";
import { Textarea } from "~/components/ui/textarea";
import { toastManager } from "~/components/ui/toast";
import { toggleVariants } from "~/components/ui/toggle";
import { pairRoomEnvironment, usePairThreadMembership } from "~/state/pairRooms";
import { useAtomCommand } from "~/state/use-atom-command";

import { PAIR_ROOM_MODE_COPY } from "./PairRoomSetupPanel";

const RESUMABLE_STATES: ReadonlySet<PairAssignment["state"]> = new Set([
  "interrupted",
  "failed",
  "cancelled",
]);

type RoomCommandRunner = (
  command: PairRoomUserCommand,
  failureTitle: string,
) => Promise<PairRoomDispatchResult | null>;

/**
 * Header chip for threads in a Pair Room: who leads, what is running and
 * what needs the user. Its popover holds every user-only room control.
 */
export const PairRoomHeaderChip = memo(function PairRoomHeaderChip(props: {
  threadRef: ScopedThreadRef | null;
}) {
  const membership = usePairThreadMembership(props.threadRef);
  const dispatch = useAtomCommand(pairRoomEnvironment.dispatch, { reportFailure: false });
  const environmentId = props.threadRef?.environmentId ?? null;

  const run = useCallback<RoomCommandRunner>(
    async (command, failureTitle) => {
      if (environmentId === null) return null;
      const result = await dispatch({ environmentId, input: command });
      if (result._tag === "Success") return result.value;
      if (!isAtomCommandInterrupted(result)) {
        const error = squashAtomCommandFailure(result);
        toastManager.add({
          type: "error",
          title: failureTitle,
          description: error instanceof Error ? error.message : "An error occurred.",
        });
      }
      return null;
    },
    [dispatch, environmentId],
  );

  if (membership === null || environmentId === null) return null;
  const { room } = membership;
  const attention = pairRoomAttention(room);
  const needsYou =
    attention.merges.length +
    attention.decisions.length +
    (room.leadSwitch?.phase === "ready" ? 1 : 0);
  const summary =
    membership.role === "former"
      ? `Earlier thread · ${pairRoomSummary(room)}`
      : pairRoomSummary(room);

  return (
    <Popover>
      <PopoverTrigger
        render={
          <Button
            type="button"
            size="xs"
            variant={needsYou > 0 ? "warning-outline" : "outline"}
            aria-label={`Pair room: ${summary}. Open room controls`}
          />
        }
      >
        <UsersIcon aria-hidden />
        <span className="max-w-56 truncate">{summary}</span>
      </PopoverTrigger>
      <PopoverPopup align="end" className="w-96 max-w-[calc(100vw-2rem)]">
        <PairRoomDetails
          room={room}
          environmentId={environmentId}
          currentThreadId={props.threadRef?.threadId ?? null}
          run={run}
        />
      </PopoverPopup>
    </Popover>
  );
});

function ThreadLink(props: {
  environmentId: ScopedThreadRef["environmentId"];
  threadId: ThreadId | null;
  currentThreadId: ThreadId | null;
  children: ReactNode;
}) {
  if (props.threadId === null || props.threadId === props.currentThreadId) {
    return <span>{props.children}</span>;
  }
  return (
    <Link
      to="/$environmentId/$threadId"
      params={{ environmentId: props.environmentId, threadId: props.threadId }}
      className="underline-offset-2 hover:underline focus-visible:underline"
    >
      {props.children}
    </Link>
  );
}

function PairRoomDetails(props: {
  room: PairRoom;
  environmentId: ScopedThreadRef["environmentId"];
  currentThreadId: ThreadId | null;
  run: RoomCommandRunner;
}) {
  const { room, run } = props;
  const headingId = useId();
  const modeLabelId = useId();
  const attention = pairRoomAttention(room);
  const runningConsult = room.consults.find((consult) => consult.status === "running") ?? null;
  // Merges are listed under "Needs you"; stopped work stays here so it can be resumed.
  const openAssignments = room.assignments
    .filter(
      (assignment) =>
        assignment.state !== "awaiting-user" &&
        (isActivePairAssignment(assignment) || RESUMABLE_STATES.has(assignment.state)),
    )
    .slice(-6);
  const lead = pairRoomParticipant(room, "lead");
  const isLiveThread =
    props.currentThreadId === null ||
    !room.formerParticipants.some((former) => former.threadId === props.currentThreadId);
  const statusText =
    room.status === "active"
      ? "Active"
      : `${room.status === "paused" ? "Paused" : "Closed"}${room.statusReason ? `: ${room.statusReason}` : ""}`;

  return (
    <article aria-labelledby={headingId} className="flex flex-col gap-3 text-sm">
      <header>
        <h2 id={headingId} className="font-medium text-foreground">
          Pair room
        </h2>
        <p className="text-xs text-muted-foreground">{statusText}</p>
        {lead?.threadId && props.currentThreadId !== null && !isLiveThread ? (
          <p className="mt-1 text-xs text-muted-foreground">
            This thread is from before the Lead changed.{" "}
            <ThreadLink
              environmentId={props.environmentId}
              threadId={lead.threadId}
              currentThreadId={props.currentThreadId}
            >
              {pairPersonaName(lead.persona)} leads now
            </ThreadLink>
            .
          </p>
        ) : null}
      </header>

      {room.status !== "closed" && isLiveThread ? (
        <LeadSwitchSection room={room} environmentId={props.environmentId} run={run} />
      ) : null}

      <section aria-label="Participants">
        <ul className="flex flex-col gap-1 text-xs">
          {room.participants.map((participant) => (
            <li key={participant.persona}>
              <ThreadLink
                environmentId={props.environmentId}
                threadId={participant.threadId}
                currentThreadId={props.currentThreadId}
              >
                <span className="font-medium text-foreground">
                  {pairPersonaName(participant.persona)}
                </span>
              </ThreadLink>{" "}
              <span className="text-muted-foreground">
                {participant.role === "lead" ? "Lead" : "Peer"}
                {participant.threadId === null ? ", not started yet" : ""}
                {participant.role === "peer" && runningConsult
                  ? `, answering: ${runningConsult.title}`
                  : ""}
              </span>
            </li>
          ))}
        </ul>
        {runningConsult ? (
          <Button
            type="button"
            size="xs"
            variant="ghost"
            className="mt-1"
            onClick={() =>
              void run(
                {
                  type: "consult.cancel",
                  roomId: room.roomId,
                  consultId: runningConsult.consultId,
                },
                "Could not cancel the consult",
              )
            }
          >
            Cancel consult
          </Button>
        ) : null}
      </section>

      {room.status !== "closed" ? <CheckoutSection room={room} run={run} /> : null}

      {attention.merges.length > 0 || attention.decisions.length > 0 ? (
        <section aria-labelledby={`${headingId}-needs-you`} className="flex flex-col gap-2">
          <h3 id={`${headingId}-needs-you`} className="text-xs font-medium text-foreground">
            Needs you
          </h3>
          {attention.merges.map((assignment) => (
            <MergeRequest
              key={assignment.assignmentId}
              room={room}
              assignment={assignment}
              environmentId={props.environmentId}
              currentThreadId={props.currentThreadId}
              run={run}
            />
          ))}
          {attention.decisions.map((decision) => (
            <DecisionRequest key={decision.decisionId} room={room} decision={decision} run={run} />
          ))}
        </section>
      ) : null}

      {openAssignments.length > 0 ? (
        <section aria-labelledby={`${headingId}-assignments`} className="flex flex-col gap-1.5">
          <h3 id={`${headingId}-assignments`} className="text-xs font-medium text-foreground">
            Assignments
          </h3>
          {openAssignments.map((assignment) => (
            <div key={assignment.assignmentId} className="text-xs">
              <ThreadLink
                environmentId={props.environmentId}
                threadId={assignment.threadId}
                currentThreadId={props.currentThreadId}
              >
                <span className="font-medium text-foreground">{assignment.title}</span>
              </ThreadLink>
              <span className="block text-muted-foreground">
                {pairPersonaName(assignment.owner)}:{" "}
                {PAIR_ASSIGNMENT_STATE_LABELS[assignment.state]}
                {assignment.note ? `. ${assignment.note}` : ""}
              </span>
              <div className="mt-1 flex gap-1">
                {RESUMABLE_STATES.has(assignment.state) || assignment.state === "blocked" ? (
                  <Button
                    type="button"
                    size="xs"
                    variant="outline"
                    onClick={() =>
                      void run(
                        {
                          type: "assignment.resume",
                          roomId: room.roomId,
                          assignmentId: assignment.assignmentId,
                        },
                        "Could not resume the assignment",
                      )
                    }
                  >
                    Resume
                  </Button>
                ) : null}
                {isActivePairAssignment(assignment) ||
                assignment.state === "interrupted" ||
                assignment.state === "failed" ? (
                  <Button
                    type="button"
                    size="xs"
                    variant="ghost"
                    onClick={() =>
                      void run(
                        {
                          type: "assignment.cancel",
                          roomId: room.roomId,
                          assignmentId: assignment.assignmentId,
                        },
                        "Could not cancel the assignment",
                      )
                    }
                  >
                    Cancel
                  </Button>
                ) : null}
              </div>
            </div>
          ))}
        </section>
      ) : null}

      <section className="flex flex-col gap-1.5">
        <span id={modeLabelId} className="text-xs font-medium text-foreground">
          Mode
        </span>
        <RadioGroup
          className="w-fit flex-row gap-0.5 rounded-lg bg-input/40 p-0.5"
          value={room.mode}
          aria-labelledby={modeLabelId}
          disabled={room.status === "closed"}
          onValueChange={(value) => {
            const mode = (["adaptive", "pair", "roundtable"] as const).find(
              (candidate) => candidate === value,
            );
            if (mode && mode !== room.mode) {
              void run({ type: "room.update", roomId: room.roomId, mode }, "Could not change mode");
            }
          }}
        >
          {(["adaptive", "pair", "roundtable"] as const).map((mode) => (
            <RadioPrimitive.Root
              key={mode}
              value={mode}
              data-pressed={room.mode === mode ? "" : undefined}
              className={toggleVariants({ variant: "segmented", size: "segmented" })}
            >
              {PAIR_ROOM_MODE_COPY[mode].label}
            </RadioPrimitive.Root>
          ))}
        </RadioGroup>
        <p className="text-xs text-muted-foreground">
          {PAIR_ROOM_MODE_COPY[room.mode].description} Up to {room.maxRoundsPerTurn}
          {room.maxRoundsPerTurn === 1 ? " conversation" : " conversations"} with the Peer per{" "}
          {lead ? `${pairPersonaName(lead.persona)} turn` : "Lead turn"}, each up to{" "}
          {PAIR_CONVERSATION_MAX_EXCHANGES} exchanges.
        </p>
      </section>

      <footer className="flex flex-wrap gap-1.5 border-t border-border/70 pt-2">
        {room.status === "active" ? (
          <>
            <Button
              type="button"
              size="xs"
              variant="outline"
              onClick={() =>
                void run(
                  { type: "room.grant-rounds", roomId: room.roomId, count: 1 },
                  "Could not allow another round",
                )
              }
            >
              Allow one more round
            </Button>
            <Button
              type="button"
              size="xs"
              variant="outline"
              onClick={() =>
                void run(
                  { type: "room.update", roomId: room.roomId, status: "paused" },
                  "Could not pause the room",
                )
              }
            >
              Pause
            </Button>
          </>
        ) : (
          <Button
            type="button"
            size="xs"
            variant="outline"
            onClick={() =>
              void run(
                { type: "room.update", roomId: room.roomId, status: "active" },
                room.status === "closed"
                  ? "Could not reopen the room"
                  : "Could not resume the room",
              )
            }
          >
            {room.status === "closed" ? "Reopen" : "Resume"}
          </Button>
        )}
        {room.status !== "closed" ? (
          <Button
            type="button"
            size="xs"
            variant="ghost"
            onClick={() =>
              void run(
                { type: "room.update", roomId: room.roomId, status: "closed" },
                "Could not close the room",
              )
            }
          >
            Close room
          </Button>
        ) : null}
      </footer>
    </article>
  );
}

function LeadSwitchSection(props: {
  room: PairRoom;
  environmentId: ScopedThreadRef["environmentId"];
  run: RoomCommandRunner;
}) {
  const { room, run } = props;
  const navigate = useNavigate();
  const headingId = useId();
  const [pending, setPending] = useState(false);
  const lead = pairRoomParticipant(room, "lead");
  if (!lead?.threadId) return null;
  const next = room.leadSwitch?.toPersona ?? (lead.persona === "fable" ? "astra" : "fable");
  const cancel = () =>
    void run({ type: "lead.switch-cancel", roomId: room.roomId }, "Could not cancel the switch");
  const start = () =>
    void run({ type: "lead.switch-start", roomId: room.roomId }, "Could not start the switch");

  if (room.leadSwitch === null) {
    if (room.status !== "active") return null;
    return (
      <section aria-label="Lead">
        <Button type="button" size="xs" variant="outline" onClick={start}>
          Switch Lead to {pairPersonaName(next)}
        </Button>
      </section>
    );
  }

  return (
    <section aria-labelledby={headingId} className="flex flex-col gap-1.5 text-xs">
      <h3 id={headingId} className="font-medium text-foreground">
        Switching Lead to {pairPersonaName(next)}
      </h3>
      {room.leadSwitch.phase === "drafting" ? (
        <p className="text-muted-foreground">
          {pairPersonaName(lead.persona)} is writing a handoff. You can read it before anything
          changes.
        </p>
      ) : null}
      {room.leadSwitch.phase === "failed" ? (
        <p className="text-muted-foreground">
          No handoff yet: {room.leadSwitch.error ?? "the Lead did not write one."}
        </p>
      ) : null}
      {room.leadSwitch.phase === "ready" && room.leadSwitch.handoff ? (
        <div
          className="max-h-56 overflow-y-auto whitespace-pre-wrap rounded-md border border-border/70 p-2 text-muted-foreground"
          tabIndex={0}
          aria-label={`Handoff from ${pairPersonaName(lead.persona)}`}
        >
          {room.leadSwitch.handoff}
        </div>
      ) : null}
      <div className="flex flex-wrap gap-1">
        {room.leadSwitch.phase === "ready" ? (
          <Button
            type="button"
            size="xs"
            disabled={pending}
            onClick={() => {
              setPending(true);
              void run(
                { type: "lead.switch-confirm", roomId: room.roomId },
                "Could not switch the Lead",
              )
                .then((result) => {
                  if (result?.threadId) {
                    void navigate({
                      to: "/$environmentId/$threadId",
                      params: { environmentId: props.environmentId, threadId: result.threadId },
                    });
                  }
                })
                .finally(() => setPending(false));
            }}
          >
            {pending ? "Handing off..." : `Hand off to ${pairPersonaName(next)}`}
          </Button>
        ) : null}
        {room.leadSwitch.phase === "failed" ? (
          <Button type="button" size="xs" variant="outline" onClick={start}>
            Try again
          </Button>
        ) : null}
        <Button type="button" size="xs" variant="ghost" disabled={pending} onClick={cancel}>
          Cancel switch
        </Button>
      </div>
    </section>
  );
}

/**
 * Where the Lead works. The room follows the Lead thread's own directory
 * unless the Lead (with pair_checkout) or the user pointed it at another
 * worktree; the Peer's snapshots, assignment bases and merges follow it.
 */
function CheckoutSection(props: { room: PairRoom; run: RoomCommandRunner }) {
  const { room, run } = props;
  const headingId = useId();
  const [editing, setEditing] = useState(false);
  const [path, setPath] = useState("");
  const [pending, setPending] = useState(false);
  const lead = pairRoomParticipant(room, "lead");
  const checkout = room.checkout;
  const submit = () => {
    const trimmed = path.trim();
    if (trimmed.length === 0) return;
    setPending(true);
    void run(
      { type: "room.checkout", roomId: room.roomId, path: trimmed },
      "Could not change the checkout",
    )
      .then((result) => {
        if (result) setEditing(false);
      })
      .finally(() => setPending(false));
  };
  return (
    <section aria-labelledby={headingId} className="text-xs">
      <h3 id={headingId} className="font-medium text-foreground">
        Checkout
      </h3>
      <p className="text-muted-foreground">
        {checkout
          ? `${checkout.branch ?? "Detached HEAD"} in ${checkout.path}, set by ${
              checkout.by === "user" ? "you" : lead ? pairPersonaName(lead.persona) : "the Lead"
            }.`
          : "The Lead thread's own directory. New assignments start from its branch and merge back into it."}
      </p>
      {editing ? (
        <form
          className="mt-1 flex items-center gap-1"
          onSubmit={(event) => {
            event.preventDefault();
            submit();
          }}
        >
          <Input
            aria-label="Worktree path"
            className="h-7 flex-1 font-mono text-xs"
            value={path}
            onChange={(event) => setPath(event.target.value)}
            placeholder="/absolute/path/to/worktree"
            disabled={pending}
          />
          <Button type="submit" size="xs" disabled={pending || path.trim().length === 0}>
            {pending ? "Checking..." : "Use"}
          </Button>
          <Button
            type="button"
            size="xs"
            variant="ghost"
            disabled={pending}
            onClick={() => setEditing(false)}
          >
            Cancel
          </Button>
        </form>
      ) : (
        <Button
          type="button"
          size="xs"
          variant="ghost"
          className="mt-1"
          onClick={() => {
            setPath(checkout?.path ?? "");
            setEditing(true);
          }}
        >
          Change
        </Button>
      )}
    </section>
  );
}

function MergeRequest(props: {
  room: PairRoom;
  assignment: PairAssignment;
  environmentId: ScopedThreadRef["environmentId"];
  currentThreadId: ThreadId | null;
  run: RoomCommandRunner;
}) {
  const { assignment } = props;
  const [pending, setPending] = useState(false);
  return (
    <div className="rounded-md border border-border/70 p-2 text-xs">
      <ThreadLink
        environmentId={props.environmentId}
        threadId={assignment.threadId}
        currentThreadId={props.currentThreadId}
      >
        <span className="font-medium text-foreground">{assignment.title}</span>
      </ThreadLink>
      <p className="text-muted-foreground">
        {pairPersonaName(assignment.owner)}'s work on {assignment.branch} is approved by the Lead
        {assignment.targetBranch ? ` and merges into ${assignment.targetBranch}` : ""}.
      </p>
      <p className="mt-1 text-muted-foreground">
        Allowed to change: <span className="font-mono">{assignment.scopeGlobs.join(", ")}</span>
        {assignment.scopeGlobs.some((glob) => glob === "**" || glob === "**/*")
          ? " (the whole repository)"
          : null}
      </p>
      <details className="mt-1 text-muted-foreground">
        <summary className="cursor-pointer select-none">
          {assignment.changedFiles.length}
          {assignment.changedFiles.length >= PAIR_ROOM_CHANGED_FILES_KEPT ? "+" : ""} changed{" "}
          {assignment.changedFiles.length === 1 ? "file" : "files"}
        </summary>
        <ul className="mt-1 max-h-40 overflow-y-auto font-mono">
          {assignment.changedFiles.map((file) => (
            <li key={file} className="break-all">
              {file}
            </li>
          ))}
        </ul>
      </details>
      {assignment.report ? (
        <p className="mt-1 text-muted-foreground">{assignment.report.summary}</p>
      ) : null}
      <div className="mt-2 flex gap-1">
        <Button
          type="button"
          size="xs"
          disabled={pending}
          onClick={() => {
            setPending(true);
            void props
              .run(
                {
                  type: "assignment.integrate",
                  roomId: props.room.roomId,
                  assignmentId: assignment.assignmentId,
                },
                "Could not merge the assignment",
              )
              .finally(() => setPending(false));
          }}
        >
          {pending
            ? "Merging..."
            : `Merge into ${assignment.targetBranch ?? "the Lead's checkout"}`}
        </Button>
        <Button
          type="button"
          size="xs"
          variant="ghost"
          disabled={pending}
          onClick={() =>
            void props.run(
              {
                type: "assignment.cancel",
                roomId: props.room.roomId,
                assignmentId: assignment.assignmentId,
              },
              "Could not cancel the assignment",
            )
          }
        >
          Don't merge
        </Button>
      </div>
    </div>
  );
}

function DecisionRequest(props: {
  room: PairRoom;
  decision: PairDecision;
  run: RoomCommandRunner;
}) {
  const { decision } = props;
  const inputId = useId();
  const [answer, setAnswer] = useState("");
  const resolve = (resolution: string) =>
    void props.run(
      {
        type: "decision.resolve",
        roomId: props.room.roomId,
        decisionId: decision.decisionId,
        resolution,
      },
      "Could not record your decision",
    );
  return (
    <div className="rounded-md border border-border/70 p-2 text-xs">
      <p className="font-medium text-foreground">
        {decision.kind === "disagreement" ? "Disagreement" : "Decision"} ({decision.category}):{" "}
        {decision.title}
      </p>
      <ul className="mt-1 flex flex-col gap-1 text-muted-foreground">
        {decision.positions.map((position) => (
          <li key={`${position.persona}:${position.summary}`}>
            <span className="text-foreground">{pairPersonaName(position.persona)}:</span>{" "}
            {position.summary}
          </li>
        ))}
      </ul>
      {decision.leadRecommendation ? (
        <p className="mt-1 text-muted-foreground">Lead recommends: {decision.leadRecommendation}</p>
      ) : null}
      {decision.consequenceOfDeferring ? (
        <p className="mt-1 text-muted-foreground">If deferred: {decision.consequenceOfDeferring}</p>
      ) : null}
      <label htmlFor={inputId} className="sr-only">
        Your decision on {decision.title}
      </label>
      <Textarea
        id={inputId}
        className="mt-2 min-h-14 text-xs"
        placeholder="Your decision"
        value={answer}
        onChange={(event) => setAnswer(event.target.value)}
      />
      <div className="mt-2 flex gap-1">
        <Button
          type="button"
          size="xs"
          disabled={answer.trim().length === 0}
          onClick={() => {
            resolve(answer.trim());
            setAnswer("");
          }}
        >
          Decide
        </Button>
        {decision.leadRecommendation ? (
          <Button
            type="button"
            size="xs"
            variant="outline"
            onClick={() =>
              resolve(`Go with the Lead's recommendation: ${decision.leadRecommendation}`)
            }
          >
            Accept recommendation
          </Button>
        ) : null}
      </div>
    </div>
  );
}
