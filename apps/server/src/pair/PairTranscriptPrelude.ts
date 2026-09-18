import type { MessageId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";

/**
 * Seam for `ProviderCommandReactor`: the catch-up block a Pair Room thread
 * puts in front of a turn, or none for any other thread. The implementation
 * lives in `PairTranscript.ts`; this module carries only the tag so the
 * reactor's import stays free of pair dependencies.
 */
export class PairTranscriptPrelude extends Context.Service<
  PairTranscriptPrelude,
  {
    readonly forTurn: (
      threadId: ThreadId,
      messageId: MessageId,
    ) => Effect.Effect<Option.Option<string>>;
  }
>()("t3/pair/PairTranscriptPrelude") {}
