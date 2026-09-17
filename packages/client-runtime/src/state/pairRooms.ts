import { WS_METHODS } from "@t3tools/contracts";
import { Atom } from "effect/unstable/reactivity";

import type { EnvironmentRegistry } from "../connection/registry.ts";
import {
  createAtomCommandScheduler,
  createEnvironmentRpcCommand,
  createEnvironmentRpcSubscriptionAtomFamily,
} from "./runtime.ts";

export function createPairRoomEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  const commandScheduler = createAtomCommandScheduler();
  return {
    // Every pair room on the environment, resent whole after each change.
    rooms: createEnvironmentRpcSubscriptionAtomFamily(runtime, {
      label: "environment-data:pair-rooms",
      tag: WS_METHODS.subscribePairRooms,
    }),
    // User-only room commands run one at a time per environment, so a double
    // click on merge cannot race two merges.
    dispatch: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:pair-rooms:dispatch",
      tag: WS_METHODS.pairRoomDispatch,
      scheduler: commandScheduler,
      concurrency: {
        mode: "serial",
        key: ({ environmentId }) => environmentId,
      },
    }),
  };
}
