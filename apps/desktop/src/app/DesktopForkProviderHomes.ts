import { DESKTOP_FORK_IDENTITY } from "@t3tools/shared/desktopForkIdentity";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";

import * as DesktopEnvironment from "./DesktopEnvironment.ts";

const HomePath = Schema.Struct({ homePath: Schema.String });

const encodeSeedSettings = Schema.encodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      enableProviderUpdateChecks: Schema.Boolean,
      providers: Schema.Struct({ claudeAgent: HomePath, codex: HomePath }),
    }),
  ),
);

/**
 * On first launch, points Claude Code and Codex at homes inside this fork's
 * T3 home, so its agents never read or write the user's ~/.claude or ~/.codex
 * and never update the installed CLIs. An existing settings file is left
 * alone, so paths the user changed in Settings stay theirs. A failure stops
 * startup rather than letting the providers fall back to the real CLI homes.
 */
export const seedForkProviderHomes = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const environment = yield* DesktopEnvironment.DesktopEnvironment;
  if (yield* fileSystem.exists(environment.serverSettingsPath)) return;
  const claudeHome = environment.path.join(
    environment.baseDir,
    DESKTOP_FORK_IDENTITY.claudeHomeDirName,
  );
  const codexHome = environment.path.join(
    environment.baseDir,
    DESKTOP_FORK_IDENTITY.codexHomeDirName,
  );
  yield* fileSystem.makeDirectory(claudeHome, { recursive: true });
  yield* fileSystem.makeDirectory(codexHome, { recursive: true });
  yield* fileSystem.makeDirectory(environment.stateDir, { recursive: true });
  const settings = yield* encodeSeedSettings({
    enableProviderUpdateChecks: false,
    providers: { claudeAgent: { homePath: claudeHome }, codex: { homePath: codexHome } },
  });
  yield* fileSystem.writeFileString(environment.serverSettingsPath, `${settings}\n`);
  yield* Effect.logInfo("seeded fork provider homes", { claudeHome, codexHome });
});
