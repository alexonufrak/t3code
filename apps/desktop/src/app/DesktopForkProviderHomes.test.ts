import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import * as DesktopConfig from "./DesktopConfig.ts";
import * as DesktopEnvironment from "./DesktopEnvironment.ts";
import { seedForkProviderHomes } from "./DesktopForkProviderHomes.ts";

const decodeSettings = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

const seedIn = (baseDir: string) =>
  seedForkProviderHomes.pipe(
    Effect.provide(
      DesktopEnvironment.layer({
        dirname: "/repo/apps/desktop/src",
        homeDirectory: baseDir,
        platform: "darwin",
        processArch: "arm64",
        appVersion: "1.2.3",
        appPath: "/repo",
        isPackaged: true,
        resourcesPath: "/missing/resources",
        runningUnderArm64Translation: false,
      }).pipe(
        Layer.provide(
          Layer.mergeAll(NodeServices.layer, DesktopConfig.layerTest({ T3CODE_HOME: baseDir })),
        ),
      ),
    ),
  );

describe("DesktopForkProviderHomes", () => {
  it.effect("points Claude and Codex at homes inside the T3 home on first launch", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const baseDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-pair-seed-" });

      yield* seedIn(baseDir);

      const settings = yield* fileSystem
        .readFileString(`${baseDir}/userdata/settings.json`)
        .pipe(Effect.flatMap(decodeSettings));
      assert.deepEqual(settings, {
        enableProviderUpdateChecks: false,
        providers: {
          claudeAgent: { homePath: `${baseDir}/claude` },
          codex: { homePath: `${baseDir}/codex` },
        },
      });
      assert.isTrue(yield* fileSystem.exists(`${baseDir}/claude`));
      assert.isTrue(yield* fileSystem.exists(`${baseDir}/codex`));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("leaves existing settings alone", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const baseDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-pair-seed-" });
      yield* fileSystem.makeDirectory(`${baseDir}/userdata`, { recursive: true });
      yield* fileSystem.writeFileString(`${baseDir}/userdata/settings.json`, "{}\n");

      yield* seedIn(baseDir);

      assert.equal(yield* fileSystem.readFileString(`${baseDir}/userdata/settings.json`), "{}\n");
      assert.isFalse(yield* fileSystem.exists(`${baseDir}/claude`));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
