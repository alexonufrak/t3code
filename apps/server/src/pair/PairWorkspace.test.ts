// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { PairRoomId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import { describe, expect } from "vite-plus/test";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as ServerConfig from "../config.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as PairWorkspace from "./PairWorkspace.ts";

const VcsProcessTestLayer = VcsProcess.layer.pipe(Layer.provide(NodeServices.layer));
const TestLayer = PairWorkspace.layer.pipe(
  Layer.provideMerge(
    CheckpointStore.layer.pipe(
      Layer.provide(VcsDriverRegistry.layer.pipe(Layer.provide(VcsProcessTestLayer))),
    ),
  ),
  Layer.provideMerge(VcsProcessTestLayer),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-pair-workspace-test-" })),
  Layer.provideMerge(NodeServices.layer),
);

const git = (cwd: string, args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const vcs = yield* VcsProcess.VcsProcess;
    const output = yield* vcs.run({ operation: "PairWorkspace.test", command: "git", cwd, args });
    return output.stdout.trim();
  });

const initRepo = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const repo = yield* fileSystem.makeTempDirectoryScoped({ prefix: "pair-workspace-repo-" });
  yield* git(repo, ["init", "-b", "main"]);
  yield* git(repo, ["config", "user.email", "test@test.com"]);
  yield* git(repo, ["config", "user.name", "Test"]);
  yield* fileSystem.makeDirectory(NodePath.join(repo, "src"), { recursive: true });
  yield* fileSystem.writeFileString(
    NodePath.join(repo, "src/retry.ts"),
    "export const tries = 3;\n",
  );
  yield* fileSystem.writeFileString(NodePath.join(repo, "README.md"), "# repo\n");
  yield* git(repo, ["add", "."]);
  yield* git(repo, ["commit", "-m", "initial"]);
  return repo;
});

it.layer(TestLayer)("PairWorkspace", (it) => {
  describe("syncReviewWorktree", () => {
    it.effect(
      "gives the Peer the Lead's uncommitted and untracked edits, then discards Peer writes",
      () =>
        Effect.gen(function* () {
          const fileSystem = yield* FileSystem.FileSystem;
          const workspace = yield* PairWorkspace.PairWorkspace;
          const repo = yield* initRepo;
          const roomId = PairRoomId.make("room-sync");

          yield* fileSystem.writeFileString(
            NodePath.join(repo, "src/retry.ts"),
            "export const tries = 5;\n",
          );
          yield* fileSystem.writeFileString(NodePath.join(repo, "src/new.ts"), "export {};\n");
          const first = yield* workspace.syncReviewWorktree({ roomId, leadCwd: repo });
          expect(
            yield* fileSystem.readFileString(NodePath.join(first.worktreePath, "src/retry.ts")),
          ).toBe("export const tries = 5;\n");
          expect(yield* fileSystem.exists(NodePath.join(first.worktreePath, "src/new.ts"))).toBe(
            true,
          );
          // The Lead's own index and working tree are untouched by the snapshot.
          expect(yield* git(repo, ["status", "--porcelain"])).toBe("M src/retry.ts\n?? src/new.ts");

          yield* fileSystem.writeFileString(
            NodePath.join(first.worktreePath, "README.md"),
            "peer edit\n",
          );
          yield* fileSystem.writeFileString(
            NodePath.join(first.worktreePath, "stray.txt"),
            "peer\n",
          );
          const second = yield* workspace.syncReviewWorktree({ roomId, leadCwd: repo });
          expect(second.worktreePath).toBe(first.worktreePath);
          expect(
            yield* fileSystem.readFileString(NodePath.join(second.worktreePath, "README.md")),
          ).toBe("# repo\n");
          expect(yield* fileSystem.exists(NodePath.join(second.worktreePath, "stray.txt"))).toBe(
            false,
          );
        }),
    );

    it.effect("explains that a folder outside git cannot host a pair room", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const workspace = yield* PairWorkspace.PairWorkspace;
        const plain = yield* fileSystem.makeTempDirectoryScoped({ prefix: "pair-plain-" });
        const error = yield* workspace
          .syncReviewWorktree({ roomId: PairRoomId.make("room-plain"), leadCwd: plain })
          .pipe(Effect.flip);
        expect(error.detail).toContain("needs a git repository");
      }),
    );
  });

  describe("assignments", () => {
    it.effect(
      "lists changed files, merges the branch, and aborts a conflicting merge cleanly",
      () =>
        Effect.gen(function* () {
          const fileSystem = yield* FileSystem.FileSystem;
          const workspace = yield* PairWorkspace.PairWorkspace;
          const repo = yield* initRepo;
          const baseCommit = yield* workspace.resolveCommit({ cwd: repo });

          const first = yield* workspace.planAssignmentWorktree({
            leadCwd: repo,
            assignmentId: "assignment-abc12345",
            title: "Tune retry policy",
          });
          yield* workspace.createAssignmentWorktree({ plan: first, baseCommit });
          expect(first.branch).toBe("pair/tune-retry-policy-abc12345");
          yield* fileSystem.writeFileString(
            NodePath.join(first.worktreePath, "src/retry.ts"),
            "export const tries = 4;\n",
          );
          yield* fileSystem.writeFileString(
            NodePath.join(first.worktreePath, "src/backoff.ts"),
            "export {};\n",
          );
          expect(
            yield* workspace.changedFiles({ worktreePath: first.worktreePath, baseCommit }),
          ).toEqual(["src/backoff.ts", "src/retry.ts"]);

          const merged = yield* workspace.integrate({
            leadCwd: repo,
            worktreePath: first.worktreePath,
            branch: first.branch,
            message: "Pair assignment: Tune retry policy",
          });
          expect(merged.status).toBe("merged");
          expect(yield* fileSystem.readFileString(NodePath.join(repo, "src/retry.ts"))).toBe(
            "export const tries = 4;\n",
          );

          const second = yield* workspace.planAssignmentWorktree({
            leadCwd: repo,
            assignmentId: "assignment-def67890",
            title: "Conflicting change",
          });
          yield* workspace.createAssignmentWorktree({ plan: second, baseCommit });
          yield* fileSystem.writeFileString(
            NodePath.join(second.worktreePath, "src/retry.ts"),
            "export const tries = 9;\n",
          );
          const conflict = yield* workspace.integrate({
            leadCwd: repo,
            worktreePath: second.worktreePath,
            branch: second.branch,
            message: "Pair assignment: Conflicting change",
          });
          expect(conflict.status).toBe("conflict");
          expect(yield* git(repo, ["status", "--porcelain"])).toBe("");
          expect(yield* fileSystem.readFileString(NodePath.join(repo, "src/retry.ts"))).toBe(
            "export const tries = 4;\n",
          );
        }),
    );

    it.effect("refuses to touch a path it did not create", () =>
      Effect.gen(function* () {
        const workspace = yield* PairWorkspace.PairWorkspace;
        const repo = yield* initRepo;
        const error = yield* workspace
          .integrate({ leadCwd: repo, worktreePath: repo, branch: "main", message: "nope" })
          .pipe(Effect.flip);
        expect(error.detail).toContain("not a pair room worktree");
      }),
    );
  });
});
