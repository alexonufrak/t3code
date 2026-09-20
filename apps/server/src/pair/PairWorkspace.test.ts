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

    it.effect("names the folder when it is outside git, before and during a room", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const workspace = yield* PairWorkspace.PairWorkspace;
        const plain = yield* fileSystem.makeTempDirectoryScoped({ prefix: "pair-plain-" });
        const refused = yield* workspace.assertRepository({ cwd: plain }).pipe(Effect.flip);
        expect(refused.detail).toContain(`${plain} is not one`);
        const error = yield* workspace
          .syncReviewWorktree({ roomId: PairRoomId.make("room-plain"), leadCwd: plain })
          .pipe(Effect.flip);
        expect(error.detail).toContain("need a git repository");
      }),
    );
  });

  describe("checkouts", () => {
    it.effect("describes a worktree of the same repository and refuses any other", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const workspace = yield* PairWorkspace.PairWorkspace;
        const repo = yield* initRepo;
        const elsewhere = yield* fileSystem.makeTempDirectoryScoped({ prefix: "pair-ux-" });
        const ux = NodePath.join(elsewhere, "ux");
        yield* git(repo, ["worktree", "add", "-q", "-b", "dev/ux", ux]);
        const described = yield* workspace.describeCheckout({
          cwd: NodePath.join(ux, "src"),
          projectCwd: repo,
        });
        expect(described).toEqual({ path: yield* fileSystem.realPath(ux), branch: "dev/ux" });
        const found = yield* workspace.findBranchWorktree({ cwd: repo, branch: "dev/ux" });
        expect(yield* fileSystem.realPath(found!)).toBe(yield* fileSystem.realPath(ux));
        expect(yield* workspace.findBranchWorktree({ cwd: repo, branch: "nope" })).toBeNull();

        const stranger = yield* fileSystem.makeTempDirectoryScoped({ prefix: "pair-stranger-" });
        yield* git(stranger, ["init", "-q", "-b", "main"]);
        const refused = yield* workspace
          .describeCheckout({ cwd: stranger, projectCwd: repo })
          .pipe(Effect.flip);
        expect(refused.detail).toContain("not a worktree of this project's repository");
        const outside = yield* workspace
          .describeCheckout({ cwd: elsewhere, projectCwd: repo })
          .pipe(Effect.flip);
        expect(outside.detail).toContain("not inside a git worktree");

        yield* git(ux, ["checkout", "-q", "--detach"]);
        expect(yield* workspace.currentBranch({ cwd: ux })).toBeNull();
        expect(
          (yield* workspace.describeCheckout({ cwd: ux, projectCwd: repo })).branch,
        ).toBeNull();
      }),
    );

    it.effect("bases assignments on a local branch and knows once it holds the work", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const workspace = yield* PairWorkspace.PairWorkspace;
        const repo = yield* initRepo;
        const head = yield* git(repo, ["rev-parse", "HEAD"]);
        expect(yield* workspace.resolveBase({ cwd: repo })).toEqual({
          commit: head,
          branch: "main",
        });
        yield* git(repo, ["branch", "dev/ux"]);
        expect(yield* workspace.resolveBase({ cwd: repo, ref: "dev/ux" })).toEqual({
          commit: head,
          branch: "dev/ux",
        });
        yield* git(repo, ["tag", "v1"]);
        // "--branches" would make rev-parse list every branch, which starts with refs/heads/ too.
        for (const ref of [head, "v1", "missing", "--branches"]) {
          const refused = yield* workspace.resolveBase({ cwd: repo, ref }).pipe(Effect.flip);
          expect(refused.detail).toContain("is not a local branch");
        }

        const plan = yield* workspace.planAssignmentWorktree({
          leadCwd: repo,
          assignmentId: "assignment-aaaa1111",
          title: "Base work",
        });
        yield* workspace.createAssignmentWorktree({ plan, baseCommit: head });
        yield* fileSystem.writeFileString(
          NodePath.join(plan.worktreePath, "src/retry.ts"),
          "export const tries = 4;\n",
        );
        const approved = yield* workspace.sealAssignment({
          worktreePath: plan.worktreePath,
          branch: plan.branch,
          message: "Pair assignment: Base work",
        });
        const contains = (ref: string) =>
          workspace.containsCommit({ cwd: repo, ref, commit: approved });
        expect(yield* contains("refs/heads/main")).toBeNull();
        expect(yield* contains("refs/heads/missing")).toBeNull();
        yield* git(repo, ["merge", "-q", "--no-ff", "--no-edit", approved]);
        expect(yield* contains("refs/heads/main")).toBe(yield* git(repo, ["rev-parse", "HEAD"]));
        expect(yield* contains("HEAD")).toBe(yield* git(repo, ["rev-parse", "HEAD"]));
      }),
    );

    it.effect("names uncommitted files a merge would overwrite and leaves them alone", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const workspace = yield* PairWorkspace.PairWorkspace;
        const repo = yield* initRepo;
        const baseCommit = yield* workspace.resolveCommit({ cwd: repo });
        const plan = yield* workspace.planAssignmentWorktree({
          leadCwd: repo,
          assignmentId: "assignment-bbbb2222",
          title: "Retry tuning",
        });
        yield* workspace.createAssignmentWorktree({ plan, baseCommit });
        yield* fileSystem.writeFileString(
          NodePath.join(plan.worktreePath, "src/retry.ts"),
          "export const tries = 4;\n",
        );
        const approved = yield* workspace.sealAssignment({
          worktreePath: plan.worktreePath,
          branch: plan.branch,
          message: "Pair assignment: Retry tuning",
        });
        const integrate = () =>
          workspace.integrate({
            targetCwd: repo,
            worktreePath: plan.worktreePath,
            branch: plan.branch,
            commit: approved,
            message: "Pair assignment: Retry tuning",
          });

        yield* fileSystem.writeFileString(
          NodePath.join(repo, "src/retry.ts"),
          "export const tries = 7;\n",
        );
        yield* fileSystem.writeFileString(NodePath.join(repo, "README.md"), "# edited\n");
        const dirty = yield* integrate();
        expect(dirty).toMatchObject({ status: "dirty", files: ["src/retry.ts"] });
        expect(dirty.status === "dirty" ? dirty.detail : "").toContain("1 uncommitted file in");
        expect(yield* fileSystem.readFileString(NodePath.join(repo, "src/retry.ts"))).toBe(
          "export const tries = 7;\n",
        );
        expect(
          (yield* git(repo, ["status", "--porcelain"])).split("\n").map((line) => line.trim()),
        ).toEqual(["M README.md", "M src/retry.ts"]);

        // An uncommitted file the merge does not write is no obstacle.
        yield* git(repo, ["checkout", "-q", "--", "src/retry.ts"]);
        const merged = yield* integrate();
        expect(merged.status).toBe("merged");
        expect(yield* git(repo, ["status", "--porcelain"])).toBe("M README.md");
        expect(yield* fileSystem.readFileString(NodePath.join(repo, "src/retry.ts"))).toBe(
          "export const tries = 4;\n",
        );
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

          const approved = yield* workspace.sealAssignment({
            worktreePath: first.worktreePath,
            branch: first.branch,
            message: "Pair assignment: Tune retry policy",
          });
          const merged = yield* workspace.integrate({
            targetCwd: repo,
            worktreePath: first.worktreePath,
            branch: first.branch,
            commit: approved,
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
            targetCwd: repo,
            worktreePath: second.worktreePath,
            branch: second.branch,
            commit: yield* workspace.sealAssignment({
              worktreePath: second.worktreePath,
              branch: second.branch,
              message: "Pair assignment: Conflicting change",
            }),
            message: "Pair assignment: Conflicting change",
          });
          expect(conflict).toEqual({ status: "conflict", detail: "Conflicts in src/retry.ts." });
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
          .integrate({
            targetCwd: repo,
            worktreePath: repo,
            branch: "main",
            commit: "HEAD",
            message: "nope",
          })
          .pipe(Effect.flip);
        expect(error.detail).toContain("not a pair room worktree");
      }),
    );

    it.effect(
      "merges only the approved commit, even when a tag shadows the branch or edits follow approval",
      () =>
        Effect.gen(function* () {
          const fileSystem = yield* FileSystem.FileSystem;
          const workspace = yield* PairWorkspace.PairWorkspace;
          const repo = yield* initRepo;
          const baseCommit = yield* workspace.resolveCommit({ cwd: repo });
          const plan = yield* workspace.planAssignmentWorktree({
            leadCwd: repo,
            assignmentId: "assignment-tag12345",
            title: "Retry tests",
          });
          yield* workspace.createAssignmentWorktree({ plan, baseCommit });
          yield* fileSystem.writeFileString(
            NodePath.join(plan.worktreePath, "src/retry.test.ts"),
            "export {};\n",
          );
          const approved = yield* workspace.sealAssignment({
            worktreePath: plan.worktreePath,
            branch: plan.branch,
            message: "Pair assignment: Retry tests",
          });

          // A tag named like the branch, pointing at an out-of-scope commit.
          yield* git(plan.worktreePath, ["checkout", "-q", "--detach"]);
          yield* fileSystem.writeFileString(NodePath.join(plan.worktreePath, "evil.ts"), "x\n");
          yield* git(plan.worktreePath, ["add", "evil.ts"]);
          yield* git(plan.worktreePath, ["commit", "-q", "-m", "evil"]);
          yield* git(plan.worktreePath, ["tag", plan.branch]);
          yield* git(plan.worktreePath, ["checkout", "-q", plan.branch]);

          const merged = yield* workspace.integrate({
            targetCwd: repo,
            worktreePath: plan.worktreePath,
            branch: plan.branch,
            commit: approved,
            message: "Pair assignment: Retry tests",
          });
          expect(merged.status).toBe("merged");
          expect(yield* fileSystem.exists(NodePath.join(repo, "src/retry.test.ts"))).toBe(true);
          expect(yield* fileSystem.exists(NodePath.join(repo, "evil.ts"))).toBe(false);

          yield* fileSystem.writeFileString(
            NodePath.join(plan.worktreePath, "README.md"),
            "edited after approval\n",
          );
          const changed = yield* workspace.integrate({
            targetCwd: repo,
            worktreePath: plan.worktreePath,
            branch: plan.branch,
            commit: approved,
            message: "Pair assignment: Retry tests",
          });
          expect(changed.status).toBe("changed");
        }),
    );

    it.effect("lists both sides of a rename so moving a file out of scope shows up", () =>
      Effect.gen(function* () {
        const workspace = yield* PairWorkspace.PairWorkspace;
        const repo = yield* initRepo;
        const baseCommit = yield* workspace.resolveCommit({ cwd: repo });
        const plan = yield* workspace.planAssignmentWorktree({
          leadCwd: repo,
          assignmentId: "assignment-mv123456",
          title: "Move retry",
        });
        yield* workspace.createAssignmentWorktree({ plan, baseCommit });
        yield* git(plan.worktreePath, ["mv", "src/retry.ts", "docs-retry.ts"]);
        yield* git(plan.worktreePath, ["commit", "-q", "-m", "move"]);
        expect(
          yield* workspace.changedFiles({ worktreePath: plan.worktreePath, baseCommit }),
        ).toEqual(["docs-retry.ts", "src/retry.ts"]);
      }),
    );
  });
});
