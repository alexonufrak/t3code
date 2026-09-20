import { CheckpointRef, type PairRoomId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as ServerConfig from "../config.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";

export class PairWorkspaceError extends Schema.TaggedError<PairWorkspaceError>()(
  "PairWorkspaceError",
  { operation: Schema.String, detail: Schema.String },
) {
  override get message(): string {
    return this.detail;
  }
}

export interface PairAssignmentWorktreePlan {
  readonly repoRoot: string;
  readonly worktreePath: string;
  readonly branch: string;
}

export type PairIntegrationResult =
  | { readonly status: "merged"; readonly commit: string }
  | { readonly status: "conflict"; readonly detail: string }
  /** Uncommitted changes in the target sit on files the merge would write; nothing was merged. */
  | { readonly status: "dirty"; readonly detail: string; readonly files: ReadonlyArray<string> }
  /** The branch or worktree moved after approval; nothing was merged. */
  | { readonly status: "changed"; readonly detail: string };

export interface PairCheckoutInfo {
  /** The worktree root, resolved. */
  readonly path: string;
  /** The branch checked out there, or null when detached. */
  readonly branch: string | null;
}

export interface PairAssignmentBase {
  readonly commit: string;
  /** The local branch the assignment merges back into, or null for a detached checkout. */
  readonly branch: string | null;
}

/**
 * Git work for a pair room. The Peer never runs in the Lead's checkout: it
 * reviews a snapshot in a room-owned detached worktree, and assignments get
 * their own branch and worktree. Destructive commands only ever target paths
 * this module created under the server's worktrees directory.
 */
export class PairWorkspace extends Context.Service<
  PairWorkspace,
  {
    /** Resolves `ref` (default HEAD) in the Lead's checkout. Fails outside a git repository. */
    /** Fails unless `cwd` is inside a git repository, which a room needs before it starts. */
    readonly assertRepository: (input: {
      readonly cwd: string;
    }) => Effect.Effect<void, PairWorkspaceError>;
    readonly resolveCommit: (input: {
      readonly cwd: string;
      readonly ref?: string | undefined;
    }) => Effect.Effect<string, PairWorkspaceError>;
    /**
     * Snapshots the Lead's checkout, including uncommitted and untracked files,
     * and moves the room's review worktree to exactly that state.
     */
    readonly syncReviewWorktree: (input: {
      readonly roomId: PairRoomId;
      readonly leadCwd: string;
    }) => Effect.Effect<
      { readonly worktreePath: string; readonly snapshotCommit: string },
      PairWorkspaceError
    >;
    /** Where an assignment's worktree and branch will live. Creates nothing. */
    readonly planAssignmentWorktree: (input: {
      readonly leadCwd: string;
      readonly assignmentId: string;
      readonly title: string;
    }) => Effect.Effect<PairAssignmentWorktreePlan, PairWorkspaceError>;
    readonly createAssignmentWorktree: (input: {
      readonly plan: PairAssignmentWorktreePlan;
      readonly baseCommit: string;
    }) => Effect.Effect<void, PairWorkspaceError>;
    /**
     * Files that differ from `baseCommit`: committed, uncommitted and untracked.
     * Renames list both sides, so moving a file out of scope is never hidden.
     */
    readonly changedFiles: (input: {
      readonly worktreePath: string;
      readonly baseCommit: string;
    }) => Effect.Effect<ReadonlyArray<string>, PairWorkspaceError>;
    /**
     * Commits leftover assignment changes on its branch and returns the commit
     * that approval covers. Fails if the worktree left its branch.
     */
    readonly sealAssignment: (input: {
      readonly worktreePath: string;
      readonly branch: string;
      readonly message: string;
    }) => Effect.Effect<string, PairWorkspaceError>;
    /** The branch checked out at `cwd`, or null when detached. */
    readonly currentBranch: (input: {
      readonly cwd: string;
    }) => Effect.Effect<string | null, PairWorkspaceError>;
    /**
     * A checkout the room can follow: a worktree of the same repository as
     * `projectCwd`, normalized to its root, with the branch checked out there.
     */
    readonly describeCheckout: (input: {
      readonly cwd: string;
      readonly projectCwd: string;
    }) => Effect.Effect<PairCheckoutInfo, PairWorkspaceError>;
    /**
     * An assignment's base: the commit to branch from and the local branch it
     * merges back into. `ref` must name a local branch; omitted, the base is
     * the checkout's HEAD and its branch.
     */
    readonly resolveBase: (input: {
      readonly cwd: string;
      readonly ref?: string | undefined;
    }) => Effect.Effect<PairAssignmentBase, PairWorkspaceError>;
    /** The worktree that has `branch` checked out, or null when none does. */
    readonly findBranchWorktree: (input: {
      readonly cwd: string;
      readonly branch: string;
    }) => Effect.Effect<string | null, PairWorkspaceError>;
    /** The head of `ref` when it already contains `commit`, else null (also when `ref` is missing). */
    readonly containsCommit: (input: {
      readonly cwd: string;
      readonly ref: string;
      readonly commit: string;
    }) => Effect.Effect<string | null, PairWorkspaceError>;
    /**
     * Merges exactly the approved commit into `targetCwd` with `--no-ff`, after
     * checking the branch still points at it, the assignment worktree is
     * clean, and no uncommitted change in the target sits on a file the merge
     * writes. A conflict aborts the merge and leaves both sides as they were.
     */
    readonly integrate: (input: {
      readonly targetCwd: string;
      readonly worktreePath: string;
      readonly branch: string;
      readonly commit: string;
      readonly message: string;
    }) => Effect.Effect<PairIntegrationResult, PairWorkspaceError>;
  }
>()("t3/pair/PairWorkspace") {}

export const pairReviewRef = (roomId: PairRoomId) => `refs/t3/pair/${roomId}/review`;

const slugify = (value: string) =>
  value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 40) || "work";

const shortId = (value: string) => value.replace(/[^a-zA-Z0-9]/g, "").slice(-8);

export const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const checkpoints = yield* CheckpointStore.CheckpointStore;
  const vcs = yield* VcsProcess.VcsProcess;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  const git = (
    operation: string,
    cwd: string,
    args: ReadonlyArray<string>,
    options: { readonly allowNonZeroExit?: boolean; readonly timeoutMs?: number } = {},
  ) =>
    vcs
      .run({
        operation: `PairWorkspace.${operation}`,
        command: "git",
        cwd,
        // Agents can write inside their worktrees, so the server never runs repository
        // hooks or an fsmonitor command on their behalf.
        args: [
          "-c",
          "core.quotepath=false",
          "-c",
          "core.hooksPath=/dev/null",
          "-c",
          "core.fsmonitor=false",
          ...args,
        ],
        ...options,
      })
      .pipe(
        Effect.mapError(
          (cause) => new PairWorkspaceError({ operation, detail: cause.message || String(cause) }),
        ),
      );

  const repoRoot = (operation: string, cwd: string) =>
    git(operation, cwd, ["rev-parse", "--show-toplevel"], { allowNonZeroExit: true }).pipe(
      Effect.flatMap((output) =>
        output.exitCode === 0 && output.stdout.trim()
          ? Effect.succeed(output.stdout.trim())
          : Effect.fail(
              new PairWorkspaceError({
                operation,
                detail: `Pair rooms need a git repository so the Peer can work on a copy instead of your files, and ${cwd} is not one.`,
              }),
            ),
      ),
    );

  const worktreesRoot = path.resolve(config.worktreesDir);

  const realPath = (value: string) =>
    fileSystem.realPath(value).pipe(Effect.orElseSucceed(() => path.resolve(value)));

  /** Refuses any destructive command on a path this module did not create. */
  const assertRoomOwned = (operation: string, target: string) => {
    const resolved = path.resolve(target);
    return resolved.startsWith(`${worktreesRoot}${path.sep}`) &&
      path.basename(resolved).startsWith("pair-")
      ? Effect.succeed(resolved)
      : Effect.fail(
          new PairWorkspaceError({
            operation,
            detail: `Refusing to modify ${resolved}: it is not a pair room worktree.`,
          }),
        );
  };

  /**
   * Confirms git sees `worktreePath` itself as a worktree root. Without this, a
   * worktree whose `.git` file was removed would send `reset --hard` and
   * `clean` to whatever repository encloses it.
   */
  const assertWorktreeRoot = (operation: string, worktreePath: string) =>
    Effect.gen(function* () {
      const output = yield* git(operation, worktreePath, ["rev-parse", "--show-toplevel"], {
        allowNonZeroExit: true,
      });
      if (
        output.exitCode !== 0 ||
        (yield* realPath(output.stdout.trim())) !== (yield* realPath(worktreePath))
      ) {
        return yield* new PairWorkspaceError({
          operation,
          detail: `Refusing to modify ${worktreePath}: git does not see it as its own worktree.`,
        });
      }
    });

  const worktreePathFor = (root: string, name: string) =>
    path.join(worktreesRoot, path.basename(root), name);

  const assertRepository = (input: { readonly cwd: string }) =>
    repoRoot("assertRepository", input.cwd).pipe(Effect.asVoid);

  const resolveCommit = (input: { readonly cwd: string; readonly ref?: string | undefined }) =>
    Effect.gen(function* () {
      if (input.ref?.startsWith("-")) {
        return yield* new PairWorkspaceError({
          operation: "resolveCommit",
          detail: `"${input.ref}" is not a git ref.`,
        });
      }
      yield* repoRoot("resolveCommit", input.cwd);
      const output = yield* git("resolveCommit", input.cwd, [
        "rev-parse",
        "--verify",
        `${input.ref ?? "HEAD"}^{commit}`,
      ]);
      return output.stdout.trim();
    });

  const currentBranch = (input: { readonly cwd: string }) =>
    git("currentBranch", input.cwd, ["symbolic-ref", "-q", "--short", "HEAD"], {
      allowNonZeroExit: true,
    }).pipe(Effect.map((output) => (output.exitCode === 0 ? output.stdout.trim() || null : null)));

  const describeCheckout = (input: { readonly cwd: string; readonly projectCwd: string }) =>
    Effect.gen(function* () {
      const operation = "describeCheckout";
      const root = yield* repoRoot(operation, input.cwd).pipe(
        Effect.mapError(
          () =>
            new PairWorkspaceError({
              operation,
              detail: `${input.cwd} is not inside a git worktree.`,
            }),
        ),
      );
      const projectRoot = yield* repoRoot(operation, input.projectCwd);
      const commonDir = (cwd: string) =>
        git(operation, cwd, ["rev-parse", "--path-format=absolute", "--git-common-dir"]).pipe(
          Effect.flatMap((output) => realPath(output.stdout.trim())),
        );
      if ((yield* commonDir(root)) !== (yield* commonDir(projectRoot))) {
        return yield* new PairWorkspaceError({
          operation,
          detail: `${root} is not a worktree of this project's repository (${projectRoot}), so the room cannot follow it.`,
        });
      }
      return {
        path: yield* realPath(root),
        branch: yield* currentBranch({ cwd: root }),
      } satisfies PairCheckoutInfo;
    });

  const resolveBase = (input: { readonly cwd: string; readonly ref?: string | undefined }) =>
    Effect.gen(function* () {
      const operation = "resolveBase";
      if (input.ref === undefined) {
        return {
          commit: yield* resolveCommit({ cwd: input.cwd }),
          branch: yield* currentBranch({ cwd: input.cwd }),
        } satisfies PairAssignmentBase;
      }
      const notBranch = new PairWorkspaceError({
        operation,
        detail: `"${input.ref}" is not a local branch. An assignment starts from a branch and merges back into it: pass a branch name, or omit baseRef to use the checkout's branch.`,
      });
      yield* repoRoot(operation, input.cwd);
      // Prints the full ref name only for a single ref; a commit id, a missing name or an option prints nothing.
      const named = yield* git(
        operation,
        input.cwd,
        ["rev-parse", "-q", "--verify", "--symbolic-full-name", input.ref],
        { allowNonZeroExit: true },
      );
      const fullName = named.exitCode === 0 ? named.stdout.trim() : "";
      if (!fullName.startsWith("refs/heads/")) return yield* notBranch;
      return {
        commit: yield* resolveCommit({ cwd: input.cwd, ref: fullName }),
        branch: fullName.slice("refs/heads/".length),
      } satisfies PairAssignmentBase;
    });

  const findBranchWorktree = (input: { readonly cwd: string; readonly branch: string }) =>
    Effect.gen(function* () {
      const operation = "findBranchWorktree";
      const root = yield* repoRoot(operation, input.cwd);
      const list = yield* git(operation, root, ["worktree", "list", "--porcelain"]);
      let current: string | null = null;
      for (const line of list.stdout.split("\n")) {
        if (line.startsWith("worktree ")) current = line.slice("worktree ".length);
        else if (line === `branch refs/heads/${input.branch}` && current) return current;
      }
      return null;
    });

  const containsCommit = (input: {
    readonly cwd: string;
    readonly ref: string;
    readonly commit: string;
  }) =>
    Effect.gen(function* () {
      const operation = "containsCommit";
      const head = yield* git(
        operation,
        input.cwd,
        ["rev-parse", "-q", "--verify", `${input.ref}^{commit}`],
        { allowNonZeroExit: true },
      );
      if (head.exitCode !== 0) return null;
      const ancestor = yield* git(
        operation,
        input.cwd,
        ["merge-base", "--is-ancestor", input.commit, head.stdout.trim()],
        { allowNonZeroExit: true },
      );
      if (ancestor.exitCode === 0) return head.stdout.trim();
      if (ancestor.exitCode === 1) return null;
      return yield* new PairWorkspaceError({
        operation,
        detail: ancestor.stderr.trim() || "git merge-base failed.",
      });
    });

  const syncReviewWorktree = (input: { readonly roomId: PairRoomId; readonly leadCwd: string }) =>
    Effect.gen(function* () {
      const operation = "syncReviewWorktree";
      const root = yield* repoRoot(operation, input.leadCwd);
      const ref = pairReviewRef(input.roomId);
      yield* checkpoints
        .captureCheckpoint({ cwd: input.leadCwd, checkpointRef: CheckpointRef.make(ref) })
        .pipe(
          Effect.mapError(
            (cause) =>
              new PairWorkspaceError({ operation, detail: cause.message || String(cause) }),
          ),
        );
      const snapshotCommit = (yield* git(operation, root, [
        "rev-parse",
        "--verify",
        `${ref}^{commit}`,
      ])).stdout.trim();
      const worktreePath = yield* assertRoomOwned(
        operation,
        worktreePathFor(root, `pair-review-${shortId(input.roomId)}`),
      );
      const exists = yield* fileSystem.exists(worktreePath).pipe(Effect.orElseSucceed(() => false));
      if (!exists) {
        // A review worktree removed from disk is still registered until pruned.
        yield* git(operation, root, ["worktree", "prune"]);
        yield* git(operation, root, ["worktree", "add", "--detach", worktreePath, snapshotCommit], {
          timeoutMs: 120_000,
        });
      } else {
        yield* assertWorktreeRoot(operation, worktreePath);
        yield* git(operation, worktreePath, ["reset", "--hard", snapshotCommit]);
        yield* git(operation, worktreePath, ["clean", "-fd"]);
      }
      return { worktreePath, snapshotCommit };
    });

  const planAssignmentWorktree = (input: {
    readonly leadCwd: string;
    readonly assignmentId: string;
    readonly title: string;
  }) =>
    Effect.gen(function* () {
      const operation = "planAssignmentWorktree";
      const repoRootPath = yield* repoRoot(operation, input.leadCwd);
      const name = `pair-${slugify(input.title)}-${shortId(input.assignmentId)}`;
      const worktreePath = yield* assertRoomOwned(operation, worktreePathFor(repoRootPath, name));
      return {
        repoRoot: repoRootPath,
        worktreePath,
        branch: `pair/${name.slice("pair-".length)}`,
      } satisfies PairAssignmentWorktreePlan;
    });

  const createAssignmentWorktree = (input: {
    readonly plan: PairAssignmentWorktreePlan;
    readonly baseCommit: string;
  }) =>
    Effect.gen(function* () {
      const operation = "createAssignmentWorktree";
      const worktreePath = yield* assertRoomOwned(operation, input.plan.worktreePath);
      yield* git(operation, input.plan.repoRoot, ["worktree", "prune"]);
      yield* git(
        operation,
        input.plan.repoRoot,
        ["worktree", "add", "-b", input.plan.branch, worktreePath, input.baseCommit],
        { timeoutMs: 120_000 },
      );
    });

  const splitNul = (value: string) => value.split("\0").filter((entry) => entry.length > 0);

  const changedFiles = (input: { readonly worktreePath: string; readonly baseCommit: string }) =>
    Effect.gen(function* () {
      const operation = "changedFiles";
      const tracked = yield* git(operation, input.worktreePath, [
        "diff",
        "--name-only",
        "--no-renames",
        "-z",
        input.baseCommit,
      ]);
      const untracked = yield* git(operation, input.worktreePath, [
        "ls-files",
        "--others",
        "--exclude-standard",
        "-z",
      ]);
      return [...new Set([...splitNul(tracked.stdout), ...splitNul(untracked.stdout)])].toSorted();
    });

  const sealAssignment = (input: {
    readonly worktreePath: string;
    readonly branch: string;
    readonly message: string;
  }) =>
    Effect.gen(function* () {
      const operation = "sealAssignment";
      const worktreePath = yield* assertRoomOwned(operation, input.worktreePath);
      yield* assertWorktreeRoot(operation, worktreePath);
      const head = yield* git(operation, worktreePath, ["symbolic-ref", "-q", "HEAD"], {
        allowNonZeroExit: true,
      });
      if (head.stdout.trim() !== `refs/heads/${input.branch}`) {
        return yield* new PairWorkspaceError({
          operation,
          detail: `The assignment worktree is no longer on ${input.branch}. Switch it back before approving.`,
        });
      }
      const status = yield* git(operation, worktreePath, ["status", "--porcelain"]);
      if (status.stdout.trim().length > 0) {
        yield* git(operation, worktreePath, ["add", "-A"]);
        yield* git(operation, worktreePath, ["commit", "--no-verify", "-m", input.message]);
      }
      return (yield* git(operation, worktreePath, [
        "rev-parse",
        "--verify",
        `refs/heads/${input.branch}^{commit}`,
      ])).stdout.trim();
    });

  const integrate = (input: {
    readonly targetCwd: string;
    readonly worktreePath: string;
    readonly branch: string;
    readonly commit: string;
    readonly message: string;
  }) =>
    Effect.gen(function* () {
      const operation = "integrate";
      const worktreePath = yield* assertRoomOwned(operation, input.worktreePath);
      yield* assertWorktreeRoot(operation, worktreePath);
      const branchHead = yield* git(
        operation,
        worktreePath,
        ["rev-parse", "-q", "--verify", `refs/heads/${input.branch}^{commit}`],
        { allowNonZeroExit: true },
      );
      const status = yield* git(operation, worktreePath, ["status", "--porcelain"]);
      if (branchHead.stdout.trim() !== input.commit || status.stdout.trim().length > 0) {
        return {
          status: "changed",
          detail: `${input.branch} changed after it was approved.`,
        } satisfies PairIntegrationResult;
      }
      // A dry run first: it names conflicts without touching the tree, and shows
      // which files the merge writes, so uncommitted work in the target is
      // reported by name instead of as git's refusal.
      const dryRun = yield* git(
        operation,
        input.targetCwd,
        ["merge-tree", "--write-tree", "--no-messages", "--name-only", "HEAD", input.commit],
        { allowNonZeroExit: true, timeoutMs: 120_000 },
      );
      const dryRunLines = dryRun.stdout.trim().split("\n");
      if (dryRun.exitCode === 1) {
        const files = dryRunLines.slice(1).filter((line) => line.length > 0);
        return {
          status: "conflict",
          detail: `Conflicts in ${files.join(", ") || "the merge"}.`,
        } satisfies PairIntegrationResult;
      }
      if (dryRun.exitCode === 0 && dryRunLines[0]) {
        const dirty = yield* git(operation, input.targetCwd, [
          "status",
          "--porcelain",
          "--no-renames",
          "--untracked-files=all",
        ]);
        const dirtyFiles = new Set(
          dirty.stdout
            .split("\n")
            .filter((line) => line.length > 3)
            .map((line) => line.slice(3)),
        );
        if (dirtyFiles.size > 0) {
          const written = yield* git(operation, input.targetCwd, [
            "diff",
            "--name-only",
            "--no-renames",
            "HEAD",
            dryRunLines[0],
          ]);
          const files = written.stdout
            .split("\n")
            .filter((file) => file.length > 0 && dirtyFiles.has(file));
          if (files.length > 0) {
            return {
              status: "dirty",
              files,
              detail: `${files.length} uncommitted ${files.length === 1 ? "file" : "files"} in ${input.targetCwd} would be overwritten by the merge: ${files.slice(0, 8).join(", ")}${files.length > 8 ? ` and ${files.length - 8} more` : ""}. Commit or stash them, then merge again.`,
            } satisfies PairIntegrationResult;
          }
        }
      }
      // Merge the approved commit by id: a branch name can be shadowed by a tag.
      const merge = yield* git(
        operation,
        input.targetCwd,
        ["merge", "--no-ff", "--no-edit", "--no-verify", "-m", input.message, input.commit],
        { allowNonZeroExit: true, timeoutMs: 120_000 },
      );
      if (merge.exitCode === 0) {
        const head = yield* git(operation, input.targetCwd, ["rev-parse", "HEAD"]);
        return { status: "merged", commit: head.stdout.trim() } satisfies PairIntegrationResult;
      }
      const mergeHead = yield* git(
        operation,
        input.targetCwd,
        ["rev-parse", "-q", "--verify", "MERGE_HEAD"],
        { allowNonZeroExit: true },
      );
      if (mergeHead.exitCode === 0) {
        yield* git(operation, input.targetCwd, ["merge", "--abort"]);
      }
      // Git's reason is the first line; the file list after it can be long.
      const lines = `${merge.stdout}\n${merge.stderr}`.trim().split("\n");
      const detail =
        lines.length > 12 ? [lines[0], ...lines.slice(1, 9), "...", ...lines.slice(-2)] : lines;
      return {
        status: "conflict",
        detail: detail.join("\n").trim() || "git merge failed.",
      } satisfies PairIntegrationResult;
    });

  return PairWorkspace.of({
    assertRepository,
    resolveCommit,
    currentBranch,
    describeCheckout,
    resolveBase,
    findBranchWorktree,
    containsCommit,
    syncReviewWorktree,
    planAssignmentWorktree,
    createAssignmentWorktree,
    changedFiles,
    sealAssignment,
    integrate,
  });
});

export const layer = Layer.effect(PairWorkspace, make);
