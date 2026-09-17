/**
 * Identity of this fork's desktop app and bundled server. Every name the stock
 * T3 Code app keys state by differs here (bundle id, data directory, Electron
 * app name, which also names the "Safe Storage" keychain item, userData folder
 * and URL scheme), so both apps install side by side on one Mac without
 * sharing a database, login, keychain entry or deep link.
 */
export const DESKTOP_FORK_IDENTITY = {
  appId: "com.alexonufrak.t3code.pair",
  appBaseName: "T3 Code Pair",
  /** Electron app name: userData, the keychain "Safe Storage" item and the updater cache use it. */
  packageName: "t3code-pair",
  artifactPrefix: "T3-Code-Pair",
  productionScheme: "t3code-pair",
  developmentScheme: "t3code-pair-dev",
  /** Default T3 home under the user's home directory, instead of `.t3`. */
  homeDirName: ".t3-pair",
  /** Provider CLI homes inside the T3 home, so the fork never writes to ~/.claude or ~/.codex. */
  claudeHomeDirName: "claude",
  codexHomeDirName: "codex",
} as const;

export const DESKTOP_FORK_RENDERER_ORIGINS = [
  `${DESKTOP_FORK_IDENTITY.productionScheme}://app`,
  `${DESKTOP_FORK_IDENTITY.developmentScheme}://app`,
] as const;
