/**
 * CommandCodeProvider — snapshot, version probe, and built-in model catalog
 * for the Command Code CLI (`cmd` / `command-code`).
 *
 * Command Code does not implement the Agent Client Protocol; the adapter
 * shells out to `cmd -p <prompt>` for each turn. The provider layer is
 * therefore responsible for:
 *
 *   - reading `cmd --version` to populate the version probe,
 *   - reading `cmd status --json` to extract an auth status (the CLI
 *     prints a single JSON line when authenticated and exits non-zero
 *     otherwise; we treat any non-zero exit as "unauthenticated"),
 *   - publishing a fixed list of built-in models mirroring the
 *     `cmd --list-models` catalog documented at
 *     https://commandcode.ai/docs/reference/cli/models.
 *
 * @module provider/Layers/CommandCodeProvider
 */
import {
  type CommandCodeSettings,
  type ModelCapabilities,
  ProviderDriverKind,
  type ServerProvider,
  type ServerProviderModel,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  buildServerProvider,
  detailFromResult,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const COMMANDCODE_PRESENTATION = {
  displayName: "Command Code",
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
  requiresNewThreadForModelChange: true,
} as const;

const PROVIDER = ProviderDriverKind.make("commandCode");
const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const VERSION_PROBE_TIMEOUT_MS = 4_000;

/**
 * Mirrors the `cmd --list-models` catalog at
 * https://commandcode.ai/docs/reference/cli/models. Updated manually when
 * Command Code ships new models — model discovery over the wire is not
 * exposed by the CLI, and shelling out per snapshot is expensive.
 */
const COMMANDCODE_BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = [
  // Open source.
  {
    slug: "moonshotai/Kimi-K2.7-Code",
    name: "Kimi K2.7 Code",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "moonshotai/Kimi-K2.7-Code-Highspeed",
    name: "Kimi K2.7 Code HighSpeed",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "moonshotai/Kimi-K2.6",
    name: "Kimi K2.6",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "moonshotai/Kimi-K2.5",
    name: "Kimi K2.5",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  { slug: "zai-org/GLM-5.1", name: "GLM 5.1", isCustom: false, capabilities: EMPTY_CAPABILITIES },
  { slug: "zai-org/GLM-5", name: "GLM 5", isCustom: false, capabilities: EMPTY_CAPABILITIES },
  {
    slug: "MiniMaxAI/MiniMax-M3",
    name: "MiniMax M3",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "MiniMaxAI/MiniMax-M2.7",
    name: "MiniMax M2.7",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "MiniMaxAI/MiniMax-M2.5",
    name: "MiniMax M2.5",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "deepseek/deepseek-v4-pro",
    name: "DeepSeek V4 Pro",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "deepseek/deepseek-v4-flash",
    name: "DeepSeek V4 Flash",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "Qwen/Qwen3.6-Max-Preview",
    name: "Qwen 3.6 Max Preview",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "Qwen/Qwen3.6-Plus",
    name: "Qwen 3.6 Plus",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "Qwen/Qwen3.7-Max",
    name: "Qwen 3.7 Max",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "Qwen/Qwen3.7-Plus",
    name: "Qwen 3.7 Plus",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "stepfun/Step-3.7-Flash",
    name: "Step 3.7 Flash",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "stepfun/Step-3.5-Flash",
    name: "Step 3.5 Flash",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "xiaomi/mimo-v2.5-pro",
    name: "MiMo V2.5 Pro",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "xiaomi/mimo-v2.5",
    name: "MiMo V2.5",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "nvidia/nemotron-3-ultra-550b-a55b",
    name: "Nemotron 3 Ultra",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  // Anthropic.
  {
    slug: "claude-sonnet-4-6",
    name: "Claude Sonnet 4.6",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "claude-fable-5",
    name: "Claude Fable 5",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "claude-opus-4-8",
    name: "Claude Opus 4.8",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "claude-opus-4-7",
    name: "Claude Opus 4.7",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "claude-haiku-4-5",
    name: "Claude Haiku 4.5",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  // OpenAI.
  { slug: "gpt-5.5", name: "GPT 5.5", isCustom: false, capabilities: EMPTY_CAPABILITIES },
  { slug: "gpt-5.4", name: "GPT 5.4", isCustom: false, capabilities: EMPTY_CAPABILITIES },
  {
    slug: "gpt-5.3-codex",
    name: "GPT 5.3 Codex",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  { slug: "gpt-5.4-mini", name: "GPT 5.4 Mini", isCustom: false, capabilities: EMPTY_CAPABILITIES },
  // Google.
  {
    slug: "google/gemini-3.5-flash",
    name: "Gemini 3.5 Flash",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "google/gemini-3.1-flash-lite",
    name: "Gemini 3.1 Flash Lite",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
];

function commandCodeModelsFromSettings(
  customModels: ReadonlyArray<string> | undefined,
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(
    COMMANDCODE_BUILT_IN_MODELS,
    PROVIDER,
    customModels ?? [],
    EMPTY_CAPABILITIES,
  );
}

export function buildInitialCommandCodeProviderSnapshot(
  commandCodeSettings: CommandCodeSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = commandCodeModelsFromSettings(commandCodeSettings.customModels);

    if (!commandCodeSettings.enabled) {
      return buildServerProvider({
        presentation: COMMANDCODE_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Command Code is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: COMMANDCODE_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking Command Code CLI availability...",
      },
    });
  });
}

const runCommandCodeCommand = (
  commandCodeSettings: CommandCodeSettings,
  args: ReadonlyArray<string>,
  environment: NodeJS.ProcessEnv = process.env,
) =>
  Effect.gen(function* () {
    const command = commandCodeSettings.binaryPath || "cmd";
    const spawnCommand = yield* resolveSpawnCommand(command, args, { env: environment });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

interface CommandCodeStatusJsonPayload {
  readonly authenticated?: unknown;
  readonly user?: unknown;
  readonly email?: unknown;
  readonly subscription?: unknown;
}

function parseCommandCodeStatusJson(raw: string): CommandCodeStatusJsonPayload | undefined {
  const trimmed = raw.trim();
  if (!trimmed) return undefined;
  // The CLI may prefix the JSON line with a short banner. Find the first
  // `{` and try to parse from there.
  const jsonStart = trimmed.indexOf("{");
  if (jsonStart === -1) return undefined;
  try {
    const parsed = JSON.parse(trimmed.slice(jsonStart)) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return undefined;
    }
    return parsed as CommandCodeStatusJsonPayload;
  } catch {
    return undefined;
  }
}

const runCommandCodeVersionCommand = (
  commandCodeSettings: CommandCodeSettings,
  environment: NodeJS.ProcessEnv = process.env,
) => runCommandCodeCommand(commandCodeSettings, ["--version"], environment);

const runCommandCodeStatusCommand = (
  commandCodeSettings: CommandCodeSettings,
  environment: NodeJS.ProcessEnv = process.env,
) => runCommandCodeCommand(commandCodeSettings, ["status", "--json"], environment);

const runCommandCodeListModelsCommand = (
  commandCodeSettings: CommandCodeSettings,
  environment: NodeJS.ProcessEnv = process.env,
) => runCommandCodeCommand(commandCodeSettings, ["--list-models"], environment);

function extractModelSlugsFromListOutput(raw: string): ReadonlyArray<string> {
  const out = new Set<string>();
  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    // The CLI prints rows like `moonshotai/Kimi-K2.5 ... description`,
    // or just `claude-sonnet-4-6`. Accept the first whitespace-delimited
    // token of every non-comment line.
    const token = trimmed.split(/\s+/, 1)[0];
    if (!token || token.startsWith("#")) continue;
    out.add(token);
  }
  return Array.from(out);
}

function mergeDiscoveredModels(
  discovered: ReadonlyArray<string>,
  fallback: ReadonlyArray<ServerProviderModel>,
): ReadonlyArray<ServerProviderModel> {
  if (discovered.length === 0) return fallback;
  const seen = new Set<string>();
  const merged: Array<ServerProviderModel> = [];
  for (const model of fallback) {
    if (!seen.has(model.slug)) {
      seen.add(model.slug);
      merged.push(model);
    }
  }
  for (const slug of discovered) {
    if (seen.has(slug)) continue;
    seen.add(slug);
    merged.push({
      slug,
      name: slug,
      isCustom: true,
      capabilities: EMPTY_CAPABILITIES,
    });
  }
  return merged;
}

export const checkCommandCodeProviderStatus = Effect.fn("checkCommandCodeProviderStatus")(
  function* (
    commandCodeSettings: CommandCodeSettings,
    environment: NodeJS.ProcessEnv = process.env,
  ): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const fallbackModels = commandCodeModelsFromSettings(commandCodeSettings.customModels);

    if (!commandCodeSettings.enabled) {
      return buildServerProvider({
        presentation: COMMANDCODE_PRESENTATION,
        enabled: false,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Command Code is disabled in T3 Code settings.",
        },
      });
    }

    const versionResult = yield* runCommandCodeVersionCommand(
      commandCodeSettings,
      environment,
    ).pipe(Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS), Effect.result);

    if (Result.isFailure(versionResult)) {
      const error = versionResult.failure;
      return buildServerProvider({
        presentation: COMMANDCODE_PRESENTATION,
        enabled: commandCodeSettings.enabled,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: !isCommandMissingCause(error),
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: isCommandMissingCause(error)
            ? "Command Code CLI (`cmd`) is not installed or not on PATH."
            : `Failed to execute Command Code CLI health check: ${error instanceof Error ? error.message : String(error)}.`,
        },
      });
    }

    if (Option.isNone(versionResult.success)) {
      return buildServerProvider({
        presentation: COMMANDCODE_PRESENTATION,
        enabled: commandCodeSettings.enabled,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: "Command Code CLI is installed but timed out while running `cmd --version`.",
        },
      });
    }

    const versionOutput = versionResult.success.value;
    const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
    if (versionOutput.code !== 0) {
      const detail = detailFromResult(versionOutput);
      return buildServerProvider({
        presentation: COMMANDCODE_PRESENTATION,
        enabled: commandCodeSettings.enabled,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: true,
          version,
          status: "error",
          auth: { status: "unknown" },
          message: detail
            ? `Command Code CLI is installed but failed to run. ${detail}`
            : "Command Code CLI is installed but failed to run.",
        },
      });
    }

    // Best-effort model discovery: `cmd --list-models` is cheap and the
    // CLI is the source of truth, so we prefer its output over our
    // hard-coded catalog when available.
    const listResult = yield* runCommandCodeListModelsCommand(
      commandCodeSettings,
      environment,
    ).pipe(Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS), Effect.exit);
    let models: ReadonlyArray<ServerProviderModel> = fallbackModels;
    if (Exit.isSuccess(listResult) && Option.isSome(listResult.value)) {
      const discoveredSlugs = extractModelSlugsFromListOutput(
        `${listResult.value.value.stdout}\n${listResult.value.value.stderr}`,
      );
      models = mergeDiscoveredModels(
        discoveredSlugs,
        commandCodeModelsFromSettings(commandCodeSettings.customModels),
      );
    } else if (Exit.isFailure(listResult)) {
      yield* Effect.logWarning("Command Code model listing failed", {
        cause: Cause.pretty(listResult.cause),
      });
    }

    // Auth probe: `cmd status --json` exits 0 when authenticated and
    // emits `{"authenticated": true, "user": "...", ...}`. Non-zero or
    // missing JSON is treated as unauthenticated.
    const statusResult = yield* runCommandCodeStatusCommand(commandCodeSettings, environment).pipe(
      Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
      Effect.result,
    );
    let auth: ServerProvider["auth"] = { status: "unknown" };
    let statusMessage: string | undefined;
    if (Result.isSuccess(statusResult) && Option.isSome(statusResult.success)) {
      const out = statusResult.success.value;
      const payload = parseCommandCodeStatusJson(`${out.stdout}\n${out.stderr}`);
      if (out.code === 0 && payload && payload.authenticated === true) {
        const email =
          typeof payload.email === "string" && payload.email.trim()
            ? payload.email.trim()
            : typeof payload.user === "string" && payload.user.trim()
              ? payload.user.trim()
              : undefined;
        const subscription =
          typeof payload.subscription === "string" && payload.subscription.trim()
            ? payload.subscription.trim()
            : undefined;
        auth = {
          status: "authenticated",
          ...(email ? { email } : {}),
          ...(subscription ? { type: subscription, label: `Command Code ${subscription}` } : {}),
        };
      } else if (out.code !== 0) {
        auth = { status: "unauthenticated" };
        statusMessage = "Command Code CLI is not authenticated. Run `cmd login` and try again.";
      } else {
        // JSON parsed but no `authenticated: true` flag.
        auth = { status: "unauthenticated" };
        statusMessage =
          "Command Code CLI did not report an authenticated session. Run `cmd login` and try again.";
      }
    } else if (Result.isFailure(statusResult)) {
      yield* Effect.logWarning("Command Code auth probe failed", {
        cause: Cause.pretty(statusResult.failure),
      });
    }

    const status: ServerProvider["status"] = auth.status === "unauthenticated" ? "error" : "ready";
    return buildServerProvider({
      presentation: COMMANDCODE_PRESENTATION,
      enabled: commandCodeSettings.enabled,
      checkedAt,
      models,
      probe: {
        installed: true,
        version,
        status,
        auth,
        ...(statusMessage ? { message: statusMessage } : {}),
      },
    });
  },
);

export const enrichCommandCodeSnapshot = (input: {
  readonly settings: CommandCodeSettings;
  readonly snapshot: ServerProvider;
  readonly getSnapshot: Effect.Effect<ServerProvider>;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
}): Effect.Effect<void> => {
  // Command Code currently ships without a managed upgrade channel. We
  // republish the snapshot as-is so the wiring is symmetrical with the
  // other drivers and easy to extend when `cmd update` integration lands.
  if (!input.settings.enabled || !input.snapshot.enabled) {
    return Effect.void;
  }
  return input.publishSnapshot(input.snapshot);
};
