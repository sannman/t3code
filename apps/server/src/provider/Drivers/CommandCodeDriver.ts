/**
 * CommandCodeDriver — `ProviderDriver` for the Command Code CLI.
 *
 * The Command Code CLI does not implement the Agent Client Protocol, so
 * this driver differs from the ACP-backed Grok/Cursor drivers in two
 * ways:
 *
 *   - the adapter shells out to `cmd -p <prompt>` per turn, so no
 *     `acp` runtime layer is required;
 *   - there is no managed upgrade channel, so the maintenance resolver
 *     is a `makeManualOnlyProviderMaintenanceCapabilities` (the operator
 *     runs `cmd update` themselves).
 *
 * Mirrors the structure of `GrokDriver`: a plain value whose `create`
 * bundles one `ProviderInstance` (snapshot / adapter / textGeneration)
 * captured over the per-instance `CommandCodeSettings`.
 *
 * @module provider/Drivers/CommandCodeDriver
 */
import {
  type CommandCodeSettings,
  ProviderDriverKind,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { HttpClient } from "effect/unstable/http";
import { ChildProcessSpawner } from "effect/unstable/process";

import { ServerConfig } from "../../config.ts";
import { makeCommandCodeTextGeneration } from "../../textGeneration/CommandCodeTextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeCommandCodeAdapter } from "../Layers/CommandCodeAdapter.ts";
import {
  buildInitialCommandCodeProviderSnapshot,
  checkCommandCodeProviderStatus,
  enrichCommandCodeSnapshot,
} from "../Layers/CommandCodeProvider.ts";
import { ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import {
  makeManualOnlyProviderMaintenanceCapabilities,
  makeStaticProviderMaintenanceResolver,
} from "../providerMaintenance.ts";

const decodeCommandCodeSettings = Schema.decodeSync(CommandCodeSettings);

const DRIVER_KIND = ProviderDriverKind.make("commandCode");
const SNAPSHOT_REFRESH_INTERVAL = Duration.minutes(5);

const UPDATE = makeStaticProviderMaintenanceResolver(
  makeManualOnlyProviderMaintenanceCapabilities({
    provider: DRIVER_KIND,
    packageName: null,
  }),
);

export type CommandCodeDriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | HttpClient.HttpClient
  | Path.Path
  | ProviderEventLoggers
  | ServerConfig;

const withInstanceIdentity =
  (input: {
    readonly instanceId: ProviderInstance["instanceId"];
    readonly displayName: string | undefined;
    readonly accentColor: string | undefined;
    readonly continuationGroupKey: string;
  }) =>
  (snapshot: Omit<ServerProvider, "instanceId" | "driver">): ServerProvider => ({
    ...snapshot,
    instanceId: input.instanceId,
    driver: DRIVER_KIND,
    ...(input.displayName ? { displayName: input.displayName } : {}),
    ...(input.accentColor ? { accentColor: input.accentColor } : {}),
    continuation: { groupKey: input.continuationGroupKey },
  });

export const CommandCodeDriver: ProviderDriver<CommandCodeSettings, CommandCodeDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "Command Code",
    supportsMultipleInstances: true,
  },
  configSchema: CommandCodeSettings,
  defaultConfig: (): CommandCodeSettings => decodeCommandCodeSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const httpClient = yield* HttpClient.HttpClient;
      const eventLoggers = yield* ProviderEventLoggers;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies CommandCodeSettings;
      const maintenanceCapabilities = UPDATE.resolve();

      const adapter = yield* makeCommandCodeAdapter(effectiveConfig, {
        environment: processEnv,
        instanceId,
      });
      const textGeneration = yield* makeCommandCodeTextGeneration(effectiveConfig, processEnv);

      const checkProvider = checkCommandCodeProviderStatus(effectiveConfig, processEnv).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );

      const snapshot = yield* makeManagedServerProvider<CommandCodeSettings>({
        maintenanceCapabilities,
        getSettings: Effect.succeed(effectiveConfig),
        streamSettings: Stream.never,
        haveSettingsChanged: () => false,
        initialSnapshot: (settings) =>
          buildInitialCommandCodeProviderSnapshot(settings).pipe(Effect.map(stampIdentity)),
        checkProvider,
        enrichSnapshot: ({ snapshot: currentSnapshot, publishSnapshot }) =>
          enrichCommandCodeSnapshot({ snapshot: currentSnapshot, publishSnapshot }),
        refreshInterval: SNAPSHOT_REFRESH_INTERVAL,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build Command Code snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};
