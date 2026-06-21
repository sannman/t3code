/**
 * CommandCodeAdapterLive — Command Code CLI via `cmd -p <prompt>`.
 *
 * Command Code does not implement ACP; instead the CLI exposes
 * `cmd -p "<query>"` for non-interactive one-shot prompts. The adapter
 * turns that into a per-turn effect:
 *
 *   startSession(threadId, cwd, model, …)  — reserves a session slot and
 *                                              returns a `ProviderSession`
 *                                              whose `resumeCursor` encodes
 *                                              the chosen model + options
 *                                              (the CLI itself does not
 *                                              support deterministic resume
 *                                              across processes, but we
 *                                              preserve the cursor so the
 *                                              thread keeps the same model
 *                                              binding across turn prompts).
 *   sendTurn(input)                        — shells out to
 *                                              `cmd -p <prompt> [--model …]`
 *                                              with `--auto-accept` and
 *                                              `--trust` reflected from
 *                                              settings, reads stdout, and
 *                                              emits it as a single
 *                                              `content.delta` event before
 *                                              settling the turn. The CLI
 *                                              prints its own tool/command
 *                                              traces to stderr which we
 *                                              surface as a `runtime.warning`
 *                                              event when the process exits
 *                                              non-zero.
 *
 * The adapter is a faithful but minimal subset: it does not attempt to
 * parse the CLI's interactive UI, nor stream partial output, because
 * `cmd -p` runs the agent to completion in one shot before printing the
 * final response. That mirrors how the Cursor / Grok adapters treat their
 * non-streaming fallback paths.
 *
 * @module CommandCodeAdapterLive
 */
import {
  type CommandCodeSettings,
  EventId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  ProviderDriverKind,
  ProviderInstanceId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { type CommandCodeAdapterShape } from "../Services/CommandCodeAdapter.ts";
import { collectStreamAsString } from "../providerSnapshot.ts";

const encodeUnknownJsonStringExit = Schema.encodeUnknownExit(Schema.UnknownFromJsonString);

const PROVIDER = ProviderDriverKind.make("commandCode");
const COMMANDCODE_RESUME_VERSION = 1 as const;

function encodeJsonStringForDiagnostics(input: unknown): string | undefined {
  const result = encodeUnknownJsonStringExit(input);
  return Exit.isSuccess(result) ? result.value : undefined;
}

export interface CommandCodeAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly instanceId?: ProviderInstanceId;
}

interface CommandCodeSessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  notificationFiber: Fiber.Fiber<void, never> | undefined;
  readonly turns: Array<{ id: TurnId; items: Array<unknown> }>;
  activeTurnId: TurnId | undefined;
  /** Number of sendTurn prompts currently in flight or being prepared. */
  promptsInFlight: number;
  currentModelId: string | undefined;
  stopped: boolean;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseCommandCodeResume(raw: unknown): { model?: string } | undefined {
  if (!isRecord(raw)) return undefined;
  if (raw.schemaVersion !== COMMANDCODE_RESUME_VERSION) return undefined;
  const model = typeof raw.model === "string" && raw.model.trim() ? raw.model.trim() : undefined;
  return model ? { model } : {};
}

function resolveCommandCodeModelId(model: string | null | undefined): string | undefined {
  const trimmed = model?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : undefined;
}

export function makeCommandCodeAdapter(
  commandCodeSettings: CommandCodeSettings,
  options?: CommandCodeAdapterLiveOptions,
) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("commandCode");
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const serverConfig = yield* Effect.service(ServerConfig);
    const crypto = yield* Crypto.Crypto;
    const environment = options?.environment ?? process.env;

    const sessions = new Map<ThreadId, CommandCodeSessionContext>();
    const threadLocksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate Command Code runtime identifier.",
            cause,
          }),
      ),
    );
    const nextEventId = Effect.map(randomUUIDv4, (id) => EventId.make(id));
    const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

    const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    const getThreadSemaphore = (threadId: string) =>
      SynchronizedRef.modifyEffect(threadLocksRef, (current) => {
        const existing: Option.Option<Semaphore.Semaphore> = Option.fromNullishOr(
          current.get(threadId),
        );
        return Option.match(existing, {
          onNone: () =>
            Semaphore.make(1).pipe(
              Effect.map((semaphore) => {
                const next = new Map(current);
                next.set(threadId, semaphore);
                return [semaphore, next] as const;
              }),
            ),
          onSome: (semaphore) => Effect.succeed([semaphore, current] as const),
        });
      });

    const withThreadLock = <A, E, R>(threadId: string, effect: Effect.Effect<A, E, R>) =>
      Effect.flatMap(getThreadSemaphore(threadId), (semaphore) => semaphore.withPermit(effect));

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<CommandCodeSessionContext, ProviderAdapterSessionNotFoundError> => {
      const ctx = sessions.get(threadId);
      if (!ctx || ctx.stopped) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }),
        );
      }
      return Effect.succeed(ctx);
    };

    const stopSessionInternal = (ctx: CommandCodeSessionContext) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        if (ctx.notificationFiber) {
          yield* Fiber.interrupt(ctx.notificationFiber);
        }
        yield* Effect.ignore(Scope.close(ctx.scope, Exit.void));
        sessions.delete(ctx.threadId);
        yield* offerRuntimeEvent({
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { exitKind: "graceful" },
        });
      });

    const startSession: CommandCodeAdapterShape["startSession"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          if (input.provider !== undefined && input.provider !== PROVIDER) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            });
          }
          if (!input.cwd?.trim()) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "cwd is required and must be non-empty.",
            });
          }

          const cwd = path.resolve(input.cwd.trim());
          const commandCodeModelSelection =
            input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
          const requestedModel = resolveCommandCodeModelId(commandCodeModelSelection?.model);
          const resumeModel = parseCommandCodeResume(input.resumeCursor)?.model;
          const boundModelId = requestedModel ?? resumeModel;
          const existing = sessions.get(input.threadId);
          if (existing && !existing.stopped) {
            yield* stopSessionInternal(existing);
          }

          const sessionScope = yield* Scope.make("sequential");
          let sessionScopeTransferred = false;
          yield* Effect.addFinalizer(() =>
            sessionScopeTransferred ? Effect.void : Scope.close(sessionScope, Exit.void),
          );

          const now = yield* nowIso;
          const session: ProviderSession = {
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            status: "ready",
            runtimeMode: input.runtimeMode,
            cwd,
            ...(boundModelId ? { model: boundModelId } : {}),
            threadId: input.threadId,
            resumeCursor: {
              schemaVersion: COMMANDCODE_RESUME_VERSION,
              ...(boundModelId ? { model: boundModelId } : {}),
            },
            createdAt: now,
            updatedAt: now,
          };

          const ctx: CommandCodeSessionContext = {
            threadId: input.threadId,
            session,
            scope: sessionScope,
            notificationFiber: undefined,
            turns: [],
            activeTurnId: undefined,
            promptsInFlight: 0,
            currentModelId: boundModelId,
            stopped: false,
          };

          sessions.set(input.threadId, ctx);
          sessionScopeTransferred = true;

          yield* offerRuntimeEvent({
            type: "session.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { message: "Command Code session ready" },
          });
          yield* offerRuntimeEvent({
            type: "session.state.changed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { state: "ready", reason: "Command Code session ready" },
          });
          yield* offerRuntimeEvent({
            type: "thread.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { providerThreadId: input.threadId },
          });

          return session;
        }).pipe(Effect.scoped),
      );

    const buildPrintArgs = (input: {
      readonly prompt: string;
      readonly model: string | undefined;
    }): ReadonlyArray<string> => {
      const args: Array<string> = [
        "-p",
        input.prompt,
        "--max-turns",
        String(Math.max(1, commandCodeSettings.maxTurns)),
      ];
      if (input.model) {
        args.push("--model", input.model);
      }
      if (commandCodeSettings.autoAccept) {
        args.push("--auto-accept");
      }
      if (commandCodeSettings.trustProject) {
        args.push("--trust");
      }
      if (commandCodeSettings.skipOnboarding) {
        args.push("--skip-onboarding");
      }
      return args;
    };

    const runCommandCodePrint = (input: {
      readonly cwd: string;
      readonly args: ReadonlyArray<string>;
    }): Effect.Effect<
      { stdout: string; stderr: string; code: number },
      ProviderAdapterRequestError
    > =>
      Effect.gen(function* () {
        const command = commandCodeSettings.binaryPath || "cmd";
        const spawnCommand = yield* resolveSpawnCommand(command, input.args, { env: environment });
        const child = yield* childProcessSpawner
          .spawn(
            ChildProcess.make(spawnCommand.command, spawnCommand.args, {
              cwd: input.cwd,
              env: environment,
              shell: spawnCommand.shell,
            }),
          )
          .pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "cmd/-p",
                  detail: `Failed to spawn Command Code CLI: ${cause.message ?? String(cause)}.`,
                  cause,
                }),
            ),
          );

        const [stdout, stderr, exitCode] = yield* Effect.all(
          [
            collectStreamAsString(child.stdout),
            collectStreamAsString(child.stderr),
            child.exitCode.pipe(Effect.map(Number)),
          ],
          { concurrency: "unbounded" },
        );

        return { stdout, stderr, code: exitCode };
      }).pipe(Effect.scoped);

    const sendTurn: CommandCodeAdapterShape["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const prepared = yield* withThreadLock(
          input.threadId,
          Effect.gen(function* () {
            const ctx = yield* requireSession(input.threadId);
            const steeringTurnId = ctx.promptsInFlight > 0 ? ctx.activeTurnId : undefined;
            const turnId = steeringTurnId ?? TurnId.make(yield* randomUUIDv4);
            ctx.promptsInFlight += 1;

            return yield* Effect.gen(function* () {
              const turnModelSelection =
                input.modelSelection?.instanceId === boundInstanceId
                  ? input.modelSelection
                  : undefined;
              const requestedModel =
                resolveCommandCodeModelId(turnModelSelection?.model) ?? ctx.currentModelId;
              const text = input.input?.trim();
              if (!text && (input.attachments?.length ?? 0) === 0) {
                return yield* new ProviderAdapterValidationError({
                  provider: PROVIDER,
                  operation: "sendTurn",
                  issue: "Turn requires non-empty text or attachments.",
                });
              }

              const promptParts: Array<string> = [];
              if (text) {
                promptParts.push(text);
              }
              if (input.attachments && input.attachments.length > 0) {
                const fileReferences: Array<string> = [];
                for (const attachment of input.attachments) {
                  const attachmentPath = resolveAttachmentPath({
                    attachmentsDir: serverConfig.attachmentsDir,
                    attachment,
                  });
                  if (!attachmentPath) {
                    return yield* new ProviderAdapterRequestError({
                      provider: PROVIDER,
                      method: "cmd/-p",
                      detail: `Invalid attachment id '${attachment.id}'.`,
                    });
                  }
                  const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
                    Effect.mapError(
                      (cause) =>
                        new ProviderAdapterRequestError({
                          provider: PROVIDER,
                          method: "cmd/-p",
                          detail: cause.message,
                          cause,
                        }),
                    ),
                  );
                  fileReferences.push(
                    `Attached file: ${attachment.name ?? attachment.id} (${attachment.mimeType}, ${bytes.byteLength} bytes, saved at ${attachmentPath})`,
                  );
                }
                promptParts.push(fileReferences.join("\n"));
              }

              ctx.currentModelId = requestedModel;
              ctx.activeTurnId = turnId;
              if (steeringTurnId === undefined) {
                ctx.session = {
                  ...ctx.session,
                  activeTurnId: turnId,
                  updatedAt: yield* nowIso,
                  ...(requestedModel ? { model: requestedModel } : {}),
                  resumeCursor: {
                    schemaVersion: COMMANDCODE_RESUME_VERSION,
                    ...(requestedModel ? { model: requestedModel } : {}),
                  },
                };
                yield* offerRuntimeEvent({
                  type: "turn.started",
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId,
                  payload: requestedModel ? { model: requestedModel } : {},
                });
              }

              return {
                prompt: promptParts.join("\n\n"),
                model: requestedModel,
                turnId,
                ctx,
              };
            }).pipe(
              Effect.tapCause(() =>
                Effect.sync(() => {
                  ctx.promptsInFlight = Math.max(0, ctx.promptsInFlight - 1);
                }),
              ),
            );
          }),
        );

        const result = yield* runCommandCodePrint({
          cwd: prepared.ctx.session.cwd,
          args: buildPrintArgs({ prompt: prepared.prompt, model: prepared.model }),
        }).pipe(
          Effect.timeoutOption(Math.max(1_000, commandCodeSettings.printTimeoutMs)),
          Effect.mapError((cause) =>
            cause instanceof ProviderAdapterRequestError && cause.method === "cmd/-p"
              ? cause
              : new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: `Command Code CLI request failed: ${cause.message ?? String(cause)}.`,
                  cause,
                }),
          ),
          Effect.flatMap((output) =>
            Option.match(output, {
              onNone: () =>
                Effect.fail(
                  new ProviderAdapterProcessError({
                    provider: PROVIDER,
                    threadId: input.threadId,
                    detail: `Command Code CLI did not complete within ${commandCodeSettings.printTimeoutMs}ms.`,
                  }),
                ),
              onSome: (value) => Effect.succeed(value),
            }),
          ),
        );

        const trimmedOutput = result.stdout.trim();
        const trimmedStderr = result.stderr.trim();

        yield* withThreadLock(
          input.threadId,
          Effect.gen(function* () {
            const ctx = yield* requireSession(input.threadId);
            if (ctx.activeTurnId !== prepared.turnId) {
              // The session was reset (or turned over) between prep and
              // completion; drop the late result on the floor.
              return;
            }
            ctx.turns = [
              ...ctx.turns,
              {
                id: prepared.turnId,
                items: [
                  {
                    prompt: prepared.prompt,
                    stdout: result.stdout,
                    stderr: result.stderr,
                    code: result.code,
                  },
                ],
              },
            ];
            ctx.session = {
              ...ctx.session,
              activeTurnId: prepared.turnId,
              updatedAt: yield* nowIso,
            };

            if (result.code !== 0 && !trimmedOutput) {
              yield* offerRuntimeEvent({
                type: "runtime.error",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: input.threadId,
                turnId: prepared.turnId,
                payload: {
                  message: trimmedStderr
                    ? `Command Code CLI exited with code ${result.code}: ${trimmedStderr.slice(0, 2000)}`
                    : `Command Code CLI exited with code ${result.code}.`,
                  class: "provider_error",
                  detail: encodeJsonStringForDiagnostics(result.stderr)?.slice(0, 2000),
                },
              });
            } else if (trimmedStderr) {
              // Surface stderr as a non-fatal warning so the operator can
              // see tool/command traces from the agent run.
              yield* offerRuntimeEvent({
                type: "runtime.warning",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: input.threadId,
                turnId: prepared.turnId,
                payload: {
                  message: trimmedStderr.slice(0, 2000),
                  detail: encodeJsonStringForDiagnostics(result.stderr)?.slice(0, 2000),
                },
              });
            }

            if (trimmedOutput) {
              yield* offerRuntimeEvent({
                type: "content.delta",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: input.threadId,
                turnId: prepared.turnId,
                payload: { streamKind: "assistant_text", delta: trimmedOutput },
              });
            }

            yield* offerRuntimeEvent({
              type: "turn.completed",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              turnId: prepared.turnId,
              payload: {
                state: result.code === 0 ? "completed" : "failed",
                stopReason: result.code === 0 ? "end_turn" : `exit_code:${result.code}`,
              },
            });
          }),
        );

        return {
          threadId: input.threadId,
          turnId: prepared.turnId,
          resumeCursor: prepared.ctx.session.resumeCursor,
        };
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            const liveCtx = sessions.get(input.threadId);
            if (liveCtx) {
              liveCtx.promptsInFlight = Math.max(0, liveCtx.promptsInFlight - 1);
            }
          }),
        ),
      );

    const interruptTurn: CommandCodeAdapterShape["interruptTurn"] = (threadId) =>
      Effect.gen(function* () {
        // The CLI is a one-shot child per turn; the only way to interrupt
        // is to stop the session. The provider's reactor will see the
        // session exit and surface that to the user.
        const ctx = yield* requireSession(threadId);
        yield* stopSessionInternal(ctx);
      });

    const respondToRequest: CommandCodeAdapterShape["respondToRequest"] = () =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session/request_permission",
          detail:
            "Command Code CLI does not surface approval requests; enable auto-accept in settings to bypass the interactive permission prompt.",
        }),
      );

    const respondToUserInput: CommandCodeAdapterShape["respondToUserInput"] = () =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session/user_input",
          detail:
            "Command Code CLI does not surface structured user-input requests; run `cmd -p` interactively to answer questions.",
        }),
      );

    const readThread: CommandCodeAdapterShape["readThread"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        return { threadId, turns: ctx.turns };
      });

    const rollbackThread: CommandCodeAdapterShape["rollbackThread"] = (threadId, numTurns) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        if (!Number.isInteger(numTurns) || numTurns < 1) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "rollbackThread",
            issue: "numTurns must be an integer >= 1.",
          });
        }
        const nextLength = Math.max(0, ctx.turns.length - numTurns);
        ctx.turns.splice(nextLength);
        return { threadId, turns: ctx.turns };
      });

    const stopSession: CommandCodeAdapterShape["stopSession"] = (threadId) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          yield* stopSessionInternal(ctx);
        }),
      );

    const listSessions: CommandCodeAdapterShape["listSessions"] = () =>
      Effect.sync(() => Array.from(sessions.values(), (c) => ({ ...c.session })));

    const hasSession: CommandCodeAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => {
        const c = sessions.get(threadId);
        return c !== undefined && !c.stopped;
      });

    const stopAll: CommandCodeAdapterShape["stopAll"] = () =>
      Effect.forEach(Array.from(sessions.values()), stopSessionInternal, { discard: true });

    yield* Effect.addFinalizer(() =>
      Effect.forEach(Array.from(sessions.values()), stopSessionInternal, { discard: true }).pipe(
        Effect.catch(() => Effect.void),
        Effect.tap(() => PubSub.shutdown(runtimeEventPubSub)),
      ),
    );

    const streamEvents = Stream.fromPubSub(runtimeEventPubSub);

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "in-session" },
      startSession,
      sendTurn,
      interruptTurn,
      readThread,
      rollbackThread,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      stopAll,
      streamEvents,
    } satisfies CommandCodeAdapterShape;
  });
}
