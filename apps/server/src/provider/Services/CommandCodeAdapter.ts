/**
 * CommandCodeAdapter — shape type for the Command Code provider adapter.
 *
 * Mirrors the Grok/Cursor shape anchors: the driver model
 * ({@link ../Drivers/CommandCodeDriver}) bundles one adapter per instance
 * as a captured closure, so this module only retains the shape interface.
 *
 * @module CommandCodeAdapter
 */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

/**
 * CommandCodeAdapterShape — per-instance Command Code adapter contract.
 */
export interface CommandCodeAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}
