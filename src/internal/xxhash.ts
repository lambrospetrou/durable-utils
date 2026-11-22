// Shared xxHash32 of `StaticShardedDO` and `ShardedReducerDO`.
// This module is internal: the utilities import it and the build bundles it, but no public entry point exports it.

import xxhash from "xxhash-wasm";

// Creates the WebAssembly instance once, when the module loads, so that all hashing after that is synchronous.
// Top-level await is the reason this package is ESM-only.
const hashers = await xxhash();

/**
 * @internal
 * Golden Ratio constant used as the default seed for better hash scattering.
 * See https://softwareengineering.stackexchange.com/a/402543
 *
 * WARNING: Never change this value. The shard of every key depends on it.
 */
export const GOLDEN_RATIO = 0x9e3779b1;

/**
 * @internal
 * Returns the xxHash32 of the UTF-8 bytes of `data`.
 *
 * WARNING: Never change the algorithm, the seed, or the encoding. `StaticShardedDO` and `ShardedReducerDO` place
 *          keys in shards with this hash, and a different value moves keys away from their stored data.
 */
export function hash32(data: string, seed: number = GOLDEN_RATIO): number {
    return hashers.h32(data, seed);
}
