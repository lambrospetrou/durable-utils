# Changelog

## 0.3.5 (2025-05-05)

- Fix the ESM module imports for each individual sub-module.

## 0.3.3 and 0.3.4 (2025-02-20)

- README updates.

## 0.3.2 (2025-02-19)

- Added `tryWhile` retry utility function.
- Deprecated the `tryN` overload with required `isRetryable` argument and introduced a new overload with `isRetryable` being part of `options`.
- Deprecated the `StaticShardedDOOptions.prefixName` option and introduced `StaticShardedDOOptions.shardGroupName` which is clearer and more descriptive.

## 0.3.1 (2025-02-16)

- Added `prefixName` to the `StaticShardedDO` constructor options. This allows multiple shard groups within the same Durable Object Namespace.

## 0.3.0 (2025-02-16)

- Renamed `FixedShardedDO` to `StaticShardedDO` to better signal the static shard number used.
- Extended `StaticShardedDO` with more methods `some()` to only query specific shards, `tryOne()/trySome()/tryAll()` for non-throwing variations of querying the shards.
- Added retry utilities `tryN()` and `jitterBackoff()`, and integrate them with the `StaticShardedDO` methods as well for automatic retries.
- Added Durable Objects utilities `stubByName()` and `isErrorRetryable()`.

### Breaking changes

None.

## 0.2.3 (2025-01-30)

- Added `FixedShardedDO` abstraction.

## 0.1.4 (2024-10-25)

- Added SQL Schema migrations helpers (`SQLSchemaMigrations`) for [SQLite Durable Objects](https://developers.cloudflare.com/durable-objects/api/sql-storage/).
