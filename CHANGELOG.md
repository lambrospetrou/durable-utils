# Changelog

## 0.3.0

- Renamed `FixedShardedDO` to `StaticShardedDO` to better signal the static shard number used.
- Extended `StaticShardedDO` with more methods `some()` to only query specific shards, `tryOne()/trySome()/tryAll()` for non-throwing variations of querying the shards.
- Added retry utilities `tryN()` and `jitterBackoff()`, and integrate them with the `StaticShardedDO` methods as well for automatic retries.
- Added Durable Objects utilities `stubByName()` and `isErrorRetryable()`.

### Breaking changes

None.

## 0.2.3

- Added `FixedShardedDO` abstraction.
