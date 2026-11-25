// Routing logic of `ShardedReducerDOPeer` (see `../do-reducer.ts`).
// This module is internal: `../do-reducer.ts` imports it and the build bundles it, but no public entry point
// exports it. It is a separate module only so that the tests can verify the routing invariants directly.

import type { ShardId } from "../do-sharding";
import type { ShardedReducerDORole } from "../do-reducer";

/**
 * @internal
 * Destination of the next hop, returned by `computeShardedReducerNextHop()`.
 */
export interface ShardedReducerDONextHop {
    role: ShardedReducerDORole;
    shardId: ShardId;
}

/**
 * @internal
 * Returns the next hop for the deltas of root `rootShardId` that are pending in ingest Durable Object `shardId`.
 *
 * Each root has its own static tree over the `N` ingest Durable Objects:
 * 1. The ingest Durable Objects form a ring, and the tree of root `r` starts at `offset = floor(r * N / R)`. The
 *    offsets are spread evenly, so different roots use different ingest Durable Objects for their upper tree
 *    levels and no single ingest Durable Object merges the keys of all the roots.
 * 2. `position = (shardId - offset) mod N` is the place of the ingest Durable Object in the tree of root `r`.
 * 3. Positions `0 .. fanIn - 1` send directly to the root, therefore a root has at most `fanIn` senders.
 * 4. Each other position `p` sends to position `floor(p / fanIn) - 1`. The children of position `q` are exactly the
 *    positions `fanIn * (q + 1) .. fanIn * (q + 1) + fanIn - 1`, therefore an ingest Durable Object has at most
 *    `fanIn` senders in each tree.
 *
 * The parent position is always smaller than the child position, so a delta never loops. If
 * `N <= fanIn + fanIn^2 + ... + fanIn^H`, each delta reaches its root in at most `H` hops
 * (for example, `N = 100` and `fanIn = 10` need at most 2 hops).
 */
export function computeShardedReducerNextHop(
    shardId: ShardId,
    rootShardId: ShardId,
    numIngestShards: number,
    numRootShards: number,
    fanIn: number,
): ShardedReducerDONextHop {
    const offset = Math.floor((rootShardId * numIngestShards) / numRootShards);
    const position = (shardId - offset + numIngestShards) % numIngestShards;
    if (position < fanIn) {
        return { role: "root", shardId: rootShardId };
    }
    const parentPosition = Math.floor(position / fanIn) - 1;
    return { role: "ingest", shardId: (parentPosition + offset) % numIngestShards };
}
