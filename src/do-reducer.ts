/**
 * # Sharded Reducer DO
 *
 * A write-scalable aggregation pattern for Cloudflare Durable Objects (DOs).
 *
 * A single DO processes its requests one at a time, so one hot aggregate (a global counter, clicks per campaign,
 * latency statistics per endpoint, ...) is capped by the throughput of one DO. Spreading the writes across `N` DOs
 * removes that cap, but then every read has to query all `N` DOs and merge their partial results.
 *
 * This module keeps both sides cheap:
 * - Writes are spread across `N` "ingest" DOs.
 * - Each `reductionKey` is owned by exactly one of `R` "root" DOs, which always holds the latest fully reduced value
 *   for that key. A read is a single RPC call to a deterministic DO.
 *
 * ## Topology: `R + N` Durable Objects per shard group
 *
 * - `${shardGroupName}-ingest-${0..N-1}`: receive client writes (`hash(hashKey) % N`) and partial results from other
 *   ingest DOs, and forward partial results towards the roots on a DO alarm.
 * - `${shardGroupName}-root-${0..R-1}`: own the reduced state of each `reductionKey` (`hash(reductionKey) % R`).
 *   Roots never receive raw client writes, only pre-aggregated batches, and they serve the reads.
 *
 * ## Data flow
 *
 * 1. `ShardedReducerDO.send()` sends a payload to the ingest DO `hash(hashKey) % N`.
 * 2. The ingest DO folds the payload into one pending delta per `reductionKey` (the `ingest` operator) and sets an
 *    alarm `flushIntervalMs` later. All writes for the same key in that window collapse into one delta.
 * 3. On the alarm, the ingest DO sends its deltas one hop closer to the root of each key, with one batched RPC
 *    call per destination. Intermediate ingest DOs merge the deltas they receive with their own (the `combine`
 *    operator) and forward them on their own alarm.
 * 4. The root DO applies the incoming deltas to its stored state (the `apply` operator).
 * 5. `ShardedReducerDO.get()` reads the state from the root DO of the key.
 *
 * A direct send from every ingest DO to the root would give each root up to `N` RPC calls per flush interval
 * (`N = 100` and `100ms` is `1000` RPS). Instead, deltas travel through a static tree in which every DO receives from
 * at most `fanIn` senders, see `computeShardedReducerNextHop()` in `internal/sharded-reducer-routing.ts`. A delta
 * reaches its root in about `log_fanIn(N)` hops, therefore reads lag behind writes by about `hops * flushIntervalMs`.
 *
 * ## Guarantees and requirements
 *
 * - The reducer operators must be associative and commutative, because deltas arrive in any order and grouping.
 * - Delivery between hops is at-least-once. If an RPC call fails after the receiver applied the batch (for example
 *   a network error or a DO reset while it sends the response), the sender sends the batch again and the deltas are
 *   applied twice. Use idempotent operators (max, min, set union, ...) if duplicates are not acceptable.
 * - `durability: "storage"` (default) persists a write in the ingest DO before `send()` resolves, and every hop keeps
 *   it in storage until the next hop acknowledges it. `durability: "memory"` keeps the write only in memory until it
 *   reaches the root, and a DO restart before that loses it. Root state is always persisted.
 * - The DO class must use the SQLite storage backend, since the state uses the synchronous `ctx.storage.kv` API.
 * - `ShardedReducerDOPeer` owns the DO alarm. Delegate `alarm()` to it and do not use alarms for other purposes in
 *   the same DO class.
 */

import { ShardId, TryResult } from "./do-sharding";
import { isErrorRetryable, stubByName } from "./do-utils";
import { tryN, tryWhile } from "./retries";
import { hash32 } from "./internal/xxhash";
import { computeShardedReducerNextHop, ShardedReducerDONextHop } from "./internal/sharded-reducer-routing";

/**
 * The role of a Durable Object in a `ShardedReducerDO` group.
 * - `"ingest"`: Receives client writes and partial results, buffers deltas, and forwards them on its alarm.
 * - `"root"`: Receives pre-aggregated batches, stores the reduced state, and serves reads.
 */
export type ShardedReducerDORole = "ingest" | "root";

/**
 * Controls how an ingest Durable Object buffers a write until it reaches its root.
 * - `"storage"`: Persisted before `send()` resolves, and at every hop until the next hop acknowledges it.
 * - `"memory"`: Kept only in memory until it reaches the root. A Durable Object restart loses it.
 */
export type ShardedReducerDODurability = "storage" | "memory";

/**
 * Topology of the group, sent with every RPC call so that each Durable Object can route its deltas.
 */
export interface ShardedReducerDOTopology {
    shardGroupName: string;
    role: ShardedReducerDORole;
    shardId: ShardId;
    numIngestShards: number;
    numRootShards: number;
    fanIn: number;
    flushIntervalMs: number;
}

/**
 * Payload of `reduceIngest`, sent by `ShardedReducerDO` to an ingest Durable Object.
 */
export interface ShardedReducerDOIngestMessage<Input> {
    topology: ShardedReducerDOTopology;
    reductionKey: string;
    payload: Input;
    durability: ShardedReducerDODurability;
}

/**
 * One reduced delta of a `reductionKey` sent between Durable Objects.
 */
export interface ShardedReducerDOMergeEntry<Delta> {
    key: string;
    delta: Delta;
    /** True if the delta contains at least one write with `durability: "storage"`. */
    durable: boolean;
}

/**
 * Payload of `reduceMerge`, sent between Durable Objects of the group.
 */
export interface ShardedReducerDOMergeMessage<Delta> {
    topology: ShardedReducerDOTopology;
    entries: Array<ShardedReducerDOMergeEntry<Delta>>;
}

/**
 * RPC methods that your Durable Object class implements by delegating to `ShardedReducerDOPeer`.
 */
export interface ShardedReducerDOTarget<Input, Delta = Input, State = Delta> extends Rpc.DurableObjectBranded {
    reduceIngest(msg: ShardedReducerDOIngestMessage<Input>): Promise<void>;
    reduceMerge(msg: ShardedReducerDOMergeMessage<Delta>): Promise<void>;
    reduceGet(reductionKey: string): Promise<State | undefined>;
}

/**
 * Reducer operators for when `Input`, `Delta`, and `State` are different types.
 * `Delta` and `State` values must not be `undefined`, since `undefined` means "no value".
 */
export interface ShardedReducerDOOperators<Input, Delta = Input, State = Delta> {
    /**
     * Folds a client payload into the pending delta of `reductionKey` on an ingest Durable Object.
     */
    ingest: (currentDelta: Delta | undefined, input: Input, reductionKey: string) => Delta;

    /**
     * Merges two deltas of the same `reductionKey`. Must be associative and commutative.
     */
    combine: (deltaA: Delta, deltaB: Delta, reductionKey: string) => Delta;

    /**
     * Applies a delta to the stored state of `reductionKey` on its root Durable Object.
     */
    apply: (currentState: State | undefined, delta: Delta, reductionKey: string) => State;
}

/**
 * Single merge function for when `Input`, `Delta`, and `State` are the same type `T`.
 * Must be associative and commutative.
 */
export type ShardedReducerDOMergeFn<T> = (a: T, b: T, reductionKey: string) => T;

/**
 * Options of `ShardedReducerDOPeer` that do not depend on the reducer types.
 */
export interface ShardedReducerDOPeerBaseOptions {
    /**
     * Maximum number of `reductionKey` entries in one `reduceMerge` RPC call.
     * Defaults to `500`.
     */
    maxKeysPerBatch?: number;

    /**
     * Maximum number of concurrent `reduceMerge` RPC calls during one alarm.
     * Defaults to `10`.
     */
    flushConcurrency?: number;

    /**
     * Maximum attempts for one `reduceMerge` RPC call with retryable errors. A batch that still fails goes back to
     * the pending deltas and the next alarm sends it again.
     * Defaults to `3`.
     */
    maxFlushAttempts?: number;

    /**
     * Prefix of all the keys this helper writes in `ctx.storage.kv`, to keep them apart from your own data.
     * Defaults to `"__du_reducer:"`.
     */
    storagePrefix?: string;

    /**
     * Location hint for the Durable Objects this helper calls.
     * Use the same function as `ShardedReducerDOOptions.shardLocationHintFn`.
     */
    shardLocationHintFn?: (role: ShardedReducerDORole, shard: ShardId) => DurableObjectLocationHint | undefined;
}

/**
 * Options of `ShardedReducerDOPeer` with the `{ ingest, combine, apply }` reducer operators.
 */
export interface ShardedReducerDOPeerOperatorsOptions<Input, Delta = Input, State = Delta>
    extends ShardedReducerDOPeerBaseOptions {
    reducer: ShardedReducerDOOperators<Input, Delta, State>;
}

/**
 * Options of `ShardedReducerDOPeer` with a single merge function, when `Input`, `Delta`, and `State` are the same type.
 */
export interface ShardedReducerDOPeerMergeOptions<T> extends ShardedReducerDOPeerBaseOptions {
    reducer: ShardedReducerDOMergeFn<T>;
}

/**
 * Options of `ShardedReducerDOPeer`.
 */
export type ShardedReducerDOPeerOptions<Input, Delta = Input, State = Delta> =
    | ShardedReducerDOPeerOperatorsOptions<Input, Delta, State>
    | ShardedReducerDOPeerMergeOptions<Input & Delta & State>;

/**
 * Options of the `ShardedReducerDO` client.
 *
 * All clients of the same `shardGroupName` must use the same `numIngestShards`, `numRootShards`, and `fanIn`.
 * Durable Objects reject calls with a different topology from the one they already use.
 */
export interface ShardedReducerDOOptions {
    /**
     * Number of ingest Durable Objects (`N >= 1`) that receive the writes.
     *
     * WARNING: Never change this value for a `shardGroupName` that is in use.
     */
    numIngestShards: number;

    /**
     * Number of root Durable Objects (`R >= 1`) that store the reduced values and serve the reads.
     * The group uses `numRootShards + numIngestShards` Durable Objects in total.
     * Defaults to `1`.
     *
     * WARNING: Never change this value for a `shardGroupName` that is in use, since the root of a key is
     *          `hash(reductionKey) % numRootShards` and existing values would become unreachable.
     */
    numRootShards?: number;

    /**
     * Maximum number of Durable Objects that send to the same Durable Object in one flush interval (`fanIn >= 2`).
     * Defaults to `10`.
     */
    fanIn?: number;

    /**
     * Milliseconds an ingest Durable Object collects deltas before it forwards them one hop.
     * Reads lag behind writes by about `hops * flushIntervalMs`.
     * Defaults to `25`.
     */
    flushIntervalMs?: number;

    /**
     * Name of this group of Durable Objects. Different groups in the same Durable Object namespace are independent.
     * Defaults to `"sharded-reducer-do"`.
     */
    shardGroupName?: string;

    /**
     * Location hint for each Durable Object of the group.
     * If it returns `undefined`, Cloudflare places the Durable Object in the closest region.
     */
    shardLocationHintFn?: (role: ShardedReducerDORole, shard: ShardId) => DurableObjectLocationHint | undefined;
}

/**
 * Write parameters of `ShardedReducerDO.send()` and `ShardedReducerDO.trySend()`.
 */
export interface ShardedReducerDOSendParams<Input> {
    /**
     * Hashed to select the ingest Durable Object that receives the write.
     */
    hashKey: string;

    /**
     * Key of the aggregated value. Selects the root Durable Object that stores the value.
     * Must be at most about 2 KiB, the Durable Object storage key size limit.
     */
    reductionKey: string;

    /**
     * Payload for the `ingest` operator.
     */
    payload: Input;
}

/**
 * Options of `ShardedReducerDO.send()` and `ShardedReducerDO.trySend()`.
 */
export interface ShardedReducerDOSendOptions {
    /**
     * How the ingest Durable Objects buffer this write until it reaches its root.
     * Defaults to `"storage"`.
     */
    durability?: ShardedReducerDODurability;

    /**
     * Returns `true` to retry the call to the ingest Durable Object after the given error.
     * WARNING: A retry after the write was applied makes the write count twice.
     */
    shouldRetry?: (error: unknown, attempt: number, shard: ShardId) => boolean;
}

/**
 * Options of `ShardedReducerDO.get()` and `ShardedReducerDO.tryGet()`.
 */
export interface ShardedReducerDOReadOptions {
    /**
     * Returns `true` to retry the read from the root Durable Object after the given error.
     */
    shouldRetry?: (error: unknown, attempt: number, shard: ShardId) => boolean;
}

function shardStubName(shardGroupName: string, role: ShardedReducerDORole, shardId: ShardId): string {
    return `${shardGroupName}-${role}-${shardId}`;
}

function hashKeyToShard(key: string, numShards: number): ShardId {
    return hash32(key) % numShards;
}

/**
 * The part of `DurableObjectNamespace` that `ShardedReducerDOPeer` uses.
 * A structural type keeps TypeScript from expanding the RPC types of your Durable Object class.
 */
export interface ShardedReducerDONamespace {
    idFromName(name: string): DurableObjectId;
    get(id: DurableObjectId, options?: DurableObjectNamespaceGetDurableObjectOptions): unknown;
}

// Values are boxed, so that a `Delta` or `State` value that is falsy (for example `0`) is not read as "missing".
interface StoredBox<V> {
    v: V;
}

// The deltas of one `reductionKey` drained into a flush, kept apart by durability.
interface FlushItem<Delta> {
    key: string;
    durable?: Delta;
    memory?: Delta;
}

interface FlushBatch<Delta> {
    hop: ShardedReducerDONextHop;
    items: Array<FlushItem<Delta>>;
}

/**
 * Helper that you create inside your Durable Object class to make it part of a `ShardedReducerDO` group.
 *
 * Implement `ShardedReducerDOTarget` and delegate `reduceIngest`, `reduceMerge`, `reduceGet`, and `alarm()` to
 * `ingest()`, `merge()`, `get()`, and `alarm()` of this helper.
 *
 * Storage layout, all keys under `storagePrefix`:
 * - `meta`: topology of an ingest Durable Object, so that `alarm()` can route its deltas after a restart.
 * - `p:<reductionKey>`: on ingest Durable Objects, the durable deltas that the next hop has not acknowledged.
 * - `s:<reductionKey>`: on root Durable Objects, the reduced state.
 */
export class ShardedReducerDOPeer<Input, Delta = Input, State = Delta> {
    #ctx: DurableObjectState;
    #doNamespace: DurableObjectNamespace<undefined>;
    #operators: ShardedReducerDOOperators<Input, Delta, State>;
    #maxKeysPerBatch: number;
    #flushConcurrency: number;
    #maxFlushAttempts: number;
    #storagePrefix: string;
    #shardLocationHintFn?: (role: ShardedReducerDORole, shard: ShardId) => DurableObjectLocationHint | undefined;

    #loaded = false;
    #topology?: ShardedReducerDOTopology;
    #flushScheduled = false;

    // Deltas not yet drained into a flush. They are kept apart by durability, so that "memory" writes never
    // touch storage. Both parts of a key are combined when the key is flushed.
    #durablePending = new Map<string, Delta>();
    #memoryPending = new Map<string, Delta>();
    // Durable deltas of the flush in progress that the next hop has not acknowledged yet.
    // Invariant: storage key `p:<key>` always holds `combine(#durableInflight[key], #durablePending[key])`.
    #durableInflight = new Map<string, Delta>();

    constructor(
        ctx: DurableObjectState,
        doNamespace: ShardedReducerDONamespace,
        options: ShardedReducerDOPeerOperatorsOptions<Input, Delta, State>,
    );
    constructor(
        ctx: DurableObjectState,
        doNamespace: ShardedReducerDONamespace,
        options: ShardedReducerDOPeerMergeOptions<Input & Delta & State>,
    );
    constructor(
        ctx: DurableObjectState,
        doNamespace: ShardedReducerDONamespace,
        options: ShardedReducerDOPeerOptions<Input, Delta, State>,
    ) {
        const maxKeysPerBatch = options.maxKeysPerBatch ?? 500;
        const flushConcurrency = options.flushConcurrency ?? 10;
        const maxFlushAttempts = options.maxFlushAttempts ?? 3;
        const storagePrefix = options.storagePrefix ?? "__du_reducer:";

        if (!Number.isInteger(maxKeysPerBatch) || maxKeysPerBatch <= 0) {
            throw new Error("Invalid maxKeysPerBatch, must be an integer greater than 0");
        }
        if (!Number.isInteger(flushConcurrency) || flushConcurrency <= 0) {
            throw new Error("Invalid flushConcurrency, must be an integer greater than 0");
        }
        if (!Number.isInteger(maxFlushAttempts) || maxFlushAttempts <= 0) {
            throw new Error("Invalid maxFlushAttempts, must be an integer greater than 0");
        }
        if (!storagePrefix) {
            throw new Error("Invalid storagePrefix, must not be empty");
        }

        this.#ctx = ctx;
        this.#doNamespace = doNamespace as unknown as DurableObjectNamespace<undefined>;
        this.#operators = normalizeOperators(options.reducer);
        this.#maxKeysPerBatch = maxKeysPerBatch;
        this.#flushConcurrency = flushConcurrency;
        this.#maxFlushAttempts = maxFlushAttempts;
        this.#storagePrefix = storagePrefix;
        this.#shardLocationHintFn = options.shardLocationHintFn;
    }

    /**
     * Handles a client write sent by `ShardedReducerDO.send()`. Call it from `reduceIngest`.
     */
    async ingest(msg: ShardedReducerDOIngestMessage<Input>): Promise<void> {
        if (msg.topology.role !== "ingest") {
            throw new Error(`Invalid topology role '${msg.topology.role}' for ingest, expected 'ingest'`);
        }
        this.#load(msg.topology);

        // Storage is written before memory, so that a failed operator or write leaves no partial change behind.
        const key = msg.reductionKey;
        if (msg.durability === "memory") {
            this.#memoryPending.set(key, this.#operators.ingest(this.#memoryPending.get(key), msg.payload, key));
        } else {
            const pending = this.#operators.ingest(this.#durablePending.get(key), msg.payload, key);
            this.#persistDurable(key, pending);
            this.#durablePending.set(key, pending);
        }
        await this.#scheduleFlush();
    }

    /**
     * Handles a batch of deltas sent by another Durable Object of the group. Call it from `reduceMerge`.
     */
    async merge(msg: ShardedReducerDOMergeMessage<Delta>): Promise<void> {
        const topology = this.#load(msg.topology)!;
        if (msg.entries.length === 0) {
            return;
        }

        if (topology.role === "root") {
            this.#ctx.storage.transactionSync(() => {
                for (const { key, delta } of msg.entries) {
                    const stateKey = this.#stateKey(key);
                    const current = this.#ctx.storage.kv.get<StoredBox<State>>(stateKey);
                    this.#ctx.storage.kv.put<StoredBox<State>>(stateKey, {
                        v: this.#operators.apply(current?.v, delta, key),
                    });
                }
            });
            return;
        }

        // Compute all the new deltas and persist them before memory changes. If an operator or the storage write
        // throws, the sender gets the error and sends the batch again, and no delta of the batch is applied twice.
        const durableUpdates = new Map<string, Delta>();
        const memoryUpdates = new Map<string, Delta>();
        for (const { key, delta, durable } of msg.entries) {
            const [updates, pending] = durable
                ? [durableUpdates, this.#durablePending]
                : [memoryUpdates, this.#memoryPending];
            updates.set(key, this.#combine(key, updates.get(key) ?? pending.get(key), delta)!);
        }
        if (durableUpdates.size > 0) {
            this.#ctx.storage.transactionSync(() => {
                for (const [key, pending] of durableUpdates) {
                    this.#persistDurable(key, pending);
                }
            });
        }
        for (const [key, pending] of durableUpdates) {
            this.#durablePending.set(key, pending);
        }
        for (const [key, pending] of memoryUpdates) {
            this.#memoryPending.set(key, pending);
        }
        await this.#scheduleFlush();
    }

    /**
     * Returns the reduced state of `reductionKey`, or `undefined` if there is none. Call it from `reduceGet`.
     * Only root Durable Objects hold state.
     */
    get(reductionKey: string): State | undefined {
        return this.#ctx.storage.kv.get<StoredBox<State>>(this.#stateKey(reductionKey))?.v;
    }

    /**
     * Deletes the reduced state of `reductionKey` on a root Durable Object.
     * Deltas of the key that are still on their way to the root create the state again when they arrive.
     * @returns `true` if a state existed and was removed, `false` otherwise.
     */
    delete(reductionKey: string): boolean {
        return this.#ctx.storage.kv.delete(this.#stateKey(reductionKey));
    }

    /**
     * Sends the pending deltas of an ingest Durable Object one hop closer to their roots.
     * Call it from the `alarm()` handler of your Durable Object class.
     */
    async alarm(): Promise<void> {
        this.#flushScheduled = false;
        const topology = this.#load();
        if (topology?.role !== "ingest") {
            return;
        }

        // Deltas left in flight by an alarm that threw go back to pending. Storage already holds them.
        for (const [key, delta] of this.#durableInflight) {
            this.#durablePending.set(key, this.#combine(key, delta, this.#durablePending.get(key))!);
        }
        this.#durableInflight.clear();

        // Drain all pending deltas before the first `await`. Writes that arrive while the RPC calls below are in
        // flight start new pending deltas, and the next alarm sends them.
        // The next hop depends only on the root of the key, so it is computed once per root. Different roots can
        // share the same next hop, and their keys go into the same batches.
        const groupsByRoot: Array<FlushBatch<Delta> | undefined> = new Array(topology.numRootShards);
        const groupsByHop = new Map<string, FlushBatch<Delta>>();
        const addItem = (key: string, durable: Delta | undefined, memory: Delta | undefined): void => {
            const rootShardId = hashKeyToShard(key, topology.numRootShards);
            let group = groupsByRoot[rootShardId];
            if (!group) {
                const hop = computeShardedReducerNextHop(
                    topology.shardId,
                    rootShardId,
                    topology.numIngestShards,
                    topology.numRootShards,
                    topology.fanIn,
                );
                const hopName = shardStubName(topology.shardGroupName, hop.role, hop.shardId);
                group = groupsByHop.get(hopName) ?? { hop, items: [] };
                groupsByHop.set(hopName, group);
                groupsByRoot[rootShardId] = group;
            }
            group.items.push({ key, durable, memory });
        };
        for (const [key, durable] of this.#durablePending) {
            this.#durableInflight.set(key, durable);
            addItem(key, durable, this.#memoryPending.get(key));
        }
        for (const [key, memory] of this.#memoryPending) {
            if (!this.#durablePending.has(key)) {
                addItem(key, undefined, memory);
            }
        }
        this.#durablePending.clear();
        this.#memoryPending.clear();

        const batches: Array<FlushBatch<Delta>> = [];
        for (const { hop, items } of groupsByHop.values()) {
            for (let i = 0; i < items.length; i += this.#maxKeysPerBatch) {
                batches.push({ hop, items: items.slice(i, i + this.#maxKeysPerBatch) });
            }
        }

        // Every batch is attempted, and all the calls complete before `alarm()` returns or throws. A call that is
        // still in flight after `alarm()` ends would race with the next alarm and send its deltas twice.
        let nextBatch = 0;
        let failed = false;
        let firstError: unknown;
        const sendNext = async (): Promise<void> => {
            while (nextBatch < batches.length) {
                const batch = batches[nextBatch++];
                try {
                    await this.#sendBatch(topology, batch);
                } catch (err) {
                    if (!failed) {
                        failed = true;
                        firstError = err;
                    }
                }
            }
        };
        await Promise.all(Array.from({ length: Math.min(this.#flushConcurrency, batches.length) }, sendNext));
        if (failed) {
            // The platform retries the alarm, and the next alarm sends the deltas left in `#durableInflight`.
            throw firstError;
        }

        // Failed batches and writes that arrived during the flush are pending again.
        if (this.#durablePending.size > 0 || this.#memoryPending.size > 0) {
            await this.#scheduleFlush();
        }
    }

    //////////////////////////////////////////////////////
    // INTERNAL IMPLEMENTATION

    // Throws only if the next hop acknowledged the batch and the storage cleanup after it failed. The deltas then
    // stay in `#durableInflight` and storage, and the next alarm sends them again (at-least-once delivery).
    async #sendBatch(topology: ShardedReducerDOTopology, { hop, items }: FlushBatch<Delta>): Promise<void> {
        try {
            const target = stubByName(
                this.#doNamespace,
                shardStubName(topology.shardGroupName, hop.role, hop.shardId),
                { locationHint: this.#shardLocationHintFn?.(hop.role, hop.shardId) },
            ) as unknown as ShardedReducerDOTarget<Input, Delta, State>;
            const msg: ShardedReducerDOMergeMessage<Delta> = {
                topology: { ...topology, role: hop.role, shardId: hop.shardId },
                entries: items.map(({ key, durable, memory }) => ({
                    key,
                    delta: this.#combine(key, durable, memory)!,
                    durable: durable !== undefined,
                })),
            };
            await tryN(this.#maxFlushAttempts, () => target.reduceMerge(msg), {
                isRetryable: isErrorRetryable,
                baseDelayMs: 25,
                maxDelayMs: 250,
            });
        } catch (err) {
            console.error({
                message: "ShardedReducerDOPeer: batch not delivered, retrying on the next alarm",
                target: shardStubName(topology.shardGroupName, hop.role, hop.shardId),
                keys: items.length,
                error: String(err),
            });
            // Move the deltas back to pending. Storage already holds the durable part (see `#durableInflight`).
            for (const { key, durable, memory } of items) {
                if (durable !== undefined) {
                    this.#durableInflight.delete(key);
                    this.#durablePending.set(key, this.#combine(key, durable, this.#durablePending.get(key))!);
                }
                if (memory !== undefined) {
                    this.#memoryPending.set(key, this.#combine(key, memory, this.#memoryPending.get(key))!);
                }
            }
            return;
        }

        // The next hop has the deltas. Storage now only needs the durable deltas that arrived during the flush.
        const durableKeys = items.filter((item) => item.durable !== undefined).map((item) => item.key);
        if (durableKeys.length === 0) {
            return;
        }
        this.#ctx.storage.transactionSync(() => {
            for (const key of durableKeys) {
                const pending = this.#durablePending.get(key);
                if (pending === undefined) {
                    this.#ctx.storage.kv.delete(this.#pendingKey(key));
                } else {
                    this.#ctx.storage.kv.put<StoredBox<Delta>>(this.#pendingKey(key), { v: pending });
                }
            }
        });
        durableKeys.forEach((key) => this.#durableInflight.delete(key));
    }

    // Loads the stored state once per Durable Object instance, and validates or records the incoming topology.
    #load(incoming?: ShardedReducerDOTopology): ShardedReducerDOTopology | undefined {
        if (!this.#loaded) {
            const topology = this.#ctx.storage.kv.get<ShardedReducerDOTopology>(this.#metaKey());
            // Only ingest Durable Objects have pending deltas. The topology is stored before the first pending
            // delta, therefore a Durable Object without a stored topology has nothing to load.
            const loaded = new Map<string, Delta>();
            if (topology?.role === "ingest") {
                const prefix = this.#pendingKey("");
                for (const [key, box] of this.#ctx.storage.kv.list<StoredBox<Delta>>({ prefix })) {
                    loaded.set(key.slice(prefix.length), box.v);
                }
            }
            // Set only after all reads succeed, so that a failed read is tried again on the next call.
            this.#topology = topology;
            this.#durablePending = loaded;
            this.#loaded = true;
        }

        if (incoming) {
            if (this.#topology) {
                validateTopologyMatch(this.#topology, incoming);
            }
            if (!this.#topology || this.#topology.flushIntervalMs !== incoming.flushIntervalMs) {
                this.#topology = { ...incoming };
                if (incoming.role === "ingest") {
                    this.#ctx.storage.kv.put<ShardedReducerDOTopology>(this.#metaKey(), this.#topology);
                }
            }
        }
        return this.#topology;
    }

    // Writes the storage value of `key` for the new durable pending delta `pending` (see `#durableInflight`).
    #persistDurable(key: string, pending: Delta): void {
        this.#ctx.storage.kv.put<StoredBox<Delta>>(this.#pendingKey(key), {
            v: this.#combine(key, this.#durableInflight.get(key), pending)!,
        });
    }

    // Sets the alarm at most once per flush interval. An earlier alarm is kept, a later alarm is moved forward.
    async #scheduleFlush(): Promise<void> {
        if (this.#flushScheduled) {
            return;
        }
        this.#flushScheduled = true;
        try {
            const flushAt = Date.now() + this.#topology!.flushIntervalMs;
            const currentAlarm = await this.#ctx.storage.getAlarm();
            if (currentAlarm === null || currentAlarm > flushAt) {
                await this.#ctx.storage.setAlarm(flushAt);
            }
        } catch (err) {
            this.#flushScheduled = false;
            throw err;
        }
    }

    #combine(key: string, a: Delta | undefined, b: Delta | undefined): Delta | undefined {
        if (a === undefined) {
            return b;
        }
        if (b === undefined) {
            return a;
        }
        return this.#operators.combine(a, b, key);
    }

    #metaKey(): string {
        return `${this.#storagePrefix}meta`;
    }

    #pendingKey(reductionKey: string): string {
        return `${this.#storagePrefix}p:${reductionKey}`;
    }

    #stateKey(reductionKey: string): string {
        return `${this.#storagePrefix}s:${reductionKey}`;
    }
}

function validateTopologyMatch(existing: ShardedReducerDOTopology, incoming: ShardedReducerDOTopology): void {
    if (
        existing.shardGroupName !== incoming.shardGroupName ||
        existing.role !== incoming.role ||
        existing.shardId !== incoming.shardId ||
        existing.numIngestShards !== incoming.numIngestShards ||
        existing.numRootShards !== incoming.numRootShards ||
        existing.fanIn !== incoming.fanIn
    ) {
        throw new Error(
            `ShardedReducerDOPeer topology mismatch on '${shardStubName(existing.shardGroupName, existing.role, existing.shardId)}': ` +
                `existing=(N=${existing.numIngestShards}, R=${existing.numRootShards}, X=${existing.fanIn}), ` +
                `incoming=(group=${incoming.shardGroupName}, role=${incoming.role}, shard=${incoming.shardId}, ` +
                `N=${incoming.numIngestShards}, R=${incoming.numRootShards}, X=${incoming.fanIn})`,
        );
    }
}

function normalizeOperators<Input, Delta, State>(
    reducer: ShardedReducerDOPeerOptions<Input, Delta, State>["reducer"],
): ShardedReducerDOOperators<Input, Delta, State> {
    if (typeof reducer !== "function") {
        return reducer;
    }
    const merge = reducer as ShardedReducerDOMergeFn<any>;
    return {
        ingest: (delta, input, key) => (delta === undefined ? input : merge(delta, input, key)),
        combine: (a, b, key) => merge(a, b, key),
        apply: (state, delta, key) => (state === undefined ? delta : merge(state, delta, key)),
    };
}

type InferReducerInput<T> = T extends { reduceIngest(msg: ShardedReducerDOIngestMessage<infer I>): any } ? I : never;
type InferReducerState<T> = T extends { reduceGet(reductionKey: string): Promise<infer S> }
    ? Exclude<S, undefined>
    : never;

/**
 * Client of a sharded reducer group of `R + N` Durable Objects. Use it from Workers or other Durable Objects.
 *
 * - `send()` spreads the writes across `numIngestShards` (`N`) ingest Durable Objects by `hashKey`.
 * - The ingest Durable Objects reduce the writes and forward them in bounded `fanIn` hops to the roots.
 * - `get()` reads a value from the one of `numRootShards` (`R`) root Durable Objects that owns its `reductionKey`.
 *
 * The Durable Object class must implement `ShardedReducerDOTarget` with `ShardedReducerDOPeer`.
 */
export class ShardedReducerDO<
    T extends ShardedReducerDOTarget<any, any, any>,
    Input = InferReducerInput<T>,
    State = InferReducerState<T>,
> {
    #doNamespace: DurableObjectNamespace<T>;
    #shardLocationHintFn?: (role: ShardedReducerDORole, shard: ShardId) => DurableObjectLocationHint | undefined;
    #topology: Omit<ShardedReducerDOTopology, "role" | "shardId">;

    constructor(doNamespace: DurableObjectNamespace<T>, options: ShardedReducerDOOptions) {
        const { numIngestShards, numRootShards = 1, fanIn = 10, flushIntervalMs = 25 } = options;
        if (!Number.isInteger(numIngestShards) || numIngestShards <= 0) {
            throw new Error("Invalid numIngestShards, must be an integer greater than 0");
        }
        if (!Number.isInteger(numRootShards) || numRootShards <= 0) {
            throw new Error("Invalid numRootShards, must be an integer greater than 0");
        }
        if (!Number.isInteger(fanIn) || fanIn < 2) {
            throw new Error("Invalid fanIn, must be an integer greater than or equal to 2");
        }
        if (!Number.isInteger(flushIntervalMs) || flushIntervalMs <= 0) {
            throw new Error("Invalid flushIntervalMs, must be an integer greater than 0");
        }
        // WARNING: Never change the default value of this otherwise there will be data loss!
        const shardGroupName = (options.shardGroupName ?? "sharded-reducer-do").trim();
        if (!shardGroupName) {
            throw new Error("Invalid shard group name, must not be empty or only whitespace");
        }

        this.#doNamespace = doNamespace;
        this.#shardLocationHintFn = options.shardLocationHintFn;
        this.#topology = { shardGroupName, numIngestShards, numRootShards, fanIn, flushIntervalMs };
    }

    /**
     * Number of ingest Durable Objects (`N`) that receive the writes.
     */
    get numIngestShards(): number {
        return this.#topology.numIngestShards;
    }

    /**
     * Number of root Durable Objects (`R`) that store the reduced values.
     */
    get numRootShards(): number {
        return this.#topology.numRootShards;
    }

    /**
     * Maximum number of Durable Objects that send to the same Durable Object in one flush interval.
     */
    get fanIn(): number {
        return this.#topology.fanIn;
    }

    /**
     * Returns the ingest Durable Object (`0 .. numIngestShards - 1`) that receives the writes of `hashKey`.
     */
    ingestShardForKey(hashKey: string): ShardId {
        return hashKeyToShard(hashKey, this.#topology.numIngestShards);
    }

    /**
     * Returns the root Durable Object (`0 .. numRootShards - 1`) that owns `reductionKey`.
     */
    rootShardForKey(reductionKey: string): ShardId {
        return hashKeyToShard(reductionKey, this.#topology.numRootShards);
    }

    /**
     * Returns the stub of the root Durable Object that owns `reductionKey`, to call your own RPC methods on it.
     */
    rootStub(reductionKey: string): DurableObjectStub<T> {
        return this.#stub("root", this.rootShardForKey(reductionKey));
    }

    /**
     * Sends a write to the ingest Durable Object of `params.hashKey`.
     * @returns A `TryResult` with the error instead of throwing it.
     */
    async trySend(
        params: ShardedReducerDOSendParams<Input>,
        options?: ShardedReducerDOSendOptions,
    ): Promise<TryResult<void>> {
        const shard = this.ingestShardForKey(params.hashKey);
        const msg: ShardedReducerDOIngestMessage<Input> = {
            topology: { ...this.#topology, role: "ingest", shardId: shard },
            reductionKey: params.reductionKey,
            payload: params.payload,
            durability: options?.durability ?? "storage",
        };
        return await this.#tryCall("ingest", shard, options?.shouldRetry, (target) => target.reduceIngest(msg));
    }

    /**
     * Sends a write to the ingest Durable Object of `params.hashKey`. Throws the error of the call if it fails.
     */
    async send(params: ShardedReducerDOSendParams<Input>, options?: ShardedReducerDOSendOptions): Promise<void> {
        const result = await this.trySend(params, options);
        if (!result.ok) {
            throw result.error;
        }
    }

    /**
     * Reads the reduced value of `reductionKey` from its root Durable Object.
     * @returns A `TryResult` with the error instead of throwing it.
     */
    async tryGet(reductionKey: string, options?: ShardedReducerDOReadOptions): Promise<TryResult<State | undefined>> {
        const shard = this.rootShardForKey(reductionKey);
        return await this.#tryCall("root", shard, options?.shouldRetry, (target) => target.reduceGet(reductionKey));
    }

    /**
     * Reads the reduced value of `reductionKey` from its root Durable Object. Throws the error of the call if it fails.
     * @returns The reduced value, or `undefined` if the key has no value yet.
     */
    async get(reductionKey: string, options?: ShardedReducerDOReadOptions): Promise<State | undefined> {
        const result = await this.tryGet(reductionKey, options);
        if (!result.ok) {
            throw result.error;
        }
        return result.result;
    }

    //////////////////////////////////////////////////////
    // INTERNAL IMPLEMENTATION

    async #tryCall<R>(
        role: ShardedReducerDORole,
        shard: ShardId,
        shouldRetry: ((error: unknown, attempt: number, shard: ShardId) => boolean) | undefined,
        fn: (target: ShardedReducerDOTarget<Input, unknown, State>) => Promise<R>,
    ): Promise<TryResult<R>> {
        const target = this.#stub(role, shard) as unknown as ShardedReducerDOTarget<Input, unknown, State>;
        try {
            const result = await tryWhile(
                () => fn(target),
                (err, attempt) => !!shouldRetry?.(err, attempt, shard),
            );
            return { ok: true, shard, result };
        } catch (error) {
            return { ok: false, shard, error };
        }
    }

    #stub(role: ShardedReducerDORole, shard: ShardId): DurableObjectStub<T> {
        return stubByName(this.#doNamespace, shardStubName(this.#topology.shardGroupName, role, shard), {
            locationHint: this.#shardLocationHintFn?.(role, shard),
        });
    }
}
