import { env } from "cloudflare:workers";
import { assert, describe, expect, it } from "vitest";
import { ShardedReducerDO, ShardedReducerDOPeer } from "../src/do-reducer";
import { computeShardedReducerNextHop } from "../src/internal/sharded-reducer-routing";
import { stubByName } from "../src/do-utils";

async function waitForValue<T>(
    readFn: () => Promise<T>,
    predicate: (value: T) => boolean,
    timeoutMs = 10_000,
): Promise<T> {
    const deadline = Date.now() + timeoutMs;
    let lastValue!: T;
    while (Date.now() < deadline) {
        lastValue = await readFn();
        if (predicate(lastValue)) {
            return lastValue;
        }
        await new Promise((resolve) => setTimeout(resolve, 25));
    }
    return lastValue;
}

describe("ShardedReducerDO", { timeout: 25_000 }, () => {
    describe("validation", () => {
        it("rejects invalid client options", () => {
            expect(() => new ShardedReducerDO(env.CounterReducerDO, { numIngestShards: 0 })).toThrowError(
                "Invalid numIngestShards, must be an integer greater than 0",
            );
            expect(() => new ShardedReducerDO(env.CounterReducerDO, { numIngestShards: 1.5 })).toThrowError(
                "Invalid numIngestShards, must be an integer greater than 0",
            );
            expect(
                () => new ShardedReducerDO(env.CounterReducerDO, { numIngestShards: 4, numRootShards: 0 }),
            ).toThrowError("Invalid numRootShards, must be an integer greater than 0");
            expect(() => new ShardedReducerDO(env.CounterReducerDO, { numIngestShards: 4, fanIn: 1 })).toThrowError(
                "Invalid fanIn, must be an integer greater than or equal to 2",
            );
            for (const flushIntervalMs of [0, 0.5, -10]) {
                expect(
                    () => new ShardedReducerDO(env.CounterReducerDO, { numIngestShards: 4, flushIntervalMs }),
                ).toThrowError("Invalid flushIntervalMs, must be an integer greater than 0");
            }
            for (const shardGroupName of ["", "   ", "\t\n"]) {
                expect(
                    () => new ShardedReducerDO(env.CounterReducerDO, { numIngestShards: 4, shardGroupName }),
                ).toThrowError("Invalid shard group name, must not be empty or only whitespace");
            }
        });

        it("rejects invalid peer options", () => {
            const fakeState = {} as DurableObjectState;
            expect(
                () =>
                    new ShardedReducerDOPeer<number>(fakeState, env.CounterReducerDO, {
                        reducer: (a, b) => a + b,
                        maxKeysPerBatch: 0,
                    }),
            ).toThrowError("Invalid maxKeysPerBatch, must be an integer greater than 0");
            expect(
                () =>
                    new ShardedReducerDOPeer<number>(fakeState, env.CounterReducerDO, {
                        reducer: (a, b) => a + b,
                        flushConcurrency: 0,
                    }),
            ).toThrowError("Invalid flushConcurrency, must be an integer greater than 0");
            expect(
                () =>
                    new ShardedReducerDOPeer<number>(fakeState, env.CounterReducerDO, {
                        reducer: (a, b) => a + b,
                        maxFlushAttempts: 0,
                    }),
            ).toThrowError("Invalid maxFlushAttempts, must be an integer greater than 0");
            expect(
                () =>
                    new ShardedReducerDOPeer<number>(fakeState, env.CounterReducerDO, {
                        reducer: (a, b) => a + b,
                        storagePrefix: "",
                    }),
            ).toThrowError("Invalid storagePrefix, must not be empty");
        });

        it("exposes configured shard dimensions", () => {
            const reducer = new ShardedReducerDO(env.CounterReducerDO, {
                numIngestShards: 32,
                numRootShards: 4,
                fanIn: 6,
            });
            expect(reducer.numIngestShards).toBe(32);
            expect(reducer.numRootShards).toBe(4);
            expect(reducer.fanIn).toBe(6);
        });
    });

    describe("computeShardedReducerNextHop routing invariants", () => {
        it("reaches the root without loops, within the hop bound, and with at most fanIn senders per receiver", () => {
            const cases = [
                { N: 1, R: 1, X: 2 },
                { N: 5, R: 3, X: 10 },
                { N: 10, R: 4, X: 10 },
                { N: 11, R: 5, X: 10 },
                { N: 16, R: 4, X: 2 },
                { N: 27, R: 9, X: 3 },
                { N: 45, R: 12, X: 10 },
                { N: 100, R: 20, X: 10 },
                { N: 101, R: 7, X: 10 },
                { N: 415, R: 6, X: 10 },
                { N: 30, R: 50, X: 4 },
            ];

            for (const { N, R, X } of cases) {
                // Smallest H with N <= X + X^2 + ... + X^H.
                let maxHops = 1;
                for (let capacity = X, level = X; capacity < N; maxHops++) {
                    level *= X;
                    capacity += level;
                }

                for (let r = 0; r < R; r++) {
                    // Senders per receiver in the tree of root r.
                    const senders = new Map<string, Set<number>>();
                    for (let s = 0; s < N; s++) {
                        let current = s;
                        let hops = 0;
                        while (true) {
                            const next = computeShardedReducerNextHop(current, r, N, R, X);
                            hops++;
                            const receiver = `${next.role}-${next.shardId}`;
                            if (!senders.has(receiver)) senders.set(receiver, new Set());
                            senders.get(receiver)!.add(current);
                            if (next.role === "root") {
                                expect(next.shardId).toBe(r);
                                break;
                            }
                            expect(next.shardId).toBeGreaterThanOrEqual(0);
                            expect(next.shardId).toBeLessThan(N);
                            expect(next.shardId).not.toBe(current);
                            current = next.shardId;
                        }
                        expect(hops).toBeLessThanOrEqual(maxHops);
                    }
                    for (const set of senders.values()) {
                        expect(set.size).toBeLessThanOrEqual(X);
                    }
                    expect(senders.get(`root-${r}`)!.size).toBe(Math.min(N, X));
                }
            }
        });

        it("uses different ingest shards as gateways for different roots", () => {
            const N = 100;
            const R = 10;
            const X = 10;
            const gatewaysPerShard = new Map<number, number>();
            for (let r = 0; r < R; r++) {
                for (let s = 0; s < N; s++) {
                    if (computeShardedReducerNextHop(s, r, N, R, X).role === "root") {
                        gatewaysPerShard.set(s, (gatewaysPerShard.get(s) ?? 0) + 1);
                    }
                }
            }
            // Every ingest shard is a gateway of exactly one root.
            expect(gatewaysPerShard.size).toBe(N);
            expect([...gatewaysPerShard.values()].every((count) => count === 1)).toBe(true);
        });
    });

    describe("end-to-end reduction", () => {
        it("returns undefined for unseen keys and preserves falsy reduced values like 0", async () => {
            const reducer = new ShardedReducerDO(env.CounterReducerDO, {
                shardGroupName: "falsy-zero-group",
                numIngestShards: 4,
                numRootShards: 2,
                fanIn: 2,
                flushIntervalMs: 30,
            });

            expect(await reducer.get("never-written")).toBeUndefined();

            await reducer.send({ hashKey: "h1", reductionKey: "zero-sum", payload: 5 });
            await reducer.send({ hashKey: "h2", reductionKey: "zero-sum", payload: -5 });

            const value = await waitForValue(
                () => reducer.get("zero-sum"),
                (v) => v === 0,
            );
            expect(value).toBe(0);

            // Verify root delete helper removes state cleanly.
            const root = reducer.rootStub("zero-sum");
            expect(await root.deleteReducedKey("zero-sum")).toBe(true);
            expect(await reducer.get("zero-sum")).toBeUndefined();
        });

        it("reduces high-fanout writes across multiple rounds into R sharded roots using only R + N DOs", async () => {
            const N = 14;
            const R = 3;
            const group = "multi-round-rn";
            const reducer = new ShardedReducerDO(env.CounterReducerDO, {
                shardGroupName: group,
                numIngestShards: N,
                numRootShards: R,
                fanIn: 3, // Forces 3-hop reduction (ceil(log_3(14)) = 3)
                flushIntervalMs: 30,
            });

            const expectedTotals = new Map<string, number>();
            const writes: Array<Promise<void>> = [];

            for (let k = 0; k < 9; k++) {
                const reductionKey = `campaign-${k}`;
                let sum = 0;
                for (let i = 0; i < 20; i++) {
                    const amount = i + 1;
                    sum += amount;
                    writes.push(
                        reducer.send({
                            hashKey: `user-${k}-${i}`,
                            reductionKey,
                            payload: amount,
                        }),
                    );
                }
                expectedTotals.set(reductionKey, sum);
            }

            await Promise.all(writes);

            for (const [reductionKey, expected] of expectedTotals.entries()) {
                const actual = await waitForValue(
                    () => reducer.get(reductionKey),
                    (v) => v === expected,
                );
                expect(actual).toBe(expected);
            }

            // Confirm ingest DOs and root DOs are strictly disjoint (R + N distinct actor IDs).
            const ingestIds = new Set<string>();
            for (let s = 0; s < N; s++) {
                ingestIds.add(await stubByName(env.CounterReducerDO, `${group}-ingest-${s}`).actorId());
            }
            const rootIds = new Set<string>();
            for (let r = 0; r < R; r++) {
                rootIds.add(await stubByName(env.CounterReducerDO, `${group}-root-${r}`).actorId());
            }
            expect(ingestIds.size).toBe(N);
            expect(rootIds.size).toBe(R);
            expect(new Set([...ingestIds, ...rootIds]).size).toBe(N + R);
        });

        it("supports 3-stage operators with distinct Input, Delta, and State types", async () => {
            const reducer = new ShardedReducerDO(env.StatsReducerDO, {
                shardGroupName: "latency-stats",
                numIngestShards: 9,
                numRootShards: 2,
                fanIn: 3,
                flushIntervalMs: 30,
            });

            const samples = [10, 20, 30, 40, 100];
            await Promise.all(
                samples.map((ms, idx) =>
                    reducer.send({
                        hashKey: `req-${idx}`,
                        reductionKey: "api:/checkout",
                        payload: { ms },
                    }),
                ),
            );

            const summary = await waitForValue(
                () => reducer.get("api:/checkout"),
                (v) => v?.count === samples.length,
            );
            expect(summary).toEqual({
                count: 5,
                sum: 200,
                min: 10,
                max: 100,
                avg: 40,
            });
        });

        it("chunks large reductionKey sets across maxKeysPerBatch", async () => {
            // CounterReducerDO sets maxKeysPerBatch: 10. Write 35 keys so each shard sends >= 4 chunks.
            const reducer = new ShardedReducerDO(env.CounterReducerDO, {
                shardGroupName: "batch-chunking",
                numIngestShards: 3,
                numRootShards: 2,
                fanIn: 2,
                flushIntervalMs: 40,
            });

            const totalKeys = 35;
            await Promise.all(
                Array.from({ length: totalKeys }, (_, k) =>
                    reducer.send({
                        hashKey: `same-shard`,
                        reductionKey: `metric-${k}`,
                        payload: k + 1,
                    }),
                ),
            );

            for (let k = 0; k < totalKeys; k++) {
                const val = await waitForValue(
                    () => reducer.get(`metric-${k}`),
                    (v) => v === k + 1,
                );
                expect(val).toBe(k + 1);
            }
        });

        it("isolates independent shardGroupName instances in the same namespace", async () => {
            const r1 = new ShardedReducerDO(env.CounterReducerDO, {
                shardGroupName: "  tenant-a  ",
                numIngestShards: 4,
                flushIntervalMs: 25,
            });
            const r2 = new ShardedReducerDO(env.CounterReducerDO, {
                shardGroupName: "tenant-b",
                numIngestShards: 4,
                flushIntervalMs: 25,
            });

            await r1.send({ hashKey: "u1", reductionKey: "clicks", payload: 10 });
            await r2.send({ hashKey: "u1", reductionKey: "clicks", payload: 99 });

            expect(
                await waitForValue(
                    () => r1.get("clicks"),
                    (v) => v === 10,
                ),
            ).toBe(10);
            expect(
                await waitForValue(
                    () => r2.get("clicks"),
                    (v) => v === 99,
                ),
            ).toBe(99);
        });
    });

    describe("durability modes", () => {
        it("keeps memory writes out of storage and combines them with storage writes on flush", async () => {
            const group = "durability-modes";
            const reducer = new ShardedReducerDO(env.CounterReducerDO, {
                shardGroupName: group,
                numIngestShards: 1,
                numRootShards: 1,
                fanIn: 2,
                flushIntervalMs: 250,
            });

            const ingestStub = stubByName(env.CounterReducerDO, `${group}-ingest-0`);

            // Memory durability writes 0 pending keys to DO storage.
            await reducer.send({ hashKey: "h1", reductionKey: "mem-only", payload: 7 }, { durability: "memory" });
            await reducer.send({ hashKey: "h2", reductionKey: "mem-only", payload: 8 }, { durability: "memory" });
            expect(await ingestStub.storagePendingCount()).toBe(0);

            // Mixed key: only the storage write is persisted, both are combined on flush.
            await reducer.send({ hashKey: "h1", reductionKey: "mixed-key", payload: 10 }, { durability: "memory" });
            expect(await ingestStub.storagePendingCount()).toBe(0);

            await reducer.send({ hashKey: "h1", reductionKey: "mixed-key", payload: 20 }, { durability: "storage" });
            expect(await ingestStub.storagePendingCount()).toBe(1);

            // Additional memory write on the same key coexists and combines on flush.
            await reducer.send({ hashKey: "h1", reductionKey: "mixed-key", payload: 5 }, { durability: "memory" });

            expect(
                await waitForValue(
                    () => reducer.get("mem-only"),
                    (v) => v === 15,
                ),
            ).toBe(15);
            expect(
                await waitForValue(
                    () => reducer.get("mixed-key"),
                    (v) => v === 35,
                ),
            ).toBe(35);

            // After flush completes, durable pending keys on ingest DO are cleaned up.
            expect(
                await waitForValue(
                    () => ingestStub.storagePendingCount(),
                    (c) => c === 0,
                ),
            ).toBe(0);
        });

        it("recovers durable pending deltas after a restart and loses memory-only deltas", async () => {
            const group = "restart-recovery";
            const reducer = new ShardedReducerDO(env.CounterReducerDO, {
                shardGroupName: group,
                numIngestShards: 1,
                numRootShards: 1,
                fanIn: 2,
                flushIntervalMs: 500,
            });

            await reducer.send({ hashKey: "h1", reductionKey: "recovered", payload: 15 }, { durability: "storage" });
            await reducer.send({ hashKey: "h1", reductionKey: "recovered", payload: 7 }, { durability: "memory" });

            const ingestStub = stubByName(env.CounterReducerDO, `${group}-ingest-0`);
            await ingestStub.simulateRestart();

            expect(
                await waitForValue(
                    () => reducer.get("recovered"),
                    (v) => v !== undefined,
                ),
            ).toBe(15);
            await new Promise((resolve) => setTimeout(resolve, 700));
            expect(await reducer.get("recovered")).toBe(15);
            expect(await ingestStub.storagePendingCount()).toBe(0);
        });
    });

    describe("error handling, retries, and topology mismatch", () => {
        it("supports trySend, tryGet, and shouldRetry on transient ingest errors", async () => {
            const group = "client-retries";
            const reducer = new ShardedReducerDO(env.CounterReducerDO, {
                shardGroupName: group,
                numIngestShards: 2,
                flushIntervalMs: 25,
            });

            const targetShard = reducer.ingestShardForKey("key-1");
            const ingestStub = stubByName(env.CounterReducerDO, `${group}-ingest-${targetShard}`);
            await ingestStub.configureFailures({ failNextIngests: 2 });

            const failedTry = await reducer.trySend({ hashKey: "key-1", reductionKey: "r1", payload: 1 });
            expect(failedTry.ok).toBe(false);
            assert(failedTry.ok === false);
            expect(failedTry.shard).toBe(targetShard);

            // Reset to fail once, then succeed on retry attempt 2.
            await ingestStub.configureFailures({ failNextIngests: 1 });
            let retryChecks = 0;
            await reducer.send(
                { hashKey: "key-1", reductionKey: "r1", payload: 42 },
                {
                    shouldRetry: (_err, attempt) => {
                        retryChecks++;
                        return attempt <= 3;
                    },
                },
            );
            expect(retryChecks).toBe(1);

            const readRes = await waitForValue(
                () => reducer.tryGet("r1"),
                (r) => r.ok && r.result === 42,
            );
            assert(readRes.ok === true);
            expect(readRes.result).toBe(42);
        });

        it("retries transient merge errors and re-queues failed chunks across alarm cycles without data loss", async () => {
            const group = "merge-recovery";
            const reducer = new ShardedReducerDO(env.CounterReducerDO, {
                shardGroupName: group,
                numIngestShards: 4,
                numRootShards: 1,
                fanIn: 4,
                flushIntervalMs: 40,
            });

            const rootStub = stubByName(env.CounterReducerDO, `${group}-root-0`);

            // First test retryable error (handled within the same alarm via tryN).
            await rootStub.configureFailures({ failNextMerges: 2, failRetryable: true });
            await reducer.send({ hashKey: "a", reductionKey: "total", payload: 10 });
            expect(
                await waitForValue(
                    () => reducer.get("total"),
                    (v) => v === 10,
                ),
            ).toBe(10);

            // Now test non-retryable error: fails the entire first alarm, re-queues locally, and delivers on next alarm.
            await rootStub.configureFailures({ failNextMerges: 1, failRetryable: false });
            await reducer.send({ hashKey: "a", reductionKey: "total", payload: 25 }, { durability: "storage" });
            await reducer.send({ hashKey: "a", reductionKey: "mem-total", payload: 12 }, { durability: "memory" });

            expect(
                await waitForValue(
                    () => reducer.get("total"),
                    (v) => v === 35,
                ),
            ).toBe(35);
            expect(
                await waitForValue(
                    () => reducer.get("mem-total"),
                    (v) => v === 12,
                ),
            ).toBe(12);
        });

        it("rejects conflicting topology configuration on an already initialized shard", async () => {
            const group = "mismatch-group";
            const r1 = new ShardedReducerDO(env.CounterReducerDO, {
                shardGroupName: group,
                numIngestShards: 2,
                numRootShards: 1,
                flushIntervalMs: 50,
            });
            const rConflicting = new ShardedReducerDO(env.CounterReducerDO, {
                shardGroupName: group,
                numIngestShards: 2,
                numRootShards: 3, // Mismatched R!
                flushIntervalMs: 50,
            });

            await r1.send({ hashKey: "same", reductionKey: "k", payload: 1 });
            await expect(rConflicting.send({ hashKey: "same", reductionKey: "k", payload: 1 })).rejects.toThrowError(
                /topology mismatch/,
            );
        });
    });
});
