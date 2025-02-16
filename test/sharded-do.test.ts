import { env } from "cloudflare:test";
import { assert, describe, expect, it } from "vitest";
import { Env } from "./workers-test";
import { StaticShardedDO } from "../src/do-sharding";

declare module "cloudflare:test" {
    interface ProvidedEnv extends Env {}
}

describe("StaticShardedDO", { timeout: 20_000 }, async () => {
    it("N", async () => {
        const randomNumbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        randomNumbers.forEach((n) => {
            const sdo = new StaticShardedDO(env.SQLDO, { numShards: n * 100 });
            expect(sdo.N).toBe(n * 100);
        });
    });

    it("one()", async () => {
        const sdo = new StaticShardedDO(env.SQLDO, { numShards: 7 });
        const id1 = await sdo.one("test", async (stub) => {
            return await stub.actorId();
        });
        const result = await sdo.one("test", async (stub) => {
            return await stub.echo("test-01");
        });
        expect(result).toBe("test-01");

        const id2 = await sdo.one("test-02-hashed-differently", async (stub) => {
            return await stub.actorId();
        });
        const result2 = await sdo.one("test-02-hashed-differently", async (stub) => {
            return await stub.echo("test-02");
        });
        expect(result2).toBe("test-02");

        expect(id1).not.to.equal(id2);
    });

    it("tryOne() with errors", async () => {
        const sdo = new StaticShardedDO(env.SQLDO, { numShards: 7 });
        const response1 = await sdo.tryOne("test", async (stub) => {
            return await stub.echo("test-01");
        });
        expect(response1.ok).toBe(true);
        assert(response1.ok === true);
        expect(response1.result).toBe("test-01");

        const response2 = await sdo.tryOne("test2", async (stub) => {
            throw new Error("test-error");
        });
        expect(response2.ok).toBe(false);
        assert(response2.ok === false);
        expect(response2.error).toEqual(new Error("test-error"));
    });

    it("all()", async () => {
        const sdo = new StaticShardedDO(env.SQLDO, { numShards: 11 });

        const shards: number[] = [];
        const ids = await sdo.all(async (stub, shard) => {
            shards.push(shard);
            return await stub.actorId();
        });
        expect(new Set(ids).size).toEqual(11);
        expect(shards).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    });

    it("tryAll()", async () => {
        const sdo = new StaticShardedDO(env.SQLDO, { numShards: 11 });

        const shards: number[] = [];
        const results = await sdo.tryAll(async (stub, shard) => {
            shards.push(shard);
            return await stub.actorId();
        });
        const errors = results.filter((r) => !r.ok).map((r) => r.error);
        expect(errors).toEqual([]);
        const ids = results.filter((r) => r.ok).map((r) => r.result);
        expect(new Set(ids).size).toEqual(11);
        expect(shards).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    });

    it("tryAll() - with errors", async () => {
        const sdo = new StaticShardedDO(env.SQLDO, { numShards: 11 });

        const shards: number[] = [];
        const results = await sdo.tryAll(async (stub, shard) => {
            shards.push(shard);
            if (shard % 2 === 0) {
                throw new Error("test-error");
            }
            return await stub.echo(String(shard));
        });

        results.forEach((r, i) => {
            if (i % 2 === 0) {
                expect(r.ok).toBe(false);
                assert(r.ok === false);
                expect(r.error).toEqual(new Error("test-error"));
            } else {
                expect(r.ok).toBe(true);
                assert(r.ok === true);
                expect(r.result).toEqual(String(i));
            }
        });
        expect(shards).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    });

    it("trySome()", async () => {
        // Use less concurrency than shards to test the iteration properly.
        const sdo = new StaticShardedDO(env.SQLDO, { numShards: 11, concurrency: 3 });

        const shards: number[] = [];
        const ids = new Set<string>();
        const results = await sdo.trySome(
            async (stub, shard) => {
                shards.push(shard);
                ids.add(await stub.actorId());
                if (shard === 3) {
                    throw new Error("test-error");
                }
                return `some-${shard}`;
            },
            {
                filterFn: (shard) => shard % 2 === 0 || shard === 3,
            },
        );
        expect(shards).toEqual([0, 2, 3, 4, 6, 8, 10]);
        expect(new Set(ids).size).toEqual(7);
        const failed = results.filter((r) => !r.ok);
        expect(failed).toEqual([{ ok: false, error: new Error("test-error"), shard: 3 }]);
        const oked = results.filter((r) => r.ok);
        oked.forEach((r, i) => {
            expect(r.result).toEqual(`some-${i * 2}`);
        });
    });

    it("should return correct results for each shard in case of some failing", async () => {
        const sdo = new StaticShardedDO(env.SQLDO, { numShards: 3, concurrency: 1 });

        const results = await sdo.tryAll(async (stub, shard) => {
            if (shard === 1) {
                throw new Error("test-error");
            }
            return `some-${shard}`;
        });
        expect(results).toEqual([
            { ok: true, result: "some-0", shard: 0 },
            { ok: false, error: new Error("test-error"), shard: 1 },
            { ok: true, result: "some-2", shard: 2 },
        ]);

        const results2 = await sdo.trySome(
            async (stub, shard) => {
                if (shard === 1) {
                    throw new Error("test-error");
                }
                return `some-${shard}`;
            },
            {
                // Skip first shard.
                filterFn: (shard) => shard > 0,
            },
        );
        expect(results2).toEqual([
            { ok: false, error: new Error("test-error"), shard: 1 },
            { ok: true, result: "some-2", shard: 2 },
        ]);
    });

    it("genAll()", async () => {
        const sdo = new StaticShardedDO(env.SQLDO, { numShards: 11 });

        const shards: number[] = [];
        const ids: string[] = [];
        for await (const res of sdo.genAll(async (stub, shard) => {
            shards.push(shard);
            return await stub.actorId();
        })) {
            ids.push(res);
        }
        expect(new Set(ids).size).toEqual(11);
        expect(shards).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    });

    describe("retries", async () => {
        it("one() with retries", async () => {
            const sdo = new StaticShardedDO(env.SQLDO, { numShards: 3 });
            let attempt = 0;
            const result = await sdo.one(
                "test",
                async (stub) => {
                    attempt++;
                    if (attempt === 3) return await stub.echo("test-01");
                    else throw new Error("test-error");
                },
                {
                    shouldRetry(error, attempt, shard) {
                        return attempt < 4;
                    },
                },
            );
            expect(result).toBe("test-01");
            expect(attempt).toBe(3);
        });

        (
            [
                { method: "trySome" },
                { method: "tryAll" },
            ] as { name: string; method: "trySome" | "tryAll" }[]
        ).forEach(({ method }) => {
            it(`${method}() with different retries among shards`, async () => {
                // Make sure concurrency is less than the shards to test the iteration properly.
                const sdo = new StaticShardedDO(env.SQLDO, { numShards: 11, concurrency: 3 });

                const attemptsByShard = new Map<number, number>();

                let totalShouldRetry = 0;
                let errors = 0;
                const responses = await sdo[method](
                    async (stub, shard) => {
                        attemptsByShard.set(shard, (attemptsByShard.get(shard) || 0) + 1);

                        // Shard 2 never throws.
                        if (shard != 2) {
                            errors++;
                            throw new Error("retryable-error");
                        }
                        return "yes";
                    },
                    {
                        filterFn(shardId) {
                            return true;
                        },
                        shouldRetry(error, attempt, shard) {
                            totalShouldRetry++;
                            // Test different retries for different shards.
                            return shard % 2 === 0 ? attempt < 4 : attempt < 3;
                        },
                    },
                );

                expect(responses.length).toEqual(11);
                expect(totalShouldRetry).toEqual(errors);
                expect(responses[2]).toEqual({ ok: true, result: "yes", shard: 2 });

                // Check that the retries were done correctly.
                for (let i = 0; i < 6; i++) {
                    if (i === 2) {
                        expect(attemptsByShard.get(i)).toBe(1);
                    } else {
                        // Make sure other shards have the right response.
                        if (
                            (i % 2 === 0 && attemptsByShard.get(i) === 3) ||
                            (i % 2 === 1 && attemptsByShard.get(i) === 2)
                        ) {
                            expect(responses[i]).toEqual({ ok: false, error: new Error("retryable-error"), shard: i });
                        } else {
                            expect(responses[i]).toEqual({ ok: true, result: "yes", shard: i });
                        }
                    }
                }
            });
        });
    });

    describe("shards vs concurrency", { timeout: 20_000 }, async () => {
        it("all() with 100 shards and 1000 subrequests", async () => {
            const sdo = new StaticShardedDO(env.SQLDO, { numShards: 10, concurrency: 1000 });

            const shards: number[] = [];
            const ids = await sdo.all(async (stub, shard) => {
                shards.push(shard);
                return await stub.actorId();
            });
            expect(new Set(ids).size).toEqual(10);
            expect(new Set(shards).size).toEqual(10);
        });

        it("all() with 100 shards and 100 subrequests", async () => {
            const sdo = new StaticShardedDO(env.SQLDO, { numShards: 10, concurrency: 10 });

            const shards: number[] = [];
            const ids = await sdo.all(async (stub, shard) => {
                shards.push(shard);
                return await stub.actorId();
            });
            expect(new Set(ids).size).toEqual(10);
            expect(new Set(shards).size).toEqual(10);
        });

        it("all() with 100 shards and 10 subrequests", async () => {
            const sdo = new StaticShardedDO(env.SQLDO, { numShards: 10, concurrency: 3 });

            const shards: number[] = [];
            const ids = await sdo.all(async (stub, shard) => {
                shards.push(shard);
                return await stub.actorId();
            });
            expect(new Set(ids).size).toEqual(10);
            expect(new Set(shards).size).toEqual(10);
        });

        it("all() with 100 shards and 1 subrequest", async () => {
            const sdo = new StaticShardedDO(env.SQLDO, { numShards: 10, concurrency: 1 });

            const shards: number[] = [];
            const ids = await sdo.all(async (stub, shard) => {
                shards.push(shard);
                return await stub.actorId();
            });
            expect(new Set(ids).size).toEqual(10);
            expect(new Set(shards).size).toEqual(10);
        });
    });

    describe("real-world scenarios", async () => {
        it("sqlite do - 01", async () => {
            const sdo = new StaticShardedDO(env.SQLDO, { numShards: 20 });

            // 5x the keys to have a higher chance of hitting all shards/DOs.
            const totalKeys = 100;
            const sql = (key: number) => `SELECT '${key}' as keystr;`;

            const results = [];
            for (let i = 0; i < totalKeys; i++) {
                const result = await sdo.one(`key-${i}`, async (stub) => {
                    const rows = await stub.sql(sql(i));
                    const actorId = await stub.actorId();
                    return { rows, actorId };
                });
                expect(result).toEqual({ rows: [{ keystr: String(i) }], actorId: expect.any(String) });
                results.push(result);
            }

            const actorIds = results.map((r) => r.actorId);
            // Make sure we spread out across all shards/DOs.
            expect(new Set(actorIds).size).toEqual(sdo.N);
        });
    });
});
