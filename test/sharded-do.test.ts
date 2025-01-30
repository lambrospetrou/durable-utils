import { env, listDurableObjectIds, runInDurableObject } from "cloudflare:test";
import { DurableObjectState } from "@cloudflare/workers-types";
import { describe, expect, it } from "vitest";
import { Env, SQLiteDO } from "./workers-test";
import { FixedShardedDO } from "../src/sharded-do.mjs";

declare module "cloudflare:test" {
    interface ProvidedEnv extends Env {}
}

describe("FixedShardedDO", { timeout: 20_000 }, async () => {
    it("one()", async () => {
        const sdo = new FixedShardedDO(env.SQLDO, { numShards: 10 });
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

    it("all()", async () => {
        const sdo = new FixedShardedDO(env.SQLDO, { numShards: 11 });

        const shards: number[] = [];
        const ids = await sdo.all(async (stub, shard) => {
            shards.push(shard);
            return await stub.actorId();
        });
        expect(new Set(ids).size).toEqual(11);
        expect(new Set(shards).size).toEqual(11);
        expect(shards).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    });

    it("genAll()", async () => {
        const sdo = new FixedShardedDO(env.SQLDO, { numShards: 11 });

        const shards: number[] = [];
        const ids: string[] = [];
        for await (const res of sdo.genAll(async (stub, shard) => {
            shards.push(shard);
            return await stub.actorId();
        })) {
            ids.push(res);
        }
        expect(new Set(ids).size).toEqual(11);
        expect(new Set(shards).size).toEqual(11);
        expect(shards).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    });

    describe("shards vs concurrency", { timeout: 20_000 }, async () => {
        it("all() with 100 shards and 1000 subrequests", async () => {
            const sdo = new FixedShardedDO(env.SQLDO, { numShards: 10, concurrency: 1000 });

            const shards: number[] = [];
            const ids = await sdo.all(async (stub, shard) => {
                shards.push(shard);
                return await stub.actorId();
            });
            expect(new Set(ids).size).toEqual(10);
            expect(new Set(shards).size).toEqual(10);
        });

        it("all() with 100 shards and 100 subrequests", async () => {
            const sdo = new FixedShardedDO(env.SQLDO, { numShards: 10, concurrency: 10 });

            const shards: number[] = [];
            const ids = await sdo.all(async (stub, shard) => {
                shards.push(shard);
                return await stub.actorId();
            });
            expect(new Set(ids).size).toEqual(10);
            expect(new Set(shards).size).toEqual(10);
        });

        it("all() with 100 shards and 10 subrequests", async () => {
            const sdo = new FixedShardedDO(env.SQLDO, { numShards: 10, concurrency: 3 });

            const shards: number[] = [];
            const ids = await sdo.all(async (stub, shard) => {
                shards.push(shard);
                return await stub.actorId();
            });
            expect(new Set(ids).size).toEqual(10);
            expect(new Set(shards).size).toEqual(10);
        });

        it("all() with 100 shards and 1 subrequest", async () => {
            const sdo = new FixedShardedDO(env.SQLDO, { numShards: 10, concurrency: 1 });

            const shards: number[] = [];
            const ids = await sdo.all(async (stub, shard) => {
                shards.push(shard);
                return await stub.actorId();
            });
            expect(new Set(ids).size).toEqual(10);
            expect(new Set(shards).size).toEqual(10);
        });
    });

});
