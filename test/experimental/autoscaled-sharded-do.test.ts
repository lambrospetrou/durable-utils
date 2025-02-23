import { env } from "cloudflare:test";
import { assert, describe, expect, it } from "vitest";
import { Env } from "../workers-test";
import { AutoscaledShardedDOClient_EXPERIMENTAL_V0 } from "../../src/experimental/autoscaled-sharded-do";

declare module "cloudflare:test" {
    interface ProvidedEnv extends Env {}
}

describe("AutoscaledShardedDO", { timeout: 20_000 }, async () => {
    it("tryOne() with errors", async () => {
        const sdo = new AutoscaledShardedDOClient_EXPERIMENTAL_V0({ 
            shardGroupName: "test",
            shardStorageNamespace: env.SHARDED_DO,
            shardConfigCacheNamespace: env.AutoscaledShardedDOShardConfigCache,
            shardConfigSnapshot: {
                history: [
                    { timestampISO: new Date().toISOString(), capacity: 10},
                ],
            }
         });
        console.log(await sdo.tryOne("test", async (rpcCallerFn) => {
            // rpcCallerFn("echo", "test-01");
            // rpcCallerFn("actorId");
            await rpcCallerFn.echo("test-01");
            await rpcCallerFn.actorId();
         }));
        // const response1 = await sdo.tryOne("test", async (stub) => {
        //     return await stub.echo("test-01");
        // });
        // expect(response1.ok).toBe(true);
        // assert(response1.ok === true);
        // expect(response1.result).toBe("test-01");

        // const response2 = await sdo.tryOne("test2", async (stub) => {
        //     throw new Error("test-error");
        // });
        // expect(response2.ok).toBe(false);
        // assert(response2.ok === false);
        // expect(response2.error).toEqual(new Error("test-error"));
    });
});
