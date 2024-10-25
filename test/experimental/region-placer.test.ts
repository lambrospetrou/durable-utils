import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";

describe(
    "test RPC method on returned RpcTarget to target location",
    {
        timeout: 5_000,
    },
    async () => {
        it("RegionPlacer auxiliary worker with binding target as name string", async () => {
            using workerStub = env.RegionPlacer.place("eeur", "TargetWorker");
            expect(await workerStub.ping("boomer")).toEqual("ping:boomer");
        });

        it("Auto infer binding from the 'this' Worker", async () => {
            using workerStub = env.TargetWorker.regionPlace("eeur");
            expect(await workerStub.ping("boomer")).toEqual("ping:boomer");
        });
    },
);
