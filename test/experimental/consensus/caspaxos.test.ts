import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { Env } from "../../workers-test";
import { CASPaxosKV } from "../../../src/experimental/consensus/caspaxos";

declare module "cloudflare:test" {
    interface ProvidedEnv extends Env {}
}

describe("CASPaxosKV", { timeout: 20_000 }, async () => {
    it("should work in happy path", async () => {
        const kv = new CASPaxosKV(env.CASPaxosDO, {
            clusterName: "test-cluster",
            topology: {
                "type": "locationHintWeighted",
                "locations": {
                    "eeur": 2,
                    "weur": 1,
                }
            }
        });

        expect(await kv.get("foo-key-1")).toBe(null);
        expect(await kv.set("foo-key-1", 123456)).toBe(123456);
        expect(await kv.get("foo-key-1")).toBe(123456);

        expect(await kv.compareAndSet("foo-key-1", 123456, 11)).toBe(11);
        expect(await kv.compareAndSet("foo-key-1", 99, 22)).toBe(11);
    });
});
