import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { Env } from "../../workers-test";
import { CASPaxosKV } from "../../../src/experimental/consensus/caspaxos";

declare module "cloudflare:test" {
    interface ProvidedEnv extends Env {}
}

function makeKV(env: Env) {
    return new CASPaxosKV(env.CASPaxosDO, {
        clusterName: "test-cluster",
        topology: {
            "type": "locationHintWeighted",
            "locations": {
                "eeur": 2,
                "weur": 1,
            }
        }
    });
}

describe("CASPaxosKV", { timeout: 20_000 }, async () => {
    it("should work in happy path", async () => {
        const kv = makeKV(env);

        expect(await kv.get("foo-key-1")).toBe(null);
        expect(await kv.set("foo-key-1", 123456)).toBe(123456);
        expect(await kv.get("foo-key-1")).toBe(123456);

        expect(await kv.compareAndSet("foo-key-1", 123456, 11)).toBe(11);
        expect(await kv.compareAndSet("foo-key-1", 99, 22)).toBe(11);

        expect(await kv.update<number>("foo-key-1", (v) => (v ?? 100) +1)).toBe(12);

        expect(await kv.delete("foo-key-1")).toBe(true);
        expect(await kv.get("foo-key-1")).toBe(null);
    });

    it("should throw with invalid options", async () => {
        expect(() => new CASPaxosKV(env.CASPaxosDO, {
            clusterName: "test-cluster",
            topology: {
                "type": "locationHintWeighted",
                "locations": {
                    "eeur": 2,
                    "weur": 2,
                }
            },
        })).toThrowError("Total number of nodes must be odd to ensure a majority quorum like 3,5,7.");

        expect(() => new CASPaxosKV(env.CASPaxosDO, {
            clusterName: "test-cluster",
            topology: {
                "type": "locationHintWeighted",
                "locations": {
                    "weur": 1,
                }
            },
        })).toThrowError("Total number of nodes must be at least 3 to ensure a majority quorum.");
    });
});
