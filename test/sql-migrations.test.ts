import { env, listDurableObjectIds, runInDurableObject, SELF } from "cloudflare:test";
import { DurableObjectState } from "@cloudflare/workers-types";
import { describe, expect, it } from "vitest";
import { SQLiteDO } from "./workers-test";

describe("happy paths", async () => {
    it("empty initial storage", async () => {
        // Check sending request directly to instance
        const id = env.SQLDO.idFromName("emptyDO");
        const stub = env.SQLDO.get(id);
        let response = await runInDurableObject(stub, (instance: SQLiteDO) => {
            expect(instance).toBeInstanceOf(SQLiteDO);
            return instance.runMigrations([
                {
                    idMonotonicInc: 1,
                    description: "test default tables",
                    sql: `SELECT * FROM sqlite_master;`,
                },
            ]);
        });

        // The default KV internal table.
        expect(response.dbSize).toEqual(4096);

        // Check direct access to instance fields and storage
        await runInDurableObject(stub, async (instance: SQLiteDO, state: DurableObjectState) => {
            expect(await state.storage.get<number>("__sql_migrations_lastID")).toBe(1);
        });

        // Check IDs can be listed
        const ids = await listDurableObjectIds(env.SQLDO);
        expect(ids.length).toBe(1);
        expect(ids[0].equals(id)).toBe(true);
    });
});
