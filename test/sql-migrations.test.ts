import { env, listDurableObjectIds, runInDurableObject, SELF } from "cloudflare:test";
import { DurableObjectState } from "@cloudflare/workers-types";
import { describe, expect, it } from "vitest";
import { SQLiteDO } from "./workers-test";
import { SQLSchemaMigration, SQLSchemaMigrations } from "../src/sql-migrations";

function makeM(state: DurableObjectState, migrations: SQLSchemaMigration[]) {
    return new SQLSchemaMigrations({
        doStorage: state.storage,
        migrations: migrations,
    });
}

describe("happy paths", async () => {
    it("empty initial storage", async () => {
        // Check sending request directly to instance
        const id = env.SQLDO.idFromName("emptyDO");
        const stub = env.SQLDO.get(id);
        await runInDurableObject(stub, async (instance: SQLiteDO, state: DurableObjectState) => {
            expect(instance).toBeInstanceOf(SQLiteDO);

            const m = makeM(state, [
                {
                    idMonotonicInc: 1,
                    description: "test default tables",
                    sql: `SELECT * FROM sqlite_master;`,
                },
            ]);
            await m.runAll();

            expect(state.storage.sql.databaseSize).toEqual(8192);
        });

        // Check direct access to instance fields and storage
        await runInDurableObject(stub, async (instance: SQLiteDO, state: DurableObjectState) => {
            expect(await state.storage.get<number>("__sql_migrations_lastID")).toBe(1);
        });

        // Check IDs can be listed
        const ids = await listDurableObjectIds(env.SQLDO);
        expect(ids.length).toBe(1);
        expect(ids[0].equals(id)).toBe(true);
    });

    it("multiple DDL", async () => {
        const id = env.SQLDO.idFromName("emptyDO");
        const stub = env.SQLDO.get(id);

        await runInDurableObject(stub, async (instance: SQLiteDO, state: DurableObjectState) => {
            expect(instance).toBeInstanceOf(SQLiteDO);
            const res = await makeM(state, [
                {
                    idMonotonicInc: 1,
                    description: "tbl1",
                    sql: `CREATE TABLE users(name TEXT PRIMARY KEY, age INTEGER);`,
                },
                {
                    idMonotonicInc: 2,
                    description: "tbl2",
                    sql: `CREATE TABLE IF NOT EXISTS usersActivities (activityType TEXT, userName TEXT, PRIMARY KEY (userName, activityType));`,
                },
            ]).runAll();

            expect(res).toEqual({
                rowsRead: 2,
                rowsWritten: 6,
            });
        });
    });

    it("run migrations multiple times", async () => {
        const id = env.SQLDO.idFromName("emptyDO");
        const stub = env.SQLDO.get(id);

        await runInDurableObject(stub, async (instance: SQLiteDO, state: DurableObjectState) => {
            const m1 = [
                {
                    idMonotonicInc: 1,
                    description: "tbl1",
                    sql: `CREATE TABLE IF NOT EXISTS users(name TEXT PRIMARY KEY, age INTEGER);`,
                },
                {
                    idMonotonicInc: 2,
                    description: "tbl2",
                    sql: `CREATE TABLE IF NOT EXISTS usersActivities (activityType TEXT, userName TEXT, PRIMARY KEY (userName, activityType));`,
                },
            ];
            const r1 = await makeM(state, m1).runAll();

            const m2 = m1.concat([
                {
                    idMonotonicInc: 3,
                    description: "round 2",
                    sql: `
                    CREATE TABLE IF NOT EXISTS marvel (heroName TEXT);
                    CREATE TABLE IF NOT EXISTS marvelMovies (name TEXT PRIMARY KEY, releaseDateMs INTEGER);
                    `,
                },
            ]);
            const r2_1 = await makeM(state, m2).runAll();

            for (let i = 0; i < 5; i++) {
                const r2_n = await makeM(state, m2).runAll();
                // Should have no effect running the same migrations.
                expect(r2_n).toEqual({
                    rowsRead: 0,
                    rowsWritten: 0,
                });
            }

            return { r1, r2: r2_1 };
        });
    });

    it("hasMigrationsToRun", async () => {
        const id = env.SQLDO.idFromName("hasMigrationsToRun");
        const stub = env.SQLDO.get(id);

        await runInDurableObject(stub, async (instance: SQLiteDO, state: DurableObjectState) => {
            expect(makeM(state, []).hasMigrationsToRun()).toEqual(false);

            const m1 = [
                {
                    idMonotonicInc: 1,
                    description: "tbl1",
                    sql: `CREATE TABLE users(name TEXT PRIMARY KEY, age INTEGER);`,
                },
            ];
            // Reuse the same SQLSchemaMigrations instance otherwise `hasMigrationsToRun()`
            // will always be true before running `runAll()`.
            const m = makeM(state, m1);
            expect(m.hasMigrationsToRun()).toEqual(true);
            await m.runAll();
            expect(m.hasMigrationsToRun()).toEqual(false);
        });
    });

    describe("expected exceptions", () => {
        it("duplicate IDs", async () => {
            const id = env.SQLDO.idFromName("emptyDO");
            const stub = env.SQLDO.get(id);

            await runInDurableObject(stub, async (instance: SQLiteDO, state: DurableObjectState) => {
                expect(
                    async () =>
                        await makeM(state, [
                            {
                                idMonotonicInc: 1,
                                description: "tbl1",
                                sql: `CREATE TABLE users(name TEXT PRIMARY KEY, age INTEGER);`,
                            },
                            {
                                idMonotonicInc: 2,
                                description: "tbl1",
                                sql: `CREATE TABLE IF NOT EXISTS users(name TEXT PRIMARY KEY, age INTEGER);`,
                            },
                            {
                                idMonotonicInc: 1,
                                description: "tbl2",
                                sql: `CREATE TABLE IF NOT EXISTS usersActivities (activityType TEXT, userName TEXT, PRIMARY KEY (userName, activityType));`,
                            },
                        ]).runAll(),
                ).rejects.toThrowError("duplicate migration ID detected: 1");
            });
        });

        it("negative IDs", async () => {
            const id = env.SQLDO.idFromName("emptyDO");
            const stub = env.SQLDO.get(id);

            await runInDurableObject(stub, async (instance: SQLiteDO, state: DurableObjectState) => {
                expect(
                    async () =>
                        await makeM(state, [
                            {
                                idMonotonicInc: -1,
                                description: "tbl1",
                                sql: `CREATE TABLE users(name TEXT PRIMARY KEY, age INTEGER);`,
                            },
                        ]).runAll(),
                ).rejects.toThrowError("migration ID cannot be negative: -1");
            });
        });
    });
});