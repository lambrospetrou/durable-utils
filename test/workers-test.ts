import {
    DurableObject,
    DurableObjectNamespace,
    DurableObjectState,
    ExportedHandler,
    Request,
    Response,
} from "@cloudflare/workers-types";

import { SQLSchemaMigration, SQLSchemaMigrations } from "../src/sql-migrations";

interface Env {
    SQLDO: DurableObjectNamespace;
}

export class SQLiteDO implements DurableObject {
    constructor(
        readonly ctx: DurableObjectState,
        readonly env: Env,
    ) {}

    fetch(request: Request): Response | Promise<Response> {
        throw new Error("Method not implemented.");
    }

    async runMigrations(sqlMigrations: SQLSchemaMigration[]) {
        const migrations = new SQLSchemaMigrations({
            migrations: sqlMigrations,
            doStorage: this.ctx.storage,
        });
        const result = await migrations.runAll();
        return { result, dbSize: this.ctx.storage.sql.databaseSize };
    }
}

export default <ExportedHandler<Env>>{
    fetch(request, env) {
        const { pathname } = new URL(request.url);
        const id = env.SQLDO.idFromName(pathname);
        const stub = env.SQLDO.get(id);
        return stub.fetch(request);
    },
};
