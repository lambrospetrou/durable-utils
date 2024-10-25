# durable-utils

Utilities for Cloudflare [Durable Objects](https://developers.cloudflare.com/durable-objects/) and Cloudflare [Workers](https://developers.cloudflare.com/workers/).

## Install

```sh
npm install durable-utils
```

## SQLite Schema migrations

There is a class `SQLSchemaMigrations` that accepts your Durable Object's instance `.storage` property ([see docs](https://developers.cloudflare.com/durable-objects/api/state/#storage)) along with a list of SQL schema migrations, and you run its `.runAll()` method anywhere in your Durable Object class right before reading or writing from the local SQLite database.

The class `SQLSchemaMigrations` keeps track of executed migrations both in memory and in the Durable Object storage so it's safe to run it as many times as you want, and it early returns when there are no further migrations to execute.

The TypeScript types have extensive documentation on the specifics, so do read them either through your IDE, or directly the `sql-migrations.d.ts` types.

### Example

In your Durable Object class:

```javascript
import {
    SQLSchemaMigration,
    SQLSchemaMigrations,
} from 'durable-utils/sql-migrations';

// Example migrations.
const Migrations: SQLSchemaMigration[] = [
    {
        idMonotonicInc: 1,
        description: 'initial version',
        sql: `
            CREATE TABLE IF NOT EXISTS tenant_info(
                tenantId TEXT PRIMARY KEY,
                dataJson TEXT
            );
            CREATE TABLE IF NOT EXISTS wikis (
                wikiId TEXT PRIMARY KEY,
                tenantId TEXT,
                name TEXT,
                wikiType TEXT
            );
        `,
    },
    {
        idMonotonicInc: 2,
        description: 'add timestamp column to wikis',
        sql: `
            ALTER TABLE wikis
            ADD createdAtMs INTEGER;
        `,
    },
];

export class TenantDO extends DurableObject {
    env: CfEnv;
    sql: SqlStorage;

    _migrations: SQLSchemaMigrations;

    constructor(ctx: DurableObjectState, env: CfEnv) {
        super(ctx, env);
        this.env = env;
        this.sql = ctx.storage.sql;

        this._migrations = new SQLSchemaMigrations({
            doStorage: ctx.storage,
            migrations: Migrations,
        });
    }

    async operationThatNeedsSQLite() {
        // Always run your migrations before accessing SQLite.
        // If they already ran, it returns immediately without overhead.
        await this._migrations.runAll();

        // Normal SQLite calls.
        return this.sql.exec("SELECT * FROM wikis;").toArray();
    }
}
```
