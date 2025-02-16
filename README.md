# durable-utils

Utilities for Cloudflare [Durable Objects](https://developers.cloudflare.com/durable-objects/) and Cloudflare [Workers](https://developers.cloudflare.com/workers/).

**Table of contents**

- [Install](#install)
- [SQLite Schema migrations](#sqlite-schema-migrations)
- [StaticShardedDO](#staticshardeddo)

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

## StaticShardedDO

`StaticShardedDO` is a helper making it easier to query `N` Durable Objects with the simplicity of single DO.

**Warning:** Once you start using `StaticShardedDO` with a specific shard size, you should NEVER change its number of shards.
At the moment, there is no re-sharding of the data stored by the Durable Objects, thus it's not safe to change the number of shards.

The TypeScript types have extensive documentation on the specifics, so do read them either through your IDE, or directly the `do-sharding.d.ts` types.

The methods `one(), some(), all()` return results as returned by the underlying DO call or throw the first error received, whereas their variations `tryOne(), trySome(), tryAll()` catch the errors within and return an object containing either the result or the error thrown.

### Example

In your Worker code (or elsewhere) when you want to call a method `actionA()` on a Durable Object of namespace `DO_ABC` you would have the following:

```javascript
import { StaticShardedDO } from "durable-utils/do-sharding";

const sdo = new StaticShardedDO(env.DO_ABC, { numShards: 11, concurrency: 6 });

// Query only the shard that handles the given partition key.
const partitionKey = "some-resource-ID-here"
const resultOfActionA = await sdo.one(partitionKey, async (stub, _shard) => {
    return await stub.actionA();
});

// Query all 11 shards and get their results in an array, or fail with the first error thrown.
const resultOfActionA = await sdo.all(async (stub, _shard) => {
    return await stub.actionA();
});

// Query all 11 shards and get their results or their errors without throwing.
const resultsList = await sdo.tryAll(async (stub, shard) => {
    return await stub.actionA();
});
const failed = resultsList.filter(r => !r.ok);
const worked = resultsList.filter(r => r.ok);

// Query only the even shards out of the 11 and get their results or their errors.
const resultsList = await sdo.trySome(async (stub, shard) => {
    return await stub.actionA();
}, {
    filterFn: (shard) => shard % 2 === 0,
});

// Automatically retry failed shards up to a total of 3 attempts.
const resultsList = await sdo.trySome(async (stub, shard) => {
    return await stub.actionA();
}, {
    filterFn: (_shard) => true,
    shouldRetry: (error: unknown, attempt: number, shard: ShardId) => attempt < 4;
});
```
