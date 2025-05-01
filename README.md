# durable-utils

Utilities and abstractions for Cloudflare [Durable Objects](https://developers.cloudflare.com/durable-objects/) and Cloudflare [Workers](https://developers.cloudflare.com/workers/).

For changes between versions checkout the [CHANGELOG](./CHANGELOG.md).

**Table of contents**

- [Install](#install)
- [SQLite Schema migrations](#sqlite-schema-migrations)
- [StaticShardedDO](#staticshardeddo)
- [Durable Objects Utilities](#durable-objects-utilities)
- [Retry Utilities](#retry-utilities)

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

        // You can run your migrations in the constructor if you are certain
        // that all requests incoming to your Durable Object (DO) are legit.
        // If you want to avoid writing to storage and incurring charges when
        // any request is processed by your DO run them right before you need them
        // as seen inside the `operationThatNeedsSQLite()` function below.
        //
        // ctx.blockConcurrencyWhile(async () => {
        //     await this._migrations.runAll();
        // });
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

> [!WARNING]
> Even though I will try not to do breaking changes to the `StaticShardedDO` API, this is still into design phase.
> Depending on feedback and using it over time, I might change its API.
> Please use it, and give me feedback on what you find hard to use.
>
> The underlying guarantee about the static number of shards **will not change to avoid data loss.**

`StaticShardedDO` is a helper making it easier to query `N` Durable Objects with the simplicity of single DO.

> [!IMPORTANT]
> Once you start using `StaticShardedDO` with a specific shard size, you should NEVER change its number of shards.
> At the moment, there is no re-sharding of the data stored by the Durable Objects, thus it's not safe to change the number of shards.

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

// Query all 11 shards.
// Get their results in an array, or fail with the first error thrown.
const resultsOfActionA = await sdo.all(async (stub, _shard) => {
    return await stub.actionA();
});

// Query all 11 shards.
// Get their results or their errors without throwing.
const resultsList = await sdo.tryAll(async (stub, shard) => {
    return await stub.actionA();
});
const failed = resultsList.filter(r => !r.ok);
const worked = resultsList.filter(r => r.ok);

// Query only the even shards out of the 11.
// Get their results or their errors without throwing.
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
    shouldRetry: (_error, attempt, _shard) => attempt < 4;
});
```

You can create multiple independent groups of shards within the same Durable Object Namespace.

Provide a unique `shardGroupName` in the `StaticShardedDO` constructor options to use different Durable Objects.

The default shard group name if none is provided is `fixed-sharded-do`.

```javascript
describe("shardGroupName", async () => {
    it("default prefix should not conflict with shard group names", async () => {
        const sdo = new StaticShardedDO(env.SQLDO, { numShards: 3 });
        const result = await sdo.one("test", async (stub) => {
            return await stub.actorId();
        });

        const sdo2 = new StaticShardedDO(env.SQLDO, {
            numShards: 3, shardGroupName: "groupOfShards2",
        });
        const result2 = await sdo2.one("test", async (stub) => {
            return await stub.actorId();
        });

        const sdo3 = new StaticShardedDO(env.SQLDO, {
            numShards: 3, shardGroupName: "groupOfShards3",
        });
        const result3 = await sdo3.one("test", async (stub) => {
            return await stub.actorId();
        });

        // Even though we use the same partition key,
        // the actual DOs handling the request are different.
        expect(result).not.toEqual(result2);
        expect(result).not.toEqual(result3);
        expect(result2).not.toEqual(result3);
    });
});
```

## Durable Objects Utilities

Helper functions for working with Cloudflare Durable Objects.

### `stubByName<T>(doNamespace, name, options)`

Creates a [Durable Object stub](https://developers.cloudflare.com/durable-objects/api/stub/) from a string name in a single function call, combining the `DurableObjectNamespace` operations `idFromName` and `get`.

```javascript
import { stubByName } from "durable-utils/do-utils";

const stub = stubByName(env.MY_DURABLE_OBJECT, "instance-1");
```

**Parameters**

- `doNamespace`: The [Durable Object namespace binding](https://developers.cloudflare.com/durable-objects/api/namespace/).
- `name`: String identifier for the Durable Object instance to access.
- `options`: Optional Durable Object configuration options, see <https://developers.cloudflare.com/durable-objects/api/namespace/#get>.

### `isErrorRetryable(err)`

Determines if a Durable Object error should be retried based on Cloudflare's error handling guidelines.

```javascript
import { isErrorRetryable } from "durable-utils/do-utils";

if (isErrorRetryable(error)) {
    // Safe to retry the operation
}
```

Returns `true` for retryable errors, excluding overloaded errors. This follows Cloudflare's official [error handling best practices](https://developers.cloudflare.com/durable-objects/best-practices/error-handling/).

## Retry Utilities

This package provides utilities for handling retries with exponential backoff and jitter.

### `tryN<T>(n, fn, options)`

Executes a function with retry logic, implementing exponential backoff with full jitter.

```javascript
import { tryN } from  "durable-utils/retries";

await tryN(
    3,
    async (_attempt) => fetch(url),
);
```

**Parameters**

- `n`: Maximum number of attempts.
- `fn`: Async function to execute, receives current attempt number and returns a value `T`.
- `options`: Optional configuration object
    - `baseDelayMs`: Base delay for exponential backoff (default: 100ms).
    - `maxDelayMs`: Maximum delay between retries (default: 3000ms).
    - `isRetryable`: Function that determines if an error should trigger a retry.
    - `verbose`: Enable logging of retry attempts.

The retry delay is calculated using the "Full Jitter" approach, which helps prevent thundering herd problems, as described in Marc Brooker's AWS post [Exponential Backoff And Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/).

Each retry attempt uses a random delay equal to `random_between(0, min(2^attempt * baseDelayMs, maxDelayMs))`.

### `tryWhile<T>(fn, isRetryable, options)`

Executes a function with retry logic, implementing exponential backoff with full jitter.

```javascript
import { tryWhile } from  "durable-utils/retries";
import { isErrorRetryable } from "durable-utils/do-utils";

await tryWhile(
    async (_attempt) => fetch(url),
    (err, nextAttempt) => nextAttempt < 5 && isErrorRetryable(err),
);
```

**Parameters**

- `fn`: Async function to execute, receives current attempt number and returns a value `T`.
- `isRetryable`: Function that determines if an error should trigger a retry.
- `options`: Optional configuration object
    - `baseDelayMs`: Base delay for exponential backoff (default: 100ms).
    - `maxDelayMs`: Maximum delay between retries (default: 3000ms).
    - `verbose`: Enable logging of retry attempts.

The retry delay is calculated using the "Full Jitter" approach, which helps prevent thundering herd problems, as described in Marc Brooker's AWS post [Exponential Backoff And Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/).

Each retry attempt uses a random delay equal to `random_between(0, min(2^attempt * baseDelayMs, maxDelayMs))`.
