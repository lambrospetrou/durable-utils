# durable-utils

Utilities for Cloudflare Durable Objects and Workers.

## SQLite Schema migrations

In your Durable Object class:

```javascript
import { SQLSchemaMigration, SQLSchemaMigrations } from 'durable-utils/sql-migrations';

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
        // First always run your migrations before accessing SQLite.
        // If they already ran, it returns immediately without overhead.
        await this._migrations.runAll();

        // normal SQLite calls.
        return this.sql.exec("SELECT * FROM wikis;").toArray();
    }
}
```
