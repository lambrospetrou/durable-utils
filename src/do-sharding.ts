import {
    DurableObjectLocationHint,
    DurableObjectNamespace,
    DurableObjectNamespaceGetDurableObjectOptions,
    DurableObjectStub,
    Rpc,
} from "@cloudflare/workers-types";
import { xxHash32 } from "js-xxhash";

// Golden Ratio constant used for better hash scattering
// See https://softwareengineering.stackexchange.com/a/402543
const GOLDEN_RATIO = 0x9e3779b1;

export interface FixedShardedDOOptions {
    /**
     * The number of shards to use for spreading out the load among all requests.
     * The number of shards will decide how many DOs will be used internally.
     *
     * WARNING: You should NEVER change this value after it has already been used.
     *          Changing this value will cause the DOs to be re-sharded and requests
     *          will be spread out differently, which can cause data loss or corruption.
     */
    numShards: number;

    /**
     * The number of concurrent subrequests to make to the DOs.
     * This value should be kept low to avoid hitting the Cloudflare subrequest limit.
     * The default value is 10.
     */
    concurrency?: number;

    /**
     * A function that returns a location hint for a specific shard.
     * The location hint is used to specify the region where the Durable Object should be placed.
     * If the function returns `undefined`, the Durable Object will be placed in the closest region.
     */
    shardLocationHintFn?: (shard: ShardId) => DurableObjectLocationHint | undefined;
}

export type ShardId = number;

export type TryResult<R> =
    | {
          ok: true;
          shard: ShardId;
          result: R;
      }
    | {
          ok: false;
          shard: ShardId;
          error: unknown;
      };

export type TryOptions = {
    filterFn: (shardId: ShardId) => boolean;
    shouldRetryFn?: (error: unknown) => boolean;
};

/**
 * @deprecated Use `tryAll` instead.
 */
export type AllMaybeResult<R> = {
    results: Array<R | undefined>;
    errors: Array<unknown>;
    hasErrors: boolean;
};

export class FixedShardedDO<T extends Rpc.DurableObjectBranded | undefined> {
    #doNamespace: DurableObjectNamespace<T>;
    #options: FixedShardedDOOptions & { concurrency: number };

    constructor(doNamespace: DurableObjectNamespace<T>, options: FixedShardedDOOptions) {
        this.#doNamespace = doNamespace;
        this.#options = {
            ...options,
            concurrency: options.concurrency || 10,
        };

        if (this.#options.numShards <= 0) {
            throw new Error("Invalid number of shards, must be greater than 0");
        }
        if (this.#options.concurrency <= 0) {
            throw new Error("Invalid number of subrequests, must be greater than 0");
        }
    }

    get N(): number {
        return this.#options.numShards;
    }

    /**
     * Execute a single request with the given shardKey.
     * @param shardKey The key to hash to determine the shard to use. The key will be hashed to determine the shard.
     * @param doer The callback function to execute with the Durable Object stub for the chosen shard.
     * @returns The result of the given `doer` callback function.
     */
    async one<R>(shardKey: string, doer: (doStub: DurableObjectStub<T>) => Promise<R>): Promise<R> {
        const shard = xxHash32(shardKey, GOLDEN_RATIO) % this.#options.numShards;
        const stub = this.#stub(shard);
        return await doer(stub);
    }

    /**
     * Execute a request to the shards selected by the `options.filterFn` callback, concurrently.
     * @param doer The callback function to execute with the Durable Object stub for each shard.
     * @param options The options to control the behavior of the `trySome` function.
     * @returns An array of results from the given `doer` callback function for each shard,
     *          and an array of errors for each shard that failed.
     *          Only shards that pass the `options.filterFn` will be returned.
     *          Items in the results array will be `undefined` if the shard failed, and similarly for the errors array.
     */
    async trySome<R>(
        doer: (doStub: DurableObjectStub<T>, shard: ShardId) => Promise<R>,
        options: TryOptions,
    ): Promise<Array<TryResult<R>>> {
        const responses = await this.#pipelineRequests(
            this.#options.concurrency,
            this.#options.numShards,
            {...options, earlyReturn: false},
            async (shard) => {
                const stub = this.#stub(shard);
                return await doer(stub, shard);
            },
        );
        return Array.from(responses.results.values(), (r) => r).sort((a, b) => a.shard - b.shard);
    }

    /**
     * Execute a request to the shards selected by the `options.filterFn` callback, concurrently.
     * @param doer The callback function to execute with the Durable Object stub for each shard.
     * @param options The options to control the behavior of the `trySome` function.
     * @returns An array of results from the given `doer` callback function for each filtered shard.
     *          In case of an error, the function will throw the error immediately.
     */
    async some<R>(
        doer: (doStub: DurableObjectStub<T>, shard: ShardId) => Promise<R>,
        options: TryOptions,
    ): Promise<Array<R>> {
        const responses = await this.#pipelineRequests(
            this.#options.concurrency,
            this.#options.numShards,
            {...options, earlyReturn: true},
            async (shard) => {
                const stub = this.#stub(shard);
                return await doer(stub, shard);
            },
        );
        return [...responses.results.values()].sort((a, b) => a.shard - b.shard).map((r) => {
            if (!r.ok) {
                throw r.error;
            }
            return r.result;
        });
    }

    /**
     * Execute a request to each of the shards, concurrently.
     * @param doer The callback function to execute with the Durable Object stub for each shard.
     * @returns An array of results from the given `doer` callback function for each shard,
     *          and an array of errors for each shard that failed.
     *          Items in the results array will be `undefined` if the shard failed, and similarly for the errors array.
     */
    async tryAll<R>(doer: (doStub: DurableObjectStub<T>, shard: ShardId) => Promise<R>): Promise<Array<TryResult<R>>> {
        if (this.#options.numShards > 1000) {
            throw new Error(
                `Too many shards [${this.#options.numShards}], Cloudflare Workers only supports up to 1000 subrequests.`,
            );
        }
        return await this.trySome(doer, { filterFn: (_shard) => true });
    }

    /**
     * Execute a request to each of the shards, concurrently.
     * @param doer The callback function to execute with the Durable Object stub for each shard.
     * @returns An array of results from the given `doer` callback function for each shard.
     *          In case of an error, the function will throw the error immediately.
     */
    async all<R>(doer: (doStub: DurableObjectStub<T>, shard: ShardId) => Promise<R>): Promise<Array<R>> {
        if (this.#options.numShards > 1000) {
            throw new Error(
                `Too many shards [${this.#options.numShards}], Cloudflare Workers only supports up to 1000 subrequests.`,
            );
        }
        return await this.some(doer, { filterFn: (_shard) => true });
    }

    /**
     * Execute a request to each of the shards, concurrently.
     * @param doer The callback function to execute with the Durable Object stub for each shard.
     * @returns An async generator of results from the given `doer` callback function for each shard.
     *          In case of an error, the function will throw the error immediately.
     */
    async *genAll<R>(doer: (doStub: DurableObjectStub<T>, shard: ShardId) => Promise<R>): AsyncGenerator<R> {
        if (this.#options.numShards > 1000) {
            throw new Error(
                `Too many shards [${this.#options.numShards}], Cloudflare Workers only supports up to 1000 subrequests.`,
            );
        }
        for await (const result of this.#genPipelineRequests(
            this.#options.concurrency,
            this.#options.numShards,
            async (shard) => {
                const stub = this.#stub(shard);
                return await doer(stub, shard);
            },
        )) {
            yield result;
        }
    }

    //////////////////////////////////////////////////////
    // DEPRECATED

    /**
     * Execute a request to each of the shards, concurrently.
     * @deprecated Use `tryAll` instead.
     * @param doer The callback function to execute with the Durable Object stub for each shard.
     * @returns An array of results from the given `doer` callback function for each shard,
     *          and an array of errors for each shard that failed.
     *          Items in the results array will be `undefined` if the shard failed, and similarly for the errors array.
     */
    async allMaybe<R>(doer: (doStub: DurableObjectStub<T>, shard: ShardId) => Promise<R>): Promise<AllMaybeResult<R>> {
        if (this.#options.numShards > 1000) {
            throw new Error(
                `Too many shards [${this.#options.numShards}], Cloudflare Workers only supports up to 1000 subrequests.`,
            );
        }
        const results = await this.#pipelineRequests(
            this.#options.concurrency,
            this.#options.numShards,
            { filterFn: () => true, earlyReturn: false },
            async (shard) => {
                const stub = this.#stub(shard);
                return await doer(stub, shard);
            },
        );
        const finalResults: AllMaybeResult<R> = {
            results: Array.from({ length: this.#options.numShards }),
            errors: Array.from({ length: this.#options.numShards }),
            hasErrors: results.hasErrors,
        };
        results.results.forEach((r) => {
            if (r.ok) {
                finalResults.results[r.shard] = r.result;
            } else {
                finalResults.errors[r.shard] = r.error;
            }
        });
        return finalResults;
    }

    //////////////////////////////////////////////////////
    // INTERNAL IMPLEMENTATION

    async #pipelineRequests<R>(
        concurrency: number,
        n: number,
        tryOptions: TryOptions & {
            earlyReturn: boolean;
        },
        doer: (shard: ShardId) => Promise<R>,
    ): Promise<{ results: Map<number, TryResult<R>>; hasErrors: boolean }> {
        const results = new Map();
        let i = 0;
        let hasErrors = false;
        const next = async () => {
            if (i >= n) {
                return;
            }
            const j = i++;
            try {
                if (!tryOptions.filterFn(j)) {
                    return;
                }
                results.set(j, {
                    ok: true,
                    result: await doer(j),
                    shard: j,
                });
            } catch (e) {
                if (tryOptions.earlyReturn) {
                    throw e;
                }
                hasErrors = true;
                results.set(j, {
                    ok: false,
                    shard: j,
                    error: e,
                });
            }
            await next();
        };
        await Promise.all(
            Array(concurrency)
                .fill(0)
                .map(() => next()),
        );
        return { results, hasErrors };
    }

    async *#genPipelineRequests<R>(
        concurrency: number,
        n: number,
        doer: (shard: ShardId) => Promise<R>,
    ): AsyncGenerator<R> {
        const results: Array<Promise<R>> = Array(n).fill(null);
        let idxAwait = 0;
        for (let shard = 0; shard < n; shard++) {
            if (shard < concurrency) {
                results[shard] = doer(shard);
            } else {
                yield await results[idxAwait];
                idxAwait++;
                results[shard] = doer(shard);
            }
        }
        for (; idxAwait < n; idxAwait++) {
            yield await results[idxAwait];
        }
    }

    #stub(shard: ShardId): DurableObjectStub<T> {
        const doId = this.#doNamespace.idFromName(`fixed-sharded-do-${shard}`);
        return this.#doNamespace.get(doId, this.#stubOptions(shard));
    }

    #stubOptions(shard: ShardId): DurableObjectNamespaceGetDurableObjectOptions {
        const options: DurableObjectNamespaceGetDurableObjectOptions = {};
        if (this.#options.shardLocationHintFn) {
            options.locationHint = this.#options.shardLocationHintFn(shard);
        }
        return options;
    }
}
