import {
    DurableObjectLocationHint,
    DurableObjectNamespace,
    DurableObjectNamespaceGetDurableObjectOptions,
    DurableObjectStub,
    Rpc,
} from "@cloudflare/workers-types";
import { xxHash32 } from "js-xxhash";
import { stubByName } from "./do-utils";
import { tryN } from "./retries";

// Golden Ratio constant used for better hash scattering
// See https://softwareengineering.stackexchange.com/a/402543
const GOLDEN_RATIO = 0x9e3779b1;

export interface StaticShardedDOOptions {
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

/**
 * A unique identifier for each shard. Zero-based index.
 */
export type ShardId = number;

/**
 * The result of a single request to a shard using the `trySome/tryAll` methods.
 */
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

/**
 * The options to control the behavior of the `trySome` method.
 */
export type TryOptions = {
    /**
     * A function to select which shards to use for the requests.
     * @param shardId The shard ID to decide if it should be queried.
     * @returns `true` for shards that should be used, and `false` for shards that should be skipped.
     */
    filterFn: (shardId: ShardId) => boolean;

    /**
     * A function to determine if a failed request should be retried.
     * The arguments of the function are:
     * @param error the error thrown by the request
     * @param attempt the attempt number (value 2 means it's the first retry after the failed first request)
     * @param shard the shard ID the request was made to
     * @returns `true` if the request should be retried, `false` otherwise.
     */
    shouldRetry?: (error: unknown, attempt: number, shard: ShardId) => boolean;
};

/**
 * The options to control the behavior of the `tryOne` method.
 */
export type TryOneOptions = Omit<TryOptions, "filterFn">;

/**
 * The options to control the behavior of the `tryAll` method.
 */
export type TryAllOptions = Omit<TryOptions, "filterFn">;

/**
 * @deprecated Use `tryAll` instead.
 */
export type AllMaybeResult<R> = {
    results: Array<R | undefined>;
    errors: Array<unknown>;
    hasErrors: boolean;
};

/**
 * A utility class to interact with a fixed number of sharded Durable Objects.
 * This class will automatically hash the given key to determine the shard to use.
 */
export class StaticShardedDO<T extends Rpc.DurableObjectBranded | undefined> {
    #doNamespace: DurableObjectNamespace<T>;
    #options: StaticShardedDOOptions & { concurrency: number };

    constructor(doNamespace: DurableObjectNamespace<T>, options: StaticShardedDOOptions) {
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

    /**
     * The number of shards used to spread out the load. Immutable after creation.
     */
    get N(): number {
        return this.#options.numShards;
    }

    /**
     * Execute a single request against the Durable Object responsible for the given partitionKey.
     * @param partitionKey The partition key is used to determine the shard.
     * @param doer The callback function to execute with the Durable Object stub for each shard.
     * @param options The options to control the execution like retries.
     * @returns An array of the results from the given `doer` callback function for each shard,
     *          Each item in the array will be a `TryResult` object with the `ok` property indicating success or failure.
     */
    async tryOne<R>(
        partitionKey: string,
        doer: (doStub: DurableObjectStub<T>, shard: ShardId) => Promise<R>,
        options?: TryOneOptions,
    ): Promise<TryResult<R>> {
        const shard = this.#shardForPartitionKey(partitionKey);
        const result = await this.#makeTryShard(doer, options)(shard);
        return result;
    }

    /**
     * Execute a single request against the Durable Object responsible for the given partitionKey.
     * @param partitionKey The partition key is used to determine the shard.
     * @param doer The callback function to execute with the Durable Object stub for the chosen shard.
     * @param options The options to control the execution like retries.
     * @returns The result of the given `doer` callback function. If `doer()` throws the error is propagated.
     */
    async one<R>(
        partitionKey: string,
        doer: (doStub: DurableObjectStub<T>) => Promise<R>,
        options?: TryOneOptions,
    ): Promise<R> {
        const response = await this.tryOne(partitionKey, doer, options);
        if (!response.ok) {
            throw response.error;
        }
        return response.result;
    }

    /**
     * Execute a request to the shards selected by the `options.filterFn` callback, concurrently.
     * @param doer The callback function to execute with the Durable Object stub for each shard.
     * @param options The options to control the execution like retries and filtering of shards.
     * @returns An array of the results from the given `doer` callback function for each shard,
     *          Each item in the array will be a `TryResult` object with the `ok` property indicating success or failure.
     *          Only shards that pass the `options.filterFn` will be returned.
     */
    async trySome<R>(
        doer: (doStub: DurableObjectStub<T>, shard: ShardId) => Promise<R>,
        options: TryOptions,
    ): Promise<Array<TryResult<R>>> {
        const responses = await this.#pipelineRequests(
            this.#options.concurrency,
            this.#options.numShards,
            { ...options, earlyReturn: false },
            this.#makeTryShard(doer, options),
        );
        return Array.from(responses.results.values(), (r) => r).sort((a, b) => a.shard - b.shard);
    }

    /**
     * Execute a request to the shards selected by the `options.filterFn` callback, concurrently.
     * @param doer The callback function to execute with the Durable Object stub for each shard.
     * @param options The options to control the execution like retries and filtering of shards.
     * @returns An array of results from the given `doer` callback function for each filtered shard.
     *          Only shards that pass the `options.filterFn` will be returned.
     *          In case of an error, the function will throw the error immediately.
     */
    async some<R>(
        doer: (doStub: DurableObjectStub<T>, shard: ShardId) => Promise<R>,
        options: TryOptions,
    ): Promise<Array<R>> {
        const responses = await this.#pipelineRequests(
            this.#options.concurrency,
            this.#options.numShards,
            { ...options, earlyReturn: true },
            this.#makeTryShard(doer, options),
        );
        return [...responses.results.values()]
            .sort((a, b) => a.shard - b.shard)
            .map((r) => {
                if (!r.ok) {
                    throw r.error;
                }
                return r.result;
            });
    }

    /**
     * Execute a request to each of the shards, concurrently.
     * @param doer The callback function to execute with the Durable Object stub for each shard.
     * @param options The options to control the execution like retries.
     * @returns An array of the results from the given `doer` callback function for each shard,
     *          Each item in the array will be a `TryResult` object with the `ok` property indicating success or failure.
     */
    async tryAll<R>(
        doer: (doStub: DurableObjectStub<T>, shard: ShardId) => Promise<R>,
        options?: TryAllOptions,
    ): Promise<Array<TryResult<R>>> {
        if (this.#options.numShards > 1000) {
            throw new Error(
                `Too many shards [${this.#options.numShards}], Cloudflare Workers only supports up to 1000 subrequests.`,
            );
        }
        return await this.trySome(doer, { ...options, filterFn: (_shard) => true });
    }

    /**
     * Execute a request to each of the shards, concurrently.
     * @param doer The callback function to execute with the Durable Object stub for each shard.
     * @param options The options to control the execution like retries.
     * @returns An array of results from the given `doer` callback function for each shard.
     *          In case of an error, the function will throw the error immediately.
     */
    async all<R>(
        doer: (doStub: DurableObjectStub<T>, shard: ShardId) => Promise<R>,
        options?: TryAllOptions,
    ): Promise<Array<R>> {
        if (this.#options.numShards > 1000) {
            throw new Error(
                `Too many shards [${this.#options.numShards}], Cloudflare Workers only supports up to 1000 subrequests.`,
            );
        }
        return await this.some(doer, { ...options, filterFn: (_shard) => true });
    }

    //////////////////////////////////////////////////////
    // DEPRECATED

    /**
     * Execute a request to each of the shards, concurrently.
     * @deprecated Use `all` or `tryAll` instead.
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
            this.#makeTryShard(doer),
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
        tryShardDoer: (shard: ShardId) => Promise<TryResult<R>>,
    ): Promise<{ results: Map<number, TryResult<R>>; hasErrors: boolean }> {
        const results = new Map();
        let i = 0;
        let hasErrors = false;
        const next = async () => {
            if (i >= n) {
                return;
            }
            const shard = i++;

            if (!tryOptions.filterFn(shard)) {
                // Skip this one and go to the next shard.
                return await next();
            }

            const shardResult = await tryShardDoer(shard);
            results.set(shard, shardResult);
            if (!shardResult.ok) {
                if (tryOptions.earlyReturn) {
                    throw shardResult.error;
                }
                hasErrors = true;
            }
            return await next();
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

    #makeTryShard<R>(
        doer: (doStub: DurableObjectStub<T>, shard: ShardId) => Promise<R>,
        tryOptions?: Omit<TryOptions, "filterFn">,
    ): (shard: ShardId) => Promise<TryResult<R>> {
        return async (shard: ShardId) => {
            const stub = this.#stub(shard);
            try {
                const result = await tryN(
                    Number.POSITIVE_INFINITY,
                    async () => await doer(stub, shard),
                    (e, attempt) => {
                        return !!tryOptions?.shouldRetry?.(e, attempt, shard);
                    },
                );
                return { ok: true, shard, result };
            } catch (e) {
                return { ok: false, shard, error: e };
            }
        };
    }

    #shardForPartitionKey(partitionKey: string): ShardId {
        const shard = xxHash32(partitionKey, GOLDEN_RATIO) % this.#options.numShards;
        return shard;
    }

    #stub(shard: ShardId): DurableObjectStub<T> {
        return stubByName(this.#doNamespace, `fixed-sharded-do-${shard}`, this.#stubOptions(shard));
    }

    #stubOptions(shard: ShardId): DurableObjectNamespaceGetDurableObjectOptions {
        const options: DurableObjectNamespaceGetDurableObjectOptions = {};
        if (this.#options.shardLocationHintFn) {
            options.locationHint = this.#options.shardLocationHintFn(shard);
        }
        return options;
    }
}

/**
 * @deprecated Use `StaticShardedDO` instead.
 */
export const FixedShardedDO = StaticShardedDO;
