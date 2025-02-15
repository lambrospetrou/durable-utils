import {
    DurableObjectGetOptions,
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
    shardLocationHintFn?: (shard: number) => DurableObjectLocationHint | undefined;
}

export class FixedShardedDO<T extends Rpc.DurableObjectBranded | undefined> {
    #doNamespace: DurableObjectNamespace<T>;
    #options: FixedShardedDOOptions;

    constructor(doNamespace: DurableObjectNamespace<T>, options: FixedShardedDOOptions) {
        this.#doNamespace = doNamespace;
        this.#options = options;
        this.#options.concurrency ||= 10;

        if (this.#options.numShards <= 0) {
            throw new Error("Invalid number of shards, must be greater than 0");
        }
        if (this.#options.concurrency <= 0) {
            throw new Error("Invalid number of subrequests, must be greater than 0");
        }
    }

    /**
     * Execute a single request with the given shardKey.
     * @param shardKey The key to hash to determine the shard to use. The key will be hashed to determine the shard.
     * @param doer The callback function to execute with the Durable Object stub for the chosen shard.
     * @returns The result of the given `doer` callback function.
     */
    async one<R>(shardKey: string, doer: (doStub: DurableObjectStub<T>) => Promise<R>): Promise<R> {
        const shard = xxHash32(shardKey, GOLDEN_RATIO) % this.#options.numShards;
        const doId = this.#doNamespace.idFromName(`fixed-sharded-do-${shard}`);
        const stub = this.#doNamespace.get(doId, this.#stubOptions(shard));
        return await doer(stub);
    }

    /**
     * Execute a request to each of the shards, concurrently.
     * @param doer The callback function to execute with the Durable Object stub for each shard.
     * @returns An array of results from the given `doer` callback function for each shard,
     *          and an array of errors for each shard that failed.
     *          Items in the results array will be `undefined` if the shard failed, and similarly for the errors array.
     */
    async tryAll<R>(doer: (doStub: DurableObjectStub<T>, shard: number) => Promise<R>): Promise<{
        results: Array<R | undefined>;
        errors: Array<unknown>;
        hasErrors: boolean;
    }> {
        if (this.#options.numShards > 1000) {
            throw new Error(
                `Too many shards [${this.#options.numShards}], Cloudflare Workers only supports up to 1000 subrequests.`,
            );
        }
        return await this.#pipelineRequests(
            this.#options.concurrency!,
            this.#options.numShards,
            false,
            async (shard) => {
                const doId = this.#doNamespace.idFromName(`fixed-sharded-do-${shard}`);
                const stub = this.#doNamespace.get(doId, this.#stubOptions(shard));
                return await doer(stub, shard);
            },
        );
    }

    /**
     * Execute a request to each of the shards, concurrently.
     * @param doer The callback function to execute with the Durable Object stub for each shard.
     * @returns An array of results from the given `doer` callback function for each shard.
     *          In case of an error, the function will throw the error immediately.
     */
    async all<R>(doer: (doStub: DurableObjectStub<T>, shard: number) => Promise<R>): Promise<Array<R | undefined>> {
        if (this.#options.numShards > 1000) {
            throw new Error(
                `Too many shards [${this.#options.numShards}], Cloudflare Workers only supports up to 1000 subrequests.`,
            );
        }
        return (
            await this.#pipelineRequests(this.#options.concurrency!, this.#options.numShards, true, async (shard) => {
                const doId = this.#doNamespace.idFromName(`fixed-sharded-do-${shard}`);
                const stub = this.#doNamespace.get(doId, this.#stubOptions(shard));
                return await doer(stub, shard);
            })
        ).results;
    }

    /**
     * Execute a request to each of the shards, concurrently.
     * @param doer The callback function to execute with the Durable Object stub for each shard.
     * @returns An async generator of results from the given `doer` callback function for each shard.
     *          In case of an error, the function will throw the error immediately.
     */
    async *genAll<R>(doer: (doStub: DurableObjectStub<T>, shard: number) => Promise<R>): AsyncGenerator<R> {
        if (this.#options.numShards > 1000) {
            throw new Error(
                `Too many shards [${this.#options.numShards}], Cloudflare Workers only supports up to 1000 subrequests.`,
            );
        }
        for await (const result of this.#genPipelineRequests(
            this.#options.concurrency!,
            this.#options.numShards,
            async (shard) => {
                const doId = this.#doNamespace.idFromName(`fixed-sharded-do-${shard}`);
                const stub = this.#doNamespace.get(doId, this.#stubOptions(shard));
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
    async allMaybe<R>(doer: (doStub: DurableObjectStub<T>, shard: number) => Promise<R>): Promise<{
        results: Array<R | undefined>;
        errors: Array<unknown>;
        hasErrors: boolean;
    }> {
        return await this.tryAll(doer);
    }

    //////////////////////////////////////////////////////
    // INTERNAL IMPLEMENTATION

    async *#genPipelineRequests<R>(
        concurrency: number,
        n: number,
        doer: (shard: number) => Promise<R>,
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

    async #pipelineRequests<R>(
        concurrency: number,
        n: number,
        earlyReturn: boolean,
        doer: (shard: number) => Promise<R>,
    ): Promise<{
        results: Array<R | undefined>;
        errors: Array<unknown>;
        hasErrors: boolean;
    }> {
        const results: Array<R | undefined> = Array(n).fill(undefined);
        const errors: Array<unknown> = Array(n).fill(undefined);
        let i = 0;
        let hasErrors = false;
        const next = async () => {
            if (i >= n) {
                return;
            }
            const j = i++;
            try {
                results[j] = await doer(j);
            } catch (e) {
                if (earlyReturn) {
                    throw e;
                }
                hasErrors = true;
                errors[j] = e;
            }
            await next();
        };
        await Promise.all(
            Array(concurrency)
                .fill(0)
                .map(() => next()),
        );
        return { results, errors, hasErrors };
    }

    #stubOptions(shard: number): DurableObjectNamespaceGetDurableObjectOptions {
        const options: DurableObjectNamespaceGetDurableObjectOptions = {};
        if (this.#options.shardLocationHintFn) {
            options.locationHint = this.#options.shardLocationHintFn(shard);
        }
        return options;
    }
}
