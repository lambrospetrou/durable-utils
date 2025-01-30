import { DurableObjectNamespace, DurableObjectStub } from "@cloudflare/workers-types";
import xxhash from "xxhash-wasm";
const { h32 } = await xxhash();

// Golden Ratio constant used for better hash scattering
// See https://softwareengineering.stackexchange.com/a/402543
const GOLDEN_RATIO = 0x9e3779b1;

interface FixedShardedDOOptions {
    /**
     * The number of shards to use to spread out the load across all requests.
     * The number of shards will decide how many DOs will be used internally.
     *
     * WARNING: You should NEVER change this value after it has already been used.
     *          Changing this value will cause the DOs to be re-sharded and requests
     *          will be spread out differently, which can cause data loss or corruption.
     */
    numShards: number;

    concurrency?: number;
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

    async one<R>(shardKey: string, doer: (doStub: DurableObjectStub<T>) => Promise<R>): Promise<R> {
        const shard = h32(shardKey, GOLDEN_RATIO) % this.#options.numShards;
        const doId = this.#doNamespace.idFromName(`fixed-sharded-do-${shard}`);
        const stub = this.#doNamespace.get(doId);
        return await doer(stub);
    }


    async all<R>(doer: (doStub: DurableObjectStub<T>, shard: number) => Promise<R>): Promise<Array<R>> {
        if (this.#options.numShards > 1000) {
            throw new Error(`Too many shards [${this.#options.numShards}], Cloudflare Workers only supports up to 1000 subrequests.`);
        }
        return await this.pipelineRequests(this.#options.concurrency!, this.#options.numShards, async (shard) => {
            const doId = this.#doNamespace.idFromName(`fixed-sharded-do-${shard}`);
            const stub = this.#doNamespace.get(doId);
            return await doer(stub, shard);
        });
    }

    async *genAll<R>(doer: (doStub: DurableObjectStub<T>, shard: number) => Promise<R>): AsyncGenerator<R> {
        if (this.#options.numShards > 1000) {
            throw new Error(`Too many shards [${this.#options.numShards}], Cloudflare Workers only supports up to 1000 subrequests.`);
        }
        for await (const result of this.genPipelineRequests(this.#options.concurrency!, this.#options.numShards, async (shard) => {
            const doId = this.#doNamespace.idFromName(`fixed-sharded-do-${shard}`);
            const stub = this.#doNamespace.get(doId);
            return await doer(stub, shard);
        })) {
            yield result;
        }
    }

    private async *genPipelineRequests<R>(concurrency: number, n: number, doer: (shard: number) => Promise<R>): AsyncGenerator<R> {
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

    private async pipelineRequests<R>(concurrency: number, n: number, doer: (shard: number) => Promise<R>): Promise<Array<R>> {
        const results: Array<R> = Array(n).fill(null);
        let i = 0;
        const next = async () => {
            if (i >= n) {
                return;
            }
            const j = i++;
            results[j] = await doer(j);
            await next();
        };
        await Promise.all(Array(concurrency).fill(0).map(() => next()));
        return results;
    } 
}
