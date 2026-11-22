import { DurableObject } from "cloudflare:workers";
import { RegionPlaceableTarget, RegionPlaceableWorkerEntrypoint } from "../src/experimental/region-placer";
import {
    AcceptorClient,
    PrepareNode,
    AcceptNode,
    PrepareResult,
    AcceptResult,
    BallotNumber,
} from "../src/experimental/consensus/caspaxos";
import {
    ShardedReducerDOIngestMessage,
    ShardedReducerDOMergeMessage,
    ShardedReducerDOPeer,
    ShardedReducerDOTarget,
} from "../src/do-reducer";
export { RegionPlacer, RegionPlacerDO } from "../src/experimental/region-placer";

export class SQLiteDO extends DurableObject {
    constructor(ctx: DurableObjectState, env: Env) {
        super(ctx, env);
    }

    async actorId() {
        return String(this.ctx.id);
    }

    async echo(s: string) {
        return s;
    }

    async sql(query: string) {
        return this.ctx.storage.sql.exec(query).toArray();
    }
}

export class CASPaxosDO extends DurableObject implements PrepareNode, AcceptNode {
    private acceptorClient: AcceptorClient;
    constructor(ctx: DurableObjectState, env: Env) {
        super(ctx, env);
        this.acceptorClient = new AcceptorClient(ctx.storage);
    }

    async actorId() {
        return String(this.ctx.id);
    }

    async prepare(key: string, tick: BallotNumber, extra?: any): Promise<PrepareResult> {
        return await this.acceptorClient.prepare(key, tick, extra);
    }

    async accept(
        key: string,
        ballot: BallotNumber,
        value: any,
        promise: BallotNumber,
        extra?: any,
    ): Promise<AcceptResult> {
        return await this.acceptorClient.accept(key, ballot, value, promise, extra);
    }
}

export class CounterReducerDO extends DurableObject<Env> implements ShardedReducerDOTarget<number> {
    #reducer: ShardedReducerDOPeer<number>;
    #failNextMerges = 0;
    #failRetryable = false;
    #failNextIngests = 0;

    constructor(ctx: DurableObjectState, env: Env) {
        super(ctx, env);
        this.#reducer = new ShardedReducerDOPeer<number>(ctx, env.CounterReducerDO, {
            reducer: (a, b) => a + b,
            maxKeysPerBatch: 10,
        });
    }

    async actorId(): Promise<string> {
        return String(this.ctx.id);
    }

    async storagePendingCount(): Promise<number> {
        let count = 0;
        for (const _ of this.ctx.storage.kv.list({ prefix: "__du_reducer:p:" })) {
            count++;
        }
        return count;
    }

    async configureFailures(opts: {
        failNextMerges?: number;
        failRetryable?: boolean;
        failNextIngests?: number;
    }): Promise<void> {
        this.#failNextMerges = opts.failNextMerges ?? 0;
        this.#failRetryable = opts.failRetryable ?? false;
        this.#failNextIngests = opts.failNextIngests ?? 0;
    }

    async deleteReducedKey(reductionKey: string): Promise<boolean> {
        return this.#reducer.delete(reductionKey);
    }

    async simulateRestart(): Promise<void> {
        // A new helper over the same storage behaves like a fresh Durable Object instance after a restart.
        this.#reducer = new ShardedReducerDOPeer<number>(this.ctx, this.env.CounterReducerDO, {
            reducer: (a, b) => a + b,
            maxKeysPerBatch: 10,
        });
    }

    async reduceIngest(msg: ShardedReducerDOIngestMessage<number>): Promise<void> {
        if (this.#failNextIngests > 0) {
            this.#failNextIngests--;
            throw new Error("simulated-ingest-error");
        }
        await this.#reducer.ingest(msg);
    }

    async reduceMerge(msg: ShardedReducerDOMergeMessage<number>): Promise<void> {
        if (this.#failNextMerges > 0) {
            this.#failNextMerges--;
            const err = new Error("simulated-merge-error") as Error & { retryable?: boolean };
            if (this.#failRetryable) {
                err.retryable = true;
            }
            throw err;
        }
        await this.#reducer.merge(msg);
    }

    async reduceGet(reductionKey: string): Promise<number | undefined> {
        return this.#reducer.get(reductionKey);
    }

    async alarm(): Promise<void> {
        await this.#reducer.alarm();
    }
}

export interface LatencySample {
    ms: number;
}

export interface LatencyDelta {
    count: number;
    sum: number;
    min: number;
    max: number;
}

export interface LatencySummary extends LatencyDelta {
    avg: number;
}

export class StatsReducerDO
    extends DurableObject<Env>
    implements ShardedReducerDOTarget<LatencySample, LatencyDelta, LatencySummary>
{
    #reducer: ShardedReducerDOPeer<LatencySample, LatencyDelta, LatencySummary>;

    constructor(ctx: DurableObjectState, env: Env) {
        super(ctx, env);
        this.#reducer = new ShardedReducerDOPeer<LatencySample, LatencyDelta, LatencySummary>(ctx, env.StatsReducerDO, {
            reducer: {
                ingest: (curr, sample) =>
                    curr === undefined
                        ? { count: 1, sum: sample.ms, min: sample.ms, max: sample.ms }
                        : {
                              count: curr.count + 1,
                              sum: curr.sum + sample.ms,
                              min: Math.min(curr.min, sample.ms),
                              max: Math.max(curr.max, sample.ms),
                          },
                combine: (a, b) => ({
                    count: a.count + b.count,
                    sum: a.sum + b.sum,
                    min: Math.min(a.min, b.min),
                    max: Math.max(a.max, b.max),
                }),
                apply: (state, delta) => {
                    const count = (state?.count ?? 0) + delta.count;
                    const sum = (state?.sum ?? 0) + delta.sum;
                    const min = state === undefined ? delta.min : Math.min(state.min, delta.min);
                    const max = state === undefined ? delta.max : Math.max(state.max, delta.max);
                    return { count, sum, min, max, avg: sum / count };
                },
            },
        });
    }

    async reduceIngest(msg: ShardedReducerDOIngestMessage<LatencySample>): Promise<void> {
        await this.#reducer.ingest(msg);
    }

    async reduceMerge(msg: ShardedReducerDOMergeMessage<LatencyDelta>): Promise<void> {
        await this.#reducer.merge(msg);
    }

    async reduceGet(reductionKey: string): Promise<LatencySummary | undefined> {
        return this.#reducer.get(reductionKey);
    }

    async alarm(): Promise<void> {
        await this.#reducer.alarm();
    }
}

export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext) {
        const { pathname, searchParams } = new URL(request.url);

        console.log("FETCH", pathname, searchParams.get("hint"));

        if (pathname === "/region-placer/autoinfer" || pathname === "/region-placer/auxiliary") {
            const locationHint = searchParams.get("hint") ?? "eeur";

            // console.log("LOCATION HINT", locationHint);

            // FIXME SUPER WEIRD that removing ALL the console.log below makes the tests hang.
            // Keeping at least one console.log everything works fine. Vitest setup is buggy...
            let stubToDipose: Disposable | null = null;
            try {
                if (pathname === "/region-placer/autoinfer") {
                    // console.log("AUTOINFER", locationHint);
                    stubToDipose = await env.TargetWorker.regionPlace(locationHint as DurableObjectLocationHint);
                    // console.log("AUTOINFER TARGET PLACED", locationHint);
                } else {
                    // console.log("AUXILIARY", locationHint);
                    stubToDipose = await env.RegionPlacer.place(
                        locationHint as DurableObjectLocationHint,
                        "TargetWorker",
                    );
                }
                const workerTarget = stubToDipose as unknown as ITargetWorker;
                // console.log("TARGET LOCATION", locationHint);
                const targetLocationHint = await workerTarget.targetLocationHint();
                // console.log("TARGET PINGING", locationHint);
                const result = await workerTarget.ping("boomer");
                // console.log("RESPONDING", locationHint);
                return new Response(`${result} @ ${targetLocationHint}`);
            } finally {
                // DO NOT REMOVE FOR NOW.
                console.log("DISPOSING", locationHint);
                if (stubToDipose) {
                    stubToDipose[Symbol.dispose]();
                }
            }
        }

        return new Response("-_-", { status: 404 });
    },
};

interface ITargetWorker extends RegionPlaceableTarget {
    ping(v: string): Promise<string>;
}

export class TargetWorker extends RegionPlaceableWorkerEntrypoint {
    async ping(v: string) {
        console.log("TargetWorker: hello from ping...", v);
        // console.log("ping: BEFORE timeout...");
        // await new Promise(function (resolve: (value: unknown) => void) {
        //     setTimeout(() => resolve(undefined), 5_000);
        // });
        // console.log("ping: AFTER timeout...");
        return "ping:" + v;
    }
}
