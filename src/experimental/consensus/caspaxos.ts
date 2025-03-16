import { DurableObjectNamespace, Rpc } from "@cloudflare/workers-types";
import { stubByName } from "../../do-utils";
import { tryWhile } from "../../retries";

///////////////////////////////////////////////////////////////////////////
// This file implements the CASPaxos consensus algorithm.
// Read [CASPaxos: Replicated State Machines without logs](https://arxiv.org/abs/1802.07000).
//
// There are 3 roles in this algorithm:
// 1. Proposer: Proposes a value to be committed to the acceptors.
// 2. Acceptor: Accepts or rejects a proposal.
// 3. Client: Requests a value change.
//
// In our case with the Cloudflare Developer platform, each role can be played as follows:
// 1. Client: Any Cloudflare Worker or Durable Object (completely stateless).
// 2. Proposer: Any Cloudflare Worker or Durable Object, or one Durable Object participating in the cluster.
//              In theory, a Cloudflare Worker will be less efficient due to not using the cache optimization for 1-RTT.
//              When the proposer is a Durable Object of the acceptors cluster, it means that only one change
//              can be processed at a time for each key by that DO, which is not ideal for performance.
// 3. Acceptor: One of the Durable Objects participating in the cluster.
//
///////////////////////////////////////////////////////////////////////////

// TODO Probably move this to the general consensus module.
export interface KV {
    // Core operations
    get<T>(key: string): Promise<T | null>;
    set<T>(key: string, value: T): Promise<T>;
    update<T>(key: string, updateFn: (currentValue: T | null) => T): Promise<T>;
    delete(key: string): Promise<boolean>;

    // Advanced operations
    compareAndSet<T>(key: string, expected: T | null, newValue: T): Promise<T | null>;
}

export type Topology = {
    type: "locationHintWeighted";
    locations: Partial<Record<DurableObjectLocationHint, number>>;

    // TODO Add more types of topologies.
    // We probably need a different addressing approach to use this within the Durable Object itself
    // since we need to somehow route the requests to the `this` DO using its `this.state.id`.
};

export interface CASPaxosKVOptions {
    topology: Topology;

    clusterName: string;
}

export class CASPaxosKV<T extends Rpc.DurableObjectBranded | undefined> implements KV {
    #doNamespace: DurableObjectNamespace<T>;
    #options: CASPaxosKVOptions;
    #proposer: Proposer;

    constructor(doNamespace: DurableObjectNamespace<T>, options: CASPaxosKVOptions) {
        this.#doNamespace = doNamespace;
        this.#options = options;

        const nodes = this.#makeNodes();
        console.log({ message: "CASPaxosKV", ...nodes });
        // TODO Improve the proposer ID when used in Workers to something deterministic?
        this.#proposer = new Proposer(BallotNumberUtils.fromId(crypto.randomUUID()), nodes.prepareNodes, nodes.acceptNodes);
    }

    async get<T>(key: string): Promise<T | null> {
        return await tryWhile(
            () => this.#proposer.change<T | null>(key, (curr) => curr),
            (e: any, nextAttempt: number) => {
                if (nextAttempt > 3) {
                    return false;
                }
                if (e instanceof ProposerError) {
                    return e.isRetryable();
                }
                return false;
            },
        );
    }

    async set<T>(key: string, value: T): Promise<T> {
        return await tryWhile(
            () => this.#proposer.change<T>(key, (_) => value),
            (e: any, nextAttempt: number) => {
                if (nextAttempt > 3) {
                    return false;
                }
                if (e instanceof ProposerError) {
                    return e.isRetryable();
                }
                return false;
            },
        );
    }

    async update<T>(key: string, updateFn: (currentValue: T | null) => T): Promise<T> {
        throw new Error("Method not implemented.");
    }

    async delete(key: string): Promise<boolean> {
        throw new Error("Method not implemented.");
    }

    async compareAndSet<T>(key: string, expected: T | null, newValue: T): Promise<T | null> {
        return await tryWhile(
            () => this.#proposer.change<T | null>(key, (curr) => (curr === expected ? newValue : curr)),
            (e: any, nextAttempt: number) => {
                if (nextAttempt > 3) {
                    return false;
                }
                if (e instanceof ProposerError) {
                    return e.isRetryable();
                }
                return false;
            }
        );
    }

    #makeNodes(): { prepareNodes: NodeGroup<PrepareNode>; acceptNodes: NodeGroup<AcceptNode> } {
        const totalNodes = Object.values(this.#options.topology.locations).reduce((acc, x) => acc + x, 0);
        const quorum = Math.floor(totalNodes / 2) + 1;

        const ns = this.#doNamespace;
        const prefixName = this.#options.clusterName;

        // TODO Make this more efficient by not creating all these objects.
        let idx = 0;
        const nodes = Object.entries(this.#options.topology.locations)
            .sort((a, b) => a[0].localeCompare(b[0]))
            .map(([location, weight]) => {
                console.log({ message: "location", location, weight });
                return Array.from({ length: weight }, (_) => {
                    const nodeId = idx++;
                    return {
                        async prepare(key: string, tick: BallotNumber, extra?: any): Promise<PrepareResult> {
                            // ATTENTION :: BE VERY CAREFUL NOT TO CHANGE THE DO STUB ID CREATION.
                            const doStub = stubByName(ns, `${prefixName}-${nodeId}`, {
                                locationHint: location as DurableObjectLocationHint,
                            });
                            console.log({ message: "prepare", key, tick, extra, location, nodeId });
                            validatePrepareNode(doStub);
                            return await doStub.prepare(key, tick, extra);
                        },

                        async accept(
                            key: string,
                            ballot: BallotNumber,
                            value: any,
                            promise: BallotNumber,
                            extra?: any,
                        ): Promise<AcceptResult> {
                            const doStub = stubByName(ns, `${prefixName}-${nodeId}`, {
                                locationHint: location as DurableObjectLocationHint,
                            });
                            validateAcceptNode(doStub);
                            return await doStub.accept(key, ballot, value, promise, extra);
                        },
                    };
                });
            }).flat(1);

        console.log({ message: "nodes", nodes });

        return { prepareNodes: { nodes, quorum }, acceptNodes: { nodes, quorum } };
    }
}

function validatePrepareNode(obj: any): asserts obj is PrepareNode {
    if (!obj || typeof obj.prepare !== "function") {
        throw new Error("Object does not implement PrepareNode interface");
    }
}

function validateAcceptNode(obj: any): asserts obj is AcceptNode {
    if (!obj || typeof obj.accept !== "function") {
        throw new Error("Object does not implement AcceptNode interface");
    }
}

///////////////////////////////////////////////////////////////////////////
// The following code is (mostly) copied from https://github.com/gryadka/js/blob/0919b723989020eee04797c304c7d58cfa82e60e/gryadka-core/.
// I adapted the code to be TypeScript, and did some minor modifications.
//
// The Proposer class is the main class that implements the CASPaxos algorithm.
// For the role of the Acceptor, we will implement a Durable Object that can accept or reject a proposal.
//

export type BallotNumber = {
    counter: number;
    id: string;
};

class BallotNumberUtils {
    static zero(): BallotNumber {
        return { counter: 0, id: "" };
    }

    static fromId(id: string): BallotNumber {
        return { counter: 0, id };
    }

    static parse(txt: string): BallotNumber {
        const [counter, id] = txt.split(",", 2);
        return { counter: parseInt(counter), id };
    }

    static isZero(bn: BallotNumber): boolean {
        return bn.counter == 0 && bn.id == "";
    }

    static inc(bn: BallotNumber): BallotNumber {
        bn.counter++;
        return bn;
    }

    static next(bn: BallotNumber): BallotNumber {
        return { counter: bn.counter + 1, id: bn.id };
    }

    static fastforwardAfter(bn: BallotNumber, tick: BallotNumber): BallotNumber {
        bn.counter = Math.max(bn.counter, tick.counter) + 1;
        return bn;
    }

    static stringify(bn: BallotNumber): string {
        return `${bn.counter},${bn.id}`;
    }

    static compareTo(bn: BallotNumber, tick: BallotNumber): number {
        if (bn.counter < tick.counter) {
            return -1;
        }
        if (bn.counter > tick.counter) {
            return 1;
        }
        if (bn.id < tick.id) {
            return -1;
        }
        if (bn.id > tick.id) {
            return 1;
        }
        return 0;
    }
}

class ProposerError extends Error {
    code: string;
    err?: any;

    static ConcurrentRequestError(): ProposerError {
        return new ProposerError("ConcurrentRequestError");
    }

    static PrepareError(): ProposerError {
        return new ProposerError("PrepareError");
    }

    static CommitError(): ProposerError {
        return new ProposerError("CommitError");
    }

    static UpdateError(err: any): ProposerError {
        const error = new ProposerError("UpdateError");
        error.err = err;
        return error;
    }

    constructor(code: string, ...args: any[]) {
        super(...args);
        this.code = code;
        if ("captureStackTrace" in Error) {
            // @ts-ignore
            Error.captureStackTrace(this, ProposerError);
        }
    }

    isRetryable() {
        return this.code === "ConcurrentRequestError" || this.code === "PrepareError" || this.code === "CommitError";
    }
}

class InsufficientQuorumError<T = any> extends Error {
    all: T[];

    constructor(all: T[], ...args: any[]) {
        super(...args);
        this.all = all;
        if ("captureStackTrace" in Error) {
            // @ts-ignore
            Error.captureStackTrace(this, InsufficientQuorumError);
        }
    }
}

export type PrepareResult =
    | {
          isPrepared: true;
          isConflict?: false;
          ballot: BallotNumber;
          value: any | null;
      }
    | {
          isPrepared: false;
          isConflict: true;
          ballot: BallotNumber;
      };

export type AcceptResult =
    | {
          isOk: true;
          isConflict?: false;
          ballot: BallotNumber;
      }
    | {
          isOk: false;
          isConflict: true;
          ballot: BallotNumber;
      };

export interface PrepareNode {
    prepare(key: string, proposedBallot: BallotNumber, extra?: any): Promise<PrepareResult>;
}

export interface AcceptNode {
    accept(
        key: string,
        ballot: BallotNumber,
        value: any,
        promiseBallot: BallotNumber,
        extra?: any,
    ): Promise<AcceptResult>;
}

type NodeGroup<T> = {
    nodes: T[];
    quorum: number;
};

export class Proposer {
    private ballot: BallotNumber;
    private prepare: NodeGroup<PrepareNode>;
    private accept: NodeGroup<AcceptNode>;
    private cache: Map<string, [BallotNumber, any]>; // [ballot/promise, value]
    private locks: Set<string>;

    constructor(ballot: BallotNumber, prepare: NodeGroup<PrepareNode>, accept: NodeGroup<AcceptNode>) {
        this.ballot = ballot;
        this.prepare = prepare;
        this.accept = accept;
        this.cache = new Map();
        this.locks = new Set();
    }

    async change<T>(key: string, update: (curr: T | null) => T, extra?: any): Promise<T> {
        if (!this.tryLock(key)) {
            throw ProposerError.ConcurrentRequestError();
        }
        try {
            const [ballot, curr] = await this.guessValue<T>(key, extra);

            let next: T | null = curr;
            let error: Error | null = null;
            try {
                next = update(curr);
            } catch (e) {
                error = e as Error;
            }

            const promise = BallotNumberUtils.next(ballot);

            await this.commitValue<T>(key, ballot, next, promise, extra);

            this.cache.set(key, [promise, next]);
            if (error != null) {
                throw ProposerError.UpdateError(error);
            }

            return next as T;
        } finally {
            this.unlock(key);
        }
    }

    async guessValue<T>(key: string, extra?: any): Promise<[BallotNumber, T | null]> {
        // FIXME Do we need this cache optimization for Durable Objects?
        //       Or will it cause more round-trips since we can't route clients (Workers) to the same proposer,
        //       therefore causing more conflicts to happen and having to do extra round-trips in the end?
        //
        //       We could route clients to the same proposer DO by hashing the key and using the same DO for each key,
        //       but this diminishes the whole point of using CASPaxos with Durable Objects in the first place, since now
        //       we have a single point of failure for all keys. I want to not depend on a single DO being available, and
        //       have the system be able to handle failures and load balancing automatically.
        //
        //       It might be better to remove this and always do the prepare phase.
        //
        if (!this.cache.has(key)) {
            const tick = BallotNumberUtils.inc(this.ballot);
            let ok: PrepareResult[] | null = null;
            try {
                [ok] = await waitFor<PrepareResult>(
                    this.prepare.nodes.map((x) => x.prepare(key, tick, extra)),
                    (x) => x.isPrepared,
                    this.prepare.quorum,
                );
            } catch (e) {
                if (e instanceof InsufficientQuorumError) {
                    for (const x of (e as InsufficientQuorumError<PrepareResult>).all.filter((x) => x.isConflict)) {
                        BallotNumberUtils.fastforwardAfter(this.ballot, x.ballot);
                    }
                    throw ProposerError.PrepareError();
                } else {
                    throw e;
                }
            }
            const maxPrepared = max(ok as PrepareResult[], (x) => x.ballot);
            if (!maxPrepared.isPrepared) {
                // Should never happen, but to satisfy TypeScript.
                console.error({ message: "maxPrepared.isPrepared is false" });
                throw ProposerError.PrepareError();
            }
            const value = maxPrepared.value;
            this.cache.set(key, [tick, value]);
        }
        return this.cache.get(key)!;
    }

    async commitValue<T>(
        key: string,
        ballot: BallotNumber,
        value: T | null,
        promise: BallotNumber,
        extra?: any,
    ): Promise<void> {
        let ok: AcceptResult[] | null = null;
        let all: AcceptResult[] = [];

        try {
            [ok, all] = await waitFor<AcceptResult>(
                this.accept.nodes.map((x) => x.accept(key, ballot, value, promise, extra)),
                (x) => x.isOk,
                this.accept.quorum,
            );
        } catch (e) {
            if (e instanceof InsufficientQuorumError) {
                all = (e as InsufficientQuorumError<AcceptResult>).all;
                throw ProposerError.CommitError();
            } else {
                throw e;
            }
        } finally {
            for (const x of all.filter((x) => x.isConflict)) {
                this.cache.delete(key);
                if (x.ballot) {
                    BallotNumberUtils.fastforwardAfter(this.ballot, x.ballot);
                }
            }
        }
    }

    private tryLock(key: string): boolean {
        if (this.locks.has(key)) {
            return false;
        }
        this.locks.add(key);
        return true;
    }

    private unlock(key: string): void {
        this.locks.delete(key);
    }
}

function max<T>(iterable: T[], selector: (item: T) => BallotNumber): T {
    return iterable.reduce((acc, e) => {
        return BallotNumberUtils.compareTo(selector(acc), selector(e)) < 0 ? e : acc;
    }, iterable[0]);
}

function waitFor<T>(promises: Promise<T>[], cond: (value: T) => boolean, wantedCount: number): Promise<[T[], T[]]> {
    return new Promise((resolve, reject) => {
        const result: T[] = [];
        const all: T[] = [];
        let isResolved = false;
        let failed = 0;
        for (let promise of promises) {
            (async function () {
                let value: T | null = null;
                let error = false;
                try {
                    value = await promise;
                    console.log({ message: "waitFor:resolved", value });
                    if (isResolved) return;
                    all.push(value);
                    if (!cond(value)) error = true;
                } catch (e) {
                    console.error({ message: "waitFor:error", e });
                    if (isResolved) return;
                    error = true;
                }
                console.log({ message: "waitFor", value, error, wantedCount, failed, all });
                if (error) {
                    failed += 1;
                    if (promises.length - failed < wantedCount) {
                        isResolved = true;
                        reject(new InsufficientQuorumError<T>(all));
                    }
                } else {
                    result.push(value as T);
                    if (result.length == wantedCount) {
                        isResolved = true;
                        resolve([result, all]);
                    }
                }
            })();
        }
    });
}

///////////////////////////////////////////////////////////////////////////
// The following code is the Acceptor implementation to be used with a Durable Object.
//

interface KeyLocalState {
    promisedBallot: BallotNumber;
    acceptedBallot: BallotNumber;
    acceptedValue: any;
}

export class AcceptorClient implements PrepareNode, AcceptNode {
    private storage: DurableObjectStorage;

    constructor(storage: DurableObjectStorage) {
        this.storage = storage;
    }

    async prepare(key: string, proposedBallot: BallotNumber, extra?: any): Promise<PrepareResult> {
        console.log({ message: "AcceptorImpl.prepare:start", key, proposedBallot });

        const result = await this.storage.transaction<PrepareResult>(async (txn) => {
            let localState: KeyLocalState = (await txn.get<KeyLocalState>(`localstate:${key}`)) || {
                promisedBallot: BallotNumberUtils.zero(),
                acceptedBallot: BallotNumberUtils.zero(),
                acceptedValue: null,
            };
            console.log({ message: "AcceptorImpl.prepare:localState", localState });

            if (BallotNumberUtils.compareTo(proposedBallot, localState.promisedBallot) <= 0) {
                return {
                    isPrepared: false,
                    isConflict: true,
                    ballot: localState.promisedBallot,
                };
            }

            if (BallotNumberUtils.compareTo(proposedBallot, localState.acceptedBallot) <= 0) {
                return {
                    isPrepared: false,
                    isConflict: true,
                    ballot: localState.acceptedBallot,
                };
            }

            await txn.put(key, {
                promisedBallot: proposedBallot,
                acceptedBallot: localState.acceptedBallot,
                value: localState.acceptedValue,
            });

            return {
                isPrepared: true,
                ballot: localState.acceptedBallot,
                value: localState.acceptedValue,
            };
        });

        console.log({ message: "AcceptorImpl.prepare:end", result });

        return result;
    }

    async accept(
        key: string,
        preparedBallot: BallotNumber,
        preparedValue: any,
        nextBallot: BallotNumber,
        extra?: any,
    ): Promise<AcceptResult> {
        console.log({ message: "AcceptorImpl.accept:start", key, preparedBallot, preparedValue, nextBallot });

        const result = await this.storage.transaction<AcceptResult>(async (txn) => {
            let localState: KeyLocalState = (await txn.get<KeyLocalState>(`localstate:${key}`)) || {
                promisedBallot: BallotNumberUtils.zero(),
                acceptedBallot: BallotNumberUtils.zero(),
                acceptedValue: null,
            };
            console.log({ message: "AcceptorImpl.accept:localState", localState });

            if (BallotNumberUtils.compareTo(preparedBallot, localState.promisedBallot) < 0) {
                return {
                    isOk: false,
                    isConflict: true,
                    ballot: localState.promisedBallot,
                };
            }

            if (BallotNumberUtils.compareTo(preparedBallot, localState.acceptedBallot) <= 0) {
                return {
                    isOk: false,
                    isConflict: true,
                    ballot: localState.acceptedBallot,
                };
            }

            await txn.put(key, {
                acceptedBallot: preparedBallot,
                value: preparedValue,
                // This is a bit confusing to understand. The paper protocol says that at this point we erase the promised ballot.
                // However, in this case we already "reserve" the next ballot number for the next prepare phase.
                // This is the `One-round trip optimization` in the paper, where the same proposer will send the next write for this key.
                //
                // This is also useful to make sure that ballots always progress forward by 1 at least and avoid race conditions.
                //
                // TODO Do we need this optimization for Durable Objects?
                //      Or will it cause more round-trips since we can't route clients (Workers) to the same proposer?
                //
                promisedBallot: nextBallot,
            });

            return {
                isOk: true,
                isConflict: false,
                ballot: localState.acceptedBallot,
            };
        });

        console.log({ message: "AcceptorImpl.accept:end", result });

        return result;
    }
}
