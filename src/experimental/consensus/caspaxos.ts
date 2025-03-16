import { DurableObject } from "cloudflare:workers";
import { DurableObjectNamespace, Rpc } from "@cloudflare/workers-types";
import { stubByName } from "../../do-utils";

///////////////////////////////////////////////////////////////////////////
// This file implements the CASPaxos consensus algorithm.
// Read [CASPaxos: Replicated State Machines without logs](https://arxiv.org/abs/1802.07000).
//
// There are 3 roles in this algorithm:
// 1. Proposer: Proposes a value to be committed to the acceptors.
// 2. Acceptor: Accepts or rejects a proposal.
// 3. Client: Requests a value to be committed.
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

///////////////////////////////////////////////////////////////////////////
// The following code is (mostly) copied from https://github.com/gryadka/js/blob/0919b723989020eee04797c304c7d58cfa82e60e/gryadka-core/.
// I adapted the code to be TypeScript, and did some minor modifications.
//
// The Proposer class is the main class that implements the CASPaxos algorithm.
// For the role of the Acceptor, we will implement a Durable Object that can accept or reject a proposal.
//

interface BallotNumberSnapshot {
    counter: number;
    id: string;
}

export class BallotNumber {
    static zero() {
        return new BallotNumber(0, "");
    }

    static zeroSnapshot(): BallotNumberSnapshot {
        return {
            counter: 0,
            id: "",
        };
    }

    static fromSnapshot(snapshot: BallotNumberSnapshot) {
        return new BallotNumber(snapshot.counter, snapshot.id);
    }

    static parse(txt: string) {
        const [counter, id] = txt.split(",", 2);
        return new BallotNumber(parseInt(counter), id);
    }

    constructor(
        private counter: number,
        private id: string,
    ) {}

    isZero() {
        return this.counter == 0 && this.id == "";
    }

    inc() {
        this.counter++;
        return new BallotNumber(this.counter, this.id);
    }

    next() {
        return new BallotNumber(this.counter + 1, this.id);
    }

    fastforwardAfter(tick: BallotNumber) {
        this.counter = Math.max(this.counter, tick.counter) + 1;
    }

    stringify() {
        return `${this.counter},${this.id}`;
    }

    snapshot(): BallotNumberSnapshot {
        return {
            counter: this.counter,
            id: this.id,
        };
    }

    compareTo(tick: BallotNumber) {
        if (this.counter < tick.counter) {
            return -1;
        }
        if (this.counter > tick.counter) {
            return 1;
        }
        if (this.id < tick.id) {
            return -1;
        }
        if (this.id > tick.id) {
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

type PrepareResult =
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

type AcceptResult = {
    isOk: true;
    isConflict?: false;
    ballot: BallotNumber;
} | {
    isOk: false;
    isConflict: true;
    ballot: BallotNumber;
}

interface PrepareNode {
    prepare(key: string, tick: BallotNumber, extra?: any): Promise<PrepareResult>;
}

interface AcceptNode {
    accept(key: string, ballot: BallotNumber, value: any, promise: any, extra?: any): Promise<AcceptResult>;
}

interface NodeGroup<T> {
    nodes: T[];
    quorum: number;
}

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

            const promise = ballot.next();

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
            const tick = this.ballot.inc();
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
                        this.ballot.fastforwardAfter(x.ballot);
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
                    this.ballot.fastforwardAfter(x.ballot);
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
        return selector(acc).compareTo(selector(e)) < 0 ? e : acc;
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
                    if (isResolved) return;
                    all.push(value);
                    if (!cond(value)) error = true;
                } catch (e) {
                    if (isResolved) return;
                    error = true;
                }
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
    promisedBallot: BallotNumberSnapshot;
    acceptedBallot: BallotNumberSnapshot;
    acceptedValue: any;
}

export class AcceptorImpl implements PrepareNode, AcceptNode {
    private storage: DurableObjectStorage;

    constructor(storage: DurableObjectStorage) {
        this.storage = storage;
    }

    async prepare(key: string, proposedBallot: BallotNumber, extra?: any): Promise<PrepareResult> {
        console.log({ message: "AcceptorImpl.prepare:start", key, proposedBallot });

        const result = await this.storage.transaction<PrepareResult>(async (txn) => {
            let localState: KeyLocalState = (await txn.get<KeyLocalState>(`localstate:${key}`)) || {
                promisedBallot: BallotNumber.zeroSnapshot(),
                acceptedBallot: BallotNumber.zeroSnapshot(),
                acceptedValue: null,
            };
            console.log({ message: "AcceptorImpl.prepare:localState", localState });

            if (proposedBallot.compareTo(BallotNumber.fromSnapshot(localState.promisedBallot)) <= 0) {
                return {
                    isPrepared: false,
                    isConflict: true,
                    ballot: BallotNumber.fromSnapshot(localState.promisedBallot),
                };
            }

            if (proposedBallot.compareTo(BallotNumber.fromSnapshot(localState.acceptedBallot)) <= 0) {
                return {
                    isPrepared: false,
                    isConflict: true,
                    ballot: BallotNumber.fromSnapshot(localState.acceptedBallot),
                };
            }

            await txn.put(key, {
                promisedBallot: proposedBallot.snapshot(),
                acceptedBallot: localState.acceptedBallot,
                value: localState.acceptedValue,
            });

            return {
                isPrepared: true,
                ballot: BallotNumber.fromSnapshot(localState.acceptedBallot),
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
                promisedBallot: BallotNumber.zeroSnapshot(),
                acceptedBallot: BallotNumber.zeroSnapshot(),
                acceptedValue: null,
            };
            console.log({ message: "AcceptorImpl.accept:localState", localState });

            if (preparedBallot.compareTo(BallotNumber.fromSnapshot(localState.promisedBallot)) < 0) {
                return {
                    isOk: false,
                    isConflict: true,
                    ballot: BallotNumber.fromSnapshot(localState.promisedBallot),
                };
            }

            if (preparedBallot.compareTo(BallotNumber.fromSnapshot(localState.acceptedBallot)) <= 0) {
                return {
                    isOk: false,
                    isConflict: true,
                    ballot: BallotNumber.fromSnapshot(localState.acceptedBallot),
                };
            }

            await txn.put(key, {
                acceptedBallot: preparedBallot.snapshot(),
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
                promisedBallot: nextBallot.snapshot(),
            });

            return {
                isOk: true,
                isConflict: false,
                ballot: BallotNumber.fromSnapshot(localState.acceptedBallot),
            };
        });

        console.log({ message: "AcceptorImpl.accept:end", result });

        return result;
    }
}
