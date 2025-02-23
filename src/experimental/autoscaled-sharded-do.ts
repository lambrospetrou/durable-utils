import { DurableObject } from "cloudflare:workers";
import { DurableObjectNamespace, Rpc } from "@cloudflare/workers-types";
import { stubByName } from "../do-utils";

///////////////////////////////////////////////////////////////////////////////
// We have 3 pieces.
//
// 1. The client library which is what users will use in their workers to address a storage shard.
//
// 2. The control plane Durable Object that manages the shards and their configuration.
//    This Durable Object is 100% owned by the library, and users just need to expose it as a binding.
//
// 3. The Durable Object Shard wrapper with the logic for the storage shards that will be integrated in the user Durable Object code.
//
// TODO Proper documentation (notes in my obsidian canvas for now).
//
///////////////////////////////////////////////

/**
 * A unique identifier for each shard. Zero-based index.
 */
export type ShardId = number;

export type TryOneOptions = {};

/**
 * The result of a single request to a shard using the `tryOne/trySome` methods.
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

export type AutoscaledShardedDOShardConfigSnapshot = {
    history: {
        timestampISO: string;
        capacity: number;
    }[];
};

export type AutoscaledShardedDOClientOptions<T extends Rpc.DurableObjectBranded | undefined> = {
    shardGroupName: string;

    shardStorageNamespace: DurableObjectNamespace<T>;
    shardConfigCacheNamespace: KVNamespace;

    shardConfigSnapshot: AutoscaledShardedDOShardConfigSnapshot;
};

export class AutoscaledShardedDOClient_EXPERIMENTAL_V0<T extends Rpc.DurableObjectBranded | undefined> {
    private options: AutoscaledShardedDOClientOptions<T>;

    constructor(options: AutoscaledShardedDOClientOptions<T>) {
        this.options = options;

        if (this.options.shardConfigSnapshot.history.length === 0) {
            throw new Error("shardConfigSnapshot must have at least one entry");
        }
        if (this.options.shardGroupName.trim() === "") {
            throw new Error("shardGroupName must not be empty");
        }
    }

    async tryOne<R>(
        partitionKey: string,
        doer: (doStub: DurableObjectStub<T>, shard: ShardId) => Promise<R>,
        options?: TryOneOptions,
    ): Promise<TryResult<R>> {
        // FIXME
        // Decide how to expose shards to users.
        // Here we need to query the shard based on the latest configuration,
        // and also potentially previus configurations if the latest one doesn't manage the partition key, yet.
        //
        // The simplest would be for each DO to return an error `NOT_FOUND` if the partition key is not managed by the shard,
        // and here we continue going to the next shard until we find one that manages the partition key.
        //
        // However, this is not very efficient if the worker is far from the Durable Objects.
        // The other approach is to have the first DO shard that receives the request to handle the redirection to the correct shard.
        //
        throw new Error("Not implemented");
    }
}

////////////////////////////////////////////////////////////////////////
// The storage shard Durable Object wrapper.
// This is the piece that users will integrate in their Durable Object code.
////////////////////////////////////////////////////////////////////////

export interface AutoscaledShardedDOStorageShardHooks_EXPERIMENTAL_V0 {
    shouldScaleOut(): Promise<{ should: boolean; reason?: string }>;
}

export type AutoscaledShardedDShardOptions = {
    /**
     * The control plane DO namespace. Used by the shard DOs to communicate with the control plane.
     */
    controlPlaneDONamespace: DurableObjectNamespace<AutoscaledShardedDOControlPlane_EXPERIMENTAL_V0>;

    /**
     * The name of the DO Namespace binding for the shard DOs, `AutoscaledShardedDOShard_EXPERIMENTAL_V0`.
     * This is passed to the control plane DO so that it can communicate with individual shard DOs if necessary.
     */
    bindingNameShardDONamespace: string;

    /**
     * The name of the Workers KV Namespace binding for the shard configuration cache.
     * This is used by the control plane DO to store the shard configuration.
     * All other DOs and Workers can read from this namespace to get the shard configuration.
     */
    bindingNameShardConfigCache: string;

    doCtx: DurableObjectState;
    hooks: AutoscaledShardedDOStorageShardHooks_EXPERIMENTAL_V0;

    scaleOutIntervalCheckMs?: number;
};

export type AutoscaledShardedDOShardRequestContext = {
    shardGroupName: string;
    shardIdx: number;
};

type ShardWrapperInstanceConfig = {
    shardGroupName: string;
    shardIdx: number;
};

export class AutoscaledShardedDOShard_EXPERIMENTAL_V0 {
    private controlPlaneDONamespace: DurableObjectNamespace<AutoscaledShardedDOControlPlane_EXPERIMENTAL_V0>;
    private bindingNameShardDONamespace: string;
    private bindingNameShardConfigCache: string;

    private doCtx: DurableObjectState;
    private shardHooks: AutoscaledShardedDOStorageShardHooks_EXPERIMENTAL_V0;

    private instanceConfig?: ShardWrapperInstanceConfig;

    constructor(options: AutoscaledShardedDShardOptions) {
        this.controlPlaneDONamespace = options.controlPlaneDONamespace;
        this.bindingNameShardDONamespace = options.bindingNameShardDONamespace;
        this.bindingNameShardConfigCache = options.bindingNameShardConfigCache;

        this.doCtx = options.doCtx;
        this.shardHooks = options.hooks;

        this.doCtx.blockConcurrencyWhile(async () => {
            this.instanceConfig = await this.doCtx.storage.get("_autoscaledShardedDO_instanceConfig");
            if (this.instanceConfig) {
                // TODO Maybe use `setTimeout` so that we control the delay between the first check and the next one.
                setInterval(() => this.periodicCapacityCheck(), options.scaleOutIntervalCheckMs ?? 10_000);
            }
        });
    }

    private async periodicCapacityCheck() {
        const shardGroupName = this.instanceConfig?.shardGroupName;
        const shardIdx = this.instanceConfig?.shardIdx;
        if (!shardGroupName || shardIdx === undefined) {
            console.warn({
                message: "AutoscaledShardedDOShard: missing shard group name or shard index",
            });
            return;
        }
        const { should, reason } = await this.shardHooks.shouldScaleOut();
        if (should) {
            // FIXME Replace this with a `StaticShardedDO` call to N autoscalers to support thousands of shards.
            const targetIdName = this.doNameAutoscaler(shardGroupName);
            const cpdo = stubByName(this.controlPlaneDONamespace, targetIdName);
            await cpdo._autoscaler_ScaleOutSignal(
                {
                    type: "autoscaler",
                    idName: targetIdName,
                    bindingNameShardDONamespace: this.bindingNameShardDONamespace,
                    bindingNameShardConfigCache: this.bindingNameShardConfigCache,
                },
                reason ?? "unspecified",
            );
        }
    }

    private doNameAutoscaler(shardGroupName: string) {
        return `${shardGroupName}::control-plane-autoscaler`;
    }
}

////////////////////////////////////////////////////////////////////////
// The control plane Durable Object.
// This is not exposed to the users, but it's a dependency of the storage shard Durable Object.
// Users should just expose the control plane Durable Object as a binding, and pass its name to the above helpers.
////////////////////////////////////////////////////////////////////////

/**
 * This should be very lean, since we want to make it straightfoward for users to use.
 */
type ControlPlaneDOEnv = {};

type ControlPlaneInstanceConfig =
    | {
          type: "coordinator";
      }
    | {
          type: "autoscaler";
      };

/**
 * First argument to every RPC call so that we don't need an `init()` step.
 */
type ControlPlaneRequestContext = {
    type: ControlPlaneInstanceConfig["type"];
    idName: string;

    bindingNameShardDONamespace: string;
    bindingNameShardConfigCache: string;
};

export class AutoscaledShardedDOControlPlane_EXPERIMENTAL_V0 extends DurableObject<ControlPlaneDOEnv> {
    private instanceConfig?: ControlPlaneInstanceConfig;

    constructor(ctx: DurableObjectState, env: ControlPlaneDOEnv) {
        super(ctx, env);

        ctx.blockConcurrencyWhile(async () => {
            this.instanceConfig = await ctx.storage.get("instanceConfig");
        });
    }

    async _autoscaler_ScaleOutSignal(rpcCtx: ControlPlaneRequestContext, reason: string) {
        // FIXME
        console.warn({
            message: "AutoscaledShardedDOControlPlane: scale out signal",
            rpcCtx,
            reason,
        });
    }
}
