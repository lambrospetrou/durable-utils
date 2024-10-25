import type { DurableObjectLocationHint, DurableObjectState } from "@cloudflare/workers-types";
// ts-ignore
import { DurableObject, WorkerEntrypoint, RpcTarget } from "cloudflare:workers";

/**
 * RegionPlacerDO is a Durable Object you have to add to your Worker bindings
 * if you want to use any of the `regionPlace(...)` functionality of this package.
 *
 * It's just used as an anchor for placing Worker invocations in specific regions.
 *
 * The Durable Object instance does NOT stay running while your Worker calls are running.
 */
export class RegionPlacerDO extends DurableObject {
    constructor(
        readonly ctx: DurableObjectState,
        readonly env: Record<string, any>,
    ) {
        super(ctx, env);
    }

    createExecutorStub(bindingName: string) {
        return this.env[bindingName].createRegionPlacedWorkerEntrypoint();
    }
}

/**
 * RegionPlacer is an auxiliary Worker class that you can export from your package,
 * and add to your bindings in order to be able to invoke any of your Worker bindings
 * at a specific Location Hint region.
 *
 * Example:
 *  `env.RegionPlacer.place("eeur", "TargetWorker").ping("boomer")`
 *
 * The above example will invoke the method `ping("boomer")` on the Worker binding `TargetWorker`
 * inside the the `eeur` region.
 */
export class RegionPlacer extends WorkerEntrypoint {
    place(locationHint: DurableObjectLocationHint, bindingName: string) {
        return createDOStub(this, locationHint).createExecutorStub(bindingName);
    }
}

/**
 * Workers can extend the RegionPlaceableWorkerEntrypoint class in order to allow
 * themselves to be called into specific Location Hints.
 * The binding name of the Worker class inheriting this class, has to be the same
 * as the binding itself, or some case-variant of it. For example, in the example
 * below we have the subclass `TargetWorker`, therefore its binding can be
 * `TARGETWORKER, TargetWorker, targetworker`.
 *
 * We do a name matching search in all the bindings of the Worker environment, and
 * will invoke the binding with the matching name as the subclass name instantiated.
 *
 * WARNING: Do no have multiple bindings with the same name but different casing,
 *          otherwise "Here be dragons" and unfortunate debugging ensues.
 *
 * Example:
 *  export class TargetWorker extends RegionPlaceableWorkerEntrypoint {
 *       async ping(v: string) { return "ping:" + v; }
 *  }
 *
 * Then, to call `ping(...)` within eeur, assuming TARGETWORKER is the binding for TargetWorker:
 *  `env.TARGETWORKER.regionPlace("eeur").ping("hello")`
 */
export class RegionPlaceableWorkerEntrypoint extends WorkerEntrypoint {
    createRegionPlacedWorkerEntrypoint() {
        return new RegionPlaceableTarget(this);
    }

    regionPlace(locationHint: DurableObjectLocationHint) {
        // This should be the name of the Binding that was specified in the worker bindings.
        // e.g. if `export class TargetWorker extends RegionPlaceableWorkerEntrypoint` thisClassName will be `TargetWorker`.
        const thisClassName = Object.getPrototypeOf(this)?.constructor?.name;
        if (!thisClassName) {
            throw new Error("could not infer the class name of the WorkerEntrypoint attempting to regionPlane()");
        }

        // We attempt to find the actual binding on the `this.env` that has the same constructor
        // as the `this`, in order to pass the binding name to the RegionPlacerDO that will create
        // the corresponding stub in the location we need.
        //
        // TODO Find a more robust way, e.g. to support underscores in the name.
        //      Make it more flexible and throw an error when multiple matches exist.
        let bindingName: string = "";
        Object.getOwnPropertyNames(this.env).forEach((value) => {
            if (value.toLowerCase() === thisClassName.toLowerCase()) {
                bindingName = value;
            }
        });
        if (!bindingName) {
            throw new Error(
                `could not find the right Binding to invoke on this.env. Inferred binding name not found is '${thisClassName}'`,
            );
        }

        return createDOStub(this, locationHint).createExecutorStub(bindingName);
    }
}

/**
 * RegionPlaceableTarget is an RpcTarget returned from the Durable Object implicitly
 * created at the location hint we want to execute our target worker.
 *
 * This RpcTarget should have all the methods of the binding Worker that is attempting
 * to be region placed.
 */
export class RegionPlaceableTarget<T> extends RpcTarget<T> {
    constructor(we: RegionPlaceableWorkerEntrypoint) {
        super();
        const targetPrototype = Object.getPrototypeOf(we);
        const thisPrototype = Object.getPrototypeOf(this);
        Object.getOwnPropertyNames(targetPrototype).forEach((value) => {
            // TODO Decide if we need the filtering just to functions.
            if (value === "constructor" || typeof targetPrototype[value] !== "function") {
                return;
            }
            thisPrototype[value] = targetPrototype[value];
        });
    }
}

//////////////// Helpers

function createDOStub(we: WorkerEntrypoint, locationHint: DurableObjectLocationHint) {
    // TODO Make this configurable.
    // Spread load across 100 DOs.
    const shard = Math.ceil(Math.random() * 100);
    const doId = we.env.RegionPlacerDO.idFromName(`region-placer-${locationHint}-${shard}`);
    const doStub = we.env.RegionPlacerDO.get(doId, {
        locationHint,
    });
    return doStub;
}
