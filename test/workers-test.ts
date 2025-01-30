import { DurableObjectNamespace } from "@cloudflare/workers-types";
import { DurableObject, RpcStub } from "cloudflare:workers";

import {
    RegionPlaceableTarget,
    RegionPlaceableWorkerEntrypoint,
    RegionPlacer,
    RegionPlacerDO,
} from "../src/experimental/region-placer";
export { RegionPlacer, RegionPlacerDO } from "../src/experimental/region-placer";

export interface Env {
    SQLDO: DurableObjectNamespace<SQLiteDO>;
    RegionPlacerDO: DurableObjectNamespace<RegionPlacerDO>;

    RegionPlacer: Service<RegionPlacer>;
    TargetWorker: Service<TargetWorker>;
}

export class SQLiteDO extends DurableObject<Env> {
    constructor(
        readonly ctx: DurableObjectState,
        readonly env: Env,
    ) {
        super(ctx, env);
    }

    async actorId() {
        return String(this.ctx.id);
    }

    async echo(s: string) {
        return s;
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
