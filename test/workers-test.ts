import {
    DurableObjectNamespace,
    DurableObjectState,
    ExportedHandler,
    Request,
    Response,
} from "@cloudflare/workers-types";
import { WorkerEntrypoint, DurableObject } from "cloudflare:workers";

import { RegionPlaceableWorkerEntrypoint } from "../src/experimental/region-placer";
export { RegionPlacer, RegionPlacerDO } from "../src/experimental/region-placer";

export interface Env {
    SQLDO: DurableObjectNamespace;
    RegionPlacerDO: DurableObjectNamespace;
    RegionPlacer: WorkerEntrypoint;
    TargetWorker: WorkerEntrypoint;
}

export class SQLiteDO extends DurableObject {
    constructor(
        readonly ctx: DurableObjectState,
        readonly env: Env,
    ) {
        super(ctx, env);
    }

    fetch(request: Request): Response | Promise<Response> {
        throw new Error("Method not implemented.");
    }
}

export default <ExportedHandler<Env>>{
    fetch(request, env) {
        const { pathname } = new URL(request.url);
        const id = env.SQLDO.idFromName(pathname);
        const stub = env.SQLDO.get(id);
        return stub.fetch(request);
    },
};

export class TargetWorker extends RegionPlaceableWorkerEntrypoint {
    async ping(v1: string) {
        // console.log("hello from ping...", v1);
        // await new Promise(function (resolve: (value: unknown) => void) {
        //     setTimeout(() => resolve(undefined), 5_000);
        // });
        return "ping:" + v1;
    }
}
