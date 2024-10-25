import {
    DurableObject,
    DurableObjectNamespace,
    DurableObjectState,
    ExportedHandler,
    Request,
    Response,
} from "@cloudflare/workers-types";

export interface Env {
    SQLDO: DurableObjectNamespace;
}

export class SQLiteDO implements DurableObject {
    constructor(
        readonly ctx: DurableObjectState,
        readonly env: Env,
    ) {}

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
