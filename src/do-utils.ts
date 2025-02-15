import {
    DurableObjectNamespace,
    DurableObjectNamespaceGetDurableObjectOptions,
    DurableObjectStub,
    Rpc,
} from "@cloudflare/workers-types";

/**
 * Tiny utility to get a Durable Object stub by name using `idFromName`.
 * @param doNamespace 
 * @param name 
 * @param options 
 * @returns 
 */
export function stubByName<T extends Rpc.DurableObjectBranded | undefined>(
    doNamespace: DurableObjectNamespace<T>,
    name: string,
    options?: DurableObjectNamespaceGetDurableObjectOptions,
): DurableObjectStub<T> {
    const doId = doNamespace.idFromName(name);
    return doNamespace.get(doId, options);
}
