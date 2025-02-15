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

/**
 * Returns true if the given error is retryable according to the Durable Object error handling.
 * See https://developers.cloudflare.com/durable-objects/best-practices/error-handling/
 * @param err
 */
export function isErrorRetryable(err: unknown): boolean {
    return (err as any)?.retryable && !((err as any)?.overloaded);
}
