import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import worker from "../workers-test";

// For now, you'll need to do something like this to get a correctly-typed
// `Request` to pass to `worker.fetch()`.
// const IncomingRequest = Request<unknown, IncomingRequestCfProperties>;

//////////////////////////////////////
// FIXME The following tests do not work. Need some custom vitest setup.
//
// I get the error about Promise resolving cross-request right after calling `regionPlace()` or `place()`.
// workerd/io/worker.c++:1213: warning: msg = NOSENTRY Warning: Cross Request Promise Resolve
// at <anonymous>
// at globalThis.clearTimeout (home/lambros/dev/github/durable-utils/node_modules/@cloudflare/vitest-pool-workers/dist/worker/index.mjs:224:24)
// at schedule (home/lambros/dev/github/durable-utils/node_modules/vitest/dist/chunks/console.CfT1Wjed.js:39:5)
// at Writable.write [as _write] (home/lambros/dev/github/durable-utils/node_modules/vitest/dist/chunks/console.CfT1Wjed.js:120:7)
// at writeOrBuffer (node-internal:streams_writable:366:12)
// at _write (node-internal:streams_writable:308:10)
// at Writable.write (node-internal:streams_writable:312:10)
// at Console.value (node:internal/console/constructor:30:12)
//
// If I remove all console logs from workers-test.ts, it's like nothing runs and the fetch hangs...
// Guessing something misconfigured with Vitest.
//

describe("test RPC method on returned RpcTarget to target location", () => {
    it("Using test worker - Auto infer binding from the WorkerEntrypoint used", async () => {
        const request = new Request("http://region-placer.com/region-placer/autoinfer");
        const ctx = createExecutionContext();
        const response = await worker.fetch(request, env, ctx);
        // Wait for all `Promise`s passed to `ctx.waitUntil()` to settle before running test assertions
        await waitOnExecutionContext(ctx);
        expect(await response.text()).toBe("ping:boomer @ eeur");
    });

    // it("Using test worker - RegionPlacer auxiliary worker with binding target as name string", async () => {
    //     const request = new Request("http://region-placer.com/region-placer/auxiliary?hint=wnam");
    //     const ctx = createExecutionContext();
    //     const response = await worker.fetch(request, env, ctx);
    //     // Wait for all `Promise`s passed to `ctx.waitUntil()` to settle before running test assertions
    //     await waitOnExecutionContext(ctx);
    //     expect(await response.text()).toBe("ping:boomer @ wnam");
    // });

    //////////////////////////////////////
    // it("RegionPlacer auxiliary worker with binding target as name string", async () => {
    //     console.log("running");
    //     const workerStub = env.RegionPlacer.place("eeur", "TargetWorker");
    //     console.log("got the stub");
    //     expect(await workerStub.ping("boomer")).toEqual("ping:boomer");
    //     console.log("did the assert");
    //     workerStub[Symbol.dispose]();
    // });
    // it("Auto infer binding from the 'this' Worker", async () => {
    //     // console.log("AUTO");
    //     const workerStub = await env.TargetWorker.regionPlace("eeur");
    //     // console.log("AUTO GOT STUB");
    //     expect(await workerStub.ping("boomer")).toEqual("ping:boomer");
    //     // console.log("AUTO DISPOSING");
    //     workerStub[Symbol.dispose]();
    // });
});
