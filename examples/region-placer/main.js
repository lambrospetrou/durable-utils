import {
    RegionPlaceableWorkerEntrypoint,
} from "../../src/experimental/region-placer";
export { RegionPlacer, RegionPlacerDO } from "../../src/experimental/region-placer";

export default {
    async fetch(request, env, ctx) {
        const { pathname, searchParams } = new URL(request.url);

        console.log("FETCH", pathname, searchParams.get("hint"));

        if (pathname === "/region-placer/autoinfer" || pathname === "/region-placer/auxiliary") {
            const locationHint = searchParams.get("hint") ?? "eeur";

            // console.log("LOCATION HINT", locationHint);

            // FIXME SUPER WEIRD that removing ALL the console.log below makes the tests hang.
            // Keeping at least one console.log everything works fine. Vitest setup is buggy...
            let stubToDipose = null;
            try {
                if (pathname === "/region-placer/autoinfer") {
                    // console.log("AUTOINFER", locationHint);
                    stubToDipose = await env.TargetWorker.regionPlace(locationHint);
                    // console.log("AUTOINFER TARGET PLACED", locationHint);
                } else {
                    // console.log("AUXILIARY", locationHint);
                    stubToDipose = await env.RegionPlacer.place(
                        locationHint,
                        "TargetWorker",
                    );
                }
                const workerTarget = stubToDipose;
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

export class TargetWorker extends RegionPlaceableWorkerEntrypoint {
    async ping(v) {
        console.log("TargetWorker: hello from ping...", v);
        // console.log("ping: BEFORE timeout...");
        // await new Promise(function (resolve: (value: unknown) => void) {
        //     setTimeout(() => resolve(undefined), 5_000);
        // });
        // console.log("ping: AFTER timeout...");
        return "ping:" + v;
    }
}
