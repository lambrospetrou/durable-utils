import {
    RegionPlaceableWorkerEntrypoint,
} from "durable-utils/experimental/region-placer.esm";
export { RegionPlacer, RegionPlacerDO } from "durable-utils/experimental/region-placer.esm";

export default {
    async fetch(request, env, ctx) {
        const { pathname, searchParams, host } = new URL(request.url);

        console.log({ message: "FETCH", pathname, hint: searchParams.get("hint") });

        if (pathname === "/region-placer/autoinfer" || pathname === "/region-placer/auxiliary") {
            const locationHint = searchParams.get("hint") ?? "eeur";

            // console.log("LOCATION HINT", locationHint);

            let cdnInfo = "";
            try {
                if (host.startsWith("localhost")) {
                    cdnInfo = host;
                } else {
                    cdnInfo = (await fetch(`https://${host}/cdn-cgi/trace`)).text();
                }
            } catch (e) {
                console.error({ message: "could not fetch CDN info from /cdn-cgi/trace", error: e })
            }

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
                const result = await workerTarget.ping("pong");
                // console.log("RESPONDING", locationHint);
                return new Response(`${result} @ ${targetLocationHint}\n${cdnInfo}`);
            } finally {
                // DO NOT REMOVE FOR NOW.
                console.log({ message: "DISPOSING", locationHint });
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
        console.log({ message: "TargetWorker: hello from ping...", v });
        // console.log("ping: BEFORE timeout...");
        // await new Promise(function (resolve: (value: unknown) => void) {
        //     setTimeout(() => resolve(undefined), 5_000);
        // });
        // console.log("ping: AFTER timeout...");
        return "ping:" + v;
    }
}
