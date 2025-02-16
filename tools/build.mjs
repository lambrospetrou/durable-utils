import { readFileSync } from "fs";
import { build } from "esbuild";

const { dependencies, peerDependencies } = readFileSync("./package.json", "utf-8");

const sharedConfig = {
    entryPoints: [
        "src/index.ts",
        "src/sql-migrations.ts",
        "src/do-sharding.ts",
        "src/do-utils.ts",
        "src/retries.ts",
        "src/experimental/region-placer.ts",
    ],
    bundle: true,
    // minify: true,
    external: [
        "cloudflare:workers",
        ...Object.keys(dependencies ?? {}),
        ...Object.keys(peerDependencies ?? {}),
    ],
};

build({
    ...sharedConfig,
    platform: "node", // for CJS
    outdir: "dist/",
});

build({
    ...sharedConfig,
    outdir: "dist/",
    outExtension: {
        ".js": ".esm.js",
    },
    platform: "neutral", // for ESM
    format: "esm",
});

// Adapted from https://janessagarrow.com/blog/typescript-and-esbuild/
