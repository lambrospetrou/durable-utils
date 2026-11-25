import { readFileSync } from "fs";
import { build } from "esbuild";

const { dependencies, peerDependencies } = JSON.parse(readFileSync("./package.json", "utf-8"));

// The package is ESM-only, because `src/internal/xxhash.ts` uses top-level await, which the CJS format does not support.
await build({
    entryPoints: [
        "src/index.ts",
        "src/sql-migrations.ts",
        "src/do-sharding.ts",
        "src/do-reducer.ts",
        "src/do-utils.ts",
        "src/retries.ts",
        "src/experimental/region-placer.ts",
    ],
    bundle: true,
    // minify: true,
    platform: "neutral",
    format: "esm",
    // Modules shared by entry points (for example the WebAssembly hasher) go into shared chunks,
    // so that an application that imports several entry points loads and instantiates them only once.
    splitting: true,
    chunkNames: "chunks/[name]-[hash]",
    outdir: "dist/",
    external: ["cloudflare:workers", ...Object.keys(dependencies ?? {}), ...Object.keys(peerDependencies ?? {})],
});

// Adapted from https://janessagarrow.com/blog/typescript-and-esbuild/
