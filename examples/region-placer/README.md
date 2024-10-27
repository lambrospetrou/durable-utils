# Experimental RegionPlacer

This is WIP, not working well yet, do not use it in production.

## Pre-requisites

Build the `durable-utils` library locally to be usable by the example.

```sh
# While in the example directory, go to the repo root directory and build the libary.
cd ../../ && npm run npm:publish:dryrun
```

This will create the `npm_dist/` directory containing the npm package contents, which is referenced directly by the example's `package.json`.

## Run region-placer locally

```sh
cd examples/region-placer
npm run dev
```

Visit:

-   http://localhost:8787/region-placer/autoinfer?hint=eeur to use the auto target inference based on the binding used.
-   http://localhost:8787/region-placer/auxiliary?hint=eeur to use the explicit `RegionPlacer` WorkerEntrypoint that accepts the target worker to run by a name argument.
