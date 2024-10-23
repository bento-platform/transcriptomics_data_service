# transcriptomics_data_service

WORK IN PROGRESS.

The Transcriptomics Data Service (TDS) is intended to ingest, organize and query data from transcriptomics experiments through an API.

## Starting a standalone TDS

Start the TDS server with a database for testing by running the following command.
```bash
# start
docker compose up --build -d

# stop
docker compose down
```
The `--build` argument forces the image to be rebuilt. Be sure to use it if you want code changes to be present.

You can now interact with the API by querying `localhost:5000/{endpoint}`

For the OpenAPI browser interface, go to `localhost:5000/docs`.

## Local dev

For local development, you can use the [docker-compose.dev.yaml](./docker-compose.dev.yaml) file to start a TDS 
[development container](https://code.visualstudio.com/docs/devcontainers/containers) that mounts the local directory.
The server starts in reload mode to quickly reflect local changes, and debugpy is listening on the container's internal port `9511`.

```bash
# start
docker compose -f ./docker-compose.dev.yaml up --build -d

# stop
docker compose -f ./docker-compose.dev.yaml down
```

You can then attach VS Code to the `tds` container, and use the preconfigured `Python Debugger (TDS)` for interactive debugging.

## Authorization plugin

The Transcriptomics-Data-Service is meant to be a reusable microservice that can be integrated in existing 
stacks. Since authorization schemes vary across projects, TDS allows adopters to code their own authorization plugin, 
enabling adopters to leverage their existing access control tools and policies.

See the [authorization docs](./docs/authz.md) for more information on how to create and use the authz plugin with TDS.

## Endpoints

* /service-info
  * GA4GH service info
* /ingest (TODO)
