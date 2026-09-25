# Ballerina integrations

Source connectors that ingest records into the Context Engine over its REST
boundary (`POST /v1/ingestions`). Each connector normalizes a source record to
inline text and submits it through the shared client; the engine stays
provider-neutral and enqueues a durable job.

This is a single Ballerina package (`wso2/context_engine_connectors`): the root
module is the runtime host, and the framework and each connector are submodules
under `modules/`. One `bal build` compiles everything into one executable. See
[ADR 0009](../../docs/decisions/0009-connector-ingestion-model.md) for the
ingestion model and
[ADR 0010](../../docs/decisions/0010-connector-manager.md) for the connector
interface, manager, and scheduling.

## Manager, types, and instances

A single **runtime host** process runs any number of configured connections. It
builds one engine client, registers the connector *types* it enables, loads the
*instance* configurations from a `ConfigProvider`, and hands them to the
`ConnectorManager`:

```ballerina
manager:ConnectorManager connectorManager = new (engineClient);
connectorManager.register(salesforce:salesforceType());

manager:ConfigProvider provider = new manager:EnvConfigProvider();
check connectorManager.'start(check provider.provide());
```

(The file-upload endpoint is a host built-in, started separately — see below — not
a registered connector type.)

A connector type provides a **poll factory**, a **listen factory**, or **both**,
and the manager runs whichever are present:

- A **poll** factory builds a `PollConnector { fetch(cursor) }`. The manager
  schedules `fetch` as a recurring `ballerina/task` job at the instance's
  interval, ingests each record, and advances the instance's checkpoint. A failed
  poll is logged and retried next tick. Checkpoints go through a `CheckpointStore`
  (`InMemoryCheckpointStore` by default); pass a durable store to
  `new ConnectorManager(engineClient, store)` to resume cursors across restarts.
- A **listen** factory builds a `ListenConnector { listen(sink) }`. The manager
  calls `listen` once; the connector attaches its listeners and returns while the
  host stays alive.

A type that sets **both** (e.g. `salesforce`: a SOQL poll for creates + backfill
and a CDC listener for updates + deletes) is scheduled *and* attached from one
config entry — the manager runs both. All of it ingests through the same
`RecordSink`, which builds the envelope, hashes content, derives the idempotency
key, and submits it. Connectors only decide how records are discovered.

## Configuration

`ConfigProvider.provide()` returns the instances to run. Today `EnvConfigProvider`
decodes a JSON array of `ConnectorInstanceConfig` from an environment variable
(`CONNECTOR_CONFIGS` by default); a future database-backed provider will implement
the same interface and lease connections so exactly one node runs each.

```json
[
  {
    "instanceId": "sf-account",
    "connectorType": "salesforce",
    "destination": {
      "spaceId": "<space-id>",
      "sourceId": "source-salesforce",
      "audience": ["source-group:sales"],
      "sourceAclVersion": "1"
    },
    "settings": { "clientId": "…", "clientSecret": "…",
      "baseUrl": "https://<instance>.my.salesforce.com",
      "sobject": "Account", "fields": ["Name", "Description"] }
  }
]
```

`settings` is the type-specific bag each connector factory decodes into its own
record (credentials, object/fields, channels). See each connector's section for its
shape. (The file-upload endpoint is not configured here — it's a host built-in.)

## Layout

One package, `wso2/context_engine_connectors`:

- `Ballerina.toml`, `main.bal` — the root module: the runtime host that registers
  types, loads configs, starts the manager, and keeps the process alive.
- `modules/core/` — the connector SDK: `EngineClient`, `RecordSink`, `SourceRecord`,
  `Destination`, the `PollConnector`/`ListenConnector` shapes, and the
  `ConnectorType` registration types. This is all a connector depends on
  (imported as `core`); it has no dependency on the manager.
- `modules/manager/` — the runtime that consumes registered types: the
  `ConnectorManager` + `task`-based `PollJob`, the `CheckpointStore` seam, and the
  `ConfigProvider` (`EnvConfigProvider`). Depends on `core`; used by the host, not
  by connectors.
- `modules/salesforce/` — the Salesforce integration as **one** connector type
  (`salesforce`) that sets both factories: a SOQL **poll** (creates + backfill, by
  `CreatedDate`) and a CDC **listener** (updates + deletes). The manager runs both
  from a single config; internally they share a client and record mapping.
- `modules/file_source/` — the **file-upload endpoint**, a host built-in (not a
  managed connector): the host starts it directly and it serves
  `POST /spaces/{spaceId}/files`, ingesting into whichever space each request
  names. Files are passed through unmodified — each upload is base64-encoded with
  its content type and submitted as-is; extraction (including PDF) happens on the
  engine side via the knowledge backend, so any file type is accepted.

A single `salesforce` config drives the full sync — the poll owns creates +
backfill, the CDC listener owns updates + deletes, and on an update CDC re-fetches
the full record so its content is identical to a backfill of the same record (the
overlap deduplicates as a clean replay).

Auth uses the OAuth2 **client-credentials** flow (no refresh token, so mandatory
refresh-token rotation doesn't apply): enable **Client Credentials Flow** on the
Connected App and set a **run-as user** with Read on the object, API Enabled, and
CDC access. The token endpoint is derived from `baseUrl` (the My Domain URL). The
object must also have **Change Data Capture enabled** in Setup (the change channel
is derived from `sobject`, e.g. `Account` → `/data/AccountChangeEvent`). One
`settings` block:

```json
{
  "instanceId": "sf-account",
  "connectorType": "salesforce",
  "destination": { "spaceId": "<space-id>", "sourceId": "source-salesforce",
                   "audience": ["source-group:sales"], "sourceAclVersion": "1" },
  "settings": {
    "clientId": "…", "clientSecret": "…",
    "baseUrl": "https://<instance>.my.salesforce.com",
    "sobject": "Account", "fields": ["Name","Description"], "replayFrom": -2
  }
}
```

## Build and run

Everything builds together — no local-repository publishing:

1. Start the engine API (from `engine/`): `uv run context-engine-api` (and
   `uv run context-engine-worker` to process jobs).
2. Create a context space and note its id:
   ```bash
   curl -s -X POST http://127.0.0.1:8000/v1/spaces \
     -H 'Content-Type: application/json' -d '{"name":"Incident response"}'
   ```
3. Run the host. Managed connectors come from `CONNECTOR_CONFIGS` (empty is fine);
   the file-upload endpoint always starts (on `fileUploadPort`, default `9090`):
   ```bash
   export CONNECTOR_CONFIGS='[]'   # or a salesforce entry as shown above
   bal run
   ```
4. Upload a text file into a space (each file's name becomes the record id):
   ```bash
   echo "hello" > note.txt
   curl -F 'file=@note.txt' http://127.0.0.1:9090/spaces/<space-id>/files
   ```

## Adding a connector

Add a submodule under `modules/<name>/` that imports
`context_engine_connectors.core`, implement `PollConnector` and/or `ListenConnector`,
and expose a `ConnectorType` (name + a `pollFactory` and/or `listenFactory` — see
`salesforceType`). Import the submodule in the host's `main.bal` and
`connectorManager.register(...)` it, then reference it from configuration by its
type name. Only discovery and normalization differ between connectors.
