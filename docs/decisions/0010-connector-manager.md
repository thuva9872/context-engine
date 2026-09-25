# ADR 0010: Connector interface, manager, and scheduling

**Status:** Accepted for the Ballerina integration layer

**Date:** 2026-09-23

## Context

The engine ingests from many source types with different triggering models: a
file upload is a one-off user action, while SaaS and data sources (Salesforce,
Google Drive, databases, Slack) are configured once and then produce new or
changed records over time. Both end in the same operation — submit a normalized
record to `POST /v1/ingestions` ([ADR 0009](0009-connector-ingestion-model.md)).

An initial design gave each connector its own process: one `main()` read one set
of `configurable` values, built one client + connector, and ran a blocking poll
loop where a single failed fetch aborted the loop. That does not scale to
operating many connections. We need to run *any number* of configured
connections (many Salesforce orgs, Drive accounts, databases) without a process
per connection, to schedule polling centrally, to use event listeners for SaaS
that pushes changes (e.g. Salesforce Change Data Capture) instead of polling, and
to swap the source of configurations — hardcoded from the environment now,
database-backed with cross-node coordination later.

## Decision

A **`ConnectorManager`** (the `manager` module) holds a registry of connector
*types* and runs any number of *instances* against one engine client. A runtime
host process wires it together.

- **Modality split.** Connectors implement one or both of two narrow shapes the
  manager knows how to drive:

  ```ballerina
  public type PollConnector object {
      public function fetch(string cursor) returns FetchResult|error;
  };
  public type ListenConnector object {
      public function listen(RecordSink sink) returns error?;
  };
  ```

  The manager owns the poll loop, so a poll connector exposes a single `fetch`
  cycle rather than its own `while true`. A listen connector attaches its
  listeners and returns; the host keeps the process alive.

- **Scheduling via `ballerina/task`.** Each `POLL` instance is registered as a
  recurring `task:Job` (`scheduleJobRecurByFrequency`) at its configured
  interval, replacing the blocking `runPolling` loop of the earlier design. The
  job runs one fetch/ingest/checkpoint cycle and **logs** errors instead of
  propagating them, so one bad poll never stops the schedule.

- **Registry + factories.** A connector type registers a `ConnectorType`
  (`name`, plus a `pollFactory?` and/or `listenFactory?`). The manager runs
  whichever factories are present, so a **single type can be multi-modal**: e.g.
  `salesforce` sets both — a SOQL poll (creates + backfill) and a CDC listener
  (updates + deletes) — and one config entry is scheduled *and* attached. (An
  earlier revision modeled `ConnectorType` as a discriminated union of exactly one
  modality; that made "both" unrepresentable, so it was reverted to the two
  optional factories, with a runtime check that at least one is set.) Registration
  is explicit in the host `main` — each connector submodule contributes its
  `ConnectorType`, imported and `register`ed by the host.

- **Single package, submodules.** Everything ships as one Ballerina package,
  `wso2/context_engine_connectors`: the root module is the host, and the framework
  (the connector SDK `modules/core` and the runtime `modules/manager`), the
  `modules/salesforce` connector, and the `modules/file_source` upload endpoint are
  submodules. A connector depends only on `core` (the manager cannot be reached
  from a connector); the host depends on both. A connector type may be multi-modal
  (`salesforce` provides both a poll and a listen factory). Because
  registration is compile-time (the host must `import` and `register` each
  connector) and Ballerina has no runtime connector discovery, a connector can
  never be added without recompiling this host — so there is no external consumer
  to justify separate packages, and the host jar bundles every connector's
  dependencies regardless. One package means one `bal build` and no
  local-repository publishing. Provider SDKs (e.g. `ballerinax/salesforce`) are
  therefore package-wide dependencies; a connector whose SDK must not load
  elsewhere would be extracted to its own package at that point.

- **`ConfigProvider` abstraction.** The manager runs whatever instances a
  `ConfigProvider.provide()` returns. `EnvConfigProvider` decodes a JSON array of
  `ConnectorInstanceConfig` from an environment variable now. A future
  `DatabaseConfigProvider` implements the same interface and additionally leases
  connections so exactly one node runs each; the manager stays unaware of how
  configs are sourced or coordinated.

Every modality still ingests through the unchanged `RecordSink`, so envelope
construction, content hashing, and idempotency (ADR 0009) are shared.

## Consequences

- Running more connections is data (config entries), not new processes. Listeners
  and pollers coexist in one host.
- The `LISTEN` path fits push SaaS directly: the Salesforce connector subscribes
  to Platform Event channels and ingests each event through the sink.
- Checkpointing is behind a `CheckpointStore` (`load`/`save`) resolved per
  instance. The default `InMemoryCheckpointStore` is process-scoped; a durable
  `DatabaseCheckpointStore` implements the same seam (aligned with the engine's
  `/sources/{sourceId}/checkpoints` surface) and arrives with the database-backed
  `ConfigProvider` and its single-owner leasing. `save` returns an `error?` so a
  durable write failure is logged without stopping the schedule; ingestion
  idempotency (deterministic `sourceObservedAt` included) makes at-least-once
  persistence and replays safe.
- The host is the package's root module; connectors are submodules under
  `modules/` (`salesforce` provides the poll and listen references), imported and
  registered by the host rather than run as standalone `main()` processes.
- The **file-upload endpoint** (`modules/file_source`) is a host built-in, not a
  managed connector: a file upload is inherently multi-space (the caller names the
  space per request, `POST /spaces/{spaceId}/files`), which doesn't fit the
  manager's fixed-`destination` model. The host starts it directly with the engine
  client and server-controlled governance (source id, audience, ACL version — never
  taken from the request), and it builds a per-request `RecordSink` for the named
  space. It reuses `core` (`RecordSink`, `EngineClient`) without touching the
  connector framework.

## Validation

- `integrations/ballerina` builds as one package (`bal build`): the `core` SDK
  and `manager` runtime modules, the `salesforce` connector, the `file_source`
  upload endpoint, and the host root module.
- The host registers the `salesforce` type, loads configs from `EnvConfigProvider`,
  and starts the manager; a poll instance schedules and re-ticks without exiting.
- The built-in file-upload endpoint ingests an uploaded file into the space named
  in its path end-to-end.
