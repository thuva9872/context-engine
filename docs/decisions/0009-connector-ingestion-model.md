# ADR 0009: Connector ingestion model

**Status:** Accepted for the ingestion transport and envelope

**Date:** 2026-09-22

## Context

Source connectors (a file source, Salesforce, HubSpot, Slack, and databases later) must deliver source records to the engine. Most of these sources are record-oriented: an object, row, message, or contact whose meaningful payload is a modest amount of text, not a large binary. The engine already exposes an asynchronous REST ingestion boundary (`POST /v1/ingestions`) that validates an envelope and enqueues a durable job.

An earlier draft proposed a gRPC transport with client-streaming content upload and a filesystem staging store. Analysis showed the transport is never the bottleneck: ingestion only validates and enqueues, and throughput is gated by the single-writer control database and, later, by provider execution in the worker. Connectors submit a bounded trickle of records, so gRPC's streaming and framing advantages do not apply.

## Decision

Connectors submit **normalized, inline text content** over the existing REST boundary. The connector owns all source-specific extraction and normalization; the engine stays provider-neutral and only enqueues a durable job.

- The ingestion envelope carries an inline `content` string (and optional `title`), bounded in size to protect the control database.
- `contentRef` and `contentHash` remain optional. An `upsert` requires `contentType` and one of inline `content` or a staged `contentRef`.
- When both `content` and `contentHash` are present, the engine verifies that the hash describes the content and rejects a mismatch.
- A shared Ballerina client (`wso2/context_engine_client`) owns the single definition of the HTTP contract; connectors depend on it and only change how records are read and normalized. The file-source connector is the reference shape.

Raw file staging is rejected as the default: the engine does not warehouse source files. Content lives transiently in the durable job payload en route to the worker; its lasting, governed home is the knowledge backend's partition-isolated passage store, which the worker populates when provider execution lands (M4).

## Alternatives

- **gRPC with streaming upload:** rejected. The async, enqueue-bound design makes transport throughput irrelevant, and it would double the public surface (proto, codegen, servicer, tests) and duplicate idempotency, auth, tracing, and error handling already solved for REST.
- **Filesystem staging store plus `/uploads`:** deferred. Nothing downstream reads content until M4, and record-oriented connectors carry no files. A by-reference path for genuinely large binaries remains a narrow future exception via `contentRef`.
- **Message broker (Kafka/NATS/SQS) between connectors and the engine:** deferred. The engine already provides a durable transactional outbox and job queue; a broker is a larger topology decision for a later milestone.

## Consequences

- Connectors must normalize source records to text and compute a content hash for a stable record version and integrity check.
- Inline content is persisted in the control database within the job payload until the job succeeds; the size cap and later pruning bound this footprint.
- Adding a new connector reuses the shared client and envelope with no engine change.
- Provider consumption of `content` (building a source record for the knowledge backend) remains M4; the M1 worker still records only the durable ledger effect.

## Validation

- `engine/tests/test_contracts.py` (schema and content-or-reference rule)
- `engine/tests/test_api.py` (inline content accepted, staged reference accepted, missing content rejected, hash mismatch rejected)
- `scripts/check_provider_boundary.py`
- `integrations/ballerina/common` builds and publishes; `integrations/ballerina/file-source` builds against it.
