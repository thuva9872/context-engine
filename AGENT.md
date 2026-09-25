# Coding Agent Guide

This file gives coding agents the repository context and working rules needed to make safe, reviewable changes to Devant Context Engine.

## Start here

Before changing code or contracts, read these files in order:

1. [README.md](README.md) for setup and verification commands.
2. [The standalone implementation plan](docs/Devant%20Context%20Engine%20Implementation%20Plan.md) for product scope and milestone order.
3. [The Milestone 0 plan](docs/Milestone%200%20Implementation%20Plan.md) and [M1 report](docs/m1/implementation-report.md) for acceptance evidence.
4. [ADR 0001](docs/decisions/0001-knowledge-backend-boundary.md) for terminology and dependency boundaries.
5. The ADRs relevant to the files being changed.
6. [The threat model](docs/m0/threat-model.md) and [M0 exit decision](docs/m0/exit-decision.md) for security gates and open risks.

Inspect the current working tree before editing and preserve user changes. Treat plans as design intent and tests plus current source as implementation evidence. If they disagree, report the mismatch and update both only when the requested change establishes a new decision.

## Current state

Milestone 1 has a runnable REST API and independent worker backed by a migrated SQLite control database. The transactional outbox, job leases, retries, idempotent record-effect ledger, structured logs, metrics, and crash-replay test are implemented. The user explicitly authorized control-plane work while the M0 live-provider security gate remains open. Do not describe the provider-backed system as release-ready until the live isolation, stale-artifact, and deletion checks pass.

## Ownership and dependency direction

| Path | Responsibility |
| --- | --- |
| `contracts/openapi/` | Provider-neutral HTTP API |
| `contracts/schemas/` | Ingestion and event schemas |
| `contracts/mcp/` | Provider-neutral MCP tool contracts |
| `engine/src/context_engine/api/` | Public REST transport and API schemas |
| `engine/src/context_engine/application/` | Commands and queries shared by external interfaces |
| `engine/src/context_engine/domain/` | Provider-neutral control-plane models |
| `engine/src/context_engine/persistence/` | SQLite migrations and durable repositories |
| `engine/src/context_engine/worker/` | Transactional-outbox dispatch and leased job execution |
| `engine/src/context_engine/observability/` | Secret-safe structured logs and metrics |
| `engine/src/context_engine/knowledge_backend/` | Engine-owned backend port and immutable types |
| `engine/src/context_engine/knowledge_backend/providers/` | Private native-provider integration and translation |
| `engine/src/context_engine/security/` | Policy decisions and authorized partition resolution |
| `engine/tests/` | Unit, adapter, contract, and live-provider tests |
| `integrations/ballerina/` | Source connectors and the shared `context_engine_client` ingestion client |
| `tests/isolation/` | Cross-audience isolation fixtures and checks |
| `docs/decisions/` | Accepted architectural decisions |
| `docs/m0/` | Milestone evidence, threats, and exit status |
| `scripts/check_provider_boundary.py` | Automated public-boundary enforcement |

The dependency flow is `REST API → application service → domain/persistence`. Client applications use the REST API only. The API and MCP packages must never import the knowledge-backend port or a provider implementation. Asynchronous provider work belongs in a worker handler, which may depend on the provider-neutral port. Only modules below `knowledge_backend/providers/` may import a native provider package or manipulate its native objects. Translate all native output, errors, and identifiers into engine-owned values before returning from an adapter.

Do not bypass the port with raw graph, vector, relational, cache, or provider calls. Do not add a broad default identity or scope. Every ingest, query, update, enrichment, and deletion must carry an explicit `PrincipalContext` and explicit access partition input resolved by engine policy.

## Public language and private concepts

Use these engine terms in APIs, domain types, application services, schemas, MCP tools, UI copy, errors, logs, and metrics:

- context space
- source
- source record
- access partition, only as a private engine implementation concept
- evidence
- enrichment
- backend reference, only as a private opaque persistence value
- ingest, query, update, enrich, and delete

Never expose the native provider product name, native identifiers, native types, native operation names, native isolation terminology, or native errors. `dataset` and `DataItem` are private provider terms. The same restriction applies to provider verbs such as `remember`, `recall`, `improve`, and `forget`.

Provider-specific ADRs, adapter implementation, adapter tests, dependency declarations, and local provider configuration are the narrow exceptions needed to maintain the integration. Keep those details out of public contracts and product-facing surfaces. Run the boundary checker after changes.

## Contract changes

Public contracts must remain provider-neutral and fail closed:

- Callers never supply or receive internal access-partition or backend identifiers.
- Mutations that affect external content or indexes return an engine job handle.
- Evidence uses engine evidence IDs and stable source lineage.
- Denied and no-evidence results reveal no inaccessible names, content, paths, or relationships.
- Source credentials authorize delivery only; they do not grant human or agent read access.
- Unknown or unmappable audiences enter quarantine instead of receiving a default scope.

When changing a contract, update the matching examples and tests. Run `scripts/check_provider_boundary.py` and add an ADR when the change establishes or reverses a material architectural decision.

## Python and dependencies

- Use Python 3.12 and uv.
- Run Python tooling from `engine/`.
- Keep versions reproducible in `engine/pyproject.toml` and `engine/uv.lock`.
- Use `uv lock` after dependency edits; never hand-edit the lock file.
- Apply migrations with `uv run context-engine-migrate`; add forward-only numbered SQL files under `engine/migrations/`.
- Keep API and worker entrypoints independently runnable.
- Give every hand-written Python file a module docstring. Document public classes, protocols, functions, and non-obvious private helpers with concise docstrings; generated files are exempt. Add inline comments for invariants, security boundaries, transaction semantics, and recovery behavior that the code alone does not make clear; do not narrate straightforward statements.
- Keep default development and test paths independent of external credentials.
- Never commit `.env`, credentials, model tokens, generated provider stores, caches, or logs.

Follow the existing immutable dataclass and protocol style unless a requested change justifies a different pattern. Prefer the smallest engine-owned interface that supports the required behavior. Preserve stable public error categories and translate native exceptions at the adapter boundary.

## Verification

For normal changes, run:

```bash
cd engine
uv run ruff format --check src tests ../tests
uv run ruff check src tests ../tests
TELEMETRY_DISABLED=1 COGNEE_LOG_FILE=false uv run pytest -m "not live_provider"
uv run context-engine-api --check
uv run context-engine-worker --check
cd ..
python3 scripts/check_provider_boundary.py
```

Run focused tests while developing, then run the complete non-live suite before finishing. If an adapter change can affect real isolation, authorization, update, evidence, cache, or deletion behavior, also run the opt-in live matrix described in [README.md](README.md). If credentials are unavailable, state that the live gate remains unverified; do not infer live safety from the dummy backend.

Update the relevant milestone report with reproducible commands and observed results. Update `docs/m0/exit-decision.md` only when every affected live-provider gate has concrete evidence.

## Completion checklist

- The implementation follows the dependency direction and fails closed on missing identity or scope.
- Public surfaces contain only engine-owned concepts.
- Contract examples and tests match any schema change.
- Relevant ADRs and milestone evidence reflect material decisions.
- Ruff, non-live pytest, and the provider-boundary check pass.
- Any required live verification is either passed and recorded or clearly reported as outstanding.
