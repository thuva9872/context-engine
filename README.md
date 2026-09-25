# Devant Context Engine

Devant Context Engine is a standalone service for ingesting governed source records and returning authorized, source-linked context. Its public contracts use engine-owned concepts such as context spaces, sources, evidence, enrichments, and jobs. Knowledge-provider details stay behind a private Python adapter.

The repository contains the Milestone 0 architecture spike and the Milestone 1 runnable control plane. The REST API, worker, SQLite migrations, transactional outbox, durable job state machine, retry policy, structured logging, metrics, and crash-replay tests are implemented. Live-provider isolation and deletion verification remains an open release gate recorded in [the M0 exit decision](docs/m0/exit-decision.md).

## Repository layout

```text
.
├── contracts/                 # Provider-neutral HTTP, event, and MCP contracts
├── docs/                      # Plans, ADRs, threat model, and milestone evidence
├── engine/                    # REST API, worker, control plane, private adapter, and tests
├── integrations/              # Ballerina source connectors and shared ingestion client
├── local/                     # Safe example configuration for local verification
├── scripts/                   # Repository policy and boundary checks
├── tests/                     # Cross-component contract, isolation, and recovery tests
└── ui/                        # Reserved for the operator UI
```

## Prerequisites

Install the following on a clean machine:

- Git
- Python 3.12 (the supported range is `>=3.12,<3.13`)
- [uv](https://docs.astral.sh/uv/) for dependency and virtual-environment management

Live-provider verification also requires credentials for the explicitly configured language and embedding models. The default test suite does not require external credentials or network access.

## Set up from scratch

Clone the repository and install the locked development dependencies:

```bash
git clone <repository-url> context-engine
cd context-engine/engine
uv sync --group dev
uv run context-engine-migrate
```

Run the normal local verification suite:

```bash
uv run ruff format --check src tests ../tests
uv run ruff check src tests ../tests
TELEMETRY_DISABLED=1 COGNEE_LOG_FILE=false uv run pytest -m "not live_provider"

cd ..
python3 scripts/check_provider_boundary.py
```

The expected result is a clean format and lint check, all non-live tests passing, and `Provider boundary check passed.`

## Run the local control plane

Both processes use the same durable SQLite database. From `engine/`, create a local profile and load it into each terminal:

```bash
cp ../local/m1.env.example .env
set -a
source .env
set +a
uv run context-engine-migrate
```

Start the REST API in the first terminal:

```bash
uv run context-engine-api --host 127.0.0.1 --port 8000
```

Start the worker independently in the second terminal:

```bash
uv run context-engine-worker
```

Verify the API:

```bash
curl http://127.0.0.1:8000/v1/health/live
curl http://127.0.0.1:8000/v1/health/ready
```

Interactive API documentation is available at `http://127.0.0.1:8000/docs`. The implemented M1 REST slice supports health checks, context-space creation/listing/lookup, ingestion job acceptance, and job-status lookup. Authentication and grants arrive in M2. Source connector ingestion and provider-backed execution arrive in M3 and M4.

## Run the live-provider verification

The live test is opt-in because it calls configured external models and writes local provider stores. Install the private provider extra, copy the example environment, and add your credentials:

```bash
cd engine
uv sync --group dev --extra cognee
cp ../local/m0.env.example .env
```

Edit `engine/.env` and set `LLM_API_KEY` and `EMBEDDING_API_KEY`. Keep the provider, model, storage, access-control, and safety settings explicit. The file is ignored by Git; never commit credentials.

Load the profile and run only the live matrix:

```bash
set -a
source .env
set +a
uv run pytest -m live_provider -v
```

The test skips unless `CONTEXT_ENGINE_RUN_LIVE_COGNEE=1` and `LLM_API_KEY` are present. Record verified results in [the spike report](docs/m0/spike-report.md) and update [the exit decision](docs/m0/exit-decision.md) only when the complete isolation, lifecycle, and residue checks pass.

## Development workflow

Run Python commands from `engine/` so uv selects the repository environment and the application package is importable. Add runtime dependencies to `[project.dependencies]`, optional private-provider dependencies to `[project.optional-dependencies]`, and developer tools to `[dependency-groups].dev` in `engine/pyproject.toml`. Regenerate `engine/uv.lock` with `uv lock`; do not edit it by hand.

Before opening a change, run:

```bash
cd engine
uv run ruff format --check src tests ../tests
uv run ruff check src tests ../tests
TELEMETRY_DISABLED=1 COGNEE_LOG_FILE=false uv run pytest -m "not live_provider"
cd ..
python3 scripts/check_provider_boundary.py
```

Changes to a public contract should update its examples and contract tests in the same change. Changes to the knowledge-backend port or adapter should also run the shared backend tests and, when external behavior is affected, the opt-in live suite.

## Architecture rules

- Client applications call the versioned REST API in `engine/src/context_engine/api/`. They never call `KnowledgeBackend` or a provider adapter directly.
- REST handlers use `ContextEngineService` for commands and queries. API schemas contain only public engine values.
- The worker owns asynchronous execution. A later provider-backed worker handler may use the provider-neutral knowledge-backend port; the API must not import it.
- Context spaces are public resources. Internal access partitions are resolved by policy and never accepted from or returned to callers.
- Every backend data operation receives an explicit principal and explicit authorized partition scope. Missing or ambiguous scope fails closed.
- Internal provider-backed worker code depends on the port in `engine/src/context_engine/knowledge_backend/`.
- Native provider imports, identifiers, types, operations, errors, and isolation terms stay within `engine/src/context_engine/knowledge_backend/providers/` and its provider-specific tests.
- Public APIs, schemas, events, MCP tools, errors, logs, metrics, and UI copy use engine terminology. In particular, `dataset` is a private provider term and must not become a public concept.
- Provider results are translated to immutable engine types before crossing the adapter boundary.

The full rationale and accepted terminology are in [ADR 0001](docs/decisions/0001-knowledge-backend-boundary.md).

## Documentation

- [Standalone implementation plan](docs/Devant%20Context%20Engine%20Implementation%20Plan.md)
- [Milestone 0 implementation plan](docs/Milestone%200%20Implementation%20Plan.md)
- [Architecture decisions](docs/decisions/)
- [Threat model](docs/m0/threat-model.md)
- [M0 spike report](docs/m0/spike-report.md)
- [M0 exit decision](docs/m0/exit-decision.md)
- [M1 implementation report](docs/m1/implementation-report.md)
- [Coding-agent guide](AGENT.md)
