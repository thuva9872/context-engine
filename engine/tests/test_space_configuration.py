"""Space model configuration: keys never stored or returned in the clear, the embedding lock,
and the configured models reaching every backend call."""

from __future__ import annotations

import json
import sqlite3
from itertools import count
from pathlib import Path

from fastapi.testclient import TestClient
from test_api import ADMIN, MEMBER, MIGRATIONS, READER, _auth, _tokens
from test_query_api import SERVICE_ID, _Stack

from context_engine.api import create_app
from context_engine.api.schemas import SpaceConfigurationRequest
from context_engine.application import ExtractiveAnswerGenerator
from context_engine.application.space_models import SpaceModelResolver
from context_engine.config import Settings
from context_engine.domain import JobOperation
from context_engine.knowledge_backend import DummyKnowledgeBackend
from context_engine.observability import MetricsRegistry
from context_engine.persistence import (
    AuthorizationRepository,
    ControlDatabase,
    ControlPlaneRepository,
    SourceRepository,
    StagingStore,
)
from context_engine.security.authorization import Authorizer
from context_engine.security.identity import StaticTokenVerifier
from context_engine.security.secrets import SecretVault, build_secret_store
from context_engine.worker import (
    EnrichmentJobHandler,
    JobAuthorizer,
    JobWorker,
    LifecycleJobHandler,
    OperationDispatcher,
    RecordIndexer,
)

ROOT = Path(__file__).resolve().parents[2]
SECRETS_KEY = SecretVault.generate_setting()
EMBEDDING = {
    "provider": "openai",
    "model": "text-embedding-3-small",
    "apiKey": "sk-embedding-literal",
    "dimensions": 1536,
}
LANGUAGE = {"provider": "anthropic", "model": "claude-sonnet-5", "apiKey": "sk-language-literal"}


class RecordingBackend(DummyKnowledgeBackend):
    """The deterministic backend, remembering which models each call carried."""

    def __init__(self):
        super().__init__()
        self.seen = []

    async def ingest(self, record, principal, partition, models=None, structured=None):
        self.seen.append(("ingest", models))
        return await super().ingest(record, principal, partition, models=models)

    async def update(self, record, principal, partition, models=None, structured=None):
        self.seen.append(("update", models))
        return await super().update(record, principal, partition, models=models)

    async def query(self, request, principal, authorized_partitions, models=None):
        self.seen.append(("query", models))
        return await super().query(request, principal, authorized_partitions, models=models)

    async def enrich(self, request, principal, authorized_partitions, models=None):
        self.seen.append(("enrich", models))
        return await super().enrich(request, principal, authorized_partitions, models=models)


class _ConfiguredStack(_Stack):
    """The query stack with a secrets key and a worker that resolves each space's models."""

    def __init__(self, tmp_path, *, secrets_key=SECRETS_KEY, provider=True, answers=None):
        self.tmp_path = tmp_path
        self.backend = RecordingBackend()
        self.settings = Settings(
            database_path=tmp_path / "control.db",
            migrations_path=MIGRATIONS,
            static_tokens_path=_tokens(tmp_path),
            staging_path=tmp_path / "staging",
            knowledge_backend="provider" if provider else "none",
            secrets_key=secrets_key,
        )
        self.client = TestClient(
            create_app(
                self.settings,
                knowledge_backend=self.backend if provider else None,
                answer_generator=answers or ExtractiveAnswerGenerator(),
            )
        )
        self.sequence = count()

    def worker(self):
        database = ControlDatabase(self.tmp_path / "control.db", MIGRATIONS)
        sources = SourceRepository(database)
        authorization = AuthorizationRepository(database)
        repository = ControlPlaneRepository(database)
        metrics = MetricsRegistry()
        staging = StagingStore(self.tmp_path / "staging")
        models_for = SpaceModelResolver(
            repository, build_secret_store(self.settings.secrets_key, "", "")
        ).resolve
        indexer = RecordIndexer(
            sources, self.backend, staging, SERVICE_ID, metrics, models_for=models_for
        )
        lifecycle = LifecycleJobHandler(sources, indexer=indexer, staging=staging)
        return JobWorker(
            repository,
            OperationDispatcher(
                {
                    JobOperation.INGESTION: lifecycle,
                    JobOperation.UPDATE: lifecycle,
                    JobOperation.DELETION: lifecycle,
                    JobOperation.ENRICHMENT: EnrichmentJobHandler(
                        sources, self.backend, SERVICE_ID, metrics, models_for=models_for
                    ),
                }
            ),
            metrics,
            JobAuthorizer(
                Authorizer(authorization, metrics),
                authorization,
                StaticTokenVerifier.from_file(self.tmp_path / "tokens.json"),
            ),
            lease_seconds=5,
        )

    def put(self, space, body, token=ADMIN):
        return self.client.put(
            f"/v1/spaces/{space['id']}/configuration", headers=_auth(token), json=body
        )

    def get(self, space, token=ADMIN):
        return self.client.get(f"/v1/spaces/{space['id']}/configuration", headers=_auth(token))


def test_keys_are_encrypted_at_rest_and_never_returned(tmp_path):
    stack = _ConfiguredStack(tmp_path)
    with stack.client:
        space, _ = stack.setup()
        before = stack.get(space)
        stored = stack.put(space, {"embedding": EMBEDDING, "llm": LANGUAGE})
        read = stack.get(space)
        again = stack.put(space, {"embedding": EMBEDDING, "llm": LANGUAGE})

    assert before.status_code == 200
    assert before.json()["embeddingLocked"] is False and "embedding" not in before.json()
    assert stored.status_code == 200, stored.text
    body = stored.json()
    assert body["embedding"]["keyKind"] == "encrypted" and body["llm"]["keyKind"] == "encrypted"
    assert body["embedding"]["dimensions"] == 1536 and body["version"] == 1
    assert read.json()["llm"]["model"] == "claude-sonnet-5" and again.json()["version"] == 2
    for response in (stored, read, again):
        assert "sk-" not in response.text and "apiKey" not in response.text
    # The control database holds no key in the clear anywhere in its bytes.
    raw = (tmp_path / "control.db").read_bytes()
    assert b"sk-embedding-literal" not in raw and b"sk-language-literal" not in raw
    assert {name for name in before.json()["storage"]} == {"vector", "graph", "relational"}


def test_literal_keys_need_the_secrets_key_but_references_do_not(tmp_path, monkeypatch):
    stack = _ConfiguredStack(tmp_path, secrets_key="")
    monkeypatch.setenv("CONTEXT_ENGINE_TEST_EMBED_KEY", "from-env")
    with stack.client:
        space, _ = stack.setup()
        literal = stack.put(space, {"embedding": EMBEDDING})
        reference = stack.put(
            space,
            {
                "embedding": {
                    **EMBEDDING,
                    "apiKey": None,
                    "apiKeyRef": "env:CONTEXT_ENGINE_TEST_EMBED_KEY",
                }
            },
        )
        control_plane = stack.put(
            space, {"embedding": {**EMBEDDING, "apiKey": None, "apiKeyRef": "cp:secret-01"}}
        )

    assert literal.status_code == 503 and literal.json()["code"] == "unavailable"
    assert reference.status_code == 200 and reference.json()["embedding"]["keyKind"] == "reference"
    # No control plane is configured, so its references cannot be resolved and are refused.
    assert control_plane.status_code == 400


def test_key_rules_and_the_control_plane_payload_shape(tmp_path):
    stack = _ConfiguredStack(tmp_path)
    with stack.client:
        space, _ = stack.setup()
        both = stack.put(space, {"llm": {**LANGUAGE, "apiKeyRef": "env:X"}})
        keyless_new = stack.put(
            space, {"llm": {"provider": "anthropic", "model": "claude-sonnet-5"}}
        )
        nothing = stack.put(space, {})
        first = stack.put(space, {"llm": LANGUAGE})
        # Resending what GET returned, without the key, keeps the stored key.
        resent = stack.put(space, {"llm": {"provider": "anthropic", "model": "claude-sonnet-5"}})
        changed_keyless = stack.put(
            space, {"llm": {"provider": "anthropic", "model": "claude-opus-5-5"}}
        )
        # The control plane sends storage and sources in the same request, and an empty key
        # for a model whose key is shared with the other.
        shaped = stack.put(
            space,
            {
                "sources": [
                    {"name": "Runbooks", "type": "file", "settings": {}, "credentials": {}}
                ],
                "embedding": EMBEDDING,
                "llm": {**LANGUAGE, "apiKey": ""},
                "storage": {"vector": {"provider": "managed"}},
            },
        )

    assert both.status_code == 400 and keyless_new.status_code == 400
    assert nothing.status_code == 400
    assert first.status_code == 200 and first.json()["version"] == 1
    assert resent.status_code == 200 and resent.json()["llm"]["keyKind"] == "encrypted"
    assert resent.json()["version"] == 2
    assert changed_keyless.status_code == 400
    # An empty key with an unchanged model keeps the key; the rest is stored.
    assert shaped.status_code == 200 and shaped.json()["embedding"]["keyKind"] == "encrypted"


def test_configuration_needs_management_rights(tmp_path):
    stack = _ConfiguredStack(tmp_path)
    with stack.client:
        space, _ = stack.setup()
        reader_put = stack.put(space, {"llm": LANGUAGE}, token=READER)
        reader_get = stack.get(space, token=READER)
        member_get = stack.get(space, token=MEMBER)
        missing = stack.client.get("/v1/spaces/spc_missing/configuration", headers=_auth(ADMIN))
        unauthenticated = stack.client.get(f"/v1/spaces/{space['id']}/configuration")

    assert reader_put.status_code == 403 and reader_get.status_code == 403
    assert member_get.status_code == 403
    assert missing.status_code == 404 and unauthenticated.status_code == 401


async def test_the_embedding_model_locks_once_content_is_indexed(tmp_path):
    stack = _ConfiguredStack(tmp_path)
    with stack.client:
        space, source = stack.setup()
        assert stack.put(space, {"embedding": EMBEDDING, "llm": LANGUAGE}).status_code == 200
        # Before anything is indexed the embedding may still change.
        other = {**EMBEDDING, "model": "text-embedding-3-large", "dimensions": 3072}
        assert stack.put(space, {"embedding": other, "llm": LANGUAGE}).status_code == 200
        stack.deliver(space, source, "runbook-1", "1", content=b"Confirm the rollback checkpoint.")
        await stack.drain()
        locked = stack.get(space)
        changed = stack.put(space, {"embedding": EMBEDDING, "llm": LANGUAGE})
        removed = stack.put(space, {"llm": LANGUAGE})
        rekeyed = stack.put(
            space, {"embedding": {**other, "apiKey": "sk-new-embedding-key"}, "llm": LANGUAGE}
        )
        new_language = stack.put(
            space,
            {
                "embedding": {k: v for k, v in other.items() if k != "apiKey"},
                "llm": {**LANGUAGE, "model": "claude-opus-5-5"},
            },
        )

    assert locked.json()["embeddingLocked"] is True
    assert changed.status_code == 409 and removed.status_code == 409
    assert rekeyed.status_code == 200
    assert (
        new_language.status_code == 200 and new_language.json()["llm"]["model"] == "claude-opus-5-5"
    )


async def test_configured_models_reach_every_backend_call(tmp_path, monkeypatch):
    monkeypatch.setenv("CONTEXT_ENGINE_TEST_LLM_KEY", "sk-resolved-language")
    stack = _ConfiguredStack(tmp_path)
    with stack.client:
        space, source = stack.setup()
        stack.put(
            space,
            {
                "embedding": EMBEDDING,
                "llm": {**LANGUAGE, "apiKey": None, "apiKeyRef": "env:CONTEXT_ENGINE_TEST_LLM_KEY"},
            },
        )
        stack.deliver(space, source, "runbook-1", "1", content=b"Confirm the rollback checkpoint.")
        await stack.drain()
        query = stack.query(MEMBER, space, "rollback checkpoint")
        stack.client.put(
            f"/v1/resources/{space['id']}/grants/enrich",
            headers=_auth(ADMIN),
            json={"group": "readers", "actions": ["context.enrich"]},
        )
        stack.client.post(
            f"/v1/spaces/{space['id']}/enrichments",
            headers=_auth(MEMBER, **{"Idempotency-Key": "enrich-000001"}),
        )
        await stack.drain()

    assert query.status_code == 200 and query.json()["state"] == "completed"
    operations = {name: models for name, models in stack.backend.seen}
    assert {"ingest", "query", "enrich"} <= set(operations)
    for name, models in stack.backend.seen:
        assert models is not None, name
        assert models.language_model.provider == "anthropic"
        assert models.language_model.model == "claude-sonnet-5"
        assert models.language_model.api_key == "sk-resolved-language"
        assert models.embedding_model.api_key == "sk-embedding-literal"
        assert models.embedding_model.dimensions == 1536
        # Keys never appear in the string form that could reach a log or an error.
        assert "sk-" not in repr(models)
    # The stored query names the space's embedding model that served it, never its key.
    stored = json.loads(
        sqlite3.connect(tmp_path / "control.db")
        .execute("SELECT models_json FROM queries WHERE id = ?", (query.json()["queryId"],))
        .fetchone()[0]
    )
    assert stored == {
        "embedding": {
            "provider": "openai",
            "model": "text-embedding-3-small",
            "configuredBy": "space",
        }
    }


class _ModelSpy(ExtractiveAnswerGenerator):
    """The extractive generator, keeping the models each answer was written with."""

    def __init__(self):
        super().__init__()
        self.models = []

    async def write(self, request, models=None):
        self.models.append(models)
        return await super().write(request, models)


async def test_answers_use_the_space_language_model(tmp_path, monkeypatch):
    monkeypatch.setenv("CONTEXT_ENGINE_TEST_LLM_KEY", "sk-resolved-language")
    spy = _ModelSpy()
    stack = _ConfiguredStack(tmp_path, answers=spy)
    with stack.client:
        space, source = stack.setup()
        stack.put(
            space,
            {"llm": {**LANGUAGE, "apiKey": None, "apiKeyRef": "env:CONTEXT_ENGINE_TEST_LLM_KEY"}},
        )
        stack.deliver(space, source, "runbook-1", "1", content=b"Confirm the rollback checkpoint.")
        await stack.drain()
        response = stack.query(MEMBER, space, "rollback checkpoint", mode="answer")

    assert response.status_code == 200 and response.json()["answer"], response.text
    [models] = spy.models
    assert models.language_model.model == "claude-sonnet-5"
    assert models.language_model.api_key == "sk-resolved-language"
    stored = json.loads(
        sqlite3.connect(tmp_path / "control.db")
        .execute("SELECT models_json FROM queries")
        .fetchone()[0]
    )
    assert stored["language"] == {
        "provider": "anthropic",
        "model": "claude-sonnet-5",
        "configuredBy": "space",
    }
    assert "sk-" not in json.dumps(stored)


async def test_unconfigured_spaces_use_the_backend_default(tmp_path):
    stack = _ConfiguredStack(tmp_path)
    with stack.client:
        space, source = stack.setup()
        stack.deliver(space, source, "runbook-1", "1", content=b"Confirm the rollback checkpoint.")
        await stack.drain()
        query = stack.query(MEMBER, space, "rollback checkpoint")

    assert query.status_code == 200
    assert stack.backend.seen and all(models is None for _, models in stack.backend.seen)


async def test_an_unavailable_key_fails_the_query_without_detail(tmp_path, monkeypatch):
    stack = _ConfiguredStack(tmp_path)
    monkeypatch.setenv("CONTEXT_ENGINE_TEST_TEMP_KEY", "present-for-now")
    with stack.client:
        space, source = stack.setup()
        stack.put(
            space,
            {"llm": {**LANGUAGE, "apiKey": None, "apiKeyRef": "env:CONTEXT_ENGINE_TEST_TEMP_KEY"}},
        )
        stack.deliver(space, source, "runbook-1", "1", content=b"Confirm the rollback checkpoint.")
        await stack.drain()
        monkeypatch.delenv("CONTEXT_ENGINE_TEST_TEMP_KEY")
        query = stack.query(MEMBER, space, "rollback checkpoint")

    assert query.status_code == 503 and query.json()["code"] == "unavailable"
    assert "CONTEXT_ENGINE_TEST_TEMP_KEY" not in query.text


def test_storage_placement_reflects_the_backend_mode(tmp_path):
    with _ConfiguredStack(tmp_path, provider=False).client as client:
        space = client.post("/v1/spaces", headers=_auth(ADMIN), json={"name": "Ledger only"}).json()
        placement = client.get(f"/v1/spaces/{space['id']}/configuration", headers=_auth(ADMIN))
    assert placement.json()["storage"] == {
        "vector": {"provider": "none"},
        "graph": {"provider": "none"},
        "relational": {"provider": "none"},
    }


def test_contract_example_is_a_valid_request():
    example = json.loads((ROOT / "contracts/examples/space-configuration.json").read_text())
    request = SpaceConfigurationRequest.model_validate(example)
    assert request.llm is not None and request.llm.to_input().api_key_ref == "cp:secret-01J8M0LLM"
    assert request.embedding is not None and request.embedding.to_input().dimensions == 1536
