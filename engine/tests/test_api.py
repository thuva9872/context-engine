"""REST boundary and public-schema tests."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from context_engine.api import create_app
from context_engine.config import Settings

ROOT = Path(__file__).resolve().parents[2]


def _client(tmp_path) -> TestClient:
    settings = Settings(
        database_path=tmp_path / "control.db",
        migrations_path=ROOT / "engine/migrations",
    )
    return TestClient(create_app(settings))


def _ingestion(space_id: str) -> dict:
    value = json.loads((ROOT / "contracts/examples/ingestion-upsert.json").read_text())
    value["spaceId"] = space_id
    return value


def test_rest_api_is_the_public_control_plane(tmp_path):
    with _client(tmp_path) as client:
        live = client.get("/v1/health/live")
        ready = client.get("/v1/health/ready")
        created = client.post(
            "/v1/spaces", json={"name": "Incident response", "description": "Runbooks"}
        )

        assert live.status_code == 200
        assert ready.status_code == 200
        assert created.status_code == 201
        assert created.headers["x-trace-id"].startswith("trace_")
        space = created.json()
        assert space["state"] == "ready"
        assert client.get("/v1/spaces").json() == [space]
        assert client.get(f"/v1/spaces/{space['id']}").json() == space

        body = _ingestion(space["id"])
        accepted = client.post(
            "/v1/ingestions",
            headers={"Idempotency-Key": body["idempotencyKey"], "X-Trace-Id": "trace-api-1"},
            json=body,
        )
        replay = client.post(
            "/v1/ingestions",
            headers={"Idempotency-Key": body["idempotencyKey"]},
            json=body,
        )

        assert accepted.status_code == 202
        assert accepted.headers["location"] == accepted.json()["statusUrl"]
        assert replay.json()["jobId"] == accepted.json()["jobId"]
        job = client.get(accepted.json()["statusUrl"])
        assert job.status_code == 200
        assert job.json()["state"] == "queued"
        assert job.json()["traceId"] == "trace-api-1"


def _space_id(client: TestClient) -> str:
    return client.post("/v1/spaces", json={"name": "Incident response"}).json()["id"]


def test_ingestion_accepts_inline_content(tmp_path):
    with _client(tmp_path) as client:
        body = _ingestion(_space_id(client))
        accepted = client.post(
            "/v1/ingestions",
            headers={"Idempotency-Key": body["idempotencyKey"]},
            json=body,
        )
    assert accepted.status_code == 202
    assert accepted.json()["jobId"].startswith("job_")


def test_ingestion_accepts_staged_reference_without_inline_content(tmp_path):
    with _client(tmp_path) as client:
        body = _ingestion(_space_id(client))
        # A staged reference is the large-binary alternative to inline content.
        for field in ("content", "contentHash"):
            body.pop(field, None)
        body["contentRef"] = "staged-object-01J8M0"
        accepted = client.post(
            "/v1/ingestions",
            headers={"Idempotency-Key": body["idempotencyKey"]},
            json=body,
        )
    assert accepted.status_code == 202


def test_ingestion_requires_content_or_reference(tmp_path):
    with _client(tmp_path) as client:
        body = _ingestion(_space_id(client))
        for field in ("content", "contentHash", "contentRef"):
            body.pop(field, None)
        rejected = client.post(
            "/v1/ingestions",
            headers={"Idempotency-Key": body["idempotencyKey"]},
            json=body,
        )
    assert rejected.status_code == 400
    assert rejected.json()["code"] == "invalid_request"


def test_ingestion_rejects_content_hash_mismatch(tmp_path):
    with _client(tmp_path) as client:
        body = _ingestion(_space_id(client))
        body["contentHash"] = "sha256:" + "0" * 64
        rejected = client.post(
            "/v1/ingestions",
            headers={"Idempotency-Key": body["idempotencyKey"]},
            json=body,
        )
    assert rejected.status_code == 400
    assert rejected.json()["code"] == "invalid_request"


def test_api_returns_stable_safe_errors(tmp_path):
    with _client(tmp_path) as client:
        missing = client.get("/v1/spaces/does-not-exist", headers={"X-Trace-Id": "trace-404"})
        invalid = client.post("/v1/spaces", json={"name": ""})

    assert missing.status_code == 404
    assert missing.json() == {
        "code": "not_found",
        "message": "Context space not found",
        "traceId": "trace-404",
    }
    assert invalid.status_code == 400
    assert invalid.json()["code"] == "invalid_request"
    assert "input" not in invalid.text


def test_api_routes_do_not_expose_backend_objects(tmp_path):
    with _client(tmp_path) as client:
        schema = client.get("/openapi.json").json()
    rendered = json.dumps(schema).lower()
    assert "knowledge_backend" not in rendered
    assert "backendreference" not in rendered
    assert "accesspartition" not in rendered
