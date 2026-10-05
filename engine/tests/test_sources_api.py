"""Source registration, staging, checkpoint, record-status, and dead-letter route tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
from test_api import (
    ADMIN,
    MEMBER,
    MIGRATIONS,
    READER,
    SERVICE,
    _auth,
    _grant,
    _ingestion,
    _principal_id,
    _register_source,
    _stage,
    _tokens,
)

from context_engine.api import create_app
from context_engine.config import Settings
from context_engine.observability import MetricsRegistry
from context_engine.persistence import (
    AuthorizationRepository,
    ControlDatabase,
    ControlPlaneRepository,
    SourceRepository,
)
from context_engine.security.authorization import Authorizer
from context_engine.security.identity import StaticTokenVerifier
from context_engine.worker import JobAuthorizer, JobWorker, LifecycleJobHandler


def _client(tmp_path, **overrides) -> TestClient:
    settings = Settings(
        database_path=tmp_path / "control.db",
        migrations_path=MIGRATIONS,
        static_tokens_path=_tokens(tmp_path),
        staging_path=tmp_path / "staging",
        **overrides,
    )
    return TestClient(create_app(settings))


async def _drain(tmp_path) -> int:
    """Run the worker on the test database until no job remains."""

    database = ControlDatabase(tmp_path / "control.db", MIGRATIONS)
    repository = ControlPlaneRepository(database)
    authorization = AuthorizationRepository(database)
    metrics = MetricsRegistry()
    worker = JobWorker(
        repository,
        LifecycleJobHandler(SourceRepository(database)),
        metrics,
        JobAuthorizer(
            Authorizer(authorization, metrics),
            authorization,
            StaticTokenVerifier.from_file(tmp_path / "tokens.json"),
        ),
        lease_seconds=5,
    )
    processed = 0
    while await worker.run_once():
        processed += 1
    return processed


def _setup(client: TestClient):
    space = client.post("/v1/spaces", headers=_auth(ADMIN), json={"name": "Ops"}).json()
    source = _register_source(client, space["id"])
    service_id = _principal_id(client, SERVICE)
    reader_id = _principal_id(client, READER)
    _grant(client, source["id"], "svc", {"principalId": service_id, "actions": ["ingest.write"]})
    _grant(client, space["id"], "rdr", {"principalId": reader_id, "actions": ["context.read"]})
    return space, source


def test_source_registration_validates_structured_mapping(tmp_path):
    with _client(tmp_path) as client:
        space = client.post("/v1/spaces", headers=_auth(ADMIN), json={"name": "Ops"}).json()
        body = {
            "name": "Accounts",
            "type": "salesforce",
            "audienceMapping": {"source-group:research": "research"},
        }

        good = client.post(
            f"/v1/spaces/{space['id']}/sources",
            headers=_auth(ADMIN),
            json={
                **body,
                "structuredMapping": {
                    "application/json": "@prefix ex: <http://example.org/> .\nex:s ex:p ex:o .",
                },
            },
        )
        assert good.status_code == 201, good.text

        unsupported = client.post(
            f"/v1/spaces/{space['id']}/sources",
            headers=_auth(ADMIN),
            json={**body, "structuredMapping": {"application/pdf": "@prefix ex: <x> ."}},
        )
        assert unsupported.status_code == 400, unsupported.text

        malformed = client.post(
            f"/v1/spaces/{space['id']}/sources",
            headers=_auth(ADMIN),
            json={
                **body,
                "structuredMapping": {"application/json": "@prefix ex: <http://e/> .\nex:s ex:p"},
            },
        )
        assert malformed.status_code == 400, malformed.text


def test_source_structured_mapping_can_be_updated(tmp_path):
    turtle_v1 = "@prefix ex: <http://example.org/> .\nex:s ex:p ex:o ."
    turtle_v2 = "@prefix ex: <http://example.org/> .\nex:a ex:b ex:c ."
    with _client(tmp_path) as client:
        space = client.post("/v1/spaces", headers=_auth(ADMIN), json={"name": "Ops"}).json()
        created = client.post(
            f"/v1/spaces/{space['id']}/sources",
            headers=_auth(ADMIN),
            json={
                "name": "Accounts",
                "type": "salesforce",
                "audienceMapping": {"source-group:research": "research"},
                "structuredMapping": {"application/json": turtle_v1},
            },
        )
        assert created.status_code == 201, created.text
        source_id = created.json()["id"]

        # Only the structured mapping changes, so the partial update must accept it alone.
        replaced = client.patch(
            f"/v1/sources/{source_id}",
            headers=_auth(ADMIN),
            json={"structuredMapping": {"application/json": turtle_v2, "text/csv": turtle_v1}},
        )
        assert replaced.status_code == 200, replaced.text

        malformed = client.patch(
            f"/v1/sources/{source_id}",
            headers=_auth(ADMIN),
            json={"structuredMapping": {"application/json": "@prefix ex: <x> .\nex:s ex:p"}},
        )
        assert malformed.status_code == 400, malformed.text

    # The stored mapping is the replacement, wholesale, and the malformed update changed nothing.
    sources = SourceRepository(ControlDatabase(tmp_path / "control.db", MIGRATIONS))
    stored = dict(sources.get_source(source_id).structured_mapping)
    assert stored == {"application/json": turtle_v2, "text/csv": turtle_v1}


def test_source_registration_visibility_and_pausing(tmp_path):
    with _client(tmp_path) as client:
        space = client.post("/v1/spaces", headers=_auth(ADMIN), json={"name": "Ops"}).json()
        source = _register_source(client, space["id"])
        hidden = client.get(f"/v1/sources/{source['id']}", headers=_auth(READER))
        hidden_list = client.get(f"/v1/spaces/{space['id']}/sources", headers=_auth(READER))
        reader_id = _principal_id(client, READER)
        _grant(client, space["id"], "rdr", {"principalId": reader_id, "actions": ["context.read"]})
        listed = client.get(f"/v1/spaces/{space['id']}/sources", headers=_auth(READER))
        reader_patch = client.patch(
            f"/v1/sources/{source['id']}", headers=_auth(READER), json={"state": "paused"}
        )
        paused = client.patch(
            f"/v1/sources/{source['id']}", headers=_auth(ADMIN), json={"state": "paused"}
        )
        service_id = _principal_id(client, SERVICE)
        _grant(
            client, source["id"], "svc", {"principalId": service_id, "actions": ["ingest.write"]}
        )
        upload_paused = client.post(
            f"/v1/sources/{source['id']}/uploads",
            headers=_auth(
                SERVICE, **{"Idempotency-Key": "up-paused-1", "Content-Type": "text/plain"}
            ),
            content=b"x",
        )
        empty_patch = client.patch(f"/v1/sources/{source['id']}", headers=_auth(ADMIN), json={})
        resumed = client.patch(
            f"/v1/sources/{source['id']}",
            headers=_auth(ADMIN),
            json={"state": "ready", "name": "Runbooks v2", "audienceMapping": {"a": "b"}},
        )

    assert source["state"] == "ready" and source["versionOrdering"] == "numeric"
    assert "audienceMapping" not in source
    assert hidden.status_code == 404 and hidden_list.status_code == 404
    assert [item["id"] for item in listed.json()] == [source["id"]]
    assert reader_patch.status_code == 403
    assert paused.json()["state"] == "paused"
    assert upload_paused.status_code == 409
    assert empty_patch.status_code == 400
    assert resumed.json()["state"] == "ready" and resumed.json()["name"] == "Runbooks v2"


def test_uploads_validate_type_size_and_replay(tmp_path):
    with _client(tmp_path, upload_max_bytes=64) as client:
        _, source = _setup(client)
        url = f"/v1/sources/{source['id']}/uploads"

        first = _stage(client, SERVICE, source["id"], b"hello", "up-same-key")
        replay = _stage(client, SERVICE, source["id"], b"hello", "up-same-key")
        conflict = client.post(
            url,
            headers=_auth(
                SERVICE, **{"Idempotency-Key": "up-same-key", "Content-Type": "text/plain"}
            ),
            content=b"different",
        )
        bad_type = client.post(
            url,
            headers=_auth(
                SERVICE,
                **{"Idempotency-Key": "up-type-1", "Content-Type": "application/x-msdownload"},
            ),
            content=b"MZ",
        )
        empty = client.post(
            url,
            headers=_auth(
                SERVICE, **{"Idempotency-Key": "up-empty-1", "Content-Type": "text/plain"}
            ),
            content=b"",
        )
        oversized = client.post(
            url,
            headers=_auth(SERVICE, **{"Idempotency-Key": "up-big-1", "Content-Type": "text/plain"}),
            content=b"x" * 65,
        )
        no_key = client.post(
            url, headers=_auth(SERVICE, **{"Content-Type": "text/plain"}), content=b"x"
        )
        unbound = client.post(
            url,
            headers=_auth(
                READER, **{"Idempotency-Key": "up-reader-1", "Content-Type": "text/plain"}
            ),
            content=b"x",
        )
        invisible = client.post(
            url,
            headers=_auth(
                MEMBER, **{"Idempotency-Key": "up-member-1", "Content-Type": "text/plain"}
            ),
            content=b"x",
        )

    assert first["contentHash"].startswith("sha256:") and first["sizeBytes"] == 5
    assert replay["uploadId"] == first["uploadId"]
    assert conflict.status_code == 409
    assert bad_type.status_code == 415 and bad_type.json()["code"] == "unsupported_content_type"
    assert empty.status_code == 400
    assert oversized.status_code == 413 and oversized.json()["code"] == "payload_too_large"
    assert no_key.status_code == 400
    assert unbound.status_code == 403
    assert invisible.status_code == 404
    assert (tmp_path / "staging" / first["uploadId"]).read_bytes() == b"hello"


def test_ingestion_rejects_mismatched_staged_content_and_bad_versions(tmp_path):
    with _client(tmp_path) as client:
        space, source = _setup(client)
        other_space = client.post("/v1/spaces", headers=_auth(ADMIN), json={"name": "Other"}).json()
        other_source = _register_source(client, other_space["id"])
        service_id = _principal_id(client, SERVICE)
        _grant(
            client,
            other_source["id"],
            "svc2",
            {"principalId": service_id, "actions": ["ingest.write"]},
        )
        upload = _stage(client, SERVICE, source["id"], b"content", "up-ok-0001")
        foreign = _stage(client, SERVICE, other_source["id"], b"content", "up-foreign-1")
        lexical = client.post(
            f"/v1/spaces/{space['id']}/sources",
            headers=_auth(ADMIN),
            json={
                "name": "Wiki",
                "type": "url",
                "audienceMapping": {},
                "versionOrdering": "lexicographic",
            },
        ).json()
        _grant(
            client, lexical["id"], "svc3", {"principalId": service_id, "actions": ["ingest.write"]}
        )
        lexical_upload = _stage(client, SERVICE, lexical["id"], b"page", "up-lex-1")

        def deliver(body: dict, key: str):
            body = {**body, "idempotencyKey": key}
            return client.post(
                "/v1/ingestions", headers=_auth(SERVICE, **{"Idempotency-Key": key}), json=body
            )

        good = _ingestion(space["id"], source["id"], upload)
        ok = deliver(good, "evt-ok-000001")
        wrong_hash = deliver({**good, "contentHash": "sha256:" + "0" * 64}, "evt-hash-00001")
        wrong_type = deliver({**good, "contentType": "application/pdf"}, "evt-type-00001")
        unknown_ref = deliver({**good, "contentRef": "stg_missing"}, "evt-ref-000001")
        foreign_ref = deliver(
            {**good, "contentRef": foreign["uploadId"], "contentHash": foreign["contentHash"]},
            "evt-foreign-001",
        )
        wrong_space = deliver({**good, "spaceId": other_space["id"]}, "evt-space-00001")
        non_numeric = deliver({**good, "sourceVersion": "v84"}, "evt-version-001")
        lexical_ok = deliver(
            {**_ingestion(space["id"], lexical["id"], lexical_upload), "sourceVersion": "v1"},
            "evt-lexical-001",
        )
        sources = SourceRepository(ControlDatabase(tmp_path / "control.db", MIGRATIONS))
        sources.expire_upload(upload["uploadId"], datetime.now(UTC) - timedelta(seconds=1))
        expired = deliver({**good, "sourceRecordId": "runbook-85"}, "evt-expired-0001")

    assert ok.status_code == 202
    for response in (wrong_hash, wrong_type, unknown_ref, foreign_ref, expired):
        assert response.status_code == 400, response.text
        assert response.json()["message"] == "Staged content does not match the ingestion event"
    assert wrong_space.status_code == 404
    assert non_numeric.status_code == 400
    assert lexical_ok.status_code == 202


async def test_checkpoints_record_status_and_dead_letters(tmp_path):
    with _client(tmp_path) as client:
        space, source = _setup(client)
        upload = _stage(client, SERVICE, source["id"], b"content", "up-ok-0001")
        body = _ingestion(space["id"], source["id"], upload)
        accepted = client.post(
            "/v1/ingestions",
            headers=_auth(SERVICE, **{"Idempotency-Key": body["idempotencyKey"]}),
            json=body,
        )
        assert accepted.status_code == 202
        put_checkpoint = client.put(
            f"/v1/sources/{source['id']}/checkpoints",
            headers=_auth(SERVICE),
            json={"cursor": "page-3"},
        )
        reader_put = client.put(
            f"/v1/sources/{source['id']}/checkpoints", headers=_auth(READER), json={"cursor": "x"}
        )
        get_checkpoint = client.get(
            f"/v1/sources/{source['id']}/checkpoints", headers=_auth(READER)
        )
        before_worker = client.get(
            f"/v1/sources/{source['id']}/records/runbook-84", headers=_auth(SERVICE)
        )

        # Move the source to another space so the queued job fails terminally as source_unknown.
        other = client.post("/v1/spaces", headers=_auth(ADMIN), json={"name": "Elsewhere"}).json()
        sources = SourceRepository(ControlDatabase(tmp_path / "control.db", MIGRATIONS))
        with sources.database.transaction() as connection:
            connection.execute(
                "UPDATE sources SET space_id = ? WHERE id = ?", (other["id"], source["id"])
            )
        assert await _drain(tmp_path) == 1
        with sources.database.transaction() as connection:
            connection.execute(
                "UPDATE sources SET space_id = ? WHERE id = ?", (space["id"], source["id"])
            )

        failed = client.get(
            f"/v1/sources/{source['id']}/jobs", params={"state": "failed"}, headers=_auth(SERVICE)
        )
        reader_jobs = client.get(f"/v1/sources/{source['id']}/jobs", headers=_auth(READER))
        admin_jobs = client.get(f"/v1/sources/{source['id']}/jobs", headers=_auth(ADMIN))

        second = _ingestion(space["id"], source["id"], upload)
        second["sourceRecordId"] = "runbook-90"
        second["idempotencyKey"] = "evt-second-0001"
        client.post(
            "/v1/ingestions",
            headers=_auth(SERVICE, **{"Idempotency-Key": "evt-second-0001"}),
            json=second,
        )
        assert await _drain(tmp_path) == 1
        status = client.get(
            f"/v1/sources/{source['id']}/records/runbook-90", headers=_auth(SERVICE)
        )
        reader_status = client.get(
            f"/v1/sources/{source['id']}/records/runbook-90", headers=_auth(READER)
        )
        member_status = client.get(
            f"/v1/sources/{source['id']}/records/runbook-90", headers=_auth(MEMBER)
        )
        unknown = client.get(f"/v1/sources/{source['id']}/records/nope", headers=_auth(SERVICE))

    assert put_checkpoint.json()["cursor"] == "page-3"
    assert reader_put.status_code == 403
    assert get_checkpoint.json()["cursor"] == "page-3"
    assert before_worker.status_code == 404
    assert [job["error"]["code"] for job in failed.json()] == ["source_unknown"]
    assert reader_jobs.status_code == 403
    assert admin_jobs.status_code == 200
    assert status.status_code == 200
    assert status.json()["state"] == "active" and status.json()["currentVersion"] == "84"
    assert status.json()["indexState"] == "pending"
    assert reader_status.status_code == 403
    assert "audience" not in status.json() and "contentRef" not in status.json()
    assert member_status.status_code == 404
    assert unknown.status_code == 404
