"""Provider convergence: writes, replacements, moves, removals, crashes, and residue."""

from __future__ import annotations

import hashlib
from itertools import count
from pathlib import Path

import pytest

from context_engine.domain import IndexState, JobOperation, LocationState, VersionOrdering
from context_engine.knowledge_backend import (
    BackendError,
    BackendErrorCode,
    DeletionResult,
    DummyKnowledgeBackend,
    PrincipalContext,
    QueryRequest,
)
from context_engine.observability import MetricsRegistry
from context_engine.persistence import (
    ControlDatabase,
    ControlPlaneRepository,
    SourceRepository,
    StagingStore,
)
from context_engine.security.partitions import partition_for
from context_engine.worker import (
    InjectedWorkerCrash,
    LifecycleJobHandler,
    OperationDispatcher,
    RecordIndexer,
    TerminalJobError,
)

MIGRATIONS = Path(__file__).resolve().parents[1] / "migrations"
SERVICE_ID = "context-engine-service"
READER = PrincipalContext("prn_reader", "trace-read")
OPERATIONS = {
    "upsert": JobOperation.INGESTION,
    "acl_changed": JobOperation.UPDATE,
    "delete": JobOperation.DELETION,
}


class _Harness:
    def __init__(self, tmp_path, backend=None, *, indexed=True):
        database = ControlDatabase(tmp_path / "control.db", MIGRATIONS)
        database.migrate()
        self.repository = ControlPlaneRepository(database)
        self.sources = SourceRepository(database)
        self.staging = StagingStore(tmp_path / "staging")
        self.space = self.repository.create_space("Ops", None)
        self.source = self.sources.create_source(
            self.space.id, "Files", "file", VersionOrdering.NUMERIC, {"t": "team", "u": "ops"}
        )
        self.backend = backend or DummyKnowledgeBackend()
        self.bound: list[str] = []
        self.sequence = count()
        indexer = None
        if indexed:
            indexer = RecordIndexer(
                self.sources,
                self.backend,
                self.staging,
                SERVICE_ID,
                MetricsRegistry(),
                on_partition_bound=self._bound,
            )
        self.handler = LifecycleJobHandler(self.sources, indexer=indexer, staging=self.staging)

    async def _bound(self, partition_id, trace_id):
        self.bound.append(partition_id)

    def partition(self, *audiences):
        return partition_for(self.space.id, audiences)

    def job(
        self,
        record_id,
        version,
        operation="upsert",
        *,
        content=b"",
        content_type="text/plain",
        audience=("t",),
        acl_version="1",
    ):
        number = next(self.sequence)
        key = f"k-{number:06d}"
        payload = {
            "schemaVersion": "1",
            "spaceId": self.space.id,
            "sourceId": self.source.id,
            "sourceRecordId": record_id,
            "sourceVersion": version,
            "operation": operation,
            "sourceObservedAt": "2026-09-24T10:00:00Z",
            "audience": list(audience),
            "sourceAclVersion": acl_version,
            "idempotencyKey": key,
        }
        if operation == "upsert":
            digest = "sha256:" + hashlib.sha256(content).hexdigest()
            upload, _ = self.sources.create_upload(
                self.source.id,
                "prn_c",
                f"up-{number:06d}",
                content_type,
                len(content),
                digest,
                3600,
            )
            self.staging.write(upload.id, content)
            payload.update(contentRef=upload.id, contentHash=digest, contentType=content_type)
        job, _ = self.repository.enqueue_job(OPERATIONS[operation], key, payload, key, 3, "prn_c")
        return job

    async def deliver(self, *args, **kwargs):
        return await self.handler.handle(self.job(*args, **kwargs))

    def record(self, record_id):
        return self.sources.get_record(self.space.id, self.source.id, record_id)

    def locations(self, record_id):
        return self.sources.list_locations(self.space.id, self.source.id, record_id)

    async def found(self, text, *audiences):
        result = await self.backend.query(QueryRequest(text), READER, (self.partition(*audiences),))
        return [(item.record_id, item.source_version) for item in result.evidence]


async def test_upsert_indexes_the_current_version_as_the_service_identity(tmp_path):
    spy = _SpyBackend()
    harness = _Harness(tmp_path, spy)

    result = await harness.deliver("doc-a", "1", content=b"alpha gateway")

    [location] = harness.locations("doc-a")
    assert result["indexState"] == "indexed"
    assert harness.record("doc-a").index_state is IndexState.INDEXED
    assert (location.state, location.version, location.parser_version) == (
        LocationState.INDEXED,
        "1",
        "plain@1",
    )
    assert location.partition_id == harness.partition("team").value
    assert await harness.found("gateway", "team") == [("doc-a", "1")]
    assert harness.bound == [harness.partition("team").value]
    assert set(spy.principals) == {SERVICE_ID}


async def test_new_version_replaces_the_old_one_and_releases_superseded_bytes(tmp_path):
    harness = _Harness(tmp_path)
    await harness.deliver("doc-a", "1", content=b"oldword text")
    first_upload = harness.record("doc-a").content_ref
    await harness.deliver("doc-a", "2", content=b"newword text")

    [location] = harness.locations("doc-a")
    assert location.version == "2"
    assert await harness.found("newword", "team") == [("doc-a", "2")]
    assert await harness.found("oldword", "team") == []
    assert not harness.staging.exists(first_upload)
    assert harness.staging.exists(harness.record("doc-a").content_ref)


async def test_identical_content_under_a_new_version_is_not_rewritten(tmp_path):
    spy = _SpyBackend()
    harness = _Harness(tmp_path, spy)

    await harness.deliver("doc-a", "1", content=b"same bytes")
    await harness.deliver("doc-a", "2", content=b"same bytes")

    assert spy.writes == ["ingest"]
    assert harness.locations("doc-a")[0].version == "2"
    # The backend still holds the first version's content and metadata.
    assert harness.locations("doc-a")[0].written_version == "1"
    assert harness.record("doc-a").index_state is IndexState.INDEXED


async def test_acl_change_moves_the_record_to_its_new_partition(tmp_path):
    harness = _Harness(tmp_path)
    await harness.deliver("doc-a", "1", content=b"gateway runbook")

    await harness.deliver("doc-a", "1", "acl_changed", audience=("t", "u"), acl_version="2")

    [location] = harness.locations("doc-a")
    assert location.partition_id == harness.partition("ops", "team").value
    assert await harness.found("gateway", "ops", "team") == [("doc-a", "1")]
    assert await harness.found("gateway", "team") == []
    assert harness.bound == [
        harness.partition("team").value,
        harness.partition("ops", "team").value,
    ]


async def test_delete_removes_the_copy_and_its_staged_bytes(tmp_path):
    harness = _Harness(tmp_path)
    await harness.deliver("doc-a", "1", content=b"gateway runbook")
    upload = harness.record("doc-a").content_ref

    result = await harness.deliver("doc-a", "2", "delete")

    assert result["indexState"] == "not_indexed"
    assert harness.locations("doc-a") == ()
    assert await harness.found("gateway", "team") == []
    assert not harness.staging.exists(upload)


async def test_quarantine_after_indexing_removes_the_copy(tmp_path):
    harness = _Harness(tmp_path)
    await harness.deliver("doc-a", "1", content=b"gateway runbook")

    await harness.deliver("doc-a", "1", "acl_changed", audience=("x",), acl_version="2")

    assert harness.record("doc-a").index_state is IndexState.NOT_INDEXED
    assert harness.locations("doc-a") == ()
    assert await harness.found("gateway", "team") == []
    # A quarantined record may be released later, so its current bytes stay.
    assert harness.staging.exists(harness.record("doc-a").content_ref)


async def test_unindexable_version_fails_and_leaves_nothing_older_searchable(tmp_path):
    harness = _Harness(tmp_path)
    await harness.deliver("doc-a", "1", content=b"oldword text")

    with pytest.raises(TerminalJobError) as error:
        await harness.deliver("doc-a", "2", content=b"%PDF-1.7", content_type="application/pdf")

    record = harness.record("doc-a")
    assert error.value.code == "extraction_unsupported"
    assert (record.index_state, record.index_error) == (IndexState.FAILED, "extraction_unsupported")
    assert harness.locations("doc-a") == ()
    assert await harness.found("oldword", "team") == []


async def test_tampered_staged_bytes_are_refused(tmp_path):
    harness = _Harness(tmp_path)
    job = harness.job("doc-a", "1", content=b"original")
    harness.staging.write(job.payload["contentRef"], b"tampered")

    with pytest.raises(TerminalJobError) as error:
        await harness.handler.handle(job)

    assert error.value.code == "content_mismatch"
    assert harness.locations("doc-a") == ()


class _SpyBackend(DummyKnowledgeBackend):
    """Record writes and acting principals; optionally interrupt around the first write."""

    def __init__(self, *, crash=None, fail=None, leak_deletes=False):
        super().__init__()
        self.writes: list[str] = []
        self.principals: list[str] = []
        self._crash = crash
        self._fail = fail
        self._leak_deletes = leak_deletes

    async def _write(self, name, record, principal, partition):
        self.principals.append(principal.principal_id)
        if self._fail is not None:
            failure, self._fail = self._fail, None
            raise failure
        if self._crash == "before":
            self._crash = None
            raise InjectedWorkerCrash("crash before the backend write")
        self.writes.append(name)
        result = await getattr(super(), name)(record, principal, partition)
        if self._crash == "after":
            self._crash = None
            raise InjectedWorkerCrash("crash after the backend write")
        return result

    async def ingest(self, record, principal, partition, models=None, structured=None):
        return await self._write("ingest", record, principal, partition)

    async def update(self, record, principal, partition, models=None, structured=None):
        return await self._write("update", record, principal, partition)

    async def delete(self, reference, principal, partition):
        if self._leak_deletes:
            return DeletionResult(record_id="leaked", deleted=True)
        return await super().delete(reference, principal, partition)


async def test_crash_after_an_unrecorded_write_requires_reconciliation(tmp_path):
    harness = _Harness(tmp_path, _SpyBackend(crash="after"))
    job = harness.job("doc-a", "1", content=b"gateway runbook")

    with pytest.raises(InjectedWorkerCrash):
        await harness.handler.handle(job)
    assert harness.locations("doc-a")[0].state is LocationState.WRITING
    with pytest.raises(TerminalJobError) as error:
        await harness.handler.handle(job)

    assert error.value.code == "reconcile_required"
    assert harness.record("doc-a").index_state is IndexState.RECONCILE_REQUIRED
    assert harness.locations("doc-a")[0].state is LocationState.RECONCILE_REQUIRED


async def test_crash_before_the_write_is_retried_safely(tmp_path):
    spy = _SpyBackend(crash="before")
    harness = _Harness(tmp_path, spy)
    job = harness.job("doc-a", "1", content=b"gateway runbook")

    with pytest.raises(InjectedWorkerCrash):
        await harness.handler.handle(job)
    result = await harness.handler.handle(job)

    assert result["outcome"] == "replayed" and result["indexState"] == "indexed"
    assert spy.writes == ["ingest"]
    assert await harness.found("gateway", "team") == [("doc-a", "1")]


async def test_a_copy_that_survives_deletion_requires_reconciliation(tmp_path):
    harness = _Harness(tmp_path, _SpyBackend(leak_deletes=True))
    await harness.deliver("doc-a", "1", content=b"gateway runbook")

    with pytest.raises(TerminalJobError) as error:
        await harness.deliver("doc-a", "2", "delete")
    with pytest.raises(TerminalJobError) as again:
        await harness.deliver("doc-a", "3", "delete")

    assert error.value.code == "residue_found"
    assert again.value.code == "reconcile_required"
    assert harness.locations("doc-a")[0].state is LocationState.RECONCILE_REQUIRED
    assert harness.record("doc-a").index_state is IndexState.RECONCILE_REQUIRED


async def test_retryable_backend_errors_propagate_and_leave_no_intent(tmp_path):
    unavailable = BackendError(BackendErrorCode.UNAVAILABLE, "down", retryable=True)
    harness = _Harness(tmp_path, _SpyBackend(fail=unavailable))
    job = harness.job("doc-a", "1", content=b"gateway runbook")

    with pytest.raises(BackendError):
        await harness.handler.handle(job)
    assert harness.locations("doc-a") == ()
    result = await harness.handler.handle(job)

    assert result["indexState"] == "indexed"


@pytest.mark.parametrize(
    ("code", "job_code", "state"),
    [
        (BackendErrorCode.INVALID_INPUT, "backend_invalid_input", IndexState.FAILED),
        (
            BackendErrorCode.PARTIAL_WRITE,
            "backend_partial_write",
            IndexState.RECONCILE_REQUIRED,
        ),
    ],
)
async def test_permanent_backend_errors_fail_the_record(tmp_path, code, job_code, state):
    harness = _Harness(tmp_path, _SpyBackend(fail=BackendError(code, "no")))

    with pytest.raises(TerminalJobError) as error:
        await harness.deliver("doc-a", "1", content=b"gateway runbook")

    assert error.value.code == job_code
    assert harness.record("doc-a").index_state is state


async def test_ledger_only_mode_releases_superseded_bytes_without_indexing(tmp_path):
    harness = _Harness(tmp_path, indexed=False)
    await harness.deliver("doc-a", "1", content=b"one")
    first_upload = harness.record("doc-a").content_ref

    result = await harness.deliver("doc-a", "2", content=b"two")

    assert result["indexState"] is None
    assert harness.record("doc-a").index_state is IndexState.PENDING
    assert not harness.staging.exists(first_upload)
    assert harness.staging.exists(harness.record("doc-a").content_ref)
    nothing = await harness.backend.query(QueryRequest("two"), READER, (harness.partition("team"),))
    assert nothing.insufficient_evidence


async def test_unsupported_operations_fail_terminally(tmp_path):
    harness = _Harness(tmp_path)
    dispatcher = OperationDispatcher({JobOperation.INGESTION: harness.handler})
    job, _ = harness.repository.enqueue_job(
        JobOperation.SPACE_DELETION, "space-delete-1", {"spaceId": harness.space.id}, "t", 3, "p"
    )

    with pytest.raises(TerminalJobError) as error:
        await dispatcher.handle(job)

    assert error.value.code == "unsupported_operation"
