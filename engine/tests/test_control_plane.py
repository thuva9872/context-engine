"""Migration, outbox, idempotency, and lease tests."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from context_engine.domain import JobOperation, JobState
from context_engine.persistence import ControlDatabase, ControlPlaneRepository, IdempotencyConflict


def _repository(tmp_path):
    migrations = Path(__file__).resolve().parents[1] / "migrations"
    database = ControlDatabase(tmp_path / "control.db", migrations)
    assert database.migrate() == (
        "0001_control_plane",
        "0002_identity_and_grants",
        "0003_sources_and_ledger",
        "0004_source_progress",
        "0005_partitions_and_backend_state",
        "0006_provider_pipeline",
        "0007_written_versions",
        "0008_space_configurations",
        "0009_stored_queries",
        "0010_query_answers",
        "0011_source_structured_mapping",
    )
    assert database.migrate() == ()
    return database, ControlPlaneRepository(database)


def test_migrations_and_transactional_outbox(tmp_path):
    database, repository = _repository(tmp_path)
    space = repository.create_space("Operations", "Runbooks")
    payload = {
        "spaceId": space.id,
        "sourceId": "source-1",
        "sourceRecordId": "record-1",
        "sourceVersion": "1",
        "operation": "upsert",
    }

    job, created = repository.enqueue_job(
        JobOperation.INGESTION, "source-1:record-1:1", payload, "trace-1", 3, "prn_test"
    )

    assert created
    assert job.state is JobState.ACCEPTED
    assert job.principal_id == "prn_test"
    with database.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM outbox_events").fetchone()[0] == 1

    assert repository.dispatch_outbox() == 1
    assert repository.get_job(job.id).state is JobState.QUEUED
    assert repository.dispatch_outbox() == 0


def test_idempotency_replay_and_conflict(tmp_path):
    _, repository = _repository(tmp_path)
    payload = {
        "spaceId": "space-1",
        "sourceId": "source-1",
        "sourceRecordId": "record-1",
        "sourceVersion": "1",
        "operation": "upsert",
    }
    first, created = repository.enqueue_job(
        JobOperation.INGESTION, "source-1:record-1:1", payload, "trace-1", 3, "prn_test"
    )
    replay, replay_created = repository.enqueue_job(
        JobOperation.INGESTION, "source-1:record-1:1", payload, "trace-2", 3, "prn_test"
    )

    assert created and not replay_created
    assert replay.id == first.id
    assert replay.trace_id == "trace-1"

    with pytest.raises(IdempotencyConflict):
        repository.enqueue_job(
            JobOperation.INGESTION,
            "source-1:record-1:1",
            {**payload, "sourceVersion": "2"},
            "trace-3",
            3,
            "prn_test",
        )


def test_expired_lease_can_be_reclaimed(tmp_path):
    _, repository = _repository(tmp_path)
    payload = {
        "spaceId": "space-1",
        "sourceId": "source-1",
        "sourceRecordId": "record-1",
        "sourceVersion": "1",
        "operation": "upsert",
    }
    job, _ = repository.enqueue_job(
        JobOperation.INGESTION, "source-1:record-1:1", payload, "trace-1", 3, "prn_test"
    )
    repository.dispatch_outbox()
    instant = datetime.now(UTC)
    first = repository.claim_job(0, instant)
    second = repository.claim_job(30, instant)

    assert first.id == job.id
    assert second.id == job.id
    assert second.attempt_count == 2
    assert second.lease_token != first.lease_token
