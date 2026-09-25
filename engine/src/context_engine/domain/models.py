"""Typed domain values for the M1 control plane."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


def utc_now() -> datetime:
    """Return an aware UTC timestamp."""

    return datetime.now(UTC)


class SpaceState(StrEnum):
    """Lifecycle states of a public context space."""

    PROVISIONING = "provisioning"
    READY = "ready"
    FAILED = "failed"
    DELETING = "deleting"


class JobOperation(StrEnum):
    """Asynchronous operations supported by the control plane."""

    INGESTION = "ingestion"
    UPDATE = "update"
    DELETION = "deletion"
    ENRICHMENT = "enrichment"
    SPACE_DELETION = "space_deletion"


class JobState(StrEnum):
    """Internal durable states of an asynchronous job."""

    ACCEPTED = "accepted"
    QUEUED = "queued"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ContextSpace:
    """Public context-space metadata stored by the control plane."""

    id: str
    name: str
    description: str | None
    state: SpaceState
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class IngestionCommand:
    """Validated application command for one source-record event."""

    schema_version: str
    space_id: str
    source_id: str
    source_record_id: str
    source_version: str
    operation: str
    source_observed_at: datetime
    audience: tuple[str, ...]
    source_acl_version: str
    idempotency_key: str
    content_type: str | None = None
    content: str | None = None
    title: str | None = None
    content_ref: str | None = None
    source_url: str | None = None
    content_hash: str | None = None

    def to_payload(self) -> dict[str, Any]:
        """Return the canonical durable payload for the worker."""

        value: dict[str, Any] = {
            "schemaVersion": self.schema_version,
            "spaceId": self.space_id,
            "sourceId": self.source_id,
            "sourceRecordId": self.source_record_id,
            "sourceVersion": self.source_version,
            "operation": self.operation,
            "sourceObservedAt": self.source_observed_at.isoformat(),
            "audience": list(self.audience),
            "sourceAclVersion": self.source_acl_version,
            "idempotencyKey": self.idempotency_key,
        }
        optional = {
            "contentType": self.content_type,
            "content": self.content,
            "title": self.title,
            "contentRef": self.content_ref,
            "sourceUrl": self.source_url,
            "contentHash": self.content_hash,
        }
        value.update({key: item for key, item in optional.items() if item is not None})
        return value

    @property
    def job_operation(self) -> JobOperation:
        """Map the ingestion event operation to a job operation."""

        return {
            "upsert": JobOperation.INGESTION,
            "acl_changed": JobOperation.UPDATE,
            "delete": JobOperation.DELETION,
        }[self.operation]


@dataclass(frozen=True, slots=True)
class Job:
    """Durable asynchronous job including private execution metadata."""

    id: str
    operation: JobOperation
    state: JobState
    idempotency_key: str
    payload: dict[str, Any]
    payload_hash: str
    trace_id: str
    attempt_count: int
    max_attempts: int
    next_attempt_at: datetime | None
    lease_token: str | None
    lease_expires_at: datetime | None
    result: dict[str, Any] | None
    error_code: str | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime

    @property
    def public_state(self) -> str:
        """Map internal scheduling states to the stable REST state model."""

        # Scheduling details are private; callers only need to know that work is queued.
        if self.state in {JobState.ACCEPTED, JobState.RETRY_WAIT}:
            return JobState.QUEUED.value
        return self.state.value
