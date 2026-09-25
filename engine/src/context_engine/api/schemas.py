"""REST request and response models using only engine-owned concepts."""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Annotated, Literal

from pydantic import AnyUrl, BaseModel, ConfigDict, Field, model_validator

from context_engine.domain import ContextSpace, IngestionCommand, Job


class _ApiModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class CreateContextSpaceRequest(_ApiModel):
    """Validate a public request to create a context space."""

    name: Annotated[str, Field(min_length=1, max_length=120)]
    description: Annotated[str | None, Field(max_length=1000)] = None


class ContextSpaceResponse(_ApiModel):
    """Represent a context space without internal storage details."""

    id: str
    name: str
    description: str | None = None
    state: str
    created_at: datetime = Field(alias="createdAt")

    @classmethod
    def from_domain(cls, value: ContextSpace) -> ContextSpaceResponse:
        """Translate a domain context space into its REST representation."""

        return cls(
            id=value.id,
            name=value.name,
            description=value.description,
            state=value.state.value,
            createdAt=value.created_at,
        )


class IngestionRequest(_ApiModel):
    """Validate the canonical public ingestion envelope."""

    schema_version: Literal["1"] = Field(alias="schemaVersion")
    space_id: Annotated[str, Field(min_length=1, max_length=200)] = Field(alias="spaceId")
    source_id: Annotated[str, Field(min_length=1, max_length=200)] = Field(alias="sourceId")
    source_record_id: Annotated[str, Field(min_length=1, max_length=500)] = Field(
        alias="sourceRecordId"
    )
    source_version: Annotated[str, Field(min_length=1, max_length=200)] = Field(
        alias="sourceVersion"
    )
    operation: Literal["upsert", "delete", "acl_changed"]
    content_type: Annotated[str | None, Field(max_length=200)] = Field(
        default=None, alias="contentType"
    )
    content: Annotated[str | None, Field(min_length=1, max_length=1_000_000)] = None
    title: Annotated[str | None, Field(max_length=1000)] = None
    content_ref: Annotated[str | None, Field(min_length=1, max_length=1000)] = Field(
        default=None, alias="contentRef"
    )
    source_url: Annotated[AnyUrl | None, Field(max_length=2000)] = Field(
        default=None, alias="sourceUrl"
    )
    content_hash: Annotated[str | None, Field(pattern=r"^sha256:[0-9a-f]{64}$")] = Field(
        default=None, alias="contentHash"
    )
    source_observed_at: datetime = Field(alias="sourceObservedAt")
    audience: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=300)]], Field(min_length=1)
    ]
    source_acl_version: Annotated[str, Field(min_length=1, max_length=200)] = Field(
        alias="sourceAclVersion"
    )
    idempotency_key: Annotated[str, Field(min_length=8, max_length=500)] = Field(
        alias="idempotencyKey"
    )

    @model_validator(mode="after")
    def validate_operation_content(self) -> IngestionRequest:
        """Enforce operation-specific content and audience requirements."""

        if len(set(self.audience)) != len(self.audience):
            raise ValueError("audience values must be unique")
        if self.operation == "upsert":
            # Connectors deliver normalized inline content; a staged reference is the
            # future large-binary exception. Either satisfies an upsert.
            if not self.content_type or not (self.content or self.content_ref):
                raise ValueError("upsert requires contentType and one of content or contentRef")
        # When inline content and a declared hash are both present, they must agree so the
        # durable payload cannot record a hash that does not describe its own content.
        if self.content is not None and self.content_hash is not None:
            digest = "sha256:" + hashlib.sha256(self.content.encode()).hexdigest()
            if digest != self.content_hash:
                raise ValueError("contentHash does not match content")
        return self

    def to_command(self) -> IngestionCommand:
        """Translate the validated request into an application command."""

        return IngestionCommand(
            schema_version=self.schema_version,
            space_id=self.space_id,
            source_id=self.source_id,
            source_record_id=self.source_record_id,
            source_version=self.source_version,
            operation=self.operation,
            source_observed_at=self.source_observed_at,
            audience=tuple(self.audience),
            source_acl_version=self.source_acl_version,
            idempotency_key=self.idempotency_key,
            content_type=self.content_type,
            content=self.content,
            title=self.title,
            content_ref=self.content_ref,
            source_url=str(self.source_url) if self.source_url else None,
            content_hash=self.content_hash,
        )


class JobAcceptedResponse(_ApiModel):
    """Return the stable handle for accepted asynchronous work."""

    job_id: str = Field(alias="jobId")
    status_url: str = Field(alias="statusUrl")


class ErrorResponse(_ApiModel):
    """Return a stable caller-safe REST error."""

    code: str
    message: str
    trace_id: str = Field(alias="traceId")


class JobErrorResponse(_ApiModel):
    """Describe the caller-safe failure associated with a job."""

    code: str
    message: str
    trace_id: str = Field(alias="traceId")


class JobResponse(_ApiModel):
    """Expose public job state without its payload or lease details."""

    id: str
    state: str
    operation: str
    trace_id: str = Field(alias="traceId")
    attempt_count: int = Field(alias="attemptCount")
    created_at: datetime = Field(alias="createdAt")
    error: JobErrorResponse | None = None

    @classmethod
    def from_domain(cls, value: Job) -> JobResponse:
        """Translate an internal job into its public status representation."""

        error = None
        if value.error_code and value.error_message:
            error = JobErrorResponse(
                code=value.error_code,
                message=value.error_message,
                traceId=value.trace_id,
            )
        return cls(
            id=value.id,
            state=value.public_state,
            operation=value.operation.value,
            traceId=value.trace_id,
            attemptCount=value.attempt_count,
            createdAt=value.created_at,
            error=error,
        )


class HealthResponse(_ApiModel):
    """Report process liveness or readiness."""

    status: Literal["ok", "unavailable"]
