"""REST request and response models using only engine-owned concepts."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import AnyUrl, BaseModel, ConfigDict, Field, field_validator, model_validator

from context_engine.application.space_models import ModelInput, SpaceConfigurationView
from context_engine.domain import (
    Action,
    ConfiguredModel,
    ContextQueryResult,
    ContextSpace,
    EvidenceLocator,
    Grant,
    IndexingSnapshot,
    IngestionCommand,
    Job,
    PublicEvidence,
    RecordStatus,
    Source,
    SourceCheckpoint,
    SourceProgress,
    StagedUpload,
    SyncRun,
    SyncRunState,
)
from context_engine.security.identity import AuthenticatedPrincipal

_GRANT_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$"


class _ApiModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class CreateContextSpaceRequest(_ApiModel):
    """Validate a public request to create a context space."""

    name: Annotated[str, Field(min_length=1, max_length=120)]
    description: Annotated[str | None, Field(max_length=1000)] = None


class ModelConfigurationRequest(_ApiModel):
    """One model as an administrator configures it.

    The key travels once, as `apiKey`, or as a reference the engine resolves at call time,
    `apiKeyRef`; never both. A literal key is encrypted before storage, which is how a
    standalone deployment holds keys. Neither is required when the model is unchanged, so a
    configuration read from the engine can be sent back without knowing the key. `baseUrl`
    and `apiVersion` are for providers that need them, such as Azure OpenAI.
    """

    provider: Annotated[str, Field(min_length=1, max_length=60)]
    model: Annotated[str, Field(min_length=1, max_length=200)]
    api_key: Annotated[str | None, Field(max_length=4000)] = Field(default=None, alias="apiKey")
    api_key_ref: Annotated[str | None, Field(max_length=500)] = Field(
        default=None, alias="apiKeyRef"
    )
    base_url: Annotated[AnyUrl | None, Field(max_length=2000)] = Field(
        default=None, alias="baseUrl"
    )
    api_version: Annotated[str | None, Field(max_length=60)] = Field(
        default=None, alias="apiVersion"
    )
    dimensions: Annotated[int | None, Field(ge=1, le=16384)] = None

    @field_validator("api_key", "api_key_ref", "api_version", mode="before")
    @classmethod
    def blank_means_unset(cls, value: object) -> object:
        """Treat an empty string as absent; the control plane sends one for an unset key."""

        if isinstance(value, str) and not value.strip():
            return None
        return value

    @model_validator(mode="after")
    def validate_key(self) -> ModelConfigurationRequest:
        """Refuse a literal key and a reference together; they would contradict each other."""

        if self.api_key is not None and self.api_key_ref is not None:
            raise ValueError("send either apiKey or apiKeyRef, not both")
        return self

    def to_input(self) -> ModelInput:
        """Translate the request into the application's model input."""

        return ModelInput(
            provider=self.provider,
            model=self.model,
            api_key=self.api_key,
            api_key_ref=self.api_key_ref,
            endpoint=str(self.base_url) if self.base_url else None,
            api_version=self.api_version,
            dimensions=self.dimensions,
        )


class SpaceConfigurationRequest(_ApiModel):
    """Replace a space's models.

    `storage` and `sources` are accepted and ignored: the control plane sends them in the
    same request, but storage placement waits for the topology decision and sources are
    registered through their own route.
    """

    embedding: ModelConfigurationRequest | None = None
    llm: ModelConfigurationRequest | None = None
    storage: dict[str, object] | None = None
    sources: list[object] | None = None


class ModelSummaryResponse(_ApiModel):
    """A configured model without its key; `keyKind` says how the key is held."""

    provider: str
    model: str
    key_kind: str = Field(alias="keyKind")
    base_url: str | None = Field(default=None, alias="baseUrl")
    api_version: str | None = Field(default=None, alias="apiVersion")
    dimensions: int | None = None

    @classmethod
    def from_domain(cls, value: ConfiguredModel, key_kind: str | None) -> ModelSummaryResponse:
        """Translate a stored model into its public summary."""

        return cls(
            provider=value.provider,
            model=value.model,
            keyKind=key_kind or "unknown",
            baseUrl=value.endpoint,
            apiVersion=value.api_version,
            dimensions=value.dimensions,
        )


class StorePlacementResponse(_ApiModel):
    """Where one kind of store runs, as the engine reports it."""

    provider: str


class SpaceConfigurationResponse(_ApiModel):
    """A space's models, whether its embedding is locked, and where its stores run."""

    embedding: ModelSummaryResponse | None = None
    llm: ModelSummaryResponse | None = None
    embedding_locked: bool = Field(alias="embeddingLocked")
    version: int | None = None
    updated_at: datetime | None = Field(default=None, alias="updatedAt")
    storage: dict[str, StorePlacementResponse]

    @classmethod
    def from_view(
        cls, view: SpaceConfigurationView, storage: dict[str, str]
    ) -> SpaceConfigurationResponse:
        """Translate the service's view into the REST representation, keys excluded."""

        configuration = view.configuration
        return cls(
            embedding=ModelSummaryResponse.from_domain(
                configuration.embedding_model, view.embedding_key_kind
            )
            if configuration and configuration.embedding_model
            else None,
            llm=ModelSummaryResponse.from_domain(
                configuration.language_model, view.language_key_kind
            )
            if configuration and configuration.language_model
            else None,
            embeddingLocked=view.embedding_locked,
            version=configuration.version if configuration else None,
            updatedAt=configuration.updated_at if configuration else None,
            storage={kind: StorePlacementResponse(provider=name) for kind, name in storage.items()},
        )


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
        if self.operation == "upsert" and not all(
            (self.content_type, self.content_ref, self.content_hash)
        ):
            raise ValueError("upsert requires contentType, contentRef, and contentHash")
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
            content_ref=self.content_ref,
            source_url=str(self.source_url) if self.source_url else None,
            content_hash=self.content_hash,
        )


class DirectIngestionEvent(IngestionRequest):
    """Validate an ingestion envelope whose content travels in the same request.

    The engine stages the content and fills in the reference and hash itself, so an upsert
    needs neither; a hash or content type that is present is checked against the bytes.
    """

    @model_validator(mode="after")
    def validate_operation_content(self) -> DirectIngestionEvent:
        """Keep the envelope's audience rule and refuse a reference to separate content."""

        if len(set(self.audience)) != len(self.audience):
            raise ValueError("audience values must be unique")
        if self.content_ref is not None:
            raise ValueError("contentRef is not used when content travels with the event")
        return self


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


class PrincipalResponse(_ApiModel):
    """Describe the calling principal as the engine resolved it from the credential."""

    id: str
    kind: str
    email: str | None = None
    groups: list[str]

    @classmethod
    def from_principal(cls, value: AuthenticatedPrincipal) -> PrincipalResponse:
        """Translate the authenticated principal into its REST representation."""

        return cls(
            id=value.principal_id,
            kind=value.kind.value,
            email=value.email,
            groups=sorted(value.groups),
        )


class EffectivePermissionsResponse(_ApiModel):
    """Return the caller's effective actions on one visible resource."""

    resource_id: str = Field(alias="resourceId")
    actions: list[str]


class PutGrantRequest(_ApiModel):
    """Validate a grant body; the actor is never part of it, only the target."""

    actions: Annotated[list[str], Field(min_length=1, max_length=len(Action))]
    principal_id: Annotated[str | None, Field(min_length=1, max_length=200)] = Field(
        default=None, alias="principalId"
    )
    group: Annotated[str | None, Field(min_length=1, max_length=300)] = None

    @model_validator(mode="after")
    def validate_subject_and_actions(self) -> PutGrantRequest:
        """Require exactly one subject and only known engine actions."""

        if (self.principal_id is None) == (self.group is None):
            raise ValueError("exactly one of principalId or group is required")
        if len(set(self.actions)) != len(self.actions):
            raise ValueError("actions must be unique")
        known = {item.value for item in Action}
        if not set(self.actions) <= known:
            raise ValueError("actions must be engine actions")
        return self

    def to_actions(self) -> frozenset[Action]:
        """Return the validated actions as domain values."""

        return frozenset(Action(item) for item in self.actions)


class GrantResponse(_ApiModel):
    """Represent one grant without exposing who created it."""

    id: str
    resource_id: str = Field(alias="resourceId")
    actions: list[str]
    principal_id: str | None = Field(default=None, alias="principalId")
    group: str | None = None

    @classmethod
    def from_domain(cls, value: Grant) -> GrantResponse:
        """Translate a domain grant into its REST representation."""

        return cls(
            id=value.id,
            resourceId=value.resource_id,
            actions=sorted(item.value for item in value.actions),
            principalId=value.principal_id,
            group=value.group,
        )


_MappingKey = Annotated[str, Field(min_length=1, max_length=300)]
_MappingValue = Annotated[str, Field(min_length=1, max_length=300)]
# An RML mapping is a Turtle document, so it is bounded far more generously than a tag mapping.
_RmlKey = Annotated[str, Field(min_length=1, max_length=100)]
_RmlValue = Annotated[str, Field(min_length=1, max_length=100_000)]


class RegisterSourceRequest(_ApiModel):
    """Validate a source registration; the mappings are write-only configuration."""

    name: Annotated[str, Field(min_length=1, max_length=120)]
    type: Annotated[str, Field(min_length=1, max_length=60)]
    audience_mapping: Annotated[dict[_MappingKey, _MappingValue], Field(max_length=500)] = Field(
        alias="audienceMapping"
    )
    version_ordering: Literal["numeric", "lexicographic"] = Field(
        default="numeric", alias="versionOrdering"
    )
    # RML mappings keyed by content type; a record of that type becomes graph nodes and edges.
    structured_mapping: Annotated[
        dict[_RmlKey, _RmlValue] | None, Field(max_length=50)
    ] = Field(default=None, alias="structuredMapping")


class UpdateSourceRequest(_ApiModel):
    """Validate a partial source update; at least one field must be present."""

    name: Annotated[str | None, Field(min_length=1, max_length=120)] = None
    state: Literal["ready", "paused"] | None = None
    audience_mapping: Annotated[dict[_MappingKey, _MappingValue] | None, Field(max_length=500)] = (
        Field(default=None, alias="audienceMapping")
    )
    structured_mapping: Annotated[
        dict[_RmlKey, _RmlValue] | None, Field(max_length=50)
    ] = Field(default=None, alias="structuredMapping")

    @model_validator(mode="after")
    def require_a_change(self) -> UpdateSourceRequest:
        """Reject an empty update."""

        if (
            self.name is None
            and self.state is None
            and self.audience_mapping is None
            and self.structured_mapping is None
        ):
            raise ValueError("at least one field is required")
        return self


class SourceResponse(_ApiModel):
    """Represent a source without its audience mapping or internal bindings."""

    id: str
    space_id: str = Field(alias="spaceId")
    name: str
    type: str
    state: str
    version_ordering: str = Field(alias="versionOrdering")
    created_at: datetime = Field(alias="createdAt")

    @classmethod
    def from_domain(cls, value: Source) -> SourceResponse:
        """Translate a domain source into its REST representation."""

        return cls(
            id=value.id,
            spaceId=value.space_id,
            name=value.name,
            type=value.type,
            state=value.state.value,
            versionOrdering=value.version_ordering.value,
            createdAt=value.created_at,
        )


class PutCheckpointRequest(_ApiModel):
    """Validate a connector cursor."""

    cursor: Annotated[str, Field(min_length=1, max_length=4000)]


class CheckpointResponse(_ApiModel):
    """Return the stored connector cursor."""

    source_id: str = Field(alias="sourceId")
    cursor: str
    updated_at: datetime = Field(alias="updatedAt")

    @classmethod
    def from_domain(cls, value: SourceCheckpoint) -> CheckpointResponse:
        """Translate a checkpoint into its REST representation."""

        return cls(sourceId=value.source_id, cursor=value.cursor, updatedAt=value.updated_at)


class UploadResponse(_ApiModel):
    """Return the staged-object handle an ingestion event references as contentRef."""

    upload_id: str = Field(alias="uploadId")
    source_id: str = Field(alias="sourceId")
    content_type: str = Field(alias="contentType")
    size_bytes: int = Field(alias="sizeBytes")
    content_hash: str = Field(alias="contentHash")
    expires_at: datetime = Field(alias="expiresAt")

    @classmethod
    def from_domain(cls, value: StagedUpload) -> UploadResponse:
        """Translate a staged upload into its REST representation."""

        return cls(
            uploadId=value.id,
            sourceId=value.source_id,
            contentType=value.content_type,
            sizeBytes=value.size_bytes,
            contentHash=value.content_hash,
            expiresAt=value.expires_at,
        )


class RecordStatusResponse(_ApiModel):
    """Expose a record's lifecycle without content, audiences, or backend references."""

    space_id: str = Field(alias="spaceId")
    source_id: str = Field(alias="sourceId")
    record_id: str = Field(alias="recordId")
    state: str
    current_version: str = Field(alias="currentVersion")
    source_acl_version: str = Field(alias="sourceAclVersion")
    content_hash: str | None = Field(default=None, alias="contentHash")
    quarantine_reason: str | None = Field(default=None, alias="quarantineReason")
    index_state: str = Field(alias="indexState")
    index_error: str | None = Field(default=None, alias="indexError")
    updated_at: datetime = Field(alias="updatedAt")

    @classmethod
    def from_domain(cls, value: RecordStatus) -> RecordStatusResponse:
        """Translate a ledger record into its REST representation."""

        return cls(
            spaceId=value.space_id,
            sourceId=value.source_id,
            recordId=value.source_record_id,
            state=value.state.value,
            currentVersion=value.current_version,
            sourceAclVersion=value.source_acl_version,
            contentHash=value.content_hash,
            quarantineReason=value.quarantine_reason,
            indexState=value.index_state.value,
            indexError=value.index_error,
            updatedAt=value.updated_at,
        )


def _percent(done: int | None, total: int | None) -> float | None:
    """Return a one-decimal percentage, or None when there is nothing to measure."""

    if not total or done is None:
        return None
    return round(min(done, total) * 100 / total, 1)


class SyncRunResponse(_ApiModel):
    """Represent a connector's reading window for a source."""

    id: str
    source_id: str = Field(alias="sourceId")
    state: str
    started_at: datetime = Field(alias="startedAt")
    completed_at: datetime | None = Field(default=None, alias="completedAt")

    @classmethod
    def from_domain(cls, value: SyncRun) -> SyncRunResponse:
        """Translate a sync run into its REST representation."""

        return cls(
            id=value.id,
            sourceId=value.source_id,
            state=value.state.value,
            startedAt=value.started_at,
            completedAt=value.completed_at,
        )


class ReadingProgressResponse(_ApiModel):
    """Say whether the connector is still reading; no percentage by design."""

    state: Literal["idle", "reading", "completed"]
    run_id: str | None = Field(default=None, alias="runId")
    started_at: datetime | None = Field(default=None, alias="startedAt")
    completed_at: datetime | None = Field(default=None, alias="completedAt")

    @classmethod
    def from_domain(cls, value: SyncRun | None) -> ReadingProgressResponse:
        """Translate the latest sync run into a reading state."""

        if value is None:
            return cls(state="idle")
        state = "reading" if value.state is SyncRunState.READING else "completed"
        return cls(
            state=state,
            runId=value.id,
            startedAt=value.started_at,
            completedAt=value.completed_at,
        )


class ProcessingProgressResponse(_ApiModel):
    """Delivered events the engine has finished applying, over the latest run's window."""

    since: datetime | None = None
    total: int
    queued: int
    running: int
    succeeded: int
    failed: int
    percent: float | None = None


class RecordCountsResponse(_ApiModel):
    """Ledger records of the source by lifecycle state."""

    active: int
    quarantined: int
    deleted: int


class IndexingProgressResponse(_ApiModel):
    """Active record versions the knowledge backend has indexed, from the last collection."""

    state: Literal["not_collected", "ok", "unavailable"]
    expected: int | None = None
    indexed: int | None = None
    indexing: int | None = None
    failed: int | None = None
    missing: int | None = None
    percent: float | None = None
    collected_at: datetime | None = Field(default=None, alias="collectedAt")

    @classmethod
    def from_domain(cls, value: IndexingSnapshot | None) -> IndexingProgressResponse:
        """Translate a stored snapshot, or report that none has been collected."""

        if value is None:
            return cls(state="not_collected")
        return cls(
            state=value.state.value,
            expected=value.expected,
            indexed=value.indexed,
            indexing=value.indexing,
            failed=value.failed,
            missing=value.missing,
            percent=_percent(value.indexed, value.expected),
            collectedAt=value.collected_at,
        )


class SourceProgressResponse(_ApiModel):
    """Reading, processing, ledger, and indexing progress of one source."""

    source_id: str = Field(alias="sourceId")
    reading: ReadingProgressResponse
    processing: ProcessingProgressResponse
    records: RecordCountsResponse
    indexing: IndexingProgressResponse

    @classmethod
    def from_domain(cls, value: SourceProgress) -> SourceProgressResponse:
        """Translate source progress into its REST representation."""

        jobs = value.jobs
        return cls(
            sourceId=value.source_id,
            reading=ReadingProgressResponse.from_domain(value.sync_run),
            processing=ProcessingProgressResponse(
                since=value.processing_since,
                total=jobs.total,
                queued=jobs.queued,
                running=jobs.running,
                succeeded=jobs.succeeded,
                failed=jobs.failed,
                percent=_percent(jobs.finished, jobs.total),
            ),
            records=RecordCountsResponse(
                active=value.records.active,
                quarantined=value.records.quarantined,
                deleted=value.records.deleted,
            ),
            indexing=IndexingProgressResponse.from_domain(value.indexing),
        )


class SpaceProgressResponse(_ApiModel):
    """Progress of every source in a space that the caller may inspect."""

    space_id: str = Field(alias="spaceId")
    sources: list[SourceProgressResponse]


class ContextQueryRequest(_ApiModel):
    """Validate a context query; the caller's identity comes from the credential."""

    space_id: Annotated[str, Field(min_length=1, max_length=200)] = Field(alias="spaceId")
    question: Annotated[str, Field(min_length=1, max_length=10000)]
    mode: Literal["context", "answer"]
    limit: Annotated[int, Field(ge=1, le=100)] = 10


class RangeResponse(_ApiModel):
    """An inclusive, 1-based range of sentences or lines."""

    first: int
    last: int


class CharacterRangeResponse(_ApiModel):
    """Code-point offsets into the version's extracted text, end exclusive."""

    start: int
    end: int


class EvidenceLocatorResponse(_ApiModel):
    """Where a passage sits in its record version; each part only when it is certain."""

    chunk_index: int | None = Field(default=None, alias="chunkIndex")
    characters: CharacterRangeResponse | None = None
    sentences: RangeResponse | None = None
    lines: RangeResponse | None = None
    heading: str | None = None
    path: str | None = None

    @classmethod
    def from_domain(cls, value: EvidenceLocator) -> EvidenceLocatorResponse:
        """Translate a locator, leaving out every part the engine could not establish."""

        characters = (
            CharacterRangeResponse(start=value.start, end=value.end)
            if value.start is not None and value.end is not None
            else None
        )
        return cls(
            chunkIndex=value.chunk_index,
            characters=characters,
            sentences=RangeResponse(first=value.sentences[0], last=value.sentences[1])
            if value.sentences
            else None,
            lines=RangeResponse(first=value.lines[0], last=value.lines[1]) if value.lines else None,
            heading=value.heading,
            path=value.path,
        )


class EvidenceResponse(_ApiModel):
    """One authorized passage with engine lineage and no backend identifiers.

    `location` keeps the compact `chunk:<n>` form earlier clients read; `locator` carries the
    typed position (M5 slice 2).
    """

    id: str
    record_id: str = Field(alias="recordId")
    source_id: str = Field(alias="sourceId")
    source_version: str = Field(alias="sourceVersion")
    passage: str
    location: str | None = None
    locator: EvidenceLocatorResponse
    source_url: str | None = Field(default=None, alias="sourceUrl")

    @classmethod
    def from_domain(cls, value: PublicEvidence) -> EvidenceResponse:
        """Translate public evidence into its REST representation."""

        chunk = value.locator.chunk_index
        return cls(
            id=value.id,
            recordId=value.record_id,
            sourceId=value.source_id,
            sourceVersion=value.source_version,
            passage=value.passage,
            location=f"chunk:{chunk}" if chunk is not None else None,
            locator=EvidenceLocatorResponse.from_domain(value.locator),
            sourceUrl=value.source_url,
        )


class ContextQueryResponse(_ApiModel):
    """Evidence for a question and, in answer mode, the checked answer, or insufficient evidence.

    Each `[n]` in the answer cites `evidence[n - 1]`. `answerWithheld` appears when a stored
    answer can no longer be shown because a passage it was written from is out of the reader's
    reach or was erased.
    """

    query_id: str = Field(alias="queryId")
    state: Literal["completed", "insufficient_evidence"]
    answer: str | None = None
    answer_withheld: bool | None = Field(default=None, alias="answerWithheld")
    evidence: list[EvidenceResponse]
    insufficient_evidence: bool = Field(alias="insufficientEvidence")
    trace_id: str = Field(alias="traceId")

    @classmethod
    def from_domain(cls, value: ContextQueryResult) -> ContextQueryResponse:
        """Translate a query result into its REST representation."""

        return cls(
            queryId=value.query_id,
            state="insufficient_evidence" if value.insufficient_evidence else "completed",
            answer=value.answer,
            answerWithheld=True if value.answer_withheld else None,
            evidence=[EvidenceResponse.from_domain(item) for item in value.evidence],
            insufficientEvidence=value.insufficient_evidence,
            traceId=value.trace_id,
        )
