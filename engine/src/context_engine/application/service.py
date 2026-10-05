"""Application service used by the REST API and future external interfaces.

Every command and query receives the authenticated principal resolved by the transport and
decides access through the authorizer before touching durable state. Invisible resources
answer with not-found so denials reveal nothing about what exists.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from uuid import uuid4

from context_engine.domain import (
    ROOT_RESOURCE_ID,
    Action,
    ConfiguredModel,
    ContextQueryResult,
    ContextSpace,
    EvidenceLocator,
    Grant,
    IndexState,
    IngestionCommand,
    Job,
    JobOperation,
    JobState,
    LocationState,
    ModelUse,
    PublicEvidence,
    RecordLocation,
    RecordState,
    RecordStatus,
    Source,
    SourceCheckpoint,
    SourceProgress,
    SourceState,
    SpaceConfiguration,
    SpaceState,
    StagedUpload,
    StoredQuery,
    SyncRun,
    SyncRunState,
    VersionOrdering,
)
from context_engine.ingestion.extraction import ExtractionError, extract
from context_engine.ingestion.structured import (
    StructuredMaterializationError,
    validate_mapping,
)
from context_engine.knowledge_backend import (
    AccessPartitionRef,
    AnswerWriter,
    BackendError,
    BackendErrorCode,
    EvidenceItem,
    KnowledgeBackend,
    ModelSelection,
    PrincipalContext,
    QueryRequest,
)
from context_engine.observability import MetricsRegistry
from context_engine.persistence import IdempotencyConflict
from context_engine.provenance import TextIndex, locate
from context_engine.security.authorization import Authorizer
from context_engine.security.identity import AuthenticatedPrincipal
from context_engine.security.policy import PolicyDecision, PolicyInput, authorize
from context_engine.security.secrets import SecretError, SecretStore

from .answers import build_answer_request, check_citations
from .errors import (
    AccessDeniedError,
    ConflictError,
    NotFoundError,
    PayloadTooLargeError,
    ServiceUnavailableError,
    UnsupportedContentTypeError,
    ValidationError,
)
from .ports import (
    ControlPlaneStore,
    GrantStore,
    QueryStore,
    ReadAccessResync,
    SourceStore,
    StagedBytes,
    utc_now,
)
from .space_models import ModelInput, SpaceConfigurationView, SpaceModelResolver

_MANAGE_SOURCES = frozenset({Action.SOURCE_MANAGE, Action.SPACE_MANAGE})
_INSPECT_DELIVERIES = frozenset({Action.SOURCE_MANAGE, Action.SPACE_MANAGE, Action.INGEST_WRITE})
ENRICHMENT_PIPELINE_VERSION = "enrich@1"
_RETRY_ONE_BY_ONE = frozenset({BackendErrorCode.ACCESS_DENIED, BackendErrorCode.NOT_FOUND})
_DELIVERY_OPERATIONS = (JobOperation.INGESTION, JobOperation.UPDATE, JobOperation.DELETION)
# Content delivered with its event is staged under the event's key in its own namespace, so
# it never collides with a key the connector used for a separate upload.
_DIRECT_UPLOAD_PREFIX = "event:"

# Content types an RML mapping may target. Each is a structured format morph-kgc can materialize.
_STRUCTURED_CONTENT_TYPES = frozenset(
    {"application/json", "text/csv", "application/xml", "text/xml"}
)


@dataclass(frozen=True, slots=True)
class UploadPolicy:
    """Limits applied to staged bytes before an ingestion event may reference them.

    Built once from settings and shared by both delivery paths, so a separate upload and a one-call
    delivery can never be held to different type, size, or expiry rules.
    """

    max_bytes: int
    ttl_seconds: int
    content_types: frozenset[str]


class ContextEngineService:
    """Coordinate public commands and queries over engine-owned ports.

    This is the one place that authorizes and validates requests. The REST API, and later MCP, call
    it with the principal their transport verified, so every interface gets the same decisions. It
    depends only on ports, never on a concrete store or provider.
    """

    def __init__(
        self,
        store: ControlPlaneStore,
        grants: GrantStore,
        authorizer: Authorizer,
        metrics: MetricsRegistry,
        sources: SourceStore,
        staging: StagedBytes,
        upload_policy: UploadPolicy,
        max_job_attempts: int = 5,
        *,
        indexing_enabled: bool = False,
        knowledge_backend: KnowledgeBackend | None = None,
        read_access: ReadAccessResync | None = None,
        secrets: SecretStore | None = None,
        queries: QueryStore | None = None,
        default_models: Mapping[str, ModelUse] | None = None,
        answers: AnswerWriter | None = None,
        answer_context_chars: int = 32_000,
    ) -> None:
        """Wire the service to its stores, authorizer, and policies.

        Every dependency is an engine-owned port, which keeps this layer testable with in-memory
        fakes. The knowledge backend and the indexing flag are set only in provider mode; without
        them, queries and enrichment answer unavailable instead of pretending nothing matches.
        `read_access` lets a query ask the worker to resynchronize backend read access when the
        backend refuses a partition that engine policy allows. `secrets` stores and reveals the
        keys of a space's models; without one, only key references can be configured.
        `queries` stores each query with its evidence; without it, queries are not kept and
        cannot be reopened. `default_models` names the environment's models, recorded on queries
        of spaces that configure none. `answers` writes answer text for answer mode, and
        `answer_context_chars` bounds how much evidence one answer sends to the model.
        """

        self._store = store
        self._grants = grants
        self._authorizer = authorizer
        self._metrics = metrics
        self._sources = sources
        self._staging = staging
        self._upload_policy = upload_policy
        self._max_job_attempts = max_job_attempts
        self._indexing_enabled = indexing_enabled
        self._backend = knowledge_backend
        self._read_access = read_access
        self._secrets = secrets or SecretStore(None)
        self._models = SpaceModelResolver(store, self._secrets)
        self._queries = queries
        self._default_models = dict(default_models or {})
        self._answers = answers
        self._answer_context_chars = answer_context_chars

    # Authorization helpers

    def _visible(self, principal: AuthenticatedPrincipal, resource_id: str) -> bool:
        """Report whether the principal holds any action on the resource.

        Grants on a parent count, so a space grant makes the space's sources visible. Callers check
        visibility before rights, so a resource the caller cannot see answers not-found rather than
        403, which would confirm it exists (threat T17).
        """

        return self._authorizer.is_visible(principal.principal_id, principal.groups, resource_id)

    def _require(self, principal: AuthenticatedPrincipal, action: Action, resource_id: str) -> None:
        """Require one action on a resource or raise.

        This fails closed. An unknown or invisible resource is not-found, and a visible one without
        the action is access-denied. The root skips the visibility check because its existence is
        not secret. Every call records an access decision with its reason code, which is the audit
        trail M2 introduced.
        """

        if resource_id != ROOT_RESOURCE_ID and not self._visible(principal, resource_id):
            raise NotFoundError()
        decision = self._authorizer.decide(
            principal.principal_id, principal.groups, action, resource_id, principal.trace_id
        )
        if not decision.allowed:
            raise AccessDeniedError()

    def _require_any(
        self, principal: AuthenticatedPrincipal, actions: frozenset[Action], resource_id: str
    ) -> None:
        """Allow when the principal holds any of the actions; record one decision either way.

        Used where more than one role may act, such as a source manager or a space manager.
        Recording a single decision, for the action that granted access or the primary action when
        denied, keeps the audit log at one entry per request.
        """

        if not self._visible(principal, resource_id):
            raise NotFoundError()
        held = self._authorizer.effective_actions(
            principal.principal_id, principal.groups, resource_id
        )
        matched = sorted(actions & (held or frozenset()), key=lambda item: item.value)
        # Record the action that granted access, or the primary action when denied.
        action = matched[0] if matched else sorted(actions, key=lambda item: item.value)[0]
        decision = self._authorizer.decide(
            principal.principal_id, principal.groups, action, resource_id, principal.trace_id
        )
        if not decision.allowed:
            raise AccessDeniedError()

    def _visible_source(self, principal: AuthenticatedPrincipal, source_id: str) -> Source:
        """Return a source the principal can see, or not-found.

        Sources are resources in the grant chain. A connector granted rights on one source can see
        that source without seeing the rest of its space.
        """

        source = self._sources.get_source(source_id)
        if source is None or not self._visible(principal, source_id):
            raise NotFoundError("Source not found")
        return source

    # Identity and permissions

    def effective_permissions(
        self, principal: AuthenticatedPrincipal, resource_id: str
    ) -> frozenset[Action]:
        """Return the caller's own effective actions on a visible resource.

        Only the caller's own actions are returned, so no one can list another principal's rights.
        An unknown resource, or one where the caller holds nothing, is not-found; holding nothing on
        the root is normal, so the root answers with an empty set.
        """

        actions = self._authorizer.effective_actions(
            principal.principal_id, principal.groups, resource_id
        )
        if actions is None or (not actions and resource_id != ROOT_RESOURCE_ID):
            raise NotFoundError()
        return actions

    # Context spaces

    def create_context_space(
        self, principal: AuthenticatedPrincipal, name: str, description: str | None
    ) -> ContextSpace:
        """Create a context space for a principal holding space.manage on the root.

        Spaces hang directly off the root, so creating one needs a root grant. The first
        administrator gets it from the bootstrap actions in the identity registry (M2).
        """

        self._require(principal, Action.SPACE_MANAGE, ROOT_RESOURCE_ID)
        normalized = name.strip()
        if not normalized:
            raise ValidationError("Context-space name is required")
        space = self._store.create_space(normalized, description)
        self._metrics.increment("context_engine_spaces_created_total")
        return space

    def list_context_spaces(self, principal: AuthenticatedPrincipal) -> tuple[ContextSpace, ...]:
        """List only the context spaces on which the principal holds some action.

        Every space passes the same visibility check a single read uses, so a listing never shows
        more than reading each space would.
        """

        return tuple(
            space for space in self._store.list_spaces() if self._visible(principal, space.id)
        )

    def get_context_space(self, principal: AuthenticatedPrincipal, space_id: str) -> ContextSpace:
        """Return a visible context space or a generic not-found error.

        Unknown and invisible spaces give the same answer, so a caller cannot probe for spaces it
        has no access to.
        """

        space = self._store.get_space(space_id)
        if space is None or not self._visible(principal, space_id):
            raise NotFoundError("Context space not found")
        return space

    def delete_context_space(self, principal: AuthenticatedPrincipal, space_id: str) -> Job:
        """Queue deletion of a space for a principal holding space.manage.

        Deletion runs as a job because every record's backend copy is removed and checked
        before the space's rows go (ADR 0007). The space moves to `deleting` at once, which
        makes it refuse new deliveries, queries, enrichment, and configuration while the job
        runs. Repeating the request returns the running job; after a failed job it queues a
        new attempt, so an operator can retry once the cause is fixed.
        """

        self.get_context_space(principal, space_id)
        self._require(principal, Action.SPACE_MANAGE, space_id)
        latest = self._store.latest_job_for_space(space_id, JobOperation.SPACE_DELETION)
        if latest is not None and latest.state is not JobState.FAILED:
            return latest
        key = (
            f"space-deletion:{space_id}"
            if latest is None
            else f"space-deletion:{space_id}:after-{latest.id}"
        )
        self._store.set_space_state(space_id, SpaceState.DELETING)
        try:
            job, _ = self._store.enqueue_job(
                JobOperation.SPACE_DELETION,
                key,
                {"spaceId": space_id},
                principal.trace_id,
                self._max_job_attempts,
                principal.principal_id,
            )
        except IdempotencyConflict as exc:
            raise ConflictError("Idempotency key is already bound to another request") from exc
        self._metrics.increment("context_engine_space_deletions_accepted_total")
        return job

    def _accepting_space(self, principal: AuthenticatedPrincipal, space_id: str) -> ContextSpace:
        """Return a visible space that still accepts work; a space being deleted does not."""

        space = self.get_context_space(principal, space_id)
        if space.state is SpaceState.DELETING:
            raise ConflictError("Space is being deleted")
        return space

    # Space configuration

    def get_space_configuration(
        self, principal: AuthenticatedPrincipal, space_id: str
    ) -> SpaceConfigurationView:
        """Return a space's models and key kinds to a principal who manages it.

        Keys are never returned, only whether each is encrypted or a reference. Managing the
        space or its sources is required, because the models are configuration, not content.
        """

        self.get_context_space(principal, space_id)
        self._require_any(principal, _MANAGE_SOURCES, space_id)
        return self._configuration_view(space_id, self._store.get_space_configuration(space_id))

    def put_space_configuration(
        self,
        principal: AuthenticatedPrincipal,
        space_id: str,
        embedding: ModelInput | None,
        language: ModelInput | None,
    ) -> SpaceConfigurationView:
        """Replace a space's models for a principal holding space.manage.

        The whole configuration is replaced, so the stored state is exactly what was sent.
        Literal keys are encrypted before storage and references are kept as given: standalone
        deployments hold encrypted keys, Devant deployments reference the control plane's (ADR
        0014). The embedding model cannot change, be added, or be removed once the space has
        indexed content, because the stored vectors would no longer be comparable; that waits
        for reindexing in M7.
        """

        self._accepting_space(principal, space_id)
        self._require(principal, Action.SPACE_MANAGE, space_id)
        if embedding is None and language is None:
            raise ValidationError("Configure at least one model")
        current = self._store.get_space_configuration(space_id)
        current_embedding = current.embedding_model if current else None
        if self._embedding_locked(space_id) and not _same_model(current_embedding, embedding):
            raise ConflictError(
                "The embedding model cannot change once the space has indexed content"
            )
        stored = self._store.put_space_configuration(
            space_id,
            self._configured(embedding, current_embedding),
            self._configured(language, current.language_model if current else None),
            principal.principal_id,
        )
        self._metrics.increment("context_engine_space_configurations_changed_total")
        return self._configuration_view(space_id, stored)

    def _configured(
        self, submitted: ModelInput | None, current: ConfiguredModel | None
    ) -> ConfiguredModel | None:
        """Turn a submitted model into its stored form, storing its key as a token.

        A submission with neither a key nor a reference keeps the current key when the model
        is otherwise unchanged, so an administrator can resend the configuration the `GET`
        route showed without knowing the key.
        """

        if submitted is None:
            return None
        if (submitted.api_key is None) == (submitted.api_key_ref is None):
            if (
                submitted.api_key is None
                and current is not None
                and _same_model(current, submitted)
            ):
                return current
            raise ValidationError("Each model needs exactly one of apiKey or apiKeyRef")
        try:
            secret = (
                self._secrets.store_literal(submitted.api_key)
                if submitted.api_key is not None
                else self._secrets.store_reference(submitted.api_key_ref or "")
            )
        except SecretError as exc:
            if exc.code == "secrets_not_configured":
                raise ServiceUnavailableError(exc.message) from exc
            raise ValidationError(exc.message) from exc
        return ConfiguredModel(
            provider=submitted.provider.strip().lower(),
            model=submitted.model.strip(),
            secret=secret,
            endpoint=submitted.endpoint,
            api_version=submitted.api_version,
            dimensions=submitted.dimensions,
        )

    def _embedding_locked(self, space_id: str) -> bool:
        """Whether the space holds indexed content, which fixes its embedding model."""

        return bool(self._sources.indexed_partitions(space_id))

    def _configuration_view(
        self, space_id: str, configuration: SpaceConfiguration | None
    ) -> SpaceConfigurationView:
        def kind(model: ConfiguredModel | None) -> str | None:
            return self._secrets.kind(model.secret) if model else None

        return SpaceConfigurationView(
            configuration,
            self._embedding_locked(space_id),
            kind(configuration.embedding_model if configuration else None),
            kind(configuration.language_model if configuration else None),
        )

    # Sources

    def register_source(
        self,
        principal: AuthenticatedPrincipal,
        space_id: str,
        name: str,
        type_: str,
        version_ordering: VersionOrdering,
        audience_mapping: dict[str, str],
        structured_mapping: dict[str, str] | None = None,
    ) -> Source:
        """Register a source in a space for a principal that manages sources or the space.

        A source is the boundary for one connector feed. It fixes the space, holds the audience
        mapping and version ordering, and is the resource a connector's delivery grant points at, so
        a connector credential can only ever write into its own feed (M3, threat T02).

        A structured mapping, when present, names an RML mapping per content type; records of that
        type become graph nodes and edges instead of indexed text.
        """

        space = self._store.get_space(space_id)
        if space is None:
            raise NotFoundError("Context space not found")
        if space.state is SpaceState.DELETING:
            raise ConflictError("Space is being deleted")
        self._require_any(principal, _MANAGE_SOURCES, space_id)
        normalized_mapping = self._validate_structured_mapping(structured_mapping)
        source = self._sources.create_source(
            space_id,
            name.strip(),
            type_.strip(),
            version_ordering,
            audience_mapping,
            normalized_mapping,
        )
        self._metrics.increment("context_engine_sources_registered_total")
        return source

    def _validate_structured_mapping(
        self, structured_mapping: dict[str, str] | None
    ) -> dict[str, str] | None:
        """Return the mapping with normalized content-type keys, rejecting unsupported ones.

        The RML turtle itself is validated where it is used, under the knowledge provider, so the
        control plane keeps no dependency on an RDF toolchain.
        """

        if not structured_mapping:
            return None
        normalized: dict[str, str] = {}
        for content_type, mapping in structured_mapping.items():
            base = content_type.split(";", 1)[0].strip().lower()
            if base not in _STRUCTURED_CONTENT_TYPES:
                raise ValidationError(f"Structured mapping content type is not supported: {base}")
            if not mapping.strip():
                raise ValidationError("Structured mapping is empty")
            try:
                validate_mapping(mapping)
            except StructuredMaterializationError as exc:
                raise ValidationError(
                    f"Structured mapping for {base} is invalid: {exc.message}"
                ) from exc
            normalized[base] = mapping
        return normalized

    def list_sources(self, principal: AuthenticatedPrincipal, space_id: str) -> tuple[Source, ...]:
        """List the sources of a visible space.

        Anyone who can see the space may list its sources. Responses never include the audience
        mapping, which is write-only configuration.
        """

        self.get_context_space(principal, space_id)
        return self._sources.list_sources(space_id)

    def get_source(self, principal: AuthenticatedPrincipal, source_id: str) -> Source:
        """Return a source the principal can see, directly or through its space."""

        return self._visible_source(principal, source_id)

    def update_source(
        self,
        principal: AuthenticatedPrincipal,
        source_id: str,
        *,
        name: str | None,
        state: SourceState | None,
        audience_mapping: dict[str, str] | None,
        structured_mapping: dict[str, str] | None = None,
    ) -> Source:
        """Rename, pause or resume, remap audiences, or replace the structured mapping.

        Needs the same management rights as registering it. Pausing stops new deliveries without
        revoking the connector's credential or touching the source's records. A given structured
        mapping replaces the stored one wholesale, like the audience mapping; records already
        indexed are not re-materialized until their next version arrives.
        """

        self._visible_source(principal, source_id)
        self._require_any(principal, _MANAGE_SOURCES, source_id)
        normalized_mapping = self._validate_structured_mapping(structured_mapping)
        updated = self._sources.update_source(
            source_id,
            name=name,
            state=state,
            audience_mapping=audience_mapping,
            structured_mapping=normalized_mapping,
        )
        if updated is None:
            raise NotFoundError("Source not found")
        return updated

    # Checkpoints

    def get_checkpoint(self, principal: AuthenticatedPrincipal, source_id: str) -> SourceCheckpoint:
        """Return the stored connector cursor for a visible source.

        The cursor is the connector's position in its source system, not record content, so seeing
        the source is enough to read it.
        """

        self._visible_source(principal, source_id)
        checkpoint = self._sources.get_checkpoint(source_id)
        if checkpoint is None:
            raise NotFoundError("Checkpoint not found")
        return checkpoint

    def put_checkpoint(
        self, principal: AuthenticatedPrincipal, source_id: str, cursor: str
    ) -> SourceCheckpoint:
        """Store the connector cursor; requires delivery rights on the source.

        The engine keeps the cursor durably so a restarted connector resumes where it stopped. Only
        a principal that may deliver to the source can move it.
        """

        self._visible_source(principal, source_id)
        self._require(principal, Action.INGEST_WRITE, source_id)
        return self._sources.put_checkpoint(source_id, cursor, principal.principal_id)

    # Sync runs and progress

    def open_sync_run(self, principal: AuthenticatedPrincipal, source_id: str) -> SyncRun:
        """Mark a source as being read by its connector.

        Reading has no reliable total, so progress reports it as a state, not a percentage, and a
        new run supersedes any unfinished one (ADR 0010). Opening a run takes the same checks as a
        delivery, so a paused source refuses it.
        """

        source = self.authorize_upload(principal, source_id)
        run = self._sources.open_sync_run(source.id, principal.principal_id)
        self._metrics.increment("context_engine_sync_runs_opened_total")
        return run

    def complete_sync_run(
        self, principal: AuthenticatedPrincipal, source_id: str, run_id: str
    ) -> SyncRun:
        """Mark a sync run's reading as completed; repeating the call is harmless.

        Completing an already completed run returns it unchanged, so a connector can retry after a
        lost reply. A run superseded by a newer one is a conflict, because its reading never
        finished.
        """

        self._visible_source(principal, source_id)
        self._require(principal, Action.INGEST_WRITE, source_id)
        run = self._sources.get_sync_run(run_id)
        if run is None or run.source_id != source_id:
            raise NotFoundError("Sync run not found")
        if run.state is SyncRunState.SUPERSEDED:
            raise ConflictError("Sync run was superseded by a newer run")
        if run.state is SyncRunState.COMPLETED:
            return run
        completed = self._sources.complete_sync_run(run_id)
        if completed is None:
            raise NotFoundError("Sync run not found")
        return completed

    def _progress_for(self, source: Source) -> SourceProgress:
        """Assemble one source's progress: its latest sync run, jobs, records, and indexing.

        Job counts cover only the latest run's window, so a new scan starts from zero. Indexing
        comes from the ledger's per-record state, the source of truth, and only in provider mode
        (ADR 0010).
        """

        run = self._sources.latest_sync_run(source.id)
        # Processing is measured over the latest run's window so a new scan starts from zero.
        since = run.started_at if run else None
        return SourceProgress(
            source_id=source.id,
            sync_run=run,
            processing_since=since,
            jobs=self._store.count_source_jobs(source.id, since),
            records=self._sources.record_counts(source.id),
            # The ledger is the source of truth for indexing (ADR 0010 granularity decision).
            indexing=self._sources.index_snapshot(source.id) if self._indexing_enabled else None,
        )

    def source_progress(self, principal: AuthenticatedPrincipal, source_id: str) -> SourceProgress:
        """Return a source's pipeline progress to principals who deliver or manage it.

        Counts reveal record volume across every audience, so read access to the space is not
        enough.
        """

        source = self._visible_source(principal, source_id)
        self._require_any(principal, _INSPECT_DELIVERIES, source_id)
        return self._progress_for(source)

    def space_progress(
        self, principal: AuthenticatedPrincipal, space_id: str
    ) -> tuple[SourceProgress, ...]:
        """Return progress for the sources in a visible space the principal may inspect.

        Sources the caller may not inspect are left out rather than refused, so one call serves a
        connector that delivers to only some of the space's sources.
        """

        self.get_context_space(principal, space_id)
        result: list[SourceProgress] = []
        for source in self._sources.list_sources(space_id):
            actions = self._authorizer.effective_actions(
                principal.principal_id, principal.groups, source.id
            )
            if actions and actions & _INSPECT_DELIVERIES:
                result.append(self._progress_for(source))
        return tuple(result)

    # Staged uploads

    def authorize_upload(self, principal: AuthenticatedPrincipal, source_id: str) -> Source:
        """Check delivery rights on a ready source before any delivery work begins.

        Routes call this before reading a request body, so a caller without rights cannot make the
        engine read or store its bytes. A paused source refuses deliveries with a conflict.
        """

        source = self._visible_source(principal, source_id)
        self._require(principal, Action.INGEST_WRITE, source_id)
        self._source_accepting(source)
        return source

    def _source_accepting(self, source: Source) -> None:
        """Refuse deliveries to a paused source or to any source of a space being deleted."""

        if source.state is not SourceState.READY:
            raise ConflictError("Source is not accepting deliveries")
        space = self._store.get_space(source.space_id)
        if space is not None and space.state is SpaceState.DELETING:
            raise ConflictError("Space is being deleted")

    def stage_upload(
        self,
        principal: AuthenticatedPrincipal,
        source_id: str,
        content_type: str,
        data: bytes,
        idempotency_key: str,
    ) -> StagedUpload:
        """Validate and stage content bytes for a later ingestion event.

        The type allowlist, size limit, and SHA-256 hash are applied here, before any event can
        reference the bytes (threat T12). Uploads are idempotent per source and key: the same bytes
        return the existing upload, and different bytes are a conflict. Metadata is recorded before
        the bytes are written, so a retry with the same key rewrites bytes a crash left missing
        instead of creating a second upload.
        """

        self.authorize_upload(principal, source_id)
        normalized_type = content_type.split(";", 1)[0].strip().lower()
        if not normalized_type or normalized_type not in self._upload_policy.content_types:
            raise UnsupportedContentTypeError()
        if not data:
            raise ValidationError("Upload body is empty")
        if len(data) > self._upload_policy.max_bytes:
            raise PayloadTooLargeError()
        content_hash = "sha256:" + hashlib.sha256(data).hexdigest()
        try:
            upload, created = self._sources.create_upload(
                source_id,
                principal.principal_id,
                idempotency_key,
                normalized_type,
                len(data),
                content_hash,
                self._upload_policy.ttl_seconds,
            )
        except IdempotencyConflict as exc:
            raise ConflictError("Idempotency key is already bound to different content") from exc
        if created or not self._staging.exists(upload.id):
            # Metadata first, bytes second: a retried upload with the same key rewrites the bytes.
            self._staging.write(upload.id, data)
            self._metrics.increment("context_engine_uploads_staged_total")
        return upload

    # Ingestion and jobs

    def accept_ingestion(
        self,
        principal: AuthenticatedPrincipal,
        command: IngestionCommand,
        header_idempotency_key: str,
    ) -> Job:
        """Validate, authorize, and durably accept an idempotent ingestion command.

        The connector's delivery right is checked on the source, so the body cannot select a space
        or source the credential is not bound to (threat T02). Staged content is verified against
        the event's type and hash before the job exists (threat T12). The event's key makes
        acceptance idempotent: the same event returns the original job, so a connector can safely
        resend after a lost reply, and a different event under the key is a conflict.
        """

        source = self._check_delivery(principal, command, header_idempotency_key)
        if command.operation == "upsert":
            self._verify_staged_content(source, command)
        try:
            job, created = self._store.enqueue_job(
                command.job_operation,
                command.idempotency_key,
                command.to_payload(),
                principal.trace_id,
                self._max_job_attempts,
                principal.principal_id,
            )
        except IdempotencyConflict as exc:
            raise ConflictError("Idempotency key is already bound to another request") from exc
        metric = (
            "context_engine_jobs_accepted_total"
            if created
            else "context_engine_jobs_replayed_total"
        )
        self._metrics.increment(metric)
        return job

    def _check_delivery(
        self,
        principal: AuthenticatedPrincipal,
        command: IngestionCommand,
        header_idempotency_key: str,
    ) -> Source:
        """Check everything about a delivery that does not depend on its content.

        That covers the key header matching the event, the source existing in the named space and
        being visible, delivery rights, the source being ready, and the version fitting the source's
        ordering. Both delivery paths run it, and the one-call path runs it before staging, so a
        refused event leaves no upload behind.
        """

        if header_idempotency_key != command.idempotency_key:
            raise ValidationError("Idempotency key header and body must match")
        # A connector may hold rights on the source alone, so the source is the visibility
        # anchor; its registered space must match the body.
        source = self._sources.get_source(command.source_id)
        if (
            source is None
            or source.space_id != command.space_id
            or not self._visible(principal, source.id)
        ):
            raise NotFoundError("Source not found")
        self._require(principal, Action.INGEST_WRITE, source.id)
        self._source_accepting(source)
        try:
            source.version_key(command.source_version)
        except ValueError as exc:
            raise ValidationError("Source version does not match the source ordering") from exc
        return source

    def accept_direct_ingestion(
        self,
        principal: AuthenticatedPrincipal,
        source_id: str,
        command: IngestionCommand,
        content: bytes | None,
        content_type: str | None,
        header_idempotency_key: str,
    ) -> Job:
        """Stage content delivered with its event, then accept the event as a staged delivery.

        This is the one-call path, which spares connectors a separate upload (docs/TODO.md). The
        engine hashes the bytes itself; a hash or type in the event is only checked against them.
        The event's idempotency key also binds the bytes, so a replay with the same event and bytes
        returns the original job and different bytes are a conflict. After staging, the event goes
        through the same acceptance as a two-call delivery, so both paths share one set of rules.
        """

        if command.source_id != source_id:
            raise ValidationError("Event source must match the request path")
        if command.content_ref is not None:
            raise ValidationError("contentRef is not used when content travels with the event")
        # Everything that can reject the event is checked before its bytes are staged, so a
        # refused delivery leaves no upload behind.
        self._check_delivery(principal, command, header_idempotency_key)
        if command.operation == "upsert":
            if content is None:
                raise ValidationError("An upsert requires a content part")
            upload = self.stage_upload(
                principal,
                source_id,
                content_type or "",
                content,
                _DIRECT_UPLOAD_PREFIX + command.idempotency_key,
            )
            declared_type = (command.content_type or "").split(";", 1)[0].strip().lower()
            if (
                command.content_hash is not None and command.content_hash != upload.content_hash
            ) or (declared_type and declared_type != upload.content_type):
                raise ValidationError("Content does not match the ingestion event")
            command = replace(
                command,
                content_ref=upload.id,
                content_hash=upload.content_hash,
                content_type=upload.content_type,
            )
        elif content is not None:
            raise ValidationError("Only an upsert carries content")
        return self.accept_ingestion(principal, command, header_idempotency_key)

    def find_ingestion(
        self, principal: AuthenticatedPrincipal, source_id: str, idempotency_key: str
    ) -> Job:
        """Return the job a delivery created under a key, for a connector that lost the reply.

        Without it, the only way to recover a job id is to resend, which on the one-call path means
        resending the content. Only principals with delivery rights on the source may look keys up,
        and the lookup is scoped to that source.
        """

        self._visible_source(principal, source_id)
        self._require(principal, Action.INGEST_WRITE, source_id)
        job = self._store.find_source_job(source_id, idempotency_key, _DELIVERY_OPERATIONS)
        if job is None:
            raise NotFoundError("Ingestion not found")
        return job

    def _verify_staged_content(self, source: Source, command: IngestionCommand) -> None:
        """Confirm an event's staged content before a job exists.

        The upload must belong to the event's source, be unexpired, match the event's hash and type,
        and still have its bytes. Every mismatch gets the same message, so a caller cannot probe
        which check failed or whether another source's upload exists.
        """

        upload = self._sources.get_upload(command.content_ref or "")
        if (
            upload is None
            or upload.source_id != source.id
            or upload.is_expired(utc_now())
            or upload.content_hash != command.content_hash
            or upload.content_type != (command.content_type or "").split(";", 1)[0].strip().lower()
            or not self._staging.exists(upload.id)
        ):
            # One message for every mismatch so a caller cannot probe which check failed.
            raise ValidationError("Staged content does not match the ingestion event")

    def get_job(self, principal: AuthenticatedPrincipal, job_id: str) -> Job:
        """Return a job to its accepting principal or to a principal managing its resource.

        Anyone else gets not-found, as if the job did not exist, so a job id alone reveals nothing
        about another feed's activity.
        """

        job = self._store.get_job(job_id)
        if job is None:
            raise NotFoundError("Job not found")
        if job.principal_id == principal.principal_id:
            return job
        resource_id = job.resource_id
        actions = (
            self._authorizer.effective_actions(
                principal.principal_id, principal.groups, resource_id
            )
            if resource_id
            else None
        )
        if not actions or not actions & _INSPECT_DELIVERIES:
            raise NotFoundError("Job not found")
        return job

    def list_source_jobs(
        self, principal: AuthenticatedPrincipal, source_id: str, state: JobState | None
    ) -> tuple[Job, ...]:
        """List deliveries for a source, for example failed jobs awaiting inspection.

        Needs delivery or management rights, because job payloads name records and versions across
        every audience.
        """

        self._visible_source(principal, source_id)
        self._require_any(principal, _INSPECT_DELIVERIES, source_id)
        return self._store.list_source_jobs(source_id, state)

    def get_record_status(
        self, principal: AuthenticatedPrincipal, source_id: str, record_id: str
    ) -> RecordStatus:
        """Return a record's lifecycle state without content or backend details.

        Status reveals versions and states for any record id, so read access to the space is not
        enough; delivery or management rights are required.
        """

        source = self._visible_source(principal, source_id)
        # Status reveals versions and states for any record id, so readers are not enough.
        self._require_any(principal, _INSPECT_DELIVERIES, source_id)
        record = self._sources.get_record(source.space_id, source.id, record_id)
        if record is None:
            raise NotFoundError("Record not found")
        return record

    # Context queries

    async def query_context(
        self,
        principal: AuthenticatedPrincipal,
        space_id: str,
        question: str,
        mode: str,
        limit: int,
    ) -> ContextQueryResult:
        """Return authorized, source-linked passages for a question, or an answer from them.

        Engine policy picks the partitions first, the backend is asked as the caller so its own
        read checks apply as a second layer, and every passage then passes the visibility
        barrier against the ledger. Each surviving passage is placed in the text the engine
        indexed, and the query is stored with its evidence so the asker can reopen it (M5 slice
        2). In answer mode the space's language model then writes an answer from those passages
        alone, and only citations of them survive (slice 3, ADR 0015). A caller outside every
        audience gets insufficient evidence with no hint of what exists; that query is stored
        too, with no partitions and no evidence.
        """

        self._accepting_space(principal, space_id)
        self._require(principal, Action.CONTEXT_READ, space_id)
        if mode not in {"context", "answer"}:
            raise ValidationError("Mode must be context or answer")
        if not question.strip():
            raise ValidationError("Question is required")
        if self._backend is None:
            raise ServiceUnavailableError("Context queries need the knowledge backend")
        if mode == "answer" and self._answers is None:
            raise ServiceUnavailableError("Answer mode needs a language model")
        query_id = f"qry_{uuid4().hex}"
        decision = self._read_decision(principal, space_id, Action.CONTEXT_READ)
        partitions = decision.partitions if decision.allowed else ()
        self._metrics.increment("context_engine_queries_total")
        retrieved: tuple[EvidenceItem, ...] = ()
        models: ModelSelection | None = None
        if partitions:
            caller = PrincipalContext(principal.principal_id, principal.trace_id)
            try:
                models = await self._models.resolve(space_id)
            except SecretError as exc:
                raise ServiceUnavailableError("The space's model key is not available") from exc
            retrieved = await self._retrieve(
                QueryRequest(question, limit), caller, partitions, models
            )
        passed = self._visible_evidence(space_id, partitions, retrieved)
        evidence = (await self._placed_evidence(space_id, passed))[:limit]
        answer: str | None = None
        if mode == "answer":
            answer, evidence = await self._answer(question, evidence, models)
            insufficient = answer is None
        else:
            insufficient = not evidence
        if self._queries is not None:
            self._queries.record_query(
                StoredQuery(
                    id=query_id,
                    space_id=space_id,
                    principal_id=principal.principal_id,
                    trace_id=principal.trace_id,
                    mode=mode,
                    question=question,
                    limit=limit,
                    policy_version=decision.policy_version,
                    partitions=tuple(item.value for item in partitions),
                    outcome="insufficient_evidence" if insufficient else "completed",
                    retrieved=len(retrieved),
                    suppressed=len(retrieved) - len(passed),
                    models=self._models_used(models, language=mode == "answer" and bool(evidence))
                    if partitions
                    else (),
                    created_at=utc_now(),
                ),
                evidence,
                answer,
            )
        return ContextQueryResult(
            query_id, evidence, insufficient, principal.trace_id, answer=answer
        )

    async def _answer(
        self,
        question: str,
        evidence: tuple[PublicEvidence, ...],
        models: ModelSelection | None,
    ) -> tuple[str | None, tuple[PublicEvidence, ...]]:
        """Have the space's language model answer from the evidence, and check its citations.

        Only passages that passed the barrier and fit the character budget are sent, and the
        result lists exactly those, in the order the prompt numbers them, so `[n]` in the
        answer always names `evidence[n - 1]`. With no evidence the model is not called. A
        reply whose citations all name passages it was not given, or that says the passages do
        not answer the question, becomes insufficient evidence; the passages are still returned
        so the caller sees what was considered.
        """

        if not evidence:
            return None, ()
        assert self._answers is not None
        request = build_answer_request(
            question, [item.passage for item in evidence], self._answer_context_chars
        )
        given = evidence[: len(request.passages)]
        try:
            reply = await self._answers.write(request, models)
        except BackendError as exc:
            raise ServiceUnavailableError("Answer generation failed") from exc
        checked = check_citations(reply, len(given))
        self._metrics.increment("context_engine_answers_total")
        if checked is None:
            self._metrics.increment("context_engine_answers_insufficient_total")
            return None, given
        return checked.text, given

    def get_query(self, principal: AuthenticatedPrincipal, query_id: str) -> ContextQueryResult:
        """Reopen a stored query for the principal who asked it, with its evidence re-checked.

        Only the asker can reopen a query; anyone else gets not-found, as for jobs, so a query
        id reveals nothing. The asker still needs context.read on the space, and every passage
        passes the visibility barrier again against the asker's current audiences and the
        record's current version, so revoked, moved, and superseded evidence drops out (threat
        model T07). Traces and other principals' queries arrive with slice 4.
        """

        query = self._queries.get_query(query_id) if self._queries is not None else None
        if query is None or query.principal_id != principal.principal_id:
            raise NotFoundError("Query not found")
        self.get_context_space(principal, query.space_id)
        self._require(principal, Action.CONTEXT_READ, query.space_id)
        assert self._queries is not None
        stored = self._queries.query_evidence(query.id)
        evidence = self._still_visible(principal, query.space_id, stored, Action.CONTEXT_READ)
        if query.mode != "answer":
            return ContextQueryResult(query.id, evidence, not evidence, query.trace_id)
        # An answer may repeat any passage it was written from, so it is shown only while the
        # reader can still see all of them and none was erased with its version (slice 3).
        answered = query.outcome == "completed"
        answer = self._queries.get_answer(query.id) if answered else None
        complete = bool(stored) and len(evidence) == len(stored)
        shown = answer if answer is not None and complete else None
        return ContextQueryResult(
            query.id,
            evidence,
            not answered,
            query.trace_id,
            answer=shown,
            answer_withheld=answered and shown is None,
        )

    def get_evidence(self, principal: AuthenticatedPrincipal, evidence_id: str) -> PublicEvidence:
        """Open one evidence item for any principal who could retrieve that passage now.

        A citation can be followed by someone other than the asker, so this needs context.read
        or evidence.read on the space, membership in an audience of the record's current
        partition, and the record still at the evidence's version. evidence.read lets a
        principal open cited evidence without asking questions. Every refusal is the same
        not-found, so an evidence id reveals neither its space nor its record (threat model T07
        and T17).
        """

        stored = self._queries.get_evidence(evidence_id) if self._queries is not None else None
        if stored is None or not self._visible(principal, stored.space_id):
            raise NotFoundError("Evidence not found")
        for action in (Action.CONTEXT_READ, Action.EVIDENCE_READ):
            visible = self._still_visible(principal, stored.space_id, (stored.evidence,), action)
            if visible:
                return visible[0]
        raise NotFoundError("Evidence not found")

    def _read_decision(
        self, principal: AuthenticatedPrincipal, space_id: str, action: Action
    ) -> PolicyDecision:
        """Decide whether the caller may read the space, and which of its partitions.

        Only partitions holding indexed records are candidates. Engine policy decides with the any-
        audience rule, where a partition is readable when the caller is in any of its audiences (ADR
        0003). The backend never makes this decision; its own read check only backs it up.
        """

        indexed = self._sources.indexed_partitions(space_id)
        candidates = tuple(
            (AccessPartitionRef(item.id), frozenset(item.audiences))
            for item in self._sources.list_partitions(space_id)
            if item.id in indexed
        )
        actions = self._authorizer.effective_actions(
            principal.principal_id, principal.groups, space_id
        )
        return authorize(
            PolicyInput(
                principal_id=principal.principal_id,
                action=action,
                space_id=space_id,
                granted_actions=actions or frozenset(),
                principal_audiences=principal.groups,
                partition_audiences=candidates,
                policy_version=str(self._authorizer.policy_version()),
            )
        )

    async def _retrieve(
        self,
        request: QueryRequest,
        caller: PrincipalContext,
        partitions: tuple[AccessPartitionRef, ...],
        models: ModelSelection | None = None,
    ) -> tuple[EvidenceItem, ...]:
        """Ask the backend; if it refuses a partition the engine allows, ask one at a time.

        A refusal means the backend's read access lags engine policy, for example just after a
        grant. The worker is told to resynchronize, and the query continues partition by partition,
        skipping refusals, so one lagging partition does not fail the whole query. Any other backend
        failure surfaces as unavailable.
        """

        assert self._backend is not None
        try:
            return (await self._backend.query(request, caller, partitions, models=models)).evidence
        except BackendError as exc:
            if exc.code not in _RETRY_ONE_BY_ONE:
                raise ServiceUnavailableError("Context retrieval failed") from exc
        # The backend's read access lags engine policy; have the worker resynchronize it.
        self._metrics.increment("context_engine_query_backend_refusals_total")
        if self._read_access is not None:
            self._read_access.invalidate()
        collected: list[EvidenceItem] = []
        for partition in partitions:
            try:
                collected.extend(
                    (
                        await self._backend.query(request, caller, (partition,), models=models)
                    ).evidence
                )
            except BackendError as exc:
                if exc.code not in _RETRY_ONE_BY_ONE:
                    raise ServiceUnavailableError("Context retrieval failed") from exc
        return tuple(collected)

    def _live_copy(
        self, space_id: str, source_id: str, record_id: str, allowed: set[str]
    ) -> tuple[RecordStatus, RecordLocation] | None:
        """Return a record and its copy when the ledger vouches for them in an allowed partition.

        This is the visibility barrier (ADR 0007, ADR 0008): the ledger, not the backend or a
        stored row, decides what may be shown. The record must be active and indexed, in a
        partition the caller may read, and physically present there at its current version, so
        leftovers, stale versions, and records mid-move stay out.
        """

        record = self._sources.get_record(space_id, source_id, record_id)
        if record is None or not record.partition_id:
            return None
        location = self._sources.get_location(space_id, source_id, record_id, record.partition_id)
        if (
            location is None
            or record.state is not RecordState.ACTIVE
            or record.index_state is not IndexState.INDEXED
            or record.partition_id not in allowed
            or location.state is not LocationState.INDEXED
            or location.version != record.current_version
        ):
            return None
        return record, location

    def _visible_evidence(
        self,
        space_id: str,
        partitions: tuple[AccessPartitionRef, ...],
        retrieved: tuple[EvidenceItem, ...],
    ) -> list[tuple[EvidenceItem, RecordStatus, RecordLocation]]:
        """Keep the retrieved passages whose records the ledger vouches for, in rank order.

        A passage may come from the version currently recorded or from the version whose bytes
        were last written, which differ when a new version arrived with unchanged content.
        """

        allowed = {item.value for item in partitions}
        passed: list[tuple[EvidenceItem, RecordStatus, RecordLocation]] = []
        for item in retrieved:
            live = self._live_copy(space_id, item.source_id, item.record_id, allowed)
            if live is None or not item.passage.strip():
                continue
            record, location = live
            if item.source_version and item.source_version not in {
                record.current_version,
                location.written_version,
            }:
                continue
            passed.append((item, record, location))
        if dropped := len(retrieved) - len(passed):
            self._metrics.increment("context_engine_evidence_suppressed_total", dropped)
        return passed

    async def _placed_evidence(
        self,
        space_id: str,
        passed: list[tuple[EvidenceItem, RecordStatus, RecordLocation]],
    ) -> tuple[PublicEvidence, ...]:
        """Place each passage in its record's text and build public evidence, without repeats.

        Passages of one record are placed together, so chunk order can settle a repeated
        passage, and the record's text is read once per query. Evidence ids are derived from
        the record version, chunk, and passage, so the same passage always has the same id.
        """

        groups: dict[tuple[str, str], list[int]] = {}
        for position, (item, _, _) in enumerate(passed):
            groups.setdefault((item.source_id, item.record_id), []).append(position)
        locators: list[EvidenceLocator] = [
            EvidenceLocator(chunk_index=item.chunk_index) for item, _, _ in passed
        ]
        for positions in groups.values():
            _, record, location = passed[positions[0]]
            index = await asyncio.to_thread(self._text_index, record, location)
            if index is None:
                continue
            placed = locate(
                index, [(passed[p][0].passage, passed[p][0].chunk_index) for p in positions]
            )
            for position, locator in zip(positions, placed, strict=True):
                locators[position] = locator
        visible: dict[str, PublicEvidence] = {}
        for (item, record, _), locator in zip(passed, locators, strict=True):
            chunk = "" if item.chunk_index is None else str(item.chunk_index)
            digest = hashlib.sha256(
                "\0".join(
                    (
                        space_id,
                        item.source_id,
                        item.record_id,
                        record.current_version,
                        chunk,
                        item.passage,
                    )
                ).encode()
            ).hexdigest()[:24]
            evidence_id = f"evi_{digest}"
            visible.setdefault(
                evidence_id,
                PublicEvidence(
                    id=evidence_id,
                    record_id=item.record_id,
                    source_id=item.source_id,
                    source_version=record.current_version,
                    passage=item.passage,
                    locator=locator,
                    source_url=record.source_url,
                ),
            )
        return tuple(visible.values())

    def _text_index(self, record: RecordStatus, location: RecordLocation) -> TextIndex | None:
        """Rebuild the text the engine indexed for the record's current version.

        The current version's staged bytes are kept while the record is live, so the engine
        holds no second copy of the content. The text is trusted only when it comes from the
        same extractor version that produced the indexed copy; otherwise its offsets may not
        match the provider's chunks, and passages keep their chunk index alone.
        """

        upload = self._sources.get_upload(record.content_ref) if record.content_ref else None
        if upload is None:
            return None
        try:
            extracted = extract(upload.content_type, self._staging.read(upload.id))
        except (FileNotFoundError, ExtractionError):
            return None
        if location.parser_version and extracted.parser_version != location.parser_version:
            return None
        return TextIndex(extracted)

    def _still_visible(
        self,
        principal: AuthenticatedPrincipal,
        space_id: str,
        stored: Sequence[PublicEvidence],
        action: Action,
    ) -> tuple[PublicEvidence, ...]:
        """Keep stored evidence the principal could retrieve now, at the version it was taken."""

        decision = self._read_decision(principal, space_id, action)
        if not decision.allowed:
            return ()
        allowed = {item.value for item in decision.partitions}
        visible = []
        for item in stored:
            live = self._live_copy(space_id, item.source_id, item.record_id, allowed)
            if live is not None and live[0].current_version == item.source_version:
                visible.append(item)
        return tuple(visible)

    def _models_used(
        self, models: ModelSelection | None, *, language: bool = False
    ) -> tuple[tuple[str, ModelUse], ...]:
        """Name the models that served a query: the space's, or the engine's defaults.

        Retrieval always embeds the question, so the embedding model is always recorded. The
        language model is recorded only when it actually wrote an answer.
        """

        used: list[tuple[str, ModelUse]] = []
        kinds = (("embedding", "embedding_model"), ("language", "language_model"))
        for kind, field in kinds if language else kinds[:1]:
            chosen = getattr(models, field) if models is not None else None
            if chosen is not None:
                used.append((kind, ModelUse(chosen.provider, chosen.model, "space")))
            elif (default := self._default_models.get(kind)) is not None:
                used.append((kind, default))
        return tuple(used)

    # Enrichment

    def accept_enrichment(
        self, principal: AuthenticatedPrincipal, space_id: str, idempotency_key: str
    ) -> Job:
        """Queue a versioned enrichment of a space for a principal holding context.enrich.

        Enrichment calls models and can take minutes, so it runs as a job. The worker enriches each
        partition separately, so derived data never spans reader sets (ADR 0004). The pipeline
        version is recorded on the job, and the idempotency key makes a resend return the same job.
        """

        self._accepting_space(principal, space_id)
        self._require(principal, Action.CONTEXT_ENRICH, space_id)
        if not self._indexing_enabled:
            raise ServiceUnavailableError("Enrichment needs the knowledge backend")
        try:
            job, created = self._store.enqueue_job(
                JobOperation.ENRICHMENT,
                idempotency_key,
                {"spaceId": space_id, "pipelineVersion": ENRICHMENT_PIPELINE_VERSION},
                principal.trace_id,
                self._max_job_attempts,
                principal.principal_id,
            )
        except IdempotencyConflict as exc:
            raise ConflictError("Idempotency key is already bound to another request") from exc
        self._metrics.increment(
            "context_engine_enrichments_accepted_total"
            if created
            else "context_engine_jobs_replayed_total"
        )
        return job

    # Grants

    def list_grants(self, principal: AuthenticatedPrincipal, resource_id: str) -> tuple[Grant, ...]:
        """List grants on a resource for a principal holding access.manage on it."""

        if self._grants.resource_chain(resource_id) is None:
            raise NotFoundError()
        self._require(principal, Action.ACCESS_MANAGE, resource_id)
        return self._grants.list_grants(resource_id)

    def put_grant(
        self,
        principal: AuthenticatedPrincipal,
        resource_id: str,
        grant_id: str,
        actions: frozenset[Action],
        target_principal_id: str | None,
        target_group: str | None,
    ) -> Grant:
        """Create or replace a grant; the actor comes from the token, the target from the body.

        A grant names exactly one principal or one group. A principal must already be known to the
        engine; a group is not checked, because membership comes from the verified identity at
        request time. Grants on a resource also apply to everything beneath it.
        """

        if self._grants.resource_chain(resource_id) is None:
            raise NotFoundError()
        self._require(principal, Action.ACCESS_MANAGE, resource_id)
        if (target_principal_id is None) == (target_group is None):
            raise ValidationError("Grant must name exactly one of principalId or group")
        if not actions:
            raise ValidationError("Grant must contain at least one action")
        if (
            target_principal_id is not None
            and self._grants.get_principal(target_principal_id) is None
        ):
            raise ValidationError("Grant target is unknown")
        grant = self._grants.put_grant(
            resource_id,
            grant_id,
            actions,
            target_principal_id,
            target_group,
            principal.principal_id,
        )
        self._metrics.increment("context_engine_grants_changed_total")
        return grant

    def delete_grant(
        self, principal: AuthenticatedPrincipal, resource_id: str, grant_id: str
    ) -> None:
        """Delete a grant for a principal holding access.manage on the resource.

        Rights are decided on every request, so the next request sees the revocation, and queued
        jobs are re-authorized when they run (M2).
        """

        if self._grants.resource_chain(resource_id) is None:
            raise NotFoundError()
        self._require(principal, Action.ACCESS_MANAGE, resource_id)
        if not self._grants.delete_grant(resource_id, grant_id):
            raise NotFoundError("Grant not found")
        self._metrics.increment("context_engine_grants_changed_total")


def _same_model(current: ConfiguredModel | None, submitted: ModelInput | None) -> bool:
    """Whether a submission names the model already stored, ignoring its key."""

    if current is None or submitted is None:
        return current is None and submitted is None
    return (
        current.provider == submitted.provider.strip().lower()
        and current.model == submitted.model.strip()
        and current.dimensions == submitted.dimensions
        and (current.endpoint or None) == (submitted.endpoint or None)
        and (current.api_version or None) == (submitted.api_version or None)
    )
