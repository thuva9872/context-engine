"""Converge the knowledge backend to the ledger, one record at a time (ADR 0007).

The ledger says which partition and version each record should have; `record_locations`
says where copies actually are. Each call compares the two and writes, replaces, moves, or
removes copies until they match. Every backend write records its intent first, so a crash is
detected on the next attempt instead of being repeated blindly (ADR 0006). Every removal and
replacement is followed by a check that the old version is gone.
"""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace

from context_engine.domain import (
    IndexState,
    LocationState,
    RecordLocation,
    RecordState,
    RecordStatus,
    Source,
    StagedUpload,
)
from context_engine.ingestion.extraction import ExtractionError, extract
from context_engine.ingestion.structured import (
    StructuredMaterializationError,
    materialize,
)
from context_engine.knowledge_backend import (
    AccessPartitionRef,
    BackendError,
    BackendErrorCode,
    BackendReference,
    ExpectedRecord,
    IndexingProgressRequest,
    KnowledgeBackend,
    ModelSelection,
    PrincipalContext,
    SourceRecord,
    StructuredContent,
)
from context_engine.observability import MetricsRegistry, get_logger, log_event
from context_engine.persistence import SourceRepository, StagingStore

from .handler import TerminalJobError

logger = get_logger(__name__)

PartitionBound = Callable[[str, str], Awaitable[object]]
# Resolves the models a space is configured with, keys included, for one backend call.
ModelsFor = Callable[[str], Awaitable[ModelSelection | None]]


class _Failure(Exception):
    """A record-level indexing failure that retrying cannot fix."""

    def __init__(self, code: str, message: str, state: IndexState = IndexState.FAILED) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.state = state


@dataclass(frozen=True, slots=True)
class _Prepared:
    """One version's content ready to write: indexed text, or a materialized graph.

    ``structured`` is set when the record's content type has an RML mapping; the backend then
    writes its graph instead of indexing ``content`` as text. ``content`` still carries the
    record's text (the raw structured bytes for the graph path) so the backend keeps provenance.
    """

    content: str
    parser_version: str
    structured: StructuredContent | None


class RecordIndexer:
    """Write, replace, move, and remove backend copies so they match the ledger."""

    def __init__(
        self,
        sources: SourceRepository,
        backend: KnowledgeBackend,
        staging: StagingStore,
        service_principal_id: str,
        metrics: MetricsRegistry,
        on_partition_bound: PartitionBound | None = None,
        models_for: ModelsFor | None = None,
    ) -> None:
        self._sources = sources
        self._backend = backend
        self._staging = staging
        self._service_principal_id = service_principal_id
        self._metrics = metrics
        self._on_partition_bound = on_partition_bound
        # Resolves a space's configured models for each write; None keeps the backend default.
        self._models_for = models_for

    async def converge(self, source: Source, source_record_id: str, trace_id: str) -> IndexState:
        """Bring one record's backend copies in line with the ledger and return its state.

        Retryable backend errors propagate so the job retries with backoff. Anything that
        retrying cannot fix fails the job terminally and is recorded on the record.
        """

        # Content is always written as the engine's service identity, never as a caller.
        principal = PrincipalContext(self._service_principal_id, trace_id)
        record = self._sources.get_record(source.space_id, source.id, source_record_id)
        if record is None:
            return IndexState.NOT_INDEXED
        try:
            state = await self._converge(source, record, principal)
        except _Failure as failure:
            self._record_failure(source, record, failure.state, failure.code, trace_id)
            raise TerminalJobError(failure.code, failure.message) from failure
        except BackendError as exc:
            if exc.retryable:
                raise
            code = f"backend_{exc.code.value}"
            self._record_failure(source, record, IndexState.FAILED, code, trace_id)
            raise TerminalJobError(code, "Knowledge backend rejected the operation") from exc
        self._sources.set_index_state(source.space_id, source.id, source_record_id, state)
        return state

    async def purge(self, source: Source, source_record_id: str, trace_id: str) -> None:
        """Retire a record's remaining copies after their partitions were removed whole.

        This is ADR 0007's fallback for a copy that could not be proven removed on its own:
        the containing unit is dropped and the binding retired. Space deletion does that for
        every partition, so here each copy only has to be shown absent before its row goes.
        Nothing is written or deleted blindly; a copy that is still searchable fails the job
        with `residue_found` and keeps its row.
        """

        principal = PrincipalContext(self._service_principal_id, trace_id)
        record = self._sources.get_record(source.space_id, source.id, source_record_id)
        if record is None:
            return
        for location in self._sources.list_locations(source.space_id, source.id, source_record_id):
            version = location.version or location.target_version
            if version and not await self._absent(
                location.partition_id, source, record, version, principal
            ):
                self._record_failure(
                    source, record, IndexState.RECONCILE_REQUIRED, "residue_found", trace_id
                )
                raise TerminalJobError(
                    "residue_found", "A copy is still searchable after its partition was removed"
                )
            self._sources.drop_location(
                source.space_id, source.id, source_record_id, location.partition_id
            )
        self._sources.set_index_state(
            source.space_id, source.id, source_record_id, IndexState.NOT_INDEXED
        )

    def _record_failure(
        self, source: Source, record: RecordStatus, state: IndexState, code: str, trace_id: str
    ) -> None:
        self._sources.set_index_state(
            source.space_id, source.id, record.source_record_id, state, code
        )
        self._metrics.increment("context_engine_index_failures_total")
        log_event(logger, "record_index_failed", error_code=code, trace_id=trace_id)

    async def _converge(
        self, source: Source, record: RecordStatus, principal: PrincipalContext
    ) -> IndexState:
        locations = self._sources.list_locations(
            source.space_id, source.id, record.source_record_id
        )
        locations = await self._recover(source, record, locations, principal)
        desired = self._desired_partition(source, record)
        if desired is not None:
            await self._place(source, record, desired, locations, principal)
        # Copies anywhere else are stale: a moved record, a deleted one, or a quarantined one.
        for location in locations:
            if location.partition_id != desired:
                await self._remove(source, record, location, principal)
        return IndexState.INDEXED if desired is not None else IndexState.NOT_INDEXED

    def _desired_partition(self, source: Source, record: RecordStatus) -> str | None:
        if record.state is not RecordState.ACTIVE:
            return None
        if record.partition_id:
            return record.partition_id
        # Records written before partitions were persisted get theirs registered now.
        return self._sources.ensure_partition(source.space_id, record.audience)

    async def _recover(
        self,
        source: Source,
        record: RecordStatus,
        locations: tuple[RecordLocation, ...],
        principal: PrincipalContext,
    ) -> list[RecordLocation]:
        """Resolve copies left mid-operation by an earlier attempt."""

        settled: list[RecordLocation] = []
        for location in locations:
            if location.state is LocationState.RECONCILE_REQUIRED:
                raise _Failure(
                    "reconcile_required",
                    "The record needs reconciliation before it can be indexed",
                    IndexState.RECONCILE_REQUIRED,
                )
            if location.state is LocationState.REMOVING:
                await self._remove(source, record, location, principal)
                continue
            if location.state is LocationState.WRITING:
                target = location.target_version
                if target and not await self._absent(
                    location.partition_id, source, record, target, principal
                ):
                    self._set_location(source, record, location, LocationState.RECONCILE_REQUIRED)
                    raise _Failure(
                        "reconcile_required",
                        "A backend write may have finished without being recorded",
                        IndexState.RECONCILE_REQUIRED,
                    )
                # Nothing landed, so the write can simply be attempted again.
                self._sources.abort_location_write(
                    source.space_id, source.id, record.source_record_id, location.partition_id
                )
                if location.version is None:
                    continue
                location = replace(location, state=LocationState.INDEXED, target_version=None)
            settled.append(location)
        return settled

    async def _place(
        self,
        source: Source,
        record: RecordStatus,
        partition_id: str,
        locations: list[RecordLocation],
        principal: PrincipalContext,
    ) -> None:
        """Make the current version searchable in the desired partition."""

        target = next((item for item in locations if item.partition_id == partition_id), None)
        replacing = target is not None and target.state is LocationState.INDEXED
        if replacing and target.version == record.current_version:
            return
        if replacing and target.content_hash == record.content_hash:
            # Same bytes under a new version number: nothing to rewrite (ADR 0007).
            self._sources.confirm_location(
                source.space_id,
                source.id,
                record.source_record_id,
                partition_id,
                record.current_version,
                target.backend_ref or "",
                record.content_hash or "",
                None,
                written=False,
            )
            return
        try:
            prepared = self._prepare(source, record)
        except _Failure:
            # The current version cannot be indexed, so no older copy may stay searchable.
            for location in locations:
                await self._remove(source, record, location, principal)
            raise
        partition = AccessPartitionRef(partition_id)
        backend_record = SourceRecord(
            record_id=record.source_record_id,
            source_id=source.id,
            version=record.current_version,
            content=prepared.content,
            content_hash=record.content_hash or "",
            source_url=record.source_url,
            attributes=(("space_id", source.space_id),),
        )
        self._sources.begin_location_write(
            source.space_id,
            source.id,
            record.source_record_id,
            partition_id,
            record.current_version,
        )
        # The space's models are revealed just before the write and live only for the call.
        models = await self._models_for(source.space_id) if self._models_for else None
        try:
            if replacing:
                result = await self._backend.update(
                    backend_record, principal, partition, models=models,
                    structured=prepared.structured,
                )
            else:
                result = await self._backend.ingest(
                    backend_record, principal, partition, models=models,
                    structured=prepared.structured,
                )
        except BackendError as exc:
            self._handle_write_error(source, record, partition_id, exc)
            raise
        self._sources.confirm_location(
            source.space_id,
            source.id,
            record.source_record_id,
            partition_id,
            record.current_version,
            result.backend_reference.value,
            record.content_hash or "",
            prepared.parser_version,
        )
        self._metrics.increment("context_engine_index_writes_total")
        if (
            replacing
            and target.version
            and not await self._absent(partition_id, source, record, target.version, principal)
        ):
            self._sources.set_location_state(
                source.space_id,
                source.id,
                record.source_record_id,
                partition_id,
                LocationState.RECONCILE_REQUIRED,
            )
            raise _Failure(
                "residue_found",
                "A replaced version is still searchable",
                IndexState.RECONCILE_REQUIRED,
            )
        if not replacing and self._on_partition_bound is not None:
            await self._on_partition_bound(partition_id, principal.trace_id)

    def _handle_write_error(
        self, source: Source, record: RecordStatus, partition_id: str, exc: BackendError
    ) -> None:
        key = (source.space_id, source.id, record.source_record_id, partition_id)
        if exc.code is BackendErrorCode.PARTIAL_WRITE:
            self._sources.set_location_state(*key, LocationState.RECONCILE_REQUIRED)
            raise _Failure(
                "backend_partial_write",
                "The backend wrote part of the record",
                IndexState.RECONCILE_REQUIRED,
            ) from exc
        if exc.code is BackendErrorCode.TIMEOUT:
            # The write may or may not have landed; the next attempt checks before retrying.
            return
        self._sources.abort_location_write(*key)

    async def _remove(
        self,
        source: Source,
        record: RecordStatus,
        location: RecordLocation,
        principal: PrincipalContext,
    ) -> None:
        """Delete one copy and confirm that nothing of it stays searchable."""

        if location.state is LocationState.RECONCILE_REQUIRED:
            raise _Failure(
                "reconcile_required",
                "The record needs reconciliation before it can be removed",
                IndexState.RECONCILE_REQUIRED,
            )
        self._set_location(source, record, location, LocationState.REMOVING)
        if location.backend_ref:
            try:
                await self._backend.delete(
                    BackendReference(location.backend_ref),
                    principal,
                    AccessPartitionRef(location.partition_id),
                )
            except BackendError as exc:
                if exc.code is not BackendErrorCode.NOT_FOUND:
                    raise
        version = location.version or location.target_version
        if version and not await self._absent(
            location.partition_id, source, record, version, principal
        ):
            self._set_location(source, record, location, LocationState.RECONCILE_REQUIRED)
            raise _Failure(
                "residue_found",
                "A removed copy is still searchable",
                IndexState.RECONCILE_REQUIRED,
            )
        self._sources.drop_location(
            source.space_id, source.id, record.source_record_id, location.partition_id
        )
        self._metrics.increment("context_engine_index_removals_total")

    def _set_location(
        self,
        source: Source,
        record: RecordStatus,
        location: RecordLocation,
        state: LocationState,
    ) -> None:
        self._sources.set_location_state(
            source.space_id, source.id, record.source_record_id, location.partition_id, state
        )

    async def _absent(
        self,
        partition_id: str,
        source: Source,
        record: RecordStatus,
        version: str,
        principal: PrincipalContext,
    ) -> bool:
        partition = AccessPartitionRef(partition_id)
        progress = await self._backend.indexing_progress(
            IndexingProgressRequest(
                source.id, (ExpectedRecord(partition, record.source_record_id, version),)
            ),
            principal,
            (partition,),
        )
        return progress.missing == 1

    def _prepare(self, source: Source, record: RecordStatus) -> _Prepared:
        """Read and verify the staged bytes, then produce text or a materialized graph.

        A content type the source registered an RML mapping for is materialized to a graph; every
        other type is extracted to text. A bad mapping or unextractable content is a record-level
        failure, so the caller removes any older copy rather than leave it searchable.
        """

        upload = self._sources.get_upload(record.content_ref) if record.content_ref else None
        if upload is None:
            raise _Failure("content_missing", "No staged content exists for the current version")
        try:
            data = self._staging.read(upload.id)
        except FileNotFoundError as exc:
            raise _Failure("content_missing", "Staged content was removed") from exc
        # Bytes are checked again at execution time, not only when the event was accepted.
        if "sha256:" + hashlib.sha256(data).hexdigest() != record.content_hash:
            raise _Failure("content_mismatch", "Staged content no longer matches its hash")

        mapping = source.mapping_for(upload.content_type)
        if mapping is not None:
            reason: str | None = None
            try:
                graph = materialize(upload.content_type, data, mapping)
                if len(graph) > 0:
                    # The structured bytes are the provenance text; the graph carries the nodes.
                    return _Prepared(
                        content=data.decode("utf-8", errors="replace"),
                        parser_version="rml@1",
                        structured=StructuredContent(graph=graph),
                    )
                # No triples: the record lacks the fields the subject needs, or the mapping does
                # not match its shape.
                reason = "structured_materialization_empty"
            except StructuredMaterializationError as exc:
                reason = exc.code
            # The mapping produced no usable graph for this record. Rather than lose it, fall
            # through to the text path so it is still indexed; the fallback is logged and counted
            # so a mapping that fails for every record stays visible.
            log_event(
                logger,
                "structured_materialization_fell_back",
                source_id=source.id,
                record_id=record.source_record_id,
                reason=reason,
            )
            self._metrics.increment("context_engine_structured_fallback_total")
            return self._as_text(upload, data, allow_plain_fallback=True)
        return self._as_text(upload, data, allow_plain_fallback=False)

    def _as_text(
        self, upload: StagedUpload, data: bytes, *, allow_plain_fallback: bool
    ) -> _Prepared:
        """Extract staged bytes to text for the text path, as a mappingless source or RML fallback.

        On the structured fallback a content type the text path cannot parse (CSV, XML) is read as
        plain text, so a structured record is indexed rather than dropped when its mapping yields
        nothing. Any extraction failure is a record-level failure, as on the ordinary text path.
        """

        try:
            extracted = extract(upload.content_type, data)
        except ExtractionError as exc:
            if not (allow_plain_fallback and exc.code == "extraction_unsupported"):
                raise _Failure(exc.code, exc.message) from exc
            try:
                extracted = extract("text/plain", data)
            except ExtractionError as plain_exc:
                raise _Failure(plain_exc.code, plain_exc.message) from plain_exc
        return _Prepared(
            content=extracted.text, parser_version=extracted.parser_version, structured=None
        )
