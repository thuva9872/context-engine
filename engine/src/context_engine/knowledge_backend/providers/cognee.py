"""Private Cognee 1.5.4 mapping for the engine-owned backend port."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import io
import json
import logging
import os
import secrets
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from context_engine.config import KnowledgeBackendSettings

from ..errors import BackendError, BackendErrorCode
from ..state import BackendStateStore, InMemoryBackendState
from ..types import (
    AccessPartitionRef,
    AnswerRequest,
    BackendCapabilities,
    BackendHealth,
    BackendReference,
    DeletionResult,
    EnrichmentRequest,
    EnrichmentResult,
    EvidenceItem,
    ExpectedRecord,
    IndexingProgress,
    IndexingProgressRequest,
    IngestionResult,
    ModelSelection,
    PrincipalContext,
    QueryRequest,
    QueryResult,
    SourceRecord,
    StructuredContent,
)


@dataclass(frozen=True, slots=True)
class _CogneeBinding:
    dataset_name: str
    dataset_id: str | None = None


@dataclass(frozen=True, slots=True)
class _NativeIngestion:
    dataset_id: str
    data_id: str
    created: bool


class _CogneeRuntimePort(Protocol):
    """Private seam between engine translation and native SDK calls."""

    async def remember(
        self,
        record: SourceRecord,
        binding: _CogneeBinding,
        user: Any,
        models: ModelSelection | None = None,
    ) -> _NativeIngestion:
        """Invoke native ingestion with the selected models and return native identifiers."""

        ...

    async def remember_datapoints(
        self,
        record: SourceRecord,
        binding: _CogneeBinding,
        user: Any,
        graph: Any,
        models: ModelSelection | None = None,
    ) -> _NativeIngestion:
        """Register one record and write its RDF graph as owned nodes and edges."""

        ...

    async def recall(
        self,
        request: QueryRequest,
        bindings: tuple[_CogneeBinding, ...],
        user: Any,
        models: ModelSelection | None = None,
    ) -> list[Any]:
        """Invoke native retrieval within explicit bindings with the selected models."""

        ...

    async def grant_read(self, binding: _CogneeBinding, reader: Any, owner: Any) -> None:
        """Give a native user read permission on one binding, acting as its owner."""

        ...

    async def revoke_read(self, binding: _CogneeBinding, reader: Any, owner: Any) -> None:
        """Remove a native user's read permission on one binding, acting as its owner."""

        ...

    async def improve(
        self, binding: _CogneeBinding, user: Any, models: ModelSelection | None = None
    ) -> Any:
        """Invoke explicit native enrichment for one binding with the selected models."""

        ...

    async def forget(self, binding: _CogneeBinding, data_id: str, user: Any) -> Any:
        """Invoke native deletion for one bound record."""

        ...

    async def forget_unit(self, binding: _CogneeBinding, user: Any) -> Any:
        """Invoke native deletion of a whole isolation unit, derived data included."""

        ...

    async def ensure_user(self, handle: str) -> Any:
        """Return the native user with this deterministic handle, creating it once."""

        ...

    async def get_user(self, native_id: str) -> Any:
        """Return a native user by its native identifier."""

        ...

    async def list_items(self, binding: _CogneeBinding, user: Any) -> list[Any]:
        """List the native content items stored in one binding."""

        ...

    async def processing_states(self, bindings: tuple[_CogneeBinding, ...]) -> dict[str, Any]:
        """Return the latest native processing-run state keyed by native binding id."""

        ...

    async def health(self) -> tuple[bool, str]:
        """Report native runtime readiness and a safe detail string."""

        ...


class _BindingStore:
    """Resolve partition bindings through the engine's durable backend state."""

    def __init__(self, state: BackendStateStore, prefix: str = "context-engine") -> None:
        self._state = state
        self._prefix = prefix

    def resolve(self, partition: AccessPartitionRef) -> _CogneeBinding:
        """Return the stored binding, or the deterministic unbound one for a new partition."""

        stored = self._state.get_binding(partition.value)
        if stored:
            value = json.loads(stored)
            return _CogneeBinding(dataset_name=value["name"], dataset_id=value.get("id"))
        # Hash the engine identifier so the native name discloses no caller-controlled value.
        suffix = hashlib.sha256(partition.value.encode()).hexdigest()[:20]
        return _CogneeBinding(dataset_name=f"{self._prefix}-{suffix}")

    def set_dataset_id(self, partition: AccessPartitionRef, dataset_id: str) -> None:
        """Persist the native isolation identifier learned during the first ingestion.

        A partition that is already bound must never move to another native unit: that would
        leave its earlier content behind under an identifier the engine no longer tracks.
        """

        current = self.resolve(partition)
        if current.dataset_id == dataset_id:
            return
        if current.dataset_id is not None:
            raise BackendError(
                BackendErrorCode.PARTIAL_WRITE,
                "Provider bound the partition to an unexpected isolation unit",
            )
        encoded = json.dumps({"name": current.dataset_name, "id": dataset_id}, sort_keys=True)
        self._state.put_binding(partition.value, encoded)


class CogneeIdentityResolver:
    """Map engine principals to native users durably; there is never a default user.

    Each principal gets its own ordinary native account under a deterministic handle, so a
    crash between creating the account and recording it recovers on the next call.
    """

    def __init__(self, runtime: _CogneeRuntimePort, state: BackendStateStore) -> None:
        self._runtime = runtime
        self._state = state

    async def __call__(self, principal: PrincipalContext) -> Any:
        """Return the native user for an engine principal."""

        stored = self._state.get_identity(principal.principal_id)
        if stored is not None:
            return await self._runtime.get_user(stored)
        user = await self._runtime.ensure_user(_native_handle(principal.principal_id))
        native_id = str(getattr(user, "id", "") or "")
        if not native_id:
            raise BackendError(
                BackendErrorCode.UNSUPPORTED, "Provider did not return a stable identity"
            )
        effective = self._state.claim_identity(principal.principal_id, native_id)
        # A concurrent resolver may have claimed first; everyone uses the stored identity.
        return user if effective == native_id else await self._runtime.get_user(effective)


def _native_handle(principal_id: str) -> str:
    """Return a deterministic native login handle that reveals nothing about the principal."""

    digest = hashlib.sha256(principal_id.encode()).hexdigest()[:32]
    # The provider validates handles as email addresses and rejects special-use domains such as
    # `.invalid`; `.internal` is reserved for private use and never delegated. No mail is sent.
    return f"engine-{digest}@context-engine.internal"


class _RecordUpload:
    """Hand one record's text to the provider as an upload.

    The provider reads a plain string as a local path, web address, or object-store key when
    it looks like one, and keeps the string form of its input in its run history, which
    deletion never clears. An upload is stored exactly as given, and its string form carries
    no content.
    """

    # The provider records a source location for uploads unless the name is in angle brackets.
    filename = "<engine-record>.txt"

    def __init__(self, text: str) -> None:
        self.file = io.BytesIO(text.encode("utf-8"))

    def __repr__(self) -> str:
        return "<engine record>"


def _native_item_id(binding: _CogneeBinding, record: SourceRecord) -> UUID:
    """Choose the native item id of one record version in one binding.

    The engine pins the id so every write names its own item: without a pin the provider
    reuses any item with the same content, and its results cannot tell which item a call
    added. The id is derived from the binding, record, version, and content, so a retried
    write lands on the same item instead of leaving an untracked one behind.
    """

    seed = "\0".join(
        (
            "context-engine-item",
            binding.dataset_name,
            record.source_id,
            record.record_id,
            record.version,
            record.content_hash,
        )
    )
    return uuid5(NAMESPACE_URL, seed)


def _native_reference(dataset_id: str, data_id: str) -> BackendReference:
    return BackendReference(f"cognee:{dataset_id}:{data_id}")


def _parse_reference(reference: BackendReference) -> tuple[str, str]:
    prefix, separator, remainder = reference.value.partition(":")
    dataset_id, second_separator, data_id = remainder.partition(":")
    if prefix != "cognee" or not separator or not second_separator or not dataset_id or not data_id:
        raise BackendError(BackendErrorCode.INVALID_INPUT, "Invalid backend reference")
    return dataset_id, data_id


class CogneeBackend:
    """Private adapter that translates all native values at the port boundary."""

    def __init__(
        self,
        runtime: _CogneeRuntimePort,
        user_resolver: Callable[[PrincipalContext], Awaitable[Any]],
        state: BackendStateStore | None = None,
    ) -> None:
        self._runtime = runtime
        self._user_resolver = user_resolver
        # Production passes the control-database store; the in-memory default serves tests.
        self._state = state or InMemoryBackendState()
        self._bindings = _BindingStore(self._state)

    @staticmethod
    def _partitions(value: tuple[AccessPartitionRef, ...]) -> None:
        if not value:
            raise BackendError(
                BackendErrorCode.ACCESS_DENIED,
                "At least one authorized access partition is required",
            )

    async def ingest(
        self,
        record: SourceRecord,
        principal: PrincipalContext,
        partition: AccessPartitionRef,
        models: ModelSelection | None = None,
        structured: StructuredContent | None = None,
    ) -> IngestionResult:
        """Translate and ingest one engine record in an explicit partition.

        A structured record is written as graph nodes and edges from its materialized RDF; an
        ordinary record is indexed as text. Both pin the same native item id and return the same
        kind of reference, so the engine tracks and reconciles them identically.
        """

        self._partitions((partition,))
        try:
            user = await self._user_resolver(principal)
            binding = self._bindings.resolve(partition)
            if structured is not None:
                result = await self._runtime.remember_datapoints(
                    record, binding, user, structured.graph, models
                )
            else:
                result = await self._runtime.remember(record, binding, user, models)
            self._bindings.set_dataset_id(partition, result.dataset_id)
            reference = _native_reference(result.dataset_id, result.data_id)
            self._state.put_record_reference(
                partition.value, record.source_id, record.record_id, reference.value
            )
            return IngestionResult(
                record.record_id,
                record.version,
                reference,
                result.created,
            )
        except BackendError:
            raise
        except Exception as exc:
            raise _translate_error(exc) from exc

    async def query(
        self,
        request: QueryRequest,
        principal: PrincipalContext,
        authorized_partitions: tuple[AccessPartitionRef, ...],
        models: ModelSelection | None = None,
    ) -> QueryResult:
        """Retrieve and translate evidence from authorized bindings."""

        self._partitions(authorized_partitions)
        bindings = tuple(self._bindings.resolve(item) for item in authorized_partitions)
        if any(binding.dataset_id is None for binding in bindings):
            raise BackendError(BackendErrorCode.NOT_FOUND, "Access partition is not initialized")
        try:
            user = await self._user_resolver(principal)
            native_results = await self._runtime.recall(request, bindings, user, models)
            units = {
                binding.dataset_id: partition.value
                for binding, partition in zip(bindings, authorized_partitions, strict=True)
                if binding.dataset_id
            }
            evidence = self._translate_results(native_results, units)
            return QueryResult(evidence[: request.limit], insufficient_evidence=not evidence)
        except BackendError:
            raise
        except Exception as exc:
            raise _translate_error(exc) from exc

    def _translate_results(
        self, native_results: list[Any], units: dict[str, str]
    ) -> tuple[EvidenceItem, ...]:
        """Turn retrieved native chunks into engine evidence through durable references.

        Each native entry is one retrieved chunk that names the item it came from. Only items
        the engine wrote, whose reference is still recorded, and that sit in an authorized
        partition become evidence; anything else, including leftovers of replaced versions and
        any entry of another shape, stays out (ADR 0008). The version is left to the engine,
        which takes it from the ledger.

        The provider returns chunks in rank order within each unit but no comparable score, so
        units are interleaved by rank and the score is derived from that rank.
        """

        ranked: list[tuple[int, int, EvidenceItem]] = []
        seen: set[str] = set()
        unit_order = {unit: position for position, unit in enumerate(units)}
        unit_ranks: dict[str, int] = {}
        single_unit = next(iter(units)) if len(units) == 1 else None
        for native in native_results:
            entry = _as_mapping(native)
            chunk = _native_chunk(entry)
            if chunk is None:
                continue
            item_id, chunk_id, passage, chunk_index = chunk
            unit = str(entry.get("dataset_id") or single_unit or "")
            partition = units.get(unit)
            if partition is None:
                continue
            rank = unit_ranks.get(unit, 0)
            unit_ranks[unit] = rank + 1
            location = self._state.find_record(_native_reference(unit, item_id).value)
            if location is None or location[0] != partition:
                continue
            _, source_id, record_id = location
            digest = hashlib.sha256(
                f"{partition}\0{source_id}\0{record_id}\0{chunk_id}\0{passage}".encode()
            ).hexdigest()[:24]
            item = EvidenceItem(
                evidence_id=f"evi_{digest}",
                record_id=record_id,
                source_id=source_id,
                source_version="",
                passage=passage,
                score=1.0 / (rank + 1),
                chunk_index=chunk_index,
            )
            if item.evidence_id in seen:
                continue
            seen.add(item.evidence_id)
            ranked.append((rank, unit_order[unit], item))
        ranked.sort(key=lambda value: (value[0], value[1]))
        return tuple(item for _, _, item in ranked)

    async def update(
        self,
        record: SourceRecord,
        principal: PrincipalContext,
        partition: AccessPartitionRef,
        models: ModelSelection | None = None,
        structured: StructuredContent | None = None,
    ) -> IngestionResult:
        """Update a previously bound engine record."""

        self._partitions((partition,))
        binding = self._bindings.resolve(partition)
        if binding.dataset_id is None:
            raise BackendError(BackendErrorCode.NOT_FOUND, "Access partition is not initialized")
        stored = self._state.get_record_reference(
            partition.value, record.source_id, record.record_id
        )
        if stored is None:
            raise BackendError(BackendErrorCode.NOT_FOUND, "Record binding is not initialized")
        _, previous_item = _parse_reference(BackendReference(stored))
        try:
            user = await self._user_resolver(principal)
            # Replace rather than edit in place: the new item carries the new version in its
            # metadata, which evidence lineage and progress rely on (ADR 0007). The old item's
            # nodes and edges are owned by its data id, so forgetting it below reclaims them.
            if structured is not None:
                result = await self._runtime.remember_datapoints(
                    record, binding, user, structured.graph, models
                )
            else:
                result = await self._runtime.remember(record, binding, user, models)
            if result.dataset_id != binding.dataset_id:
                raise BackendError(
                    BackendErrorCode.PARTIAL_WRITE,
                    "Provider wrote the replacement to an unexpected isolation unit",
                )
            reference = _native_reference(result.dataset_id, result.data_id)
            self._state.put_record_reference(
                partition.value, record.source_id, record.record_id, reference.value
            )
            if result.data_id != previous_item:
                await self._runtime.forget(binding, previous_item, user)
            return IngestionResult(record.record_id, record.version, reference, created=False)
        except BackendError:
            raise
        except Exception as exc:
            raise _translate_error(exc) from exc

    async def grant_read(
        self,
        partition: AccessPartitionRef,
        reader: PrincipalContext,
        principal: PrincipalContext,
    ) -> None:
        """Give a reader native read permission on the partition's binding."""

        await self._change_read(partition, reader, principal, grant=True)

    async def revoke_read(
        self,
        partition: AccessPartitionRef,
        reader: PrincipalContext,
        principal: PrincipalContext,
    ) -> None:
        """Remove a reader's native read permission on the partition's binding."""

        await self._change_read(partition, reader, principal, grant=False)

    async def _change_read(
        self,
        partition: AccessPartitionRef,
        reader: PrincipalContext,
        principal: PrincipalContext,
        *,
        grant: bool,
    ) -> None:
        self._partitions((partition,))
        binding = self._bindings.resolve(partition)
        if binding.dataset_id is None:
            raise BackendError(BackendErrorCode.NOT_FOUND, "Access partition is not initialized")
        try:
            owner = await self._user_resolver(principal)
            user = await self._user_resolver(reader)
            if grant:
                await self._runtime.grant_read(binding, user, owner)
            else:
                await self._runtime.revoke_read(binding, user, owner)
        except BackendError:
            raise
        except Exception as exc:
            raise _translate_error(exc) from exc

    async def enrich(
        self,
        request: EnrichmentRequest,
        principal: PrincipalContext,
        authorized_partitions: tuple[AccessPartitionRef, ...],
        models: ModelSelection | None = None,
    ) -> EnrichmentResult:
        """Run explicit native enrichment for authorized bindings."""

        self._partitions(authorized_partitions)
        bindings = tuple(self._bindings.resolve(item) for item in authorized_partitions)
        if any(binding.dataset_id is None for binding in bindings):
            raise BackendError(BackendErrorCode.NOT_FOUND, "Access partition is not initialized")
        try:
            user = await self._user_resolver(principal)
            for binding in bindings:
                await self._runtime.improve(binding, user, models)
            return EnrichmentResult(request.operation_id, affected_records=0)
        except BackendError:
            raise
        except Exception as exc:
            raise _translate_error(exc) from exc

    async def delete(
        self,
        reference: BackendReference,
        principal: PrincipalContext,
        partition: AccessPartitionRef,
    ) -> DeletionResult:
        """Delete one bound record after validating its partition scope."""

        self._partitions((partition,))
        dataset_id, data_id = _parse_reference(reference)
        binding = self._bindings.resolve(partition)
        # A valid opaque reference is still unusable outside its engine-resolved partition.
        if binding.dataset_id != dataset_id:
            raise BackendError(BackendErrorCode.ACCESS_DENIED, "Backend reference is out of scope")
        try:
            user = await self._user_resolver(principal)
            await self._runtime.forget(binding, data_id, user)
            location = self._state.find_record(reference.value)
            record_id = location[2] if location and location[0] == partition.value else "unknown"
            self._state.delete_record_reference(reference.value)
            return DeletionResult(record_id=record_id, deleted=True)
        except BackendError:
            raise
        except Exception as exc:
            raise _translate_error(exc) from exc

    async def delete_partition(
        self,
        partition: AccessPartitionRef,
        principal: PrincipalContext,
    ) -> None:
        """Remove the partition's isolation unit and the engine's memory of it.

        The native unit, its graph, its vectors, and its permissions go together; the binding
        and record references are dropped afterwards so a later write to the same partition
        creates a fresh unit. A unit the provider no longer has counts as removed, and a
        partition that never received a write has no unit to remove.
        """

        self._partitions((partition,))
        binding = self._bindings.resolve(partition)
        if binding.dataset_id is not None:
            try:
                user = await self._user_resolver(principal)
                await self._runtime.forget_unit(binding, user)
            except BackendError as exc:
                if exc.code is not BackendErrorCode.NOT_FOUND:
                    raise
            except Exception as exc:
                translated = _translate_error(exc)
                if translated.code is not BackendErrorCode.NOT_FOUND:
                    raise translated from exc
        self._state.delete_partition(partition.value)

    async def indexing_progress(
        self,
        request: IndexingProgressRequest,
        principal: PrincipalContext,
        authorized_partitions: tuple[AccessPartitionRef, ...],
    ) -> IndexingProgress:
        """Attribute native per-binding processing state to one source's record versions.

        Native status is kept per binding run, not per item, so a present item takes the state
        of its binding's latest run. Items are matched to the source through the engine
        metadata attached at ingestion. Uninitialized bindings hold nothing.
        """

        self._partitions(authorized_partitions)
        allowed = {item.value for item in authorized_partitions}
        if any(item.partition.value not in allowed for item in request.records):
            raise BackendError(BackendErrorCode.ACCESS_DENIED, "Record partition is out of scope")
        grouped: dict[str, list[ExpectedRecord]] = {}
        for item in request.records:
            grouped.setdefault(item.partition.value, []).append(item)
        bindings = {value: self._bindings.resolve(AccessPartitionRef(value)) for value in grouped}
        initialized = tuple(binding for binding in bindings.values() if binding.dataset_id)
        counts = {"indexed": 0, "indexing": 0, "failed": 0, "missing": 0}
        try:
            user = await self._user_resolver(principal)
            states = await self._runtime.processing_states(initialized) if initialized else {}
            for value, items in grouped.items():
                binding = bindings[value]
                if binding.dataset_id is None:
                    counts["missing"] += len(items)
                    continue
                present = set()
                for entry in await self._runtime.list_items(binding, user):
                    metadata = _item_metadata(entry)
                    if metadata.get("source_id") == request.source_id:
                        present.add(
                            (str(metadata.get("record_id")), str(metadata.get("source_version")))
                        )
                state = _run_state(states.get(binding.dataset_id))
                for item in items:
                    key = (item.record_id, item.version)
                    counts[state if key in present else "missing"] += 1
            return IndexingProgress(expected=len(request.records), **counts)
        except BackendError:
            raise
        except Exception as exc:
            raise _translate_error(exc) from exc

    async def health(self) -> BackendHealth:
        """Translate native runtime readiness into engine health."""

        try:
            ready, detail = await self._runtime.health()
            return BackendHealth(
                ready,
                detail,
                BackendCapabilities(True, True, True, isolation_enforced=True),
            )
        except Exception as exc:
            return BackendHealth(False, f"provider unavailable: {type(exc).__name__}")


# The storage root the SDK was first loaded with in this process.
_loaded_storage: Path | None = None


class CogneeAnswerWriter:
    """Write answers with the provider's model client (M5 slice 3, ADR 0015).

    Errors leave as engine errors that name no provider detail. That matters here more than
    anywhere: the provider's content-policy error carries the whole prompt, passages included,
    in its message.
    """

    def __init__(self, runtime: CogneeRuntime) -> None:
        """Use a runtime bound to the engine's provider settings."""

        self._runtime = runtime

    async def write(self, request: AnswerRequest, models: ModelSelection | None = None) -> str:
        """Return the model's reply to the engine's prompt."""

        try:
            return await self._runtime.write_answer(request.instructions, request.prompt, models)
        except BackendError:
            raise
        except Exception as exc:
            # Not chained: the original exception's message may hold the prompt.
            raise _translate_error(exc) from None


class CogneeRuntime:
    """Lazy SDK wrapper. All Cognee imports and native calls stay in this module."""

    def __init__(self, settings: KnowledgeBackendSettings | None = None) -> None:
        self._settings = settings or KnowledgeBackendSettings.from_env()
        self._prepared = False
        self._preparing: asyncio.Lock | None = None

    async def _ready(self) -> Any:
        """Load the SDK and create the provider's own stores once per process.

        The provider does not create its relational store on first use, so every operation
        waits for this. Creating it is idempotent, which keeps separate processes safe.
        """

        cognee = self._module()
        if self._prepared:
            return cognee
        if self._preparing is None:
            self._preparing = asyncio.Lock()
        async with self._preparing:
            if not self._prepared:
                from cognee.modules.engine.operations.setup import setup

                await setup()
                self._prepared = True
        return cognee

    def _module(self) -> Any:
        """Load the SDK only after applying the engine-owned configuration."""

        global _loaded_storage
        storage = self._settings.storage_path.resolve()
        # The SDK reads its storage roots once per process, so a second root would silently
        # share the first one's stores.
        if _loaded_storage is not None and storage != _loaded_storage:
            raise BackendError(
                BackendErrorCode.UNSUPPORTED,
                "The knowledge provider is already bound to another storage root in this process",
            )
        _apply_native_environment(self._settings)
        try:
            with _keep_process_logging():
                import cognee
        except ImportError as exc:
            raise BackendError(
                BackendErrorCode.UNAVAILABLE,
                "The private provider dependency is not installed",
            ) from exc
        _loaded_storage = storage
        return cognee

    async def remember(
        self,
        record: SourceRecord,
        binding: _CogneeBinding,
        user: Any,
        models: ModelSelection | None = None,
    ) -> _NativeIngestion:
        """Create the native content item and invoke native ingestion."""

        cognee = await self._ready()
        from cognee.tasks.ingestion.data_item import DataItem

        item_id = _native_item_id(binding, record)
        native_record = DataItem(
            data=_RecordUpload(record.content),
            data_id=item_id,
            label=record.title,
            external_metadata={
                "record_id": record.record_id,
                "source_id": record.source_id,
                "source_version": record.version,
                "content_hash": record.content_hash,
                **dict(record.attributes),
            },
        )
        kwargs: dict[str, Any] = {
            "data": native_record,
            "dataset_name": binding.dataset_name,
            "user": user,
            "self_improvement": False,
            "run_in_background": False,
            "raise_on_error": True,
        }
        if binding.dataset_id:
            kwargs["dataset_id"] = UUID(binding.dataset_id)
        kwargs.update(_native_model_kwargs(models))
        chunk_size = await self._chunk_size(models)
        if chunk_size is not None:
            kwargs["chunk_size"] = chunk_size
        result = await cognee.remember(**kwargs)
        if getattr(result, "status", None) == "errored":
            raise BackendError(BackendErrorCode.PARTIAL_WRITE, "Provider ingestion failed")
        dataset_id = getattr(result, "dataset_id", None)
        if not dataset_id:
            raise BackendError(
                BackendErrorCode.UNSUPPORTED,
                "Provider did not return a stable isolation identifier",
            )
        # The result lists every item the unit's run touched, not only the one written here,
        # so the pinned id is what identifies this write; it must be among them.
        written = {str(item.get("id")) for item in getattr(result, "items", None) or []}
        if str(item_id) not in written:
            raise BackendError(
                BackendErrorCode.PARTIAL_WRITE, "Provider did not confirm the written item"
            )
        return _NativeIngestion(str(dataset_id), str(item_id), created=True)

    async def remember_datapoints(
        self,
        record: SourceRecord,
        binding: _CogneeBinding,
        user: Any,
        graph: Any,
        models: ModelSelection | None = None,
    ) -> _NativeIngestion:
        """Register one record, then write its RDF graph as nodes and edges it owns.

        The record is registered as a native content item (so listing and progress see it), but
        native graph extraction is skipped: the graph comes from the record's structure, not a
        model. Every node and edge is stamped with the item's dataset and data id, so a later
        update or delete reclaims exactly this record's graph through the same `forget` the text
        path uses. Node ids are namespaced by the binding, so a foreign-key reference lands on its
        target node and two partitions never share identity.
        """

        cognee = await self._ready()
        from cognee.modules.data.methods.get_authorized_dataset import get_authorized_dataset
        from cognee.modules.pipelines.models import PipelineContext
        from cognee.tasks.ingestion.data_item import DataItem
        from cognee.tasks.storage.add_data_points import add_data_points

        from .rml_adapter import rdf_to_datapoints

        item_id = _native_item_id(binding, record)
        native_record = DataItem(
            data=_RecordUpload(record.content),
            data_id=item_id,
            label=record.title,
            external_metadata={
                "record_id": record.record_id,
                "source_id": record.source_id,
                "source_version": record.version,
                "content_hash": record.content_hash,
                **dict(record.attributes),
            },
        )
        add_kwargs: dict[str, Any] = {
            "data": native_record,
            "dataset_name": binding.dataset_name,
            "user": user,
            "run_in_background": False,
        }
        if binding.dataset_id:
            add_kwargs["dataset_id"] = UUID(binding.dataset_id)
        add_kwargs.update(_native_model_kwargs(models))
        result = await cognee.add(**add_kwargs)
        if getattr(result, "status", "") == "PipelineRunErrored":
            raise BackendError(BackendErrorCode.PARTIAL_WRITE, "Provider ingestion failed")
        dataset_id = getattr(result, "dataset_id", None)
        if not dataset_id:
            raise BackendError(
                BackendErrorCode.UNSUPPORTED,
                "Provider did not return a stable isolation identifier",
            )
        dataset = await get_authorized_dataset(user, UUID(str(dataset_id)), "write")
        if dataset is None:
            raise BackendError(BackendErrorCode.PARTIAL_WRITE, "Provider did not bind the dataset")
        # The binding's native dataset name namespaces node ids: stable for this partition and
        # distinct from every other, so identical source IRIs never collide across datasets. The
        # item id is the record's document id, so retrieval attributes its chunks back to it.
        datapoints, edges = rdf_to_datapoints(graph, binding.dataset_name, item_id)
        ctx = PipelineContext(
            user=user,
            data_item=native_record,
            dataset=dataset,
            pipeline_run_id=getattr(result, "pipeline_run_id", None),
        )
        # The space's embedding model indexes the chunks the adapter emits for retrieval; native
        # graph extraction never runs, so no language model is called here.
        with _native_model_context(models):
            await add_data_points(datapoints, custom_edges=edges, ctx=ctx)
        return _NativeIngestion(str(dataset_id), str(item_id), created=True)

    async def recall(
        self,
        request: QueryRequest,
        bindings: tuple[_CogneeBinding, ...],
        user: Any,
        models: ModelSelection | None = None,
    ) -> list[Any]:
        """Invoke native retrieval with the pinned retriever and explicit binding identifiers."""

        cognee = await self._ready()
        from cognee.modules.search.types import SearchType

        ids = [UUID(item.dataset_id) for item in bindings if item.dataset_id]
        # A fixed retriever keeps result shapes stable, and turning routing off means no
        # question can be routed to raw graph queries however it is phrased. The chunk
        # retriever is the one whose results name the item each chunk came from; graph and
        # completion retrievers return rendered context that cannot be traced to a record.
        # Leaving context-only mode off is what keeps chunks separate; this retriever never
        # calls a model to produce them.
        return await cognee.recall(
            query_text=request.text,
            query_type=SearchType.CHUNKS,
            auto_route=False,
            dataset_ids=ids,
            top_k=request.limit,
            only_context=False,
            user=user,
            **_native_model_kwargs(models),
        )

    async def grant_read(self, binding: _CogneeBinding, reader: Any, owner: Any) -> None:
        """Give read permission on one binding through the native sharing API."""

        await self._ready()
        from cognee.modules.users.permissions.methods import (
            authorized_give_permission_on_datasets,
        )

        await authorized_give_permission_on_datasets(
            reader.id, [UUID(binding.dataset_id)], "read", owner.id
        )

    async def revoke_read(self, binding: _CogneeBinding, reader: Any, owner: Any) -> None:
        """Remove read permission on one binding through the native sharing API."""

        await self._ready()
        from cognee.modules.users.permissions.methods import (
            authorized_revoke_permission_on_datasets,
        )

        await authorized_revoke_permission_on_datasets(
            reader.id, [UUID(binding.dataset_id)], "read", owner.id
        )

    async def improve(
        self, binding: _CogneeBinding, user: Any, models: ModelSelection | None = None
    ) -> Any:
        """Invoke explicit native enrichment for one binding.

        Enrichment takes no per-call model settings, so the selected models are applied
        through the SDK's context variables for the duration of the call, the same mechanism
        its pipelines use internally.
        """

        cognee = await self._ready()
        with _native_model_context(models):
            return await cognee.improve(dataset=UUID(binding.dataset_id), user=user)

    async def forget(self, binding: _CogneeBinding, data_id: str, user: Any) -> Any:
        """Invoke native deletion for one explicitly bound record."""

        cognee = await self._ready()
        return await cognee.forget(
            data_id=UUID(data_id),
            dataset_id=UUID(binding.dataset_id),
            user=user,
        )

    async def forget_unit(self, binding: _CogneeBinding, user: Any) -> Any:
        """Invoke native deletion of one isolation unit, as its owner."""

        cognee = await self._ready()
        return await cognee.forget(dataset_id=UUID(binding.dataset_id), user=user)

    async def _chunk_size(self, models: ModelSelection | None) -> int | None:
        """Return the configured chunk size, capped at the limit the models accept.

        Without a setting the provider sizes chunks itself, to the smaller of the embedding
        model's input limit and half the language model's output budget. With one, the engine
        asks the provider for that same limit with the space's models applied and never goes
        above it, so a space whose embedding model takes less input still gets chunks it can
        embed. Chunks stay the provider's sentence-joining slices, with no overlap, as evidence
        placement assumes (M5 slice 2).
        """

        configured = self._settings.chunk_tokens
        if configured is None:
            return None
        from cognee.infrastructure.llm import get_max_chunk_tokens

        with _native_model_context(models):
            limit = await get_max_chunk_tokens()
        return min(configured, limit)

    async def write_answer(
        self, instructions: str, prompt: str, models: ModelSelection | None = None
    ) -> str:
        """Ask the provider's model client for a plain-text reply, with the space's models.

        This is the provider's bare model client, not its completion mode: the engine has
        already chosen the passages, so nothing here retrieves. The client needs the
        provider's configuration but none of its stores. No usage session is open, so the
        provider records token counts only, never the prompt.
        """

        self._module()
        from cognee.infrastructure.llm.LLMGateway import LLMGateway

        with _native_model_context(models):
            reply = await LLMGateway.acreate_structured_output(
                text_input=prompt, system_prompt=instructions, response_model=str
            )
        return str(reply)

    async def ensure_user(self, handle: str) -> Any:
        """Find the ordinary native account for a handle, creating it on first use."""

        await self._ready()
        from cognee.modules.users.methods import create_user, get_user_by_email

        existing = await get_user_by_email(handle)
        if existing is not None:
            return existing
        try:
            # The account is an internal handle; its password is never used or stored.
            return await create_user(handle, secrets.token_urlsafe(32))
        except Exception as exc:
            if "alreadyexists" not in type(exc).__name__.lower():
                raise
            # Another process created it between the lookup and the create.
            concurrent = await get_user_by_email(handle)
            if concurrent is None:
                raise
            return concurrent

    async def get_user(self, native_id: str) -> Any:
        """Load a native account by its native identifier."""

        await self._ready()
        from cognee.modules.users.methods import get_user

        return await get_user(UUID(native_id))

    async def list_items(self, binding: _CogneeBinding, user: Any) -> list[Any]:
        """List native content items in one binding with the resolved user."""

        cognee = await self._ready()
        return list(await cognee.datasets.list_data(UUID(binding.dataset_id), user=user))

    async def processing_states(self, bindings: tuple[_CogneeBinding, ...]) -> dict[str, Any]:
        """Read the latest processing-run state per binding from the native status API."""

        cognee = await self._ready()
        ids = [UUID(item.dataset_id) for item in bindings if item.dataset_id]
        # With no pipeline names the SDK returns a flat map for its processing pipeline.
        result = await cognee.datasets.get_progress(ids)
        return {str(key): value for key, value in (result or {}).items()}

    async def health(self) -> tuple[bool, str]:
        """Load the pinned SDK and return an engine-owned readiness detail."""

        await self._ready()
        return True, "knowledge provider ready"


_PROCESSING_PIPELINE = "cognify_pipeline"


def _as_mapping(value: Any) -> dict[str, Any]:
    """Return a plain mapping for a native model, mapping, or anything else."""

    if value is None:
        return {}
    if hasattr(value, "model_dump"):
        dumped = value.model_dump()
        return dumped if isinstance(dumped, dict) else {}
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _native_chunk(entry: Mapping[str, Any]) -> tuple[str, str, str, int | None] | None:
    """Return the item id, chunk id, passage, and chunk index of one native chunk entry."""

    if entry.get("kind") != "chunk":
        return None
    metadata = _as_mapping(entry.get("metadata"))
    raw = _as_mapping(entry.get("raw"))
    item_id = str(metadata.get("data_id") or raw.get("document_id") or "")
    passage = str(raw.get("text") or entry.get("text") or "").strip()
    if not item_id or not passage:
        return None
    chunk_id = str(metadata.get("chunk_id") or raw.get("id") or "")
    chunk_index = metadata.get("chunk_index", raw.get("chunk_index"))
    if isinstance(chunk_index, bool) or not isinstance(chunk_index, int):
        chunk_index = None
    return item_id, chunk_id, passage, chunk_index


def _item_metadata(entry: Any) -> Mapping[str, Any]:
    """Return the engine metadata attached to a native content item.

    The native listing returns stored rows rather than serializable models, so the metadata
    is read as an attribute unless the entry is a mapping or a model.
    """

    if isinstance(entry, Mapping):
        metadata = entry.get("external_metadata")
    elif hasattr(entry, "model_dump"):
        metadata = _as_mapping(entry).get("external_metadata")
    else:
        metadata = getattr(entry, "external_metadata", None)
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except ValueError:
            return {}
    return metadata if isinstance(metadata, Mapping) else {}


def _run_state(value: Any) -> str:
    """Map a native processing-run state to an engine indexing state name."""

    if isinstance(value, Mapping) and "status" not in value and _PROCESSING_PIPELINE in value:
        value = value[_PROCESSING_PIPELINE]
    status = value.get("status") if isinstance(value, Mapping) else value
    name = str(getattr(status, "value", status) or "").upper()
    if name.endswith("COMPLETED"):
        return "indexed"
    if name.endswith("ERRORED"):
        return "failed"
    # A started, initiated, or not-yet-recorded run means the items are still processing.
    return "indexing"


def _translate_error(exc: Exception) -> BackendError:
    name = type(exc).__name__.lower()
    if "permission" in name or "unauthorized" in name or "forbidden" in name:
        return BackendError(BackendErrorCode.ACCESS_DENIED, "Knowledge access denied")
    if "timeout" in name:
        return BackendError(BackendErrorCode.TIMEOUT, "Knowledge backend timed out", retryable=True)
    if "notfound" in name or "not_found" in name:
        return BackendError(BackendErrorCode.NOT_FOUND, "Knowledge resource not found")
    if "validation" in name or isinstance(exc, (TypeError, ValueError)):
        return BackendError(BackendErrorCode.INVALID_INPUT, "Knowledge request is invalid")
    return BackendError(
        BackendErrorCode.UNAVAILABLE,
        "Knowledge backend operation failed",
        retryable=True,
    )


# Engine provider names to native ones. The engine's vocabulary is the control plane's; the
# provider spells Azure OpenAI differently and infers unknown prefixes from the model name.
_NATIVE_PROVIDERS = {
    "openai": "openai",
    "anthropic": "anthropic",
    "mistral": "mistral",
    "azure_openai": "azure",
    "azure": "azure",
    "gemini": "gemini",
    "ollama": "ollama",
}


def _native_model_name(native_provider: str, model: str) -> str:
    """Return the routed model name the SDK expects, `<provider>/<model>`, once."""

    return model if "/" in model else f"{native_provider}/{model}"


def _native_model_configs(models: ModelSelection | None) -> tuple[Any | None, Any | None]:
    """Translate a selection into the SDK's language-model and embedding configurations.

    Only the selected models are translated; `None` for either leaves the SDK on the
    environment the adapter configured, which is the engine's default.
    """

    if models is None:
        return None, None
    language = embedding = None
    if models.language_model is not None:
        from cognee.infrastructure.llm.config import LLMConfig

        chosen = models.language_model
        provider = _native_provider(chosen.provider)
        fields: dict[str, Any] = {
            "llm_provider": provider,
            "llm_model": _native_model_name(provider, chosen.model),
            "llm_api_key": chosen.api_key,
        }
        if chosen.endpoint:
            fields["llm_endpoint"] = chosen.endpoint
        if chosen.api_version:
            fields["llm_api_version"] = chosen.api_version
        language = LLMConfig(**fields)
    if models.embedding_model is not None:
        from cognee.infrastructure.databases.vector.embeddings.config import EmbeddingConfig

        chosen = models.embedding_model
        provider = _native_provider(chosen.provider)
        fields = {
            "embedding_provider": provider,
            "embedding_model": _native_model_name(provider, chosen.model),
            "embedding_api_key": chosen.api_key,
        }
        if chosen.endpoint:
            fields["embedding_endpoint"] = chosen.endpoint
        if chosen.api_version:
            fields["embedding_api_version"] = chosen.api_version
        if chosen.dimensions:
            fields["embedding_dimensions"] = chosen.dimensions
        embedding = EmbeddingConfig(**fields)
    return language, embedding


def _native_provider(provider: str) -> str:
    native = _NATIVE_PROVIDERS.get(provider.strip().lower())
    if native is None:
        raise BackendError(BackendErrorCode.UNSUPPORTED, "Model provider is not supported")
    return native


def _native_model_kwargs(models: ModelSelection | None) -> dict[str, Any]:
    """Keyword arguments carrying the selected models to a native call that accepts them."""

    language, embedding = _native_model_configs(models)
    kwargs: dict[str, Any] = {}
    if language is not None:
        kwargs["llm_config"] = language
    if embedding is not None:
        kwargs["embedding_config"] = embedding
    return kwargs


@contextmanager
def _native_model_context(models: ModelSelection | None) -> Iterator[None]:
    """Apply the selected models through the SDK's context variables for one call."""

    language, embedding = _native_model_configs(models)
    if language is None and embedding is None:
        yield
        return
    from cognee.context_global_variables import embedding_config, llm_config

    tokens = []
    if language is not None:
        tokens.append((llm_config, llm_config.set(language)))
    if embedding is not None:
        tokens.append((embedding_config, embedding_config.set(embedding)))
    try:
        yield
    finally:
        for variable, token in reversed(tokens):
            variable.reset(token)


def _native_bool(value: bool) -> str:
    """Format an engine boolean for the native provider environment."""

    return "true" if value else "false"


@contextmanager
def _keep_process_logging() -> Iterator[None]:
    """Keep the engine's log handlers across the provider's import.

    Importing the SDK replaces every root handler with its own console handler, which would
    drop the engine's allowlisted fields, including those of audit events. Its file handler,
    added only when the engine enables provider file logging, is kept.
    """

    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    try:
        yield
    finally:
        added = [handler for handler in root.handlers if handler not in handlers]
        for handler in added:
            if not isinstance(handler, logging.FileHandler):
                root.removeHandler(handler)
        for handler in handlers:
            if handler not in root.handlers:
                root.addHandler(handler)
        root.setLevel(level)


def _apply_native_environment(settings: KnowledgeBackendSettings) -> None:
    """Translate engine settings into native names inside the private boundary."""

    storage = settings.storage_path.resolve()
    native_values = {
        "TELEMETRY_DISABLED": _native_bool(not settings.telemetry_enabled),
        "COGNEE_LOG_FILE": _native_bool(settings.file_logging_enabled),
        "COGNEE_LOGS_DIR": str(storage / "logs"),
        "CACHING": _native_bool(settings.query_cache_enabled),
        "ENABLE_BACKEND_ACCESS_CONTROL": _native_bool(settings.access_control_required),
        "REQUIRE_AUTHENTICATION": _native_bool(settings.access_control_required),
        # The provider stores each upload in its own data directory and reads it back through
        # its local-file loader, so local paths must be accepted. The engine never passes a
        # path, and confining local reads to the provider's data directory keeps any other
        # file unreadable. Wider local reads happen only if the engine allows them.
        "ACCEPT_LOCAL_FILE_PATH": "true",
        "COGNEE_ALLOWED_LOCAL_FILE_ROOTS": ""
        if settings.local_content_access_enabled
        else str(storage / "data"),
        "ALLOW_HTTP_REQUESTS": _native_bool(settings.remote_content_access_enabled),
        "ALLOW_CYPHER_QUERY": _native_bool(settings.raw_graph_query_enabled),
        "DB_PROVIDER": settings.relational_store,
        "GRAPH_DATABASE_PROVIDER": settings.graph_store,
        "GRAPH_DATASET_DATABASE_HANDLER": settings.graph_store,
        "VECTOR_DB_PROVIDER": settings.vector_store,
        "VECTOR_DATASET_DATABASE_HANDLER": settings.vector_store,
        "LLM_PROVIDER": settings.model_provider,
        "LLM_MODEL": settings.model_name,
        "LLM_API_KEY": settings.model_api_key,
        "EMBEDDING_PROVIDER": settings.embedding_provider,
        "EMBEDDING_MODEL": settings.embedding_model,
        "EMBEDDING_DIMENSIONS": str(settings.embedding_dimensions),
        "EMBEDDING_API_KEY": settings.embedding_api_key,
        "SYSTEM_ROOT_DIRECTORY": str(storage / "system"),
        "DATA_ROOT_DIRECTORY": str(storage / "data"),
        "CACHE_ROOT_DIRECTORY": str(storage / "cache"),
    }
    # Overwrite inherited native settings so callers cannot bypass engine policy.
    os.environ.update(native_values)


def assert_runtime_matches_pinned_sdk(runtime: CogneeRuntime | None = None) -> None:
    """Fail fast when the installed SDK signatures drift from the M0 mapping."""

    cognee = (runtime or CogneeRuntime())._module()
    required = {
        "remember": {"data", "dataset_name", "dataset_id", "self_improvement", "chunk_size"},
        "add": {"data", "dataset_name", "dataset_id", "user"},
        "recall": {
            "query_text",
            "query_type",
            "auto_route",
            "dataset_ids",
            "top_k",
            "only_context",
            "user",
            "llm_config",
            "embedding_config",
        },
        "forget": {"data_id", "dataset_id", "user"},
        "improve": {"dataset"},
    }
    for operation, parameters in required.items():
        function = getattr(cognee, operation, None)
        if function is None:
            raise RuntimeError(f"Pinned provider is missing {operation}")
        actual = set(inspect.signature(function).parameters)
        missing = parameters - actual
        if missing:
            raise RuntimeError(
                f"Pinned provider {operation} signature is missing {sorted(missing)}"
            )
    # A configured chunk size is capped at the limit the provider computes for the models.
    from cognee.infrastructure import llm as provider_llm

    if not callable(getattr(provider_llm, "get_max_chunk_tokens", None)):
        raise RuntimeError("Pinned provider no longer reports the models' chunk limit")
    # Answers go through the bare model client, with the reply as plain text.
    from cognee.infrastructure.llm.LLMGateway import LLMGateway

    gateway = set(inspect.signature(LLMGateway.acreate_structured_output).parameters)
    if not {"text_input", "system_prompt", "response_model"} <= gateway:
        raise RuntimeError("Pinned provider model client no longer takes a prompt and a reply type")
    # Ingestion takes the per-call model settings through a typed keyword bundle rather than
    # named parameters, so the bundle is what pins them.
    from cognee.api.v1.remember.remember import RememberKwargs

    bundled = set(getattr(RememberKwargs, "__annotations__", {}))
    if not {"llm_config", "embedding_config"} <= bundled:
        raise RuntimeError("Pinned provider ingestion no longer takes per-call model settings")
    from cognee.modules.users import methods as user_methods
    from cognee.modules.users.permissions import methods as permission_methods

    for operation in (
        "authorized_give_permission_on_datasets",
        "authorized_revoke_permission_on_datasets",
    ):
        function = getattr(permission_methods, operation, None)
        if function is None:
            raise RuntimeError(f"Pinned provider permission API is missing {operation}")
        expected = {"principal_id", "dataset_ids", "permission_name", "owner_id"}
        missing = expected - set(inspect.signature(function).parameters)
        if missing:
            raise RuntimeError(
                f"Pinned provider {operation} signature is missing {sorted(missing)}"
            )

    for operation, parameters in {
        "create_user": {"email", "password"},
        "get_user": {"user_id"},
        "get_user_by_email": {"user_email"},
    }.items():
        function = getattr(user_methods, operation, None)
        if function is None:
            raise RuntimeError(f"Pinned provider identity API is missing {operation}")
        missing = parameters - set(inspect.signature(function).parameters)
        if missing:
            raise RuntimeError(
                f"Pinned provider {operation} signature is missing {sorted(missing)}"
            )
    from cognee.modules.search.types import SearchType

    if not hasattr(SearchType, "CHUNKS"):
        raise RuntimeError("Pinned provider no longer offers the chunk retriever")
    status_api = getattr(cognee, "datasets", None)
    for operation, parameters in {
        "get_progress": {"dataset_ids", "pipeline_names"},
        "list_data": {"dataset_id", "user"},
    }.items():
        function = getattr(status_api, operation, None)
        if function is None:
            raise RuntimeError(f"Pinned provider status API is missing {operation}")
        missing = parameters - set(inspect.signature(function).parameters)
        if missing:
            raise RuntimeError(
                f"Pinned provider {operation} signature is missing {sorted(missing)}"
            )
    # The structured path writes graph nodes directly and scopes them to the record's dataset and
    # data id through a pipeline context, so a drift in any of these breaks per-record ownership.
    from cognee.modules.data.methods.get_authorized_dataset import get_authorized_dataset
    from cognee.modules.pipelines.models import PipelineContext
    from cognee.tasks.storage.add_data_points import add_data_points

    add_points_params = set(inspect.signature(add_data_points).parameters)
    if not {"data_points", "custom_edges", "ctx"} <= add_points_params:
        raise RuntimeError("Pinned provider no longer takes datapoints with a pipeline context")
    context_fields = set(getattr(PipelineContext, "__dataclass_fields__", {}))
    if not {"user", "data_item", "dataset"} <= context_fields:
        raise RuntimeError("Pinned provider pipeline context no longer carries the owning item")
    if not {"user", "permission_type"} <= set(inspect.signature(get_authorized_dataset).parameters):
        raise RuntimeError("Pinned provider no longer resolves an authorized dataset")
