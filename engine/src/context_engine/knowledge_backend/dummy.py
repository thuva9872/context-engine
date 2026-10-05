"""Deterministic backend used to prove the engine contract and isolation model."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from .errors import BackendError, BackendErrorCode
from .types import (
    AccessPartitionRef,
    BackendCapabilities,
    BackendHealth,
    BackendReference,
    DeletionResult,
    EnrichmentRequest,
    EnrichmentResult,
    EvidenceItem,
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


def _tokens(text: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", text.lower()))


def _version_key(version: str) -> tuple[int, int | str]:
    try:
        return (1, int(version))
    except ValueError:
        return (0, version)


@dataclass(slots=True)
class _StoredRecord:
    record: SourceRecord
    reference: BackendReference
    artifacts: tuple[str, ...] = ()


class DummyKnowledgeBackend:
    """In-memory backend with partition-scoped storage, graph data and cache.

    With `chunk_lines` set, each record is split into chunks of that many lines, each an exact
    slice of the record's text, as the private provider's chunker keeps them; the default keeps
    every record as one chunk.
    """

    def __init__(self, *, chunk_lines: int = 0) -> None:
        self._records: dict[str, dict[str, _StoredRecord]] = {}
        self._references: dict[str, tuple[str, str]] = {}
        self._query_cache: dict[tuple[str, tuple[str, ...], str], QueryResult] = {}
        self._readers: dict[str, set[str]] = {}
        self._chunk_lines = chunk_lines

    def _chunks(self, content: str) -> list[str]:
        """Split a record into exact slices of its text, as the provider's chunks are.

        Tests use this to exercise chunk order and repeated passages without the provider.
        """

        if self._chunk_lines <= 0:
            return [content]
        lines = content.split("\n")
        size = self._chunk_lines
        return ["\n".join(lines[start : start + size]) for start in range(0, len(lines), size)]

    @staticmethod
    def _check_context(
        principal: PrincipalContext, partitions: tuple[AccessPartitionRef, ...]
    ) -> None:
        if not principal.principal_id or not principal.trace_id:
            raise BackendError(BackendErrorCode.INVALID_INPUT, "Explicit principal is required")
        if not partitions:
            raise BackendError(
                BackendErrorCode.ACCESS_DENIED,
                "At least one authorized access partition is required",
            )

    @staticmethod
    def _reference(partition: AccessPartitionRef, record: SourceRecord) -> BackendReference:
        digest = hashlib.sha256(
            f"{partition.value}\0{record.source_id}\0{record.record_id}".encode()
        ).hexdigest()
        return BackendReference(f"dummy:{digest}")

    def _invalidate(self, partition: AccessPartitionRef) -> None:
        # Cache entries include every authorized partition, so any matching entry is stale.
        self._query_cache = {
            key: value for key, value in self._query_cache.items() if partition.value not in key[1]
        }

    async def ingest(
        self,
        record: SourceRecord,
        principal: PrincipalContext,
        partition: AccessPartitionRef,
        models: ModelSelection | None = None,
        structured: StructuredContent | None = None,
    ) -> IngestionResult:
        """Store or idempotently replay a partition-scoped record."""

        self._check_context(principal, (partition,))
        records = self._records.setdefault(partition.value, {})
        current = records.get(record.record_id)
        if current:
            if _version_key(record.version) < _version_key(current.record.version):
                raise BackendError(BackendErrorCode.CONFLICT, "Source version is older")
            if record.version == current.record.version:
                if record.content_hash != current.record.content_hash:
                    raise BackendError(
                        BackendErrorCode.CONFLICT,
                        "The same source version has different content",
                    )
                return IngestionResult(
                    record.record_id, record.version, current.reference, created=False
                )
            return await self.update(record, principal, partition)

        reference = self._reference(partition, record)
        records[record.record_id] = _StoredRecord(record, reference)
        self._references[reference.value] = (partition.value, record.record_id)
        self._invalidate(partition)
        return IngestionResult(record.record_id, record.version, reference, created=True)

    async def update(
        self,
        record: SourceRecord,
        principal: PrincipalContext,
        partition: AccessPartitionRef,
        models: ModelSelection | None = None,
        structured: StructuredContent | None = None,
    ) -> IngestionResult:
        """Replace a record only with an acceptable source version."""

        self._check_context(principal, (partition,))
        records = self._records.setdefault(partition.value, {})
        current = records.get(record.record_id)
        if current is None:
            return await self.ingest(record, principal, partition)
        if _version_key(record.version) <= _version_key(current.record.version):
            if (
                record.version == current.record.version
                and record.content_hash == current.record.content_hash
            ):
                return IngestionResult(record.record_id, record.version, current.reference, False)
            raise BackendError(BackendErrorCode.CONFLICT, "Source version is not newer")
        records[record.record_id] = _StoredRecord(record, current.reference)
        self._invalidate(partition)
        return IngestionResult(record.record_id, record.version, current.reference, False)

    async def query(
        self,
        request: QueryRequest,
        principal: PrincipalContext,
        authorized_partitions: tuple[AccessPartitionRef, ...],
        models: ModelSelection | None = None,
    ) -> QueryResult:
        """Return deterministic evidence from authorized partitions only."""

        self._check_context(principal, authorized_partitions)
        partition_values = tuple(sorted({item.value for item in authorized_partitions}))
        cache_key = (principal.principal_id, partition_values, request.text)
        if cache_key in self._query_cache:
            return self._query_cache[cache_key]

        query_tokens = _tokens(request.text)
        matches: list[EvidenceItem] = []
        for partition_value in partition_values:
            for stored in self._records.get(partition_value, {}).values():
                extra = (stored.record.title or "", *stored.record.entities, *stored.artifacts)
                for chunk_index, chunk in enumerate(self._chunks(stored.record.content)):
                    overlap = query_tokens & _tokens(" ".join((chunk, *extra)))
                    if not overlap:
                        continue
                    digest = hashlib.sha256(
                        f"{partition_value}\0{stored.record.record_id}\0"
                        f"{stored.record.version}\0{chunk_index}".encode()
                    ).hexdigest()[:24]
                    matches.append(
                        EvidenceItem(
                            evidence_id=f"evi_{digest}",
                            record_id=stored.record.record_id,
                            source_id=stored.record.source_id,
                            source_version=stored.record.version,
                            passage=chunk,
                            score=len(overlap) / max(len(query_tokens), 1),
                            chunk_index=chunk_index,
                            graph_path=stored.record.entities,
                        )
                    )
        matches.sort(key=lambda item: (-item.score, item.evidence_id))
        result = QueryResult(tuple(matches[: request.limit]), insufficient_evidence=not matches)
        self._query_cache[cache_key] = result
        return result

    async def enrich(
        self,
        request: EnrichmentRequest,
        principal: PrincipalContext,
        authorized_partitions: tuple[AccessPartitionRef, ...],
        models: ModelSelection | None = None,
    ) -> EnrichmentResult:
        """Create deterministic derived artifacts inside each partition."""

        self._check_context(principal, authorized_partitions)
        artifacts: list[str] = []
        affected = 0
        for partition in authorized_partitions:
            for stored in self._records.get(partition.value, {}).values():
                stored.artifacts = tuple(
                    f"{entity}:{request.pipeline_version}" for entity in stored.record.entities
                )
                artifacts.extend(stored.artifacts)
                affected += 1
            self._invalidate(partition)
        return EnrichmentResult(request.operation_id, affected, tuple(sorted(artifacts)))

    async def delete(
        self,
        reference: BackendReference,
        principal: PrincipalContext,
        partition: AccessPartitionRef,
    ) -> DeletionResult:
        """Delete a record only when its reference belongs to the partition."""

        self._check_context(principal, (partition,))
        location = self._references.get(reference.value)
        if location is None or location[0] != partition.value:
            return DeletionResult(record_id="unknown", deleted=False)
        record_id = location[1]
        self._records.get(partition.value, {}).pop(record_id, None)
        self._references.pop(reference.value, None)
        self._invalidate(partition)
        return DeletionResult(record_id=record_id, deleted=True)

    async def delete_partition(
        self,
        partition: AccessPartitionRef,
        principal: PrincipalContext,
    ) -> None:
        """Drop everything held for a partition, including anything derived inside it."""

        self._check_context(principal, (partition,))
        self._records.pop(partition.value, None)
        self._references = {
            reference: location
            for reference, location in self._references.items()
            if location[0] != partition.value
        }
        self._readers.pop(partition.value, None)
        self._invalidate(partition)

    async def grant_read(
        self,
        partition: AccessPartitionRef,
        reader: PrincipalContext,
        principal: PrincipalContext,
    ) -> None:
        """Record a reader; like a real backend, an empty partition cannot be shared yet."""

        self._check_context(principal, (partition,))
        if partition.value not in self._records:
            raise BackendError(BackendErrorCode.NOT_FOUND, "Access partition is not initialized")
        self._readers.setdefault(partition.value, set()).add(reader.principal_id)

    async def revoke_read(
        self,
        partition: AccessPartitionRef,
        reader: PrincipalContext,
        principal: PrincipalContext,
    ) -> None:
        """Remove a reader."""

        self._check_context(principal, (partition,))
        self._readers.get(partition.value, set()).discard(reader.principal_id)

    def readers(self, partition: AccessPartitionRef) -> frozenset[str]:
        """Return the recorded readers of a partition, for tests."""

        return frozenset(self._readers.get(partition.value, set()))

    async def indexing_progress(
        self,
        request: IndexingProgressRequest,
        principal: PrincipalContext,
        authorized_partitions: tuple[AccessPartitionRef, ...],
    ) -> IndexingProgress:
        """Count expected record versions held in their partitions; writes are synchronous."""

        self._check_context(principal, authorized_partitions)
        allowed = {item.value for item in authorized_partitions}
        if any(item.partition.value not in allowed for item in request.records):
            raise BackendError(BackendErrorCode.ACCESS_DENIED, "Record partition is out of scope")
        indexed = 0
        for item in request.records:
            stored = self._records.get(item.partition.value, {}).get(item.record_id)
            if (
                stored is not None
                and stored.record.source_id == request.source_id
                and stored.record.version == item.version
            ):
                indexed += 1
        expected = len(request.records)
        return IndexingProgress(expected, indexed, 0, 0, expected - indexed)

    async def health(self) -> BackendHealth:
        """Report the deterministic backend as ready for all test operations."""

        return BackendHealth(
            ready=True,
            detail="deterministic in-memory backend",
            capabilities=BackendCapabilities(
                supports_update=True,
                supports_enrichment=True,
                supports_record_deletion=True,
                isolation_enforced=True,
            ),
        )
