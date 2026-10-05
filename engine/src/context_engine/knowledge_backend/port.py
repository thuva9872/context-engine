"""The only backend interface available to application services."""

from __future__ import annotations

from typing import Protocol

from .types import (
    AccessPartitionRef,
    AnswerRequest,
    BackendHealth,
    BackendReference,
    DeletionResult,
    EnrichmentRequest,
    EnrichmentResult,
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


class AnswerWriter(Protocol):
    """A model client that writes answer text from a prompt the engine built (M5 slice 3).

    This is the answer-generation port the application layer calls. The private provider's
    writer is one implementation and the application's extractive generator another; the
    engine checks every reply before anyone sees it.
    """

    async def write(self, request: AnswerRequest, models: ModelSelection | None = None) -> str:
        """Return the model's reply, with the space's models applied when given."""

        ...


class KnowledgeBackend(Protocol):
    """Internal provider-neutral interface used by worker pipelines."""

    async def ingest(
        self,
        record: SourceRecord,
        principal: PrincipalContext,
        partition: AccessPartitionRef,
        models: ModelSelection | None = None,
        structured: StructuredContent | None = None,
    ) -> IngestionResult:
        """Store a new source record in one explicit access partition.

        When ``structured`` is given, the record's graph is written as precise nodes and edges
        instead of the record being indexed as text.
        """

        ...

    async def query(
        self,
        request: QueryRequest,
        principal: PrincipalContext,
        authorized_partitions: tuple[AccessPartitionRef, ...],
        models: ModelSelection | None = None,
    ) -> QueryResult:
        """Retrieve evidence from explicitly authorized partitions."""

        ...

    async def update(
        self,
        record: SourceRecord,
        principal: PrincipalContext,
        partition: AccessPartitionRef,
        models: ModelSelection | None = None,
        structured: StructuredContent | None = None,
    ) -> IngestionResult:
        """Replace an existing source record in its access partition.

        On success the new version is searchable and the previous one is not. The returned
        reference may differ from the previous one and replaces it. When ``structured`` is given,
        the replacement is written as graph nodes and edges instead of indexed text.
        """

        ...

    async def enrich(
        self,
        request: EnrichmentRequest,
        principal: PrincipalContext,
        authorized_partitions: tuple[AccessPartitionRef, ...],
        models: ModelSelection | None = None,
    ) -> EnrichmentResult:
        """Run explicit enrichment within authorized partitions."""

        ...

    async def delete(
        self,
        reference: BackendReference,
        principal: PrincipalContext,
        partition: AccessPartitionRef,
    ) -> DeletionResult:
        """Delete one referenced record from its access partition."""

        ...

    async def delete_partition(
        self,
        partition: AccessPartitionRef,
        principal: PrincipalContext,
    ) -> None:
        """Remove a partition's isolation unit once the engine holds no records in it.

        Space deletion calls this after every record's copy is gone and checked, so the unit
        should be empty; removing it also drops derived data and read grants that lived only
        there. A partition the backend never bound is a no-op.
        """

        ...

    async def grant_read(
        self,
        partition: AccessPartitionRef,
        reader: PrincipalContext,
        principal: PrincipalContext,
    ) -> None:
        """Let a reader query one partition; the acting principal must own it.

        Raises a not-found error while the partition holds nothing yet.
        """

        ...

    async def revoke_read(
        self,
        partition: AccessPartitionRef,
        reader: PrincipalContext,
        principal: PrincipalContext,
    ) -> None:
        """Remove a reader's access to one partition; the acting principal must own it."""

        ...

    async def indexing_progress(
        self,
        request: IndexingProgressRequest,
        principal: PrincipalContext,
        authorized_partitions: tuple[AccessPartitionRef, ...],
    ) -> IndexingProgress:
        """Report how many expected record versions are indexed, in progress, failed, or absent.

        Every expected record must sit in one of the explicitly authorized partitions.
        """

        ...

    async def health(self) -> BackendHealth:
        """Report backend readiness and supported capabilities."""

        ...
