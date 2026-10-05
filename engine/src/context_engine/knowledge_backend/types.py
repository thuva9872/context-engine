"""Engine-owned values crossing the knowledge backend boundary."""

from __future__ import annotations

from dataclasses import dataclass, field


def _required(value: str, field_name: str) -> None:
    if not value or not value.strip():
        raise ValueError(f"{field_name} is required")


@dataclass(frozen=True, slots=True)
class PrincipalContext:
    """Authenticated engine principal and request trace context."""

    principal_id: str
    trace_id: str

    def __post_init__(self) -> None:
        _required(self.principal_id, "principal_id")
        _required(self.trace_id, "trace_id")


@dataclass(frozen=True, slots=True)
class AccessPartitionRef:
    """Internal isolation reference. Public serializers must reject this type."""

    value: str

    def __post_init__(self) -> None:
        _required(self.value, "access partition")


@dataclass(frozen=True, slots=True)
class BackendReference:
    """Opaque provider handle. Public serializers must reject this type."""

    value: str

    def __post_init__(self) -> None:
        _required(self.value, "backend reference")


@dataclass(frozen=True, slots=True)
class SourceRecord:
    """Engine-owned content and lineage for one source-record version."""

    record_id: str
    source_id: str
    version: str
    content: str
    content_hash: str
    title: str | None = None
    source_url: str | None = None
    entities: tuple[str, ...] = ()
    attributes: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        for name in ("record_id", "source_id", "version", "content", "content_hash"):
            _required(getattr(self, name), name)


@dataclass(frozen=True, slots=True)
class StructuredContent:
    """A record's structured content materialized to RDF, ready for graph-node ingestion.

    Carries the ``rdflib.Graph`` that ``context_engine.ingestion.structured.materialize`` built
    from the record's bytes and its source's RML mapping. The graph is typed ``object`` so this
    boundary module keeps no RDF dependency. When a record's content type has a mapping, the
    worker attaches this so the backend writes precise nodes and edges instead of indexed text.
    """

    graph: object


@dataclass(frozen=True, slots=True, repr=False)
class ModelSettings:
    """One model a backend call should use: provider, name, and credentials.

    The key travels with the call and lives no longer than it, which is how a space's own
    models reach the backend without the backend storing anything. It never appears in the
    string form, so a logged or raised value cannot leak it. The provider and model names
    are the engine's; the private adapter translates them.
    """

    provider: str
    model: str
    api_key: str
    endpoint: str | None = None
    api_version: str | None = None
    dimensions: int | None = None

    def __post_init__(self) -> None:
        _required(self.provider, "model provider")
        _required(self.model, "model name")

    def __repr__(self) -> str:
        return f"ModelSettings(provider={self.provider!r}, model={self.model!r})"


@dataclass(frozen=True, slots=True)
class ModelSelection:
    """The models for one backend call; `None` for either means the backend's default.

    Spaces configure their own language and embedding models (M5). A call without a
    selection falls back to the engine's environment settings, so spaces configured before
    this existed keep working.
    """

    language_model: ModelSettings | None = None
    embedding_model: ModelSettings | None = None


@dataclass(frozen=True, slots=True)
class QueryRequest:
    """Bounded evidence retrieval request."""

    text: str
    limit: int = 10

    def __post_init__(self) -> None:
        _required(self.text, "query text")
        if not 1 <= self.limit <= 100:
            raise ValueError("limit must be between 1 and 100")


@dataclass(frozen=True, slots=True)
class EnrichmentRequest:
    """Explicit versioned enrichment operation."""

    operation_id: str
    pipeline_version: str

    def __post_init__(self) -> None:
        _required(self.operation_id, "operation_id")
        _required(self.pipeline_version, "pipeline_version")


@dataclass(frozen=True, slots=True)
class EvidenceItem:
    """Authorized source-linked evidence returned by a backend.

    `chunk_index` is the passage's position among its record's chunks, when the backend
    reports one. The engine places the passage in the record's text itself (M5 slice 2), so
    the order of chunks is the only position a backend needs to give.
    """

    evidence_id: str
    record_id: str
    source_id: str
    source_version: str
    passage: str
    score: float
    chunk_index: int | None = None
    graph_path: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class AnswerRequest:
    """What a language model is asked to answer from, built by the engine (M5 slice 3).

    `instructions` and `prompt` are the complete text the model receives; `passages` are the
    evidence passages it holds, in the order the prompt numbers them, so a generator that
    calls no model can still answer from them.
    """

    question: str
    passages: tuple[str, ...]
    instructions: str
    prompt: str


@dataclass(frozen=True, slots=True)
class IngestionResult:
    """Result of storing or replaying one source record."""

    record_id: str
    source_version: str
    backend_reference: BackendReference
    created: bool


@dataclass(frozen=True, slots=True)
class QueryResult:
    """Evidence result with an explicit insufficient-evidence state."""

    evidence: tuple[EvidenceItem, ...]
    insufficient_evidence: bool


@dataclass(frozen=True, slots=True)
class EnrichmentResult:
    """Summary of an explicit enrichment operation."""

    operation_id: str
    affected_records: int
    created_artifacts: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DeletionResult:
    """Outcome and residual failures for one record deletion."""

    record_id: str
    deleted: bool
    residual_failures: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class BackendCapabilities:
    """Safety and lifecycle operations supported by a backend."""

    supports_update: bool
    supports_enrichment: bool
    supports_record_deletion: bool
    isolation_enforced: bool
    notes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class BackendHealth:
    """Readiness and capabilities reported by a backend."""

    ready: bool
    detail: str = ""
    capabilities: BackendCapabilities = field(
        default_factory=lambda: BackendCapabilities(False, False, False, False)
    )


@dataclass(frozen=True, slots=True)
class ExpectedRecord:
    """One record version the engine expects a backend to hold in one partition."""

    partition: AccessPartitionRef
    record_id: str
    version: str

    def __post_init__(self) -> None:
        _required(self.record_id, "record_id")
        _required(self.version, "version")


@dataclass(frozen=True, slots=True)
class IndexingProgressRequest:
    """Ask a backend how far it has indexed a source's expected record versions."""

    source_id: str
    records: tuple[ExpectedRecord, ...]

    def __post_init__(self) -> None:
        _required(self.source_id, "source_id")


@dataclass(frozen=True, slots=True)
class IndexingProgress:
    """Counts of expected record versions by backend indexing state."""

    expected: int
    indexed: int
    indexing: int
    failed: int
    missing: int

    def __post_init__(self) -> None:
        values = (self.expected, self.indexed, self.indexing, self.failed, self.missing)
        if any(value < 0 for value in values):
            raise ValueError("indexing counts cannot be negative")
        if self.indexed + self.indexing + self.failed + self.missing != self.expected:
            raise ValueError("indexing counts must add up to the expected total")
