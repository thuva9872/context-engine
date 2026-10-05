"""Provider-neutral knowledge backend boundary."""

from .dummy import DummyKnowledgeBackend
from .errors import BackendError, BackendErrorCode
from .port import AnswerWriter, KnowledgeBackend
from .state import BackendStateStore, InMemoryBackendState
from .types import (
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
    ModelSettings,
    PrincipalContext,
    QueryRequest,
    QueryResult,
    SourceRecord,
    StructuredContent,
)

__all__ = [
    "AccessPartitionRef",
    "AnswerRequest",
    "AnswerWriter",
    "BackendCapabilities",
    "BackendError",
    "BackendErrorCode",
    "BackendHealth",
    "BackendReference",
    "BackendStateStore",
    "DeletionResult",
    "EnrichmentRequest",
    "EnrichmentResult",
    "EvidenceItem",
    "ExpectedRecord",
    "DummyKnowledgeBackend",
    "IndexingProgress",
    "IndexingProgressRequest",
    "InMemoryBackendState",
    "IngestionResult",
    "KnowledgeBackend",
    "PrincipalContext",
    "ModelSelection",
    "ModelSettings",
    "QueryRequest",
    "QueryResult",
    "SourceRecord",
    "StructuredContent",
]
