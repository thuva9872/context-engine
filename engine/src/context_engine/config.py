"""Environment-backed process settings with safe local defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    """Read a boolean environment value or return the supplied default."""

    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


# Smaller chunks multiply embeddings and extraction calls without making citations any finer.
MIN_CHUNK_TOKENS = 128


def _env_chunk_tokens() -> int | None:
    """Read the target chunk size in tokens, or None to let the provider size chunks.

    Unset keeps the provider's default, chunks as large as the models accept. The value is
    checked by `KnowledgeBackendSettings` itself, so a bad one stops startup.
    """

    value = os.getenv("CONTEXT_ENGINE_CHUNK_TOKENS", "").strip()
    return int(value) if value else None


def _env_list(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    """Read a comma-separated environment value or return the supplied default."""

    value = os.getenv(name)
    if value is None:
        return default
    return tuple(item.strip().lower() for item in value.split(",") if item.strip())


@dataclass(frozen=True, slots=True)
class Settings:
    """Configure API, worker, database, and migration behavior."""

    database_path: Path
    migrations_path: Path
    log_level: str = "INFO"
    worker_poll_seconds: float = 1.0
    worker_lease_seconds: int = 30
    worker_max_attempts: int = 5
    # Only the static mode exists in this milestone; there is no mode without a verifier.
    auth_mode: str = "static"
    static_tokens_path: Path = Path(".context-engine/static-tokens.json")
    # "none" keeps the worker ledger-only; "provider" converges the knowledge backend (M4).
    knowledge_backend: str = "none"
    # How often the worker cross-checks the backend against the ledger in provider mode.
    indexing_check_seconds: float = 300.0
    staging_path: Path = Path(".context-engine/staging")
    upload_max_bytes: int = 25 * 1024 * 1024
    upload_ttl_seconds: int = 24 * 60 * 60
    upload_content_types: tuple[str, ...] = (
        "text/plain",
        "text/markdown",
        "text/html",
        "application/json",
        "application/pdf",
        "text/csv",
        "application/xml",
        "text/xml",
    )
    # Master key that encrypts literal model keys at rest (base64url, 32 bytes). Unset means
    # the engine accepts only key references.
    secrets_key: str = ""
    # Where `cp:` key references are resolved; unset means such references are refused.
    control_plane_url: str = ""
    control_plane_token: str = ""
    # Characters of evidence one answer may send to the model; bounds the cost of an answer.
    answer_context_chars: int = 32_000

    @classmethod
    def from_env(cls) -> Settings:
        """Load settings from environment variables and safe local defaults."""

        default_migrations = Path(__file__).resolve().parents[2] / "migrations"
        return cls(
            database_path=Path(
                os.getenv("CONTEXT_ENGINE_DB_PATH", ".context-engine/control.db")
            ).expanduser(),
            migrations_path=Path(
                os.getenv("CONTEXT_ENGINE_MIGRATIONS_PATH", str(default_migrations))
            ).expanduser(),
            log_level=os.getenv("CONTEXT_ENGINE_LOG_LEVEL", "INFO").upper(),
            worker_poll_seconds=float(os.getenv("CONTEXT_ENGINE_WORKER_POLL_SECONDS", "1")),
            worker_lease_seconds=int(os.getenv("CONTEXT_ENGINE_WORKER_LEASE_SECONDS", "30")),
            worker_max_attempts=int(os.getenv("CONTEXT_ENGINE_WORKER_MAX_ATTEMPTS", "5")),
            auth_mode=os.getenv("CONTEXT_ENGINE_AUTH_MODE", "static").strip().lower(),
            static_tokens_path=Path(
                os.getenv("CONTEXT_ENGINE_STATIC_TOKENS_PATH", ".context-engine/static-tokens.json")
            ).expanduser(),
            knowledge_backend=os.getenv("CONTEXT_ENGINE_KNOWLEDGE_BACKEND", "none").strip().lower(),
            indexing_check_seconds=float(os.getenv("CONTEXT_ENGINE_INDEXING_CHECK_SECONDS", "300")),
            staging_path=Path(
                os.getenv("CONTEXT_ENGINE_STAGING_PATH", ".context-engine/staging")
            ).expanduser(),
            upload_max_bytes=int(
                os.getenv("CONTEXT_ENGINE_UPLOAD_MAX_BYTES", str(25 * 1024 * 1024))
            ),
            upload_ttl_seconds=int(os.getenv("CONTEXT_ENGINE_UPLOAD_TTL_SECONDS", "86400")),
            upload_content_types=_env_list(
                "CONTEXT_ENGINE_UPLOAD_CONTENT_TYPES",
                (
                    "text/plain",
                    "text/markdown",
                    "text/html",
                    "application/json",
                    "application/pdf",
                    "text/csv",
                    "application/xml",
                    "text/xml",
                ),
            ),
            secrets_key=os.getenv("CONTEXT_ENGINE_SECRETS_KEY", "").strip(),
            control_plane_url=os.getenv("CONTEXT_ENGINE_CONTROL_PLANE_URL", "").strip(),
            control_plane_token=os.getenv("CONTEXT_ENGINE_CONTROL_PLANE_TOKEN", "").strip(),
            answer_context_chars=int(os.getenv("CONTEXT_ENGINE_ANSWER_CONTEXT_CHARS", "32000")),
        )


@dataclass(frozen=True, slots=True)
class KnowledgeBackendSettings:
    """Configure the private knowledge backend with engine-owned settings."""

    telemetry_enabled: bool = False
    file_logging_enabled: bool = False
    query_cache_enabled: bool = False
    access_control_required: bool = True
    local_content_access_enabled: bool = False
    remote_content_access_enabled: bool = False
    raw_graph_query_enabled: bool = False
    relational_store: str = "sqlite"
    graph_store: str = "kuzu"
    vector_store: str = "lancedb"
    model_provider: str = "openai"
    model_name: str = "openai/gpt-5-mini"
    model_api_key: str = ""
    embedding_provider: str = "openai"
    embedding_model: str = "openai/text-embedding-3-small"
    embedding_dimensions: int = 1536
    embedding_api_key: str = ""
    storage_path: Path = Path(".context-engine/knowledge")
    # Target chunk size in tokens for new writes; None keeps the provider's default, chunks as
    # large as the models accept. Smaller chunks give finer passages and citations.
    chunk_tokens: int | None = None
    # The engine's own identity; it owns every backend isolation unit and hands out read access.
    service_principal_id: str = "context-engine-service"
    live_test_enabled: bool = False
    live_test_password: str = ""

    def __post_init__(self) -> None:
        """Refuse a chunk size too small to be useful, whether from the environment or code."""

        if self.chunk_tokens is not None and self.chunk_tokens < MIN_CHUNK_TOKENS:
            raise ValueError(
                f"CONTEXT_ENGINE_CHUNK_TOKENS must be at least {MIN_CHUNK_TOKENS} tokens"
            )

    @classmethod
    def from_env(cls) -> KnowledgeBackendSettings:
        """Load backend settings exclusively from engine-owned environment names."""

        return cls(
            telemetry_enabled=_env_bool("CONTEXT_ENGINE_TELEMETRY_ENABLED", False),
            file_logging_enabled=_env_bool("CONTEXT_ENGINE_FILE_LOGGING_ENABLED", False),
            query_cache_enabled=_env_bool("CONTEXT_ENGINE_QUERY_CACHE_ENABLED", False),
            access_control_required=_env_bool("CONTEXT_ENGINE_ACCESS_CONTROL_REQUIRED", True),
            local_content_access_enabled=_env_bool(
                "CONTEXT_ENGINE_LOCAL_CONTENT_ACCESS_ENABLED", False
            ),
            remote_content_access_enabled=_env_bool(
                "CONTEXT_ENGINE_REMOTE_CONTENT_ACCESS_ENABLED", False
            ),
            raw_graph_query_enabled=_env_bool("CONTEXT_ENGINE_RAW_GRAPH_QUERY_ENABLED", False),
            relational_store=os.getenv("CONTEXT_ENGINE_RELATIONAL_STORE", "sqlite"),
            graph_store=os.getenv("CONTEXT_ENGINE_GRAPH_STORE", "kuzu"),
            vector_store=os.getenv("CONTEXT_ENGINE_VECTOR_STORE", "lancedb"),
            model_provider=os.getenv("CONTEXT_ENGINE_MODEL_PROVIDER", "openai"),
            model_name=os.getenv("CONTEXT_ENGINE_MODEL_NAME", "openai/gpt-5-mini"),
            model_api_key=os.getenv("CONTEXT_ENGINE_MODEL_API_KEY", ""),
            embedding_provider=os.getenv("CONTEXT_ENGINE_EMBEDDING_PROVIDER", "openai"),
            embedding_model=os.getenv(
                "CONTEXT_ENGINE_EMBEDDING_MODEL", "openai/text-embedding-3-small"
            ),
            embedding_dimensions=int(os.getenv("CONTEXT_ENGINE_EMBEDDING_DIMENSIONS", "1536")),
            embedding_api_key=os.getenv("CONTEXT_ENGINE_EMBEDDING_API_KEY", ""),
            storage_path=Path(
                os.getenv("CONTEXT_ENGINE_KNOWLEDGE_STORAGE_PATH", ".context-engine/knowledge")
            ).expanduser(),
            chunk_tokens=_env_chunk_tokens(),
            service_principal_id=os.getenv(
                "CONTEXT_ENGINE_SERVICE_PRINCIPAL_ID", "context-engine-service"
            ).strip(),
            live_test_enabled=_env_bool("CONTEXT_ENGINE_RUN_LIVE_PROVIDER", False),
            live_test_password=os.getenv("CONTEXT_ENGINE_LIVE_TEST_PASSWORD", ""),
        )
