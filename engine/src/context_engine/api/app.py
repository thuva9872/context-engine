"""FastAPI composition for the public REST boundary."""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from typing import Annotated
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, Path, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import ValidationError as SchemaValidationError

from context_engine.application import (
    AccessDeniedError,
    ApplicationError,
    ConflictError,
    ContextEngineService,
    NotFoundError,
    PayloadTooLargeError,
    ServiceUnavailableError,
    UnauthenticatedError,
    UnsupportedContentTypeError,
    UploadPolicy,
    ValidationError,
    build_answer_generator,
    build_query_backend,
)
from context_engine.config import KnowledgeBackendSettings, Settings
from context_engine.domain import (
    IngestionCommand,
    JobState,
    ModelUse,
    SourceState,
    VersionOrdering,
)
from context_engine.observability import (
    MetricsRegistry,
    configure_logging,
    get_logger,
    log_event,
)
from context_engine.persistence import (
    AuthorizationRepository,
    ControlDatabase,
    ControlPlaneRepository,
    QueryRepository,
    ReadAccessRepository,
    SourceRepository,
    StagingStore,
)
from context_engine.security.authorization import Authorizer
from context_engine.security.identity import (
    AuthenticatedPrincipal,
    AuthenticationError,
    TokenVerifier,
    build_token_verifier,
    provision_static_identities,
)
from context_engine.security.secrets import build_secret_store

from .multipart import read_ingestion_parts
from .progress import build_progress_router
from .schemas import (
    CheckpointResponse,
    ContextQueryRequest,
    ContextQueryResponse,
    ContextSpaceResponse,
    CreateContextSpaceRequest,
    DirectIngestionEvent,
    EffectivePermissionsResponse,
    ErrorResponse,
    EvidenceResponse,
    GrantResponse,
    HealthResponse,
    IngestionRequest,
    JobAcceptedResponse,
    JobResponse,
    PrincipalResponse,
    PutCheckpointRequest,
    PutGrantRequest,
    RecordStatusResponse,
    RegisterSourceRequest,
    SourceResponse,
    SpaceConfigurationRequest,
    SpaceConfigurationResponse,
    SyncRunResponse,
    UpdateSourceRequest,
    UploadResponse,
)

logger = get_logger(__name__)

ResourceId = Annotated[str, Path(min_length=1, max_length=200)]
GrantId = Annotated[str, Path(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")]
RecordId = Annotated[str, Path(min_length=1, max_length=500)]
IdempotencyKey = Annotated[str, Header(alias="Idempotency-Key", min_length=8, max_length=200)]
DeliveryKey = Annotated[str, Path(min_length=8, max_length=200)]


def _trace_id(request: Request) -> str:
    return getattr(request.state, "trace_id", f"trace_{uuid4().hex}")


def _error_status(error: ApplicationError) -> int:
    if isinstance(error, NotFoundError):
        return 404
    if isinstance(error, ConflictError):
        return 409
    if isinstance(error, UnauthenticatedError):
        return 401
    if isinstance(error, AccessDeniedError):
        return 403
    if isinstance(error, PayloadTooLargeError):
        return 413
    if isinstance(error, UnsupportedContentTypeError):
        return 415
    if isinstance(error, ServiceUnavailableError):
        return 503
    return 400


def _error_body(code: str, message: str, trace_id: str) -> dict[str, str]:
    return ErrorResponse(code=code, message=message, traceId=trace_id).model_dump(
        by_alias=True, exclude_none=True
    )


def create_app(
    settings: Settings | None = None,
    service: ContextEngineService | None = None,
    database: ControlDatabase | None = None,
    metrics: MetricsRegistry | None = None,
    verifier: TokenVerifier | None = None,
    knowledge_backend: object | None = None,
    readiness_checks: Sequence[Callable[[], bool]] = (),
    answer_generator: object | None = None,
) -> FastAPI:
    """Build the REST application and wire its control-plane dependencies.

    `knowledge_backend` lets tests inject a backend; it is typed opaquely because this package
    never depends on the backend port. Otherwise the application layer builds it from settings.
    `readiness_checks` lets the process that serves the app add conditions to readiness, such
    as a worker loop running in the same process. `answer_generator` writes answers; whoever
    injects a backend injects the generator built from the same provider settings, because the
    provider binds one storage root per process. Without either, provider mode builds both
    from the environment.
    """

    settings = settings or Settings.from_env()
    configure_logging(settings.log_level)
    metrics = metrics or MetricsRegistry()
    database = database or ControlDatabase(settings.database_path, settings.migrations_path)
    database.migrate()
    authorization = AuthorizationRepository(database)
    if verifier is None:
        static_verifier = build_token_verifier(settings)
        provision_static_identities(static_verifier, authorization)
        verifier = static_verifier
    upload_policy = UploadPolicy(
        max_bytes=settings.upload_max_bytes,
        ttl_seconds=settings.upload_ttl_seconds,
        content_types=frozenset(settings.upload_content_types),
    )
    if service is None:
        backend = knowledge_backend or build_query_backend(settings, database)
        repository = ControlPlaneRepository(database)
        service = ContextEngineService(
            repository,
            authorization,
            Authorizer(authorization, metrics),
            metrics,
            SourceRepository(database),
            StagingStore(settings.staging_path),
            upload_policy,
            settings.worker_max_attempts,
            indexing_enabled=settings.knowledge_backend == "provider",
            knowledge_backend=backend,  # type: ignore[arg-type]
            read_access=ReadAccessRepository(database),
            secrets=build_secret_store(
                settings.secrets_key, settings.control_plane_url, settings.control_plane_token
            ),
            queries=QueryRepository(database),
            default_models=_default_models(settings),
            answers=answer_generator  # type: ignore[arg-type]
            if answer_generator is not None or knowledge_backend is not None
            else build_answer_generator(settings),
            answer_context_chars=settings.answer_context_chars,
        )

    app = FastAPI(title="Context Engine API", version="0.4.0")
    app.state.database = database
    app.state.metrics = metrics
    app.state.service = service

    async def current_principal(
        request: Request,
        authorization_header: Annotated[str | None, Header(alias="Authorization")] = None,
    ) -> AuthenticatedPrincipal:
        # The principal is built only from the verified credential; bodies cannot supply it.
        scheme, _, token = (authorization_header or "").strip().partition(" ")
        token = token.strip()
        if scheme.lower() != "bearer" or not token:
            raise UnauthenticatedError()
        try:
            identity = await verifier.verify(token)
        except AuthenticationError as exc:
            raise UnauthenticatedError() from exc
        principal = authorization.resolve_principal(
            identity.issuer, identity.subject, identity.kind, identity.email
        )
        return AuthenticatedPrincipal(
            principal_id=principal.id,
            kind=principal.kind,
            email=principal.email,
            groups=identity.groups,
            trace_id=_trace_id(request),
        )

    @app.middleware("http")
    async def trace_requests(request: Request, call_next):
        # Keep request bodies and credentials out of logs; only bounded trace metadata is retained.
        incoming = request.headers.get("X-Trace-Id", "").strip()
        request.state.trace_id = incoming[:200] if incoming else f"trace_{uuid4().hex}"
        started = time.perf_counter()
        response = await call_next(request)
        response.headers["X-Trace-Id"] = request.state.trace_id
        log_event(
            logger,
            "http_request_completed",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=round((time.perf_counter() - started) * 1000, 2),
            trace_id=request.state.trace_id,
        )
        metrics.increment("context_engine_http_requests_total")
        return response

    @app.exception_handler(ApplicationError)
    async def application_error(request: Request, error: ApplicationError) -> JSONResponse:
        status = _error_status(error)
        headers = {"WWW-Authenticate": "Bearer"} if status == 401 else None
        return JSONResponse(
            status_code=status,
            content=_error_body(error.code, error.message, _trace_id(request)),
            headers=headers,
        )

    @app.exception_handler(RequestValidationError)
    async def request_validation_error(
        request: Request, _error: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content=_error_body("invalid_request", "Request is invalid", _trace_id(request)),
        )

    @app.get("/v1/health/live", response_model=HealthResponse)
    async def liveness() -> HealthResponse:
        return HealthResponse(status="ok")

    @app.get("/v1/health/ready", response_model=HealthResponse)
    async def readiness(response: Response) -> HealthResponse:
        ready = database.ping() and all(check() for check in readiness_checks)
        if not ready:
            response.status_code = 503
        return HealthResponse(status="ok" if ready else "unavailable")

    @app.get("/internal/metrics", include_in_schema=False)
    async def process_metrics() -> PlainTextResponse:
        return PlainTextResponse(metrics.render_prometheus(), media_type="text/plain")

    @app.get(
        "/v1/auth/me",
        response_model=PrincipalResponse,
        response_model_by_alias=True,
        response_model_exclude_none=True,
    )
    async def current_principal_view(
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> PrincipalResponse:
        return PrincipalResponse.from_principal(principal)

    @app.get(
        "/v1/auth/permissions",
        response_model=EffectivePermissionsResponse,
        response_model_by_alias=True,
    )
    async def effective_permissions(
        resource_id: Annotated[str, Query(alias="resourceId", min_length=1, max_length=200)],
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> EffectivePermissionsResponse:
        actions = service.effective_permissions(principal, resource_id)
        return EffectivePermissionsResponse(
            resourceId=resource_id, actions=sorted(item.value for item in actions)
        )

    @app.post(
        "/v1/spaces",
        response_model=ContextSpaceResponse,
        response_model_by_alias=True,
        response_model_exclude_none=True,
        status_code=201,
    )
    async def create_context_space(
        body: CreateContextSpaceRequest,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> ContextSpaceResponse:
        return ContextSpaceResponse.from_domain(
            service.create_context_space(principal, body.name, body.description)
        )

    @app.get(
        "/v1/spaces",
        response_model=list[ContextSpaceResponse],
        response_model_by_alias=True,
        response_model_exclude_none=True,
    )
    async def list_context_spaces(
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> list[ContextSpaceResponse]:
        return [
            ContextSpaceResponse.from_domain(space)
            for space in service.list_context_spaces(principal)
        ]

    @app.get(
        "/v1/spaces/{space_id}",
        response_model=ContextSpaceResponse,
        response_model_by_alias=True,
        response_model_exclude_none=True,
    )
    async def get_context_space(
        space_id: str,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> ContextSpaceResponse:
        return ContextSpaceResponse.from_domain(service.get_context_space(principal, space_id))

    @app.post(
        "/v1/spaces/{space_id}/sources",
        response_model=SourceResponse,
        response_model_by_alias=True,
        status_code=201,
    )
    async def register_source(
        space_id: ResourceId,
        body: RegisterSourceRequest,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> SourceResponse:
        return SourceResponse.from_domain(
            service.register_source(
                principal,
                space_id,
                body.name,
                body.type,
                VersionOrdering(body.version_ordering),
                dict(body.audience_mapping),
                dict(body.structured_mapping) if body.structured_mapping else None,
            )
        )

    @app.get(
        "/v1/spaces/{space_id}/sources",
        response_model=list[SourceResponse],
        response_model_by_alias=True,
    )
    async def list_sources(
        space_id: ResourceId, principal: AuthenticatedPrincipal = Depends(current_principal)
    ) -> list[SourceResponse]:
        return [
            SourceResponse.from_domain(source)
            for source in service.list_sources(principal, space_id)
        ]

    @app.get("/v1/sources/{source_id}", response_model=SourceResponse, response_model_by_alias=True)
    async def get_source(
        source_id: ResourceId, principal: AuthenticatedPrincipal = Depends(current_principal)
    ) -> SourceResponse:
        return SourceResponse.from_domain(service.get_source(principal, source_id))

    @app.patch(
        "/v1/sources/{source_id}", response_model=SourceResponse, response_model_by_alias=True
    )
    async def update_source(
        source_id: ResourceId,
        body: UpdateSourceRequest,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> SourceResponse:
        return SourceResponse.from_domain(
            service.update_source(
                principal,
                source_id,
                name=body.name,
                state=SourceState(body.state) if body.state else None,
                audience_mapping=(
                    dict(body.audience_mapping) if body.audience_mapping is not None else None
                ),
                structured_mapping=(
                    dict(body.structured_mapping) if body.structured_mapping is not None else None
                ),
            )
        )

    @app.get(
        "/v1/sources/{source_id}/checkpoints",
        response_model=CheckpointResponse,
        response_model_by_alias=True,
    )
    async def get_checkpoint(
        source_id: ResourceId, principal: AuthenticatedPrincipal = Depends(current_principal)
    ) -> CheckpointResponse:
        return CheckpointResponse.from_domain(service.get_checkpoint(principal, source_id))

    @app.put(
        "/v1/sources/{source_id}/checkpoints",
        response_model=CheckpointResponse,
        response_model_by_alias=True,
    )
    async def put_checkpoint(
        source_id: ResourceId,
        body: PutCheckpointRequest,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> CheckpointResponse:
        return CheckpointResponse.from_domain(
            service.put_checkpoint(principal, source_id, body.cursor)
        )

    @app.post(
        "/v1/sources/{source_id}/uploads",
        response_model=UploadResponse,
        response_model_by_alias=True,
        status_code=201,
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {
                    "application/octet-stream": {"schema": {"type": "string", "format": "binary"}}
                },
            }
        },
    )
    async def stage_upload(
        source_id: ResourceId,
        request: Request,
        idempotency_key: IdempotencyKey,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> UploadResponse:
        # Authorize before reading the body so an unbound caller cannot occupy the size budget.
        service.authorize_upload(principal, source_id)
        chunks: list[bytes] = []
        total = 0
        async for chunk in request.stream():
            total += len(chunk)
            if total > upload_policy.max_bytes:
                raise PayloadTooLargeError()
            chunks.append(chunk)
        upload = service.stage_upload(
            principal,
            source_id,
            request.headers.get("content-type", ""),
            b"".join(chunks),
            idempotency_key,
        )
        return UploadResponse.from_domain(upload)

    @app.get(
        "/v1/sources/{source_id}/records/{record_id}",
        response_model=RecordStatusResponse,
        response_model_by_alias=True,
        response_model_exclude_none=True,
    )
    async def get_record_status(
        source_id: ResourceId,
        record_id: RecordId,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> RecordStatusResponse:
        return RecordStatusResponse.from_domain(
            service.get_record_status(principal, source_id, record_id)
        )

    @app.post(
        "/v1/sources/{source_id}/sync-runs",
        response_model=SyncRunResponse,
        response_model_by_alias=True,
        status_code=201,
    )
    async def open_sync_run(
        source_id: ResourceId,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> SyncRunResponse:
        return SyncRunResponse.from_domain(service.open_sync_run(principal, source_id))

    @app.post(
        "/v1/sources/{source_id}/sync-runs/{run_id}/complete",
        response_model=SyncRunResponse,
        response_model_by_alias=True,
    )
    async def complete_sync_run(
        source_id: ResourceId,
        run_id: ResourceId,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> SyncRunResponse:
        return SyncRunResponse.from_domain(service.complete_sync_run(principal, source_id, run_id))

    @app.get(
        "/v1/sources/{source_id}/jobs",
        response_model=list[JobResponse],
        response_model_by_alias=True,
        response_model_exclude_none=True,
    )
    async def list_source_jobs(
        source_id: ResourceId,
        state: Annotated[str | None, Query(pattern="^(queued|running|succeeded|failed)$")] = None,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> list[JobResponse]:
        jobs = service.list_source_jobs(principal, source_id, JobState(state) if state else None)
        return [JobResponse.from_domain(job) for job in jobs]

    @app.post(
        "/v1/queries",
        response_model=ContextQueryResponse,
        response_model_by_alias=True,
        response_model_exclude_none=True,
    )
    async def query_context(
        body: ContextQueryRequest,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> ContextQueryResponse:
        result = await service.query_context(
            principal, body.space_id, body.question, body.mode, body.limit
        )
        return ContextQueryResponse.from_domain(result)

    @app.get(
        "/v1/queries/{query_id}",
        response_model=ContextQueryResponse,
        response_model_by_alias=True,
        response_model_exclude_none=True,
    )
    async def get_query(
        query_id: ResourceId,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> ContextQueryResponse:
        """Reopen a stored query for its asker; the service re-checks every passage."""

        return ContextQueryResponse.from_domain(service.get_query(principal, query_id))

    @app.get(
        "/v1/queries/{query_id}/evidence",
        response_model=list[EvidenceResponse],
        response_model_by_alias=True,
        response_model_exclude_none=True,
    )
    async def list_query_evidence(
        query_id: ResourceId,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> list[EvidenceResponse]:
        """List a stored query's currently authorized evidence, under the same rules."""

        result = service.get_query(principal, query_id)
        return [EvidenceResponse.from_domain(item) for item in result.evidence]

    @app.get(
        "/v1/evidence/{evidence_id}",
        response_model=EvidenceResponse,
        response_model_by_alias=True,
        response_model_exclude_none=True,
    )
    async def get_evidence(
        evidence_id: ResourceId,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> EvidenceResponse:
        """Open one evidence item for a caller who could retrieve it now, else 404."""

        return EvidenceResponse.from_domain(service.get_evidence(principal, evidence_id))

    @app.post(
        "/v1/spaces/{space_id}/enrichments",
        response_model=JobAcceptedResponse,
        response_model_by_alias=True,
        status_code=202,
    )
    async def accept_enrichment(
        space_id: ResourceId,
        response: Response,
        idempotency_key: IdempotencyKey,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> JobAcceptedResponse:
        job = service.accept_enrichment(principal, space_id, idempotency_key)
        status_url = f"/v1/jobs/{job.id}"
        response.headers["Location"] = status_url
        return JobAcceptedResponse(jobId=job.id, statusUrl=status_url)

    @app.post(
        "/v1/ingestions",
        response_model=JobAcceptedResponse,
        response_model_by_alias=True,
        status_code=202,
    )
    async def accept_ingestion(
        body: IngestionRequest,
        response: Response,
        idempotency_key: IdempotencyKey,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> JobAcceptedResponse:
        job = service.accept_ingestion(principal, body.to_command(), idempotency_key)
        status_url = f"/v1/jobs/{job.id}"
        response.headers["Location"] = status_url
        return JobAcceptedResponse(jobId=job.id, statusUrl=status_url)

    @app.post(
        "/v1/sources/{source_id}/ingestions",
        response_model=JobAcceptedResponse,
        response_model_by_alias=True,
        status_code=202,
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {
                    "multipart/form-data": {
                        "schema": {
                            "type": "object",
                            "required": ["event"],
                            "properties": {
                                "event": {"type": "string", "format": "json"},
                                "content": {"type": "string", "format": "binary"},
                            },
                        },
                        "encoding": {"event": {"contentType": "application/json"}},
                    }
                },
            }
        },
    )
    async def accept_direct_ingestion(
        source_id: ResourceId,
        request: Request,
        response: Response,
        idempotency_key: IdempotencyKey,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> JobAcceptedResponse:
        # Authorize before reading the body so an unbound caller cannot occupy the size budget.
        service.authorize_upload(principal, source_id)

        def parse_event(raw: bytes) -> IngestionCommand:
            try:
                event = DirectIngestionEvent.model_validate_json(raw)
            except SchemaValidationError as exc:
                raise ValidationError("Ingestion event is invalid") from exc
            if event.source_id != source_id:
                raise ValidationError("Event source must match the request path")
            return event.to_command()

        parts = await read_ingestion_parts(
            request.headers.get("content-type", ""),
            request.stream(),
            parse_event,
            upload_policy.max_bytes,
        )
        job = service.accept_direct_ingestion(
            principal,
            source_id,
            parts.event,
            parts.content,
            parts.content_type,
            idempotency_key,
        )
        status_url = f"/v1/jobs/{job.id}"
        response.headers["Location"] = status_url
        return JobAcceptedResponse(jobId=job.id, statusUrl=status_url)

    @app.get(
        "/v1/sources/{source_id}/ingestions/{idempotency_key}",
        response_model=JobAcceptedResponse,
        response_model_by_alias=True,
    )
    async def find_ingestion(
        source_id: ResourceId,
        idempotency_key: DeliveryKey,
        response: Response,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> JobAcceptedResponse:
        # A connector that lost the reply finds its job here instead of resending the content.
        job = service.find_ingestion(principal, source_id, idempotency_key)
        status_url = f"/v1/jobs/{job.id}"
        response.headers["Location"] = status_url
        return JobAcceptedResponse(jobId=job.id, statusUrl=status_url)

    @app.get(
        "/v1/jobs/{job_id}",
        response_model=JobResponse,
        response_model_by_alias=True,
        response_model_exclude_none=True,
    )
    async def get_job(
        job_id: str,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> JobResponse:
        return JobResponse.from_domain(service.get_job(principal, job_id))

    @app.get(
        "/v1/resources/{resource_id}/grants",
        response_model=list[GrantResponse],
        response_model_by_alias=True,
        response_model_exclude_none=True,
    )
    async def list_grants(
        resource_id: ResourceId,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> list[GrantResponse]:
        return [
            GrantResponse.from_domain(grant)
            for grant in service.list_grants(principal, resource_id)
        ]

    @app.put(
        "/v1/resources/{resource_id}/grants/{grant_id}",
        response_model=GrantResponse,
        response_model_by_alias=True,
        response_model_exclude_none=True,
    )
    async def put_grant(
        resource_id: ResourceId,
        grant_id: GrantId,
        body: PutGrantRequest,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> GrantResponse:
        return GrantResponse.from_domain(
            service.put_grant(
                principal,
                resource_id,
                grant_id,
                body.to_actions(),
                body.principal_id,
                body.group,
            )
        )

    @app.delete("/v1/resources/{resource_id}/grants/{grant_id}", status_code=204)
    async def delete_grant(
        resource_id: ResourceId,
        grant_id: GrantId,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> Response:
        service.delete_grant(principal, resource_id, grant_id)
        return Response(status_code=204)

    storage = _storage_placement(settings)

    @app.delete(
        "/v1/spaces/{space_id}",
        response_model=JobAcceptedResponse,
        response_model_by_alias=True,
        status_code=202,
    )
    async def delete_context_space(
        space_id: ResourceId,
        response: Response,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> JobAcceptedResponse:
        job = service.delete_context_space(principal, space_id)
        status_url = f"/v1/jobs/{job.id}"
        response.headers["Location"] = status_url
        return JobAcceptedResponse(jobId=job.id, statusUrl=status_url)

    @app.get(
        "/v1/spaces/{space_id}/configuration",
        response_model=SpaceConfigurationResponse,
        response_model_by_alias=True,
        response_model_exclude_none=True,
    )
    async def get_space_configuration(
        space_id: ResourceId,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> SpaceConfigurationResponse:
        # Models and key kinds only; a key never leaves the engine.
        return SpaceConfigurationResponse.from_view(
            service.get_space_configuration(principal, space_id), storage
        )

    @app.put(
        "/v1/spaces/{space_id}/configuration",
        response_model=SpaceConfigurationResponse,
        response_model_by_alias=True,
        response_model_exclude_none=True,
    )
    async def put_space_configuration(
        space_id: ResourceId,
        body: SpaceConfigurationRequest,
        principal: AuthenticatedPrincipal = Depends(current_principal),
    ) -> SpaceConfigurationResponse:
        view = service.put_space_configuration(
            principal,
            space_id,
            body.embedding.to_input() if body.embedding else None,
            body.llm.to_input() if body.llm else None,
        )
        return SpaceConfigurationResponse.from_view(view, storage)

    app.include_router(build_progress_router(service, current_principal))
    return app


def _default_models(settings: Settings) -> dict[str, ModelUse]:
    """Name the environment's models, which serve every space that configures none.

    Stored queries record the model that served them, so a query of an unconfigured space names
    the environment's model as it was at that moment, not a blank.
    """

    if settings.knowledge_backend != "provider":
        return {}
    backend = KnowledgeBackendSettings.from_env()
    return {
        "embedding": ModelUse(backend.embedding_provider, backend.embedding_model, "environment"),
        "language": ModelUse(backend.model_provider, backend.model_name, "environment"),
    }


def _storage_placement(settings: Settings) -> dict[str, str]:
    """Report where each kind of store runs; the control plane shows this per space.

    Placement is engine-wide until per-space storage is decided, so every space reports the
    same stores. Without a knowledge backend there is nothing to report.
    """

    if settings.knowledge_backend != "provider":
        return {"vector": "none", "graph": "none", "relational": "none"}
    backend = KnowledgeBackendSettings.from_env()
    return {
        "vector": backend.vector_store,
        "graph": backend.graph_store,
        "relational": backend.relational_store,
    }
