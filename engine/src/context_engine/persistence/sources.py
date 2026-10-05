"""SQLite repository for sources, checkpoints, staged uploads, and the record ledger."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from sqlite3 import Connection, Row
from uuid import uuid4

from context_engine.domain import (
    AccessPartition,
    IndexingSnapshot,
    IndexingState,
    IndexState,
    Job,
    LocationState,
    RecordCounts,
    RecordLocation,
    RecordOutcome,
    RecordState,
    RecordStatus,
    RecordTransition,
    Source,
    SourceCheckpoint,
    SourceState,
    StagedUpload,
    SyncRun,
    SyncRunState,
    VersionOrdering,
    partition_key,
)

from .database import ControlDatabase
from .queries import delete_released_evidence
from .repository import EffectConflict, IdempotencyConflict, insert_source_effect


def _timestamp(value: datetime | None = None) -> str:
    return (value or datetime.now(UTC)).astimezone(UTC).isoformat()


def _source(row: Row) -> Source:
    mapping = json.loads(row["audience_mapping_json"])
    structured_raw = row["structured_mapping_json"]
    structured = json.loads(structured_raw) if structured_raw else {}
    return Source(
        id=row["id"],
        space_id=row["space_id"],
        name=row["name"],
        type=row["type"],
        state=SourceState(row["state"]),
        version_ordering=VersionOrdering(row["version_ordering"]),
        audience_mapping=tuple(sorted((str(key), str(value)) for key, value in mapping.items())),
        created_at=datetime.fromisoformat(row["created_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
        structured_mapping=tuple(
            sorted((str(key), str(value)) for key, value in structured.items())
        ),
    )


def _checkpoint(row: Row) -> SourceCheckpoint:
    return SourceCheckpoint(
        source_id=row["source_id"],
        cursor=row["cursor"],
        updated_by=row["updated_by"],
        updated_at=datetime.fromisoformat(row["updated_at"]),
    )


def _upload(row: Row) -> StagedUpload:
    return StagedUpload(
        id=row["id"],
        source_id=row["source_id"],
        principal_id=row["principal_id"],
        idempotency_key=row["idempotency_key"],
        content_type=row["content_type"],
        size_bytes=row["size_bytes"],
        content_hash=row["content_hash"],
        created_at=datetime.fromisoformat(row["created_at"]),
        expires_at=datetime.fromisoformat(row["expires_at"]),
    )


def _record(row: Row) -> RecordStatus:
    return RecordStatus(
        space_id=row["space_id"],
        source_id=row["source_id"],
        source_record_id=row["source_record_id"],
        state=RecordState(row["state"]),
        current_version=row["current_version"],
        source_acl_version=row["source_acl_version"],
        content_hash=row["content_hash"],
        content_ref=row["content_ref"],
        audience=tuple(json.loads(row["audience_json"])),
        quarantine_reason=row["quarantine_reason"],
        created_at=datetime.fromisoformat(row["created_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
        partition_id=row["partition_id"],
        source_url=row["source_url"],
        index_state=IndexState(row["index_state"]),
        index_error=row["index_error"],
    )


def _location(row: Row) -> RecordLocation:
    return RecordLocation(
        space_id=row["space_id"],
        source_id=row["source_id"],
        source_record_id=row["source_record_id"],
        partition_id=row["partition_id"],
        state=LocationState(row["state"]),
        version=row["version"],
        target_version=row["target_version"],
        backend_ref=row["backend_ref"],
        content_hash=row["content_hash"],
        parser_version=row["parser_version"],
        updated_at=datetime.fromisoformat(row["updated_at"]),
        written_version=row["written_version"],
    )


def _partition(row: Row) -> AccessPartition:
    return AccessPartition(
        id=row["id"],
        space_id=row["space_id"],
        audiences=tuple(json.loads(row["audiences_json"])),
        created_at=datetime.fromisoformat(row["created_at"]),
    )


def _optional_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _sync_run(row: Row) -> SyncRun:
    return SyncRun(
        id=row["id"],
        source_id=row["source_id"],
        state=SyncRunState(row["state"]),
        started_by=row["started_by"],
        started_at=datetime.fromisoformat(row["started_at"]),
        completed_at=_optional_datetime(row["completed_at"]),
    )


def _snapshot(row: Row) -> IndexingSnapshot:
    return IndexingSnapshot(
        source_id=row["source_id"],
        state=IndexingState(row["state"]),
        collected_at=datetime.fromisoformat(row["collected_at"]),
        expected=row["expected"],
        indexed=row["indexed"],
        indexing=row["indexing"],
        failed=row["failed"],
        missing=row["missing"],
        error_code=row["error_code"],
    )


class SourceRepository:
    """Persist source registration, delivery state, and the authoritative record ledger."""

    def __init__(self, database: ControlDatabase) -> None:
        self.database = database

    # Sources

    def create_source(
        self,
        space_id: str,
        name: str,
        type_: str,
        version_ordering: VersionOrdering,
        audience_mapping: dict[str, str],
        structured_mapping: dict[str, str] | None = None,
    ) -> Source:
        """Register a ready source inside a context space."""

        now = _timestamp()
        source_id = f"src_{uuid4().hex}"
        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO sources(
                    id, space_id, name, type, state, version_ordering, audience_mapping_json,
                    created_at, updated_at, structured_mapping_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_id,
                    space_id,
                    name,
                    type_,
                    SourceState.READY.value,
                    version_ordering.value,
                    json.dumps(audience_mapping, sort_keys=True, separators=(",", ":")),
                    now,
                    now,
                    json.dumps(structured_mapping, sort_keys=True, separators=(",", ":"))
                    if structured_mapping
                    else None,
                ),
            )
            row = connection.execute("SELECT * FROM sources WHERE id = ?", (source_id,)).fetchone()
        return _source(row)

    def list_sources(self, space_id: str) -> tuple[Source, ...]:
        """Return the sources registered in a space in creation order."""

        with self.database.connection() as connection:
            rows = connection.execute(
                "SELECT * FROM sources WHERE space_id = ? ORDER BY created_at, id", (space_id,)
            ).fetchall()
        return tuple(_source(row) for row in rows)

    def get_source(self, source_id: str) -> Source | None:
        """Return one source when it exists."""

        with self.database.connection() as connection:
            row = connection.execute("SELECT * FROM sources WHERE id = ?", (source_id,)).fetchone()
        return _source(row) if row else None

    def update_source(
        self,
        source_id: str,
        *,
        name: str | None = None,
        state: SourceState | None = None,
        audience_mapping: dict[str, str] | None = None,
        structured_mapping: dict[str, str] | None = None,
    ) -> Source | None:
        """Change a source's name, delivery state, audience mapping, or structured mapping."""

        with self.database.transaction() as connection:
            row = connection.execute("SELECT * FROM sources WHERE id = ?", (source_id,)).fetchone()
            if row is None:
                return None
            connection.execute(
                """
                UPDATE sources SET name = ?, state = ?, audience_mapping_json = ?,
                    structured_mapping_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    name if name is not None else row["name"],
                    state.value if state is not None else row["state"],
                    json.dumps(audience_mapping, sort_keys=True, separators=(",", ":"))
                    if audience_mapping is not None
                    else row["audience_mapping_json"],
                    json.dumps(structured_mapping, sort_keys=True, separators=(",", ":"))
                    if structured_mapping is not None
                    else row["structured_mapping_json"],
                    _timestamp(),
                    source_id,
                ),
            )
            row = connection.execute("SELECT * FROM sources WHERE id = ?", (source_id,)).fetchone()
        return _source(row)

    # Checkpoints

    def get_checkpoint(self, source_id: str) -> SourceCheckpoint | None:
        """Return the connector cursor stored for a source."""

        with self.database.connection() as connection:
            row = connection.execute(
                "SELECT * FROM source_checkpoints WHERE source_id = ?", (source_id,)
            ).fetchone()
        return _checkpoint(row) if row else None

    def put_checkpoint(self, source_id: str, cursor: str, updated_by: str) -> SourceCheckpoint:
        """Store or replace the connector cursor for a source."""

        now = _timestamp()
        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO source_checkpoints(source_id, cursor, updated_by, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                    cursor = excluded.cursor,
                    updated_by = excluded.updated_by,
                    updated_at = excluded.updated_at
                """,
                (source_id, cursor, updated_by, now),
            )
            row = connection.execute(
                "SELECT * FROM source_checkpoints WHERE source_id = ?", (source_id,)
            ).fetchone()
        return _checkpoint(row)

    # Staged uploads

    def create_upload(
        self,
        source_id: str,
        principal_id: str,
        idempotency_key: str,
        content_type: str,
        size_bytes: int,
        content_hash: str,
        ttl_seconds: int,
    ) -> tuple[StagedUpload, bool]:
        """Record staged bytes; an identical replay returns the existing upload.

        The boolean is True when a new upload row was created.
        """

        now = datetime.now(UTC)
        with self.database.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM staged_uploads WHERE source_id = ? AND idempotency_key = ?",
                (source_id, idempotency_key),
            ).fetchone()
            if existing:
                if existing["content_hash"] != content_hash:
                    raise IdempotencyConflict
                return _upload(existing), False
            upload_id = f"stg_{uuid4().hex}"
            connection.execute(
                """
                INSERT INTO staged_uploads(
                    id, source_id, principal_id, idempotency_key, content_type, size_bytes,
                    content_hash, created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    upload_id,
                    source_id,
                    principal_id,
                    idempotency_key,
                    content_type,
                    size_bytes,
                    content_hash,
                    _timestamp(now),
                    _timestamp(now + timedelta(seconds=ttl_seconds)),
                ),
            )
            row = connection.execute(
                "SELECT * FROM staged_uploads WHERE id = ?", (upload_id,)
            ).fetchone()
        return _upload(row), True

    def get_upload(self, upload_id: str) -> StagedUpload | None:
        """Return one staged upload when it exists."""

        with self.database.connection() as connection:
            row = connection.execute(
                "SELECT * FROM staged_uploads WHERE id = ?", (upload_id,)
            ).fetchone()
        return _upload(row) if row else None

    def expire_upload(self, upload_id: str, at: datetime) -> None:
        """Force an upload's expiry; used by tests and future cleanup."""

        with self.database.transaction() as connection:
            connection.execute(
                "UPDATE staged_uploads SET expires_at = ? WHERE id = ?", (_timestamp(at), upload_id)
            )

    # Ledger

    def get_record(
        self, space_id: str, source_id: str, source_record_id: str
    ) -> RecordStatus | None:
        """Return the current ledger state of one record."""

        with self.database.connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM source_records
                WHERE space_id = ? AND source_id = ? AND source_record_id = ?
                """,
                (space_id, source_id, source_record_id),
            ).fetchone()
        return _record(row) if row else None

    def list_record_versions(
        self, space_id: str, source_id: str, source_record_id: str
    ) -> tuple[dict[str, str], ...]:
        """Return the applied event history of one record, oldest first."""

        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT source_version, operation, outcome, resulting_state, job_id
                FROM record_versions
                WHERE space_id = ? AND source_id = ? AND source_record_id = ?
                ORDER BY applied_at, id
                """,
                (space_id, source_id, source_record_id),
            ).fetchall()
        return tuple(dict(row) for row in rows)

    def apply_record_event(self, job: Job, source: Source) -> RecordTransition:
        """Apply one delivered event to the ledger under ADR 0006 ordering rules.

        The idempotent effect row, the record state, and the version history change in one
        transaction, so a crash after commit replays as a no-op and a crash before commit
        leaves nothing behind. Raises `EffectConflict` when the same version arrives with
        different content, and `ValueError` when a version violates the source's ordering.
        """

        payload = job.payload
        operation = payload["operation"]
        version = payload["sourceVersion"]
        acl_version = payload["sourceAclVersion"]
        tags = tuple(payload.get("audience", ()))
        key = source.version_key(version)
        with self.database.transaction() as connection:
            row = connection.execute(
                """
                SELECT * FROM source_records
                WHERE space_id = ? AND source_id = ? AND source_record_id = ?
                """,
                (source.space_id, source.id, payload["sourceRecordId"]),
            ).fetchone()
            if not insert_source_effect(connection, job):
                state = RecordState(row["state"]) if row else RecordState.DELETED
                current = row["current_version"] if row else version
                return RecordTransition(RecordOutcome.REPLAYED, state, current, "effect_exists")

            transition = self._transition(
                source, row, operation, version, key, acl_version, tags, payload.get("contentHash")
            )
            self._write_transition(connection, job, source, row, transition, payload)
            return transition

    def _transition(
        self,
        source: Source,
        row: Row | None,
        operation: str,
        version: str,
        key: tuple[int, int | str],
        acl_version: str,
        tags: tuple[str, ...],
        content_hash: str | None,
    ) -> RecordTransition:
        """Decide the ledger outcome without touching storage."""

        if row is None:
            if operation == "delete":
                # A tombstone at this version stops older upserts from resurrecting the record.
                return RecordTransition(RecordOutcome.APPLIED, RecordState.DELETED, version)
            if operation == "acl_changed":
                return RecordTransition(
                    RecordOutcome.IGNORED_OLDER, RecordState.DELETED, version, "record_unknown"
                )
            return self._content_state(source, tags, version)

        current_state = RecordState(row["state"])
        current_version = row["current_version"]
        current_key = source.version_key(current_version)

        if operation == "upsert":
            if key < current_key:
                return RecordTransition(
                    RecordOutcome.IGNORED_OLDER, current_state, current_version, "older_version"
                )
            if key == current_key:
                if current_state is RecordState.DELETED:
                    return RecordTransition(
                        RecordOutcome.IGNORED_OLDER, current_state, current_version, "deleted"
                    )
                if row["content_hash"] != content_hash:
                    # ADR 0006: one version has one content; anything else is a source defect.
                    raise EffectConflict
                return RecordTransition(
                    RecordOutcome.REPLAYED, current_state, current_version, "same_version"
                )
            return self._content_state(source, tags, version)

        if operation == "delete":
            if key < current_key or (key == current_key and current_state is RecordState.DELETED):
                return RecordTransition(
                    RecordOutcome.IGNORED_OLDER, current_state, current_version, "older_version"
                )
            return RecordTransition(RecordOutcome.APPLIED, RecordState.DELETED, version)

        # acl_changed: applies to the current content only, ordered by the ACL version.
        if current_state is RecordState.DELETED:
            return RecordTransition(
                RecordOutcome.IGNORED_OLDER, current_state, current_version, "deleted"
            )
        if key < current_key:
            return RecordTransition(
                RecordOutcome.IGNORED_OLDER, current_state, current_version, "older_version"
            )
        if _acl_key(source, acl_version) <= _acl_key(source, row["source_acl_version"]):
            return RecordTransition(
                RecordOutcome.IGNORED_OLDER, current_state, current_version, "acl_not_newer"
            )
        _, unmapped = source.map_audience(tags)
        if unmapped:
            return RecordTransition(
                RecordOutcome.QUARANTINED,
                RecordState.QUARANTINED,
                current_version,
                "unmapped_audience",
            )
        return RecordTransition(RecordOutcome.APPLIED, RecordState.ACTIVE, current_version)

    @staticmethod
    def _content_state(source: Source, tags: tuple[str, ...], version: str) -> RecordTransition:
        _, unmapped = source.map_audience(tags)
        if unmapped:
            return RecordTransition(
                RecordOutcome.QUARANTINED, RecordState.QUARANTINED, version, "unmapped_audience"
            )
        return RecordTransition(RecordOutcome.APPLIED, RecordState.ACTIVE, version)

    def _write_transition(
        self,
        connection: Connection,
        job: Job,
        source: Source,
        row: Row | None,
        transition: RecordTransition,
        payload: dict,
    ) -> None:
        """Persist the record state and the version history for one applied event."""

        now = _timestamp()
        record_id = payload["sourceRecordId"]
        if transition.outcome in {RecordOutcome.APPLIED, RecordOutcome.QUARANTINED}:
            operation = payload["operation"]
            mapped, _ = source.map_audience(tuple(payload.get("audience", ())))
            source_url = row["source_url"] if row else None
            if operation == "delete":
                content_hash = row["content_hash"] if row else None
                content_ref = None
                audience: tuple[str, ...] = ()
                acl_version = payload["sourceAclVersion"]
            elif operation == "acl_changed":
                content_hash = row["content_hash"] if row else None
                content_ref = row["content_ref"] if row else None
                audience = mapped
                acl_version = payload["sourceAclVersion"]
            else:
                content_hash = payload.get("contentHash")
                content_ref = payload.get("contentRef")
                source_url = payload.get("sourceUrl")
                audience = mapped
                acl_version = payload["sourceAclVersion"]
            located = connection.execute(
                """
                SELECT 1 FROM record_locations
                WHERE space_id = ? AND source_id = ? AND source_record_id = ? LIMIT 1
                """,
                (source.space_id, source.id, record_id),
            ).fetchone()
            # Pending means the backend still has to converge: write an active record, or remove
            # copies of one that is no longer active. Nothing to do otherwise.
            index_state = (
                IndexState.PENDING
                if transition.state is RecordState.ACTIVE or located
                else IndexState.NOT_INDEXED
            )
            partition_id = None
            if transition.state is RecordState.ACTIVE and audience:
                # Records sharing the same mapped audiences share one partition.
                partition_id = partition_key(source.space_id, audience)
                connection.execute(
                    """
                    INSERT OR IGNORE INTO access_partitions(
                        id, space_id, audiences_json, created_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        partition_id,
                        source.space_id,
                        json.dumps(sorted(set(audience)), separators=(",", ":")),
                        now,
                    ),
                )
            connection.execute(
                """
                INSERT INTO source_records(
                    space_id, source_id, source_record_id, state, current_version,
                    source_acl_version, content_hash, content_ref, audience_json,
                    quarantine_reason, created_at, updated_at, partition_id,
                    source_url, index_state, index_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                ON CONFLICT(space_id, source_id, source_record_id) DO UPDATE SET
                    state = excluded.state,
                    current_version = excluded.current_version,
                    source_acl_version = excluded.source_acl_version,
                    content_hash = excluded.content_hash,
                    content_ref = excluded.content_ref,
                    audience_json = excluded.audience_json,
                    quarantine_reason = excluded.quarantine_reason,
                    updated_at = excluded.updated_at,
                    partition_id = excluded.partition_id,
                    source_url = excluded.source_url,
                    index_state = excluded.index_state,
                    index_error = NULL
                """,
                (
                    source.space_id,
                    source.id,
                    record_id,
                    transition.state.value,
                    transition.current_version,
                    acl_version,
                    content_hash,
                    content_ref,
                    json.dumps(list(audience), separators=(",", ":")),
                    transition.reason if transition.state is RecordState.QUARANTINED else None,
                    row["created_at"] if row else now,
                    now,
                    partition_id,
                    source_url,
                    index_state.value,
                ),
            )
        connection.execute(
            """
            INSERT INTO record_versions(
                id, space_id, source_id, source_record_id, source_version, operation,
                source_acl_version, content_hash, outcome, resulting_state, job_id, applied_at,
                content_ref
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"ver_{uuid4().hex}",
                source.space_id,
                source.id,
                record_id,
                payload["sourceVersion"],
                payload["operation"],
                payload["sourceAclVersion"],
                payload.get("contentHash"),
                transition.outcome.value,
                transition.state.value,
                job.id,
                now,
                payload.get("contentRef"),
            ),
        )

    # Sync runs

    def open_sync_run(self, source_id: str, principal_id: str) -> SyncRun:
        """Start a reading window; an unfinished earlier run is marked superseded.

        A connector that crashed mid-scan must be able to start again, so a new run never
        fails because an old one is still open.
        """

        now = _timestamp()
        run_id = f"run_{uuid4().hex}"
        with self.database.transaction() as connection:
            connection.execute(
                """
                UPDATE sync_runs SET state = ?, completed_at = ?
                WHERE source_id = ? AND state = ?
                """,
                (SyncRunState.SUPERSEDED.value, now, source_id, SyncRunState.READING.value),
            )
            connection.execute(
                """
                INSERT INTO sync_runs(id, source_id, state, started_by, started_at, completed_at)
                VALUES (?, ?, ?, ?, ?, NULL)
                """,
                (run_id, source_id, SyncRunState.READING.value, principal_id, now),
            )
            row = connection.execute("SELECT * FROM sync_runs WHERE id = ?", (run_id,)).fetchone()
        return _sync_run(row)

    def get_sync_run(self, run_id: str) -> SyncRun | None:
        """Return one sync run when it exists."""

        with self.database.connection() as connection:
            row = connection.execute("SELECT * FROM sync_runs WHERE id = ?", (run_id,)).fetchone()
        return _sync_run(row) if row else None

    def latest_sync_run(self, source_id: str) -> SyncRun | None:
        """Return the most recently started sync run of a source."""

        with self.database.connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM sync_runs WHERE source_id = ?
                ORDER BY started_at DESC, rowid DESC LIMIT 1
                """,
                (source_id,),
            ).fetchone()
        return _sync_run(row) if row else None

    def complete_sync_run(self, run_id: str) -> SyncRun | None:
        """Mark a reading run completed; runs in any other state are returned unchanged."""

        with self.database.transaction() as connection:
            connection.execute(
                "UPDATE sync_runs SET state = ?, completed_at = ? WHERE id = ? AND state = ?",
                (SyncRunState.COMPLETED.value, _timestamp(), run_id, SyncRunState.READING.value),
            )
            row = connection.execute("SELECT * FROM sync_runs WHERE id = ?", (run_id,)).fetchone()
        return _sync_run(row) if row else None

    # Progress inputs

    def list_all_sources(self) -> tuple[Source, ...]:
        """Return every registered source for background collection."""

        with self.database.connection() as connection:
            rows = connection.execute("SELECT * FROM sources ORDER BY created_at, id").fetchall()
        return tuple(_source(row) for row in rows)

    def list_source_records(self, source_id: str) -> tuple[RecordStatus, ...]:
        """Return every record of a source in any state, for deletion of the whole source."""

        with self.database.connection() as connection:
            rows = connection.execute(
                "SELECT * FROM source_records WHERE source_id = ? ORDER BY source_record_id",
                (source_id,),
            ).fetchall()
        return tuple(_record(row) for row in rows)

    def list_space_uploads(self, space_id: str) -> tuple[str, ...]:
        """Return the ids of every staged upload under a space's sources, released or not."""

        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT u.id FROM staged_uploads AS u
                JOIN sources AS s ON s.id = u.source_id
                WHERE s.space_id = ? ORDER BY u.id
                """,
                (space_id,),
            ).fetchall()
        return tuple(row["id"] for row in rows)

    def delete_record_as_engine(
        self, job: Job, source: Source, source_record_id: str
    ) -> RecordTransition | None:
        """Tombstone one record at its current version on the engine's behalf.

        Space deletion has no source version to offer, so the record is deleted at the version
        it holds. Under ADR 0006 that stops a resend of the same version from bringing it back
        while a strictly newer version from the source still can, which is the right outcome
        for a source that keeps changing after its space was removed. The effect row is keyed
        by the deletion job, so a retried job replays as a no-op. Returns None when the record
        is unknown, already deleted, or already handled by this job.
        """

        with self.database.transaction() as connection:
            row = connection.execute(
                """
                SELECT * FROM source_records
                WHERE space_id = ? AND source_id = ? AND source_record_id = ?
                """,
                (source.space_id, source.id, source_record_id),
            ).fetchone()
            if row is None or RecordState(row["state"]) is RecordState.DELETED:
                return None
            version = row["current_version"]
            payload = {
                "schemaVersion": "1",
                "spaceId": source.space_id,
                "sourceId": source.id,
                "sourceRecordId": source_record_id,
                "sourceVersion": version,
                "operation": "delete",
                "sourceAclVersion": row["source_acl_version"],
                "idempotencyKey": f"{job.idempotency_key}:{source.id}:{source_record_id}",
            }
            encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            effect_job = replace(
                job,
                idempotency_key=payload["idempotencyKey"],
                payload=payload,
                payload_hash="sha256:" + hashlib.sha256(encoded.encode()).hexdigest(),
            )
            if not insert_source_effect(connection, effect_job):
                return None
            transition = self._transition(
                source,
                row,
                "delete",
                version,
                source.version_key(version),
                row["source_acl_version"],
                (),
                None,
            )
            self._write_transition(connection, effect_job, source, row, transition, payload)
            return transition

    def list_active_records(self, source_id: str) -> tuple[RecordStatus, ...]:
        """Return the active records of a source, which are the ones a backend should hold."""

        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM source_records WHERE source_id = ? AND state = ?
                ORDER BY source_record_id
                """,
                (source_id, RecordState.ACTIVE.value),
            ).fetchall()
        return tuple(_record(row) for row in rows)

    def record_counts(self, source_id: str) -> RecordCounts:
        """Count a source's ledger records by lifecycle state."""

        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT state, COUNT(*) AS n FROM source_records
                WHERE source_id = ? GROUP BY state
                """,
                (source_id,),
            ).fetchall()
        counts = {row["state"]: row["n"] for row in rows}
        return RecordCounts(
            active=counts.get(RecordState.ACTIVE.value, 0),
            quarantined=counts.get(RecordState.QUARANTINED.value, 0),
            deleted=counts.get(RecordState.DELETED.value, 0),
        )

    def put_indexing_snapshot(self, snapshot: IndexingSnapshot) -> None:
        """Store the latest collected indexing progress of a source."""

        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO indexing_snapshots(
                    source_id, state, expected, indexed, indexing, failed, missing,
                    error_code, collected_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                    state = excluded.state,
                    expected = excluded.expected,
                    indexed = excluded.indexed,
                    indexing = excluded.indexing,
                    failed = excluded.failed,
                    missing = excluded.missing,
                    error_code = excluded.error_code,
                    collected_at = excluded.collected_at
                """,
                (
                    snapshot.source_id,
                    snapshot.state.value,
                    snapshot.expected,
                    snapshot.indexed,
                    snapshot.indexing,
                    snapshot.failed,
                    snapshot.missing,
                    snapshot.error_code,
                    _timestamp(snapshot.collected_at),
                ),
            )

    def get_indexing_snapshot(self, source_id: str) -> IndexingSnapshot | None:
        """Return the last collected indexing progress, or None when never collected."""

        with self.database.connection() as connection:
            row = connection.execute(
                "SELECT * FROM indexing_snapshots WHERE source_id = ?", (source_id,)
            ).fetchone()
        return _snapshot(row) if row else None

    # Partitions

    def list_partitions(self, space_id: str) -> tuple[AccessPartition, ...]:
        """Return every partition of a space; used by policy to resolve readable partitions."""

        with self.database.connection() as connection:
            rows = connection.execute(
                "SELECT * FROM access_partitions WHERE space_id = ? ORDER BY created_at, id",
                (space_id,),
            ).fetchall()
        return tuple(_partition(row) for row in rows)

    def get_partition(self, partition_id: str) -> AccessPartition | None:
        """Return one partition when it exists."""

        with self.database.connection() as connection:
            row = connection.execute(
                "SELECT * FROM access_partitions WHERE id = ?", (partition_id,)
            ).fetchone()
        return _partition(row) if row else None

    def ensure_partition(self, space_id: str, audiences: tuple[str, ...]) -> str:
        """Register the partition for these audiences if needed and return its identifier."""

        partition_id = partition_key(space_id, audiences)
        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO access_partitions(id, space_id, audiences_json, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    partition_id,
                    space_id,
                    json.dumps(sorted(set(audiences)), separators=(",", ":")),
                    _timestamp(),
                ),
            )
        return partition_id

    def list_all_partitions(self) -> tuple[AccessPartition, ...]:
        """Return every partition, for full read-access synchronization."""

        with self.database.connection() as connection:
            rows = connection.execute(
                "SELECT * FROM access_partitions ORDER BY created_at, id"
            ).fetchall()
        return tuple(_partition(row) for row in rows)

    # Physical record locations in the knowledge backend

    def list_locations(
        self, space_id: str, source_id: str, source_record_id: str
    ) -> tuple[RecordLocation, ...]:
        """Return every backend copy of a record, in stable order."""

        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM record_locations
                WHERE space_id = ? AND source_id = ? AND source_record_id = ?
                ORDER BY partition_id
                """,
                (space_id, source_id, source_record_id),
            ).fetchall()
        return tuple(_location(row) for row in rows)

    def begin_location_write(
        self,
        space_id: str,
        source_id: str,
        source_record_id: str,
        partition_id: str,
        target_version: str,
    ) -> None:
        """Record the intent to write a version before calling the backend."""

        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO record_locations(
                    space_id, source_id, source_record_id, partition_id, state,
                    version, target_version, updated_at
                ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?)
                ON CONFLICT(space_id, source_id, source_record_id, partition_id) DO UPDATE SET
                    state = excluded.state,
                    target_version = excluded.target_version,
                    updated_at = excluded.updated_at
                """,
                (
                    space_id,
                    source_id,
                    source_record_id,
                    partition_id,
                    LocationState.WRITING.value,
                    target_version,
                    _timestamp(),
                ),
            )

    def abort_location_write(
        self, space_id: str, source_id: str, source_record_id: str, partition_id: str
    ) -> None:
        """Undo a write intent known not to have reached the backend."""

        with self.database.transaction() as connection:
            key = (space_id, source_id, source_record_id, partition_id)
            clause = "space_id = ? AND source_id = ? AND source_record_id = ? AND partition_id = ?"
            # A copy that already held a confirmed version returns to it; a new copy disappears.
            connection.execute(
                f"""
                UPDATE record_locations SET state = ?, target_version = NULL, updated_at = ?
                WHERE {clause} AND version IS NOT NULL
                """,
                (LocationState.INDEXED.value, _timestamp(), *key),
            )
            connection.execute(
                f"DELETE FROM record_locations WHERE {clause} AND version IS NULL", key
            )

    def confirm_location(
        self,
        space_id: str,
        source_id: str,
        source_record_id: str,
        partition_id: str,
        version: str,
        backend_ref: str,
        content_hash: str,
        parser_version: str | None,
        *,
        written: bool = True,
    ) -> None:
        """Record that the backend now holds this version in this partition.

        `written` is False when an identical-content version is confirmed without a rewrite;
        the written version then stays on the earlier version the backend's metadata carries.
        """

        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO record_locations(
                    space_id, source_id, source_record_id, partition_id, state, version,
                    target_version, backend_ref, content_hash, parser_version, updated_at,
                    written_version
                ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)
                ON CONFLICT(space_id, source_id, source_record_id, partition_id) DO UPDATE SET
                    state = excluded.state,
                    version = excluded.version,
                    target_version = NULL,
                    backend_ref = excluded.backend_ref,
                    content_hash = excluded.content_hash,
                    parser_version = COALESCE(excluded.parser_version, parser_version),
                    updated_at = excluded.updated_at,
                    written_version = COALESCE(excluded.written_version, written_version)
                """,
                (
                    space_id,
                    source_id,
                    source_record_id,
                    partition_id,
                    LocationState.INDEXED.value,
                    version,
                    backend_ref,
                    content_hash,
                    parser_version,
                    _timestamp(),
                    version if written else None,
                ),
            )

    def get_location(
        self, space_id: str, source_id: str, source_record_id: str, partition_id: str
    ) -> RecordLocation | None:
        """Return one backend copy of a record when it exists."""

        with self.database.connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM record_locations
                WHERE space_id = ? AND source_id = ? AND source_record_id = ? AND partition_id = ?
                """,
                (space_id, source_id, source_record_id, partition_id),
            ).fetchone()
        return _location(row) if row else None

    def indexed_partitions(self, space_id: str) -> frozenset[str]:
        """Return the partitions of a space that hold at least one indexed copy."""

        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT partition_id FROM record_locations
                WHERE space_id = ? AND state = ?
                """,
                (space_id, LocationState.INDEXED.value),
            ).fetchall()
        return frozenset(row["partition_id"] for row in rows)

    def list_indexed_locations(self, source_id: str) -> tuple[RecordLocation, ...]:
        """Return a source's copies the engine believes the backend holds."""

        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM record_locations WHERE source_id = ? AND state = ?
                ORDER BY source_record_id, partition_id
                """,
                (source_id, LocationState.INDEXED.value),
            ).fetchall()
        return tuple(_location(row) for row in rows)

    def set_location_state(
        self,
        space_id: str,
        source_id: str,
        source_record_id: str,
        partition_id: str,
        state: LocationState,
    ) -> None:
        """Mark a copy as being removed or as needing reconciliation."""

        with self.database.transaction() as connection:
            connection.execute(
                """
                UPDATE record_locations SET state = ?, updated_at = ?
                WHERE space_id = ? AND source_id = ? AND source_record_id = ? AND partition_id = ?
                """,
                (state.value, _timestamp(), space_id, source_id, source_record_id, partition_id),
            )

    def drop_location(
        self, space_id: str, source_id: str, source_record_id: str, partition_id: str
    ) -> None:
        """Drop a copy once the backend no longer holds it."""

        with self.database.transaction() as connection:
            connection.execute(
                """
                DELETE FROM record_locations
                WHERE space_id = ? AND source_id = ? AND source_record_id = ? AND partition_id = ?
                """,
                (space_id, source_id, source_record_id, partition_id),
            )

    def set_index_state(
        self,
        space_id: str,
        source_id: str,
        source_record_id: str,
        state: IndexState,
        error: str | None = None,
    ) -> None:
        """Record whether the backend matches the ledger for one record."""

        with self.database.transaction() as connection:
            connection.execute(
                """
                UPDATE source_records SET index_state = ?, index_error = ?
                WHERE space_id = ? AND source_id = ? AND source_record_id = ?
                """,
                (state.value, error, space_id, source_id, source_record_id),
            )

    def index_snapshot(self, source_id: str) -> IndexingSnapshot:
        """Count a source's active records by index state; the ledger is the source of truth."""

        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT index_state, COUNT(*) AS n FROM source_records
                WHERE source_id = ? AND state = ? GROUP BY index_state
                """,
                (source_id, RecordState.ACTIVE.value),
            ).fetchall()
        counts = {row["index_state"]: row["n"] for row in rows}
        indexed = counts.get(IndexState.INDEXED.value, 0)
        pending = counts.get(IndexState.PENDING.value, 0)
        failed = counts.get(IndexState.FAILED.value, 0) + counts.get(
            IndexState.RECONCILE_REQUIRED.value, 0
        )
        return IndexingSnapshot(
            source_id=source_id,
            state=IndexingState.OK,
            collected_at=datetime.now(UTC),
            expected=indexed + pending + failed,
            indexed=indexed,
            indexing=pending,
            failed=failed,
            missing=0,
        )

    # Staged content release

    def release_candidates(
        self, space_id: str, source_id: str, source_record_id: str
    ) -> tuple[str, ...]:
        """Return this record's staged uploads that no live record still points at.

        That covers every version of a deleted record and superseded versions of a live one.
        """

        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT v.content_ref AS upload_id FROM record_versions AS v
                JOIN staged_uploads AS u ON u.id = v.content_ref AND u.released_at IS NULL
                WHERE v.space_id = ? AND v.source_id = ? AND v.source_record_id = ?
                    AND v.content_ref NOT IN (
                        SELECT content_ref FROM source_records
                        WHERE content_ref IS NOT NULL AND state != ?
                    )
                ORDER BY v.content_ref
                """,
                (space_id, source_id, source_record_id, RecordState.DELETED.value),
            ).fetchall()
        return tuple(row["upload_id"] for row in rows)

    def mark_released(self, upload_ids: tuple[str, ...]) -> None:
        """Record that staged bytes were removed, and erase the evidence taken from them.

        The metadata row stays for audit. Stored evidence of the versions whose content this
        was goes in the same transaction, so a passage never outlives its version (M5 slice 2).
        """

        if not upload_ids:
            return
        now = _timestamp()
        with self.database.transaction() as connection:
            connection.executemany(
                "UPDATE staged_uploads SET released_at = ? WHERE id = ?",
                [(now, upload_id) for upload_id in upload_ids],
            )
            delete_released_evidence(connection, upload_ids)


def _acl_key(source: Source, acl_version: str) -> tuple[int, int | str]:
    """Compare ACL versions with the source policy, falling back to text when not numeric."""

    try:
        return source.version_key(acl_version)
    except ValueError:
        return (1, acl_version)
