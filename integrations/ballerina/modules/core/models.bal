// Provider-neutral data a connector produces and is configured with.

# A normalized source record produced by a connector.
#
# The connector supplies engine-neutral content; the framework derives the
# content hash, version, and idempotency key before ingestion.
public type SourceRecord record {|
    # Stable logical identity of the record within its source.
    string recordId;
    # Normalized textual content of the record.
    string content;
    # Source-provided monotonic version of this record state; defaults to the
    # content hash when omitted. Sent to the engine as `sourceVersion`.
    string sourceVersion?;
    # Time at which the source produced this record state (RFC 3339); defaults to
    # now() at ingest. Supplying the source's real timestamp makes re-ingesting the
    # same record byte-identical, so replays are clean no-ops instead of conflicts.
    string sourceObservedAt?;
    # Optional human-readable title.
    string title?;
    # MIME type of the content.
    string contentType = "text/plain";
    # Optional canonical source URL for lineage.
    string sourceUrl?;
    # Optional per-record audience override; defaults to the connector audience.
    string[] audience?;
|};

# The engine-side destination a connector writes to.
#
# Identifies the target space and the source identity/authorization every record
# from this connector instance is ingested under.
public type Destination record {|
    # Target context space that owns the ingested records.
    string spaceId;
    # Registered source identifier for the connector.
    string sourceId;
    # Trusted source audience used by engine policy for routing.
    string[] audience;
    # Version of the source ACL that authorizes the audience.
    string sourceAclVersion;
|};
