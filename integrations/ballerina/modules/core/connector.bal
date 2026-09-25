// The connector contract for Context Engine source integrations.
//
// The managed runtime drives connectors by their modality:
//   - Pull connectors implement `PollConnector`; the `ConnectorManager` schedules
//     `fetch` on an interval (see `poll_job.bal`).
//   - Push/streaming connectors implement `ListenConnector`; the manager calls
//     `listen` once to attach listeners (a SaaS event listener, an HTTP upload
//     endpoint, ...).
//
// The framework owns everything provider-neutral behind `RecordSink` (see
// `sink.bal`): building the ingestion envelope, hashing content, deriving the
// idempotency key, and advancing the checkpoint. Connectors only decide how
// records are produced and normalized.

# A pull connector. The manager owns the loop and schedules `fetch` on an
# interval; each call returns the records changed since `cursor` plus the next
# cursor. A failing `fetch` is logged and retried on the next tick rather than
# stopping the connector (see `poll_job.bal`).
public type PollConnector object {
    # Fetch records changed since `cursor`.
    #
    # + cursor - the last persisted checkpoint ("" on the first poll)
    # + return - new records plus the next cursor, or an error
    public function fetch(string cursor) returns FetchResult|error;
};

# A push/streaming connector. `listen` attaches its listeners (a SaaS event
# listener, an HTTP upload endpoint, ...) into the sink and returns promptly; the
# manager keeps the runtime alive, so `listen` must not block.
public type ListenConnector object {
    # Attach listeners that ingest into `sink`, then return.
    #
    # + sink - the ingestion sink provided by the runtime
    # + return - an error if a listener fails to attach or start
    public function listen(RecordSink sink) returns error?;
};

# Result of one poll: the records to ingest and the next checkpoint cursor.
public type FetchResult record {|
    # Records discovered since the supplied cursor.
    SourceRecord[] records;
    # Cursor to persist and pass to the next poll.
    string cursor;
|};
