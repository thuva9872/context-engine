// The scheduled unit of work for a managed pull connector.

import ballerina/log;
import ballerina/task;

import context_engine_connectors.core;

# One scheduled poll cycle for a pull connector.
#
# Runs `fetch`, ingests each record, and saves the advanced cursor through the
# checkpoint store. Errors are logged rather than propagated so one bad poll
# never stops the schedule; the next tick retries from the last saved cursor.
#
# The working cursor is cached in memory (seeded once at construction) and
# advanced only after a successful save, so a failed save is retried and, with
# idempotent ingestion, replaying the last batch is safe.
class PollJob {
    *task:Job;

    private final core:PollConnector connector;
    private final core:RecordSink sink;
    private final CheckpointStore checkpoints;
    private final string instanceId;
    private string cursor;

    function init(core:PollConnector connector, core:RecordSink sink, string instanceId,
            CheckpointStore checkpoints, string cursor) {
        self.connector = connector;
        self.sink = sink;
        self.instanceId = instanceId;
        self.checkpoints = checkpoints;
        self.cursor = cursor;
    }

    public function execute() {
        core:FetchResult|error result = self.connector.fetch(self.cursor);
        if result is error {
            log:printError("poll failed", 'error = result, instance = self.instanceId);
            return;
        }
        foreach core:SourceRecord sourceRecord in result.records {
            core:JobAccepted|error accepted = self.sink->ingest(sourceRecord);
            if accepted is error {
                log:printError("ingest failed", 'error = accepted, instance = self.instanceId,
                        recordId = sourceRecord.recordId);
            }
        }
        if result.cursor == self.cursor {
            return;
        }
        error? saved = self.checkpoints.save(self.instanceId, result.cursor);
        if saved is error {
            log:printError("checkpoint persist failed", 'error = saved, instance = self.instanceId,
                    cursor = result.cursor);
            return;
        }
        self.cursor = result.cursor;
    }
}
