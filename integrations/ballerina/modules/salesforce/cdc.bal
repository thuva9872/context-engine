// Salesforce Change Data Capture listener: updates + deletes.
//
// Runs alongside the SOQL poll (which owns creates + backfill by CreatedDate), so
// no record is ingested by both. On an update (or undelete) CDC delivers only the
// changed fields, so this listener re-fetches the FULL record by Id (via
// `fetchRecordById`) and ingests that — identical to a backfill of the same record,
// making the backfill/CDC overlap a clean idempotent replay. Creates are ignored
// (SOQL owns them); deletes ingest a deletion by identity.

import ballerina/lang.runtime;
import ballerina/log;
import ballerina/time;

import ballerinax/salesforce;

import context_engine_connectors.core;

// Derive the CDC channel for an SObject, e.g. "Account" -> "/data/AccountChangeEvent"
// and "Employee__c" -> "/data/Employee__ChangeEvent".
isolated function deriveChangeChannel(string sobject) returns string {
    if sobject.endsWith("__c") {
        return string `/data/${sobject.substring(0, sobject.length() - 3)}__ChangeEvent`;
    }
    return string `/data/${sobject}ChangeEvent`;
}

# Listener connector that subscribes to an SObject's change channel.
public class SalesforceCdcConnector {
    *core:ListenConnector;

    private final SalesforceSettings settings;
    private final salesforce:Client sfClient;

    # Create a connector bound to its settings and a Salesforce client.
    #
    # + settings - the Salesforce connector settings
    # + sfClient - authenticated Salesforce REST client, used to re-fetch records
    public function init(SalesforceSettings settings, salesforce:Client sfClient) {
        self.settings = settings;
        self.sfClient = sfClient;
    }

    # Subscribe to the object's change channel and ingest received events, then return.
    #
    # + sink - the ingestion sink provided by the runtime
    # + return - an error if the listener fails to attach or start
    public function listen(core:RecordSink sink) returns error? {
        salesforce:RestBasedListenerConfig listenerConfig = {
            auth: {
                tokenUrl: tokenEndpoint(self.settings.baseUrl),
                clientId: self.settings.clientId,
                clientSecret: self.settings.clientSecret
            },
            baseUrl: self.settings.baseUrl,
            replayFrom: self.settings.replayFrom
        };
        string channel = deriveChangeChannel(self.settings.sobject);
        salesforce:Listener changeListener = check new (listenerConfig);
        ChangeEventIngestService svc = new (sink, self.sfClient, self.settings.sobject,
                self.settings.fields, channel);
        check changeListener.attach(svc, channel);
        check changeListener.'start();
        runtime:registerListener(changeListener);
        log:printInfo("subscribed to salesforce change channel", channel = channel,
                sobject = self.settings.sobject);
    }
}

# Ingests one channel's change events into the sink.
service class ChangeEventIngestService {
    *salesforce:CdcService;

    private final core:RecordSink sink;
    private final salesforce:Client sfClient;
    private final string sobject;
    private final string[] fields;
    private final string channel;

    function init(core:RecordSink sink, salesforce:Client sfClient, string sobject,
            string[] fields, string channel) {
        self.sink = sink;
        self.sfClient = sfClient;
        self.sobject = sobject;
        self.fields = fields;
        self.channel = channel;
    }

    # A created record: ignored. The SOQL poll owns creates and backfill, so
    # ingesting here would duplicate them.
    #
    # + payload - the change event
    # + return - always `()`
    remote function onCreate(salesforce:EventData payload) returns error? {
    }

    # An updated record: re-fetch the full record and ingest it as an upsert.
    #
    # + payload - the change event
    # + return - an error if the re-fetch or ingestion fails
    remote function onUpdate(salesforce:EventData payload) returns error? {
        return self.refetchAndIngest(payload);
    }

    # An undeleted record: re-fetch the full record and ingest it as an upsert.
    #
    # + payload - the change event
    # + return - an error if the re-fetch or ingestion fails
    remote function onRestore(salesforce:EventData payload) returns error? {
        return self.refetchAndIngest(payload);
    }

    # A deleted record: ingest a deletion by identity.
    #
    # + payload - the change event
    # + return - an error if ingestion fails
    remote function onDelete(salesforce:EventData payload) returns error? {
        map<json> meta = changeMetadata(payload);
        json rid = meta["recordId"];
        string recordId = rid is string ? rid : self.channel;
        _ = check self.sink->remove(recordId, commitVersion(meta), commitObservedAt(meta));
    }

    # Log listener transport errors; the listener manages reconnection.
    #
    # + err - the listener error
    # + return - always `()`
    remote function onError(error err) returns error? {
        log:printError("salesforce cdc listener error", 'error = err, channel = self.channel);
    }

    private function refetchAndIngest(salesforce:EventData payload) returns error? {
        json rid = changeMetadata(payload)["recordId"];
        if rid !is string {
            log:printError("cdc event without recordId; skipping", channel = self.channel);
            return;
        }
        core:SourceRecord? sourceRecord =
            check fetchRecordById(self.sfClient, self.sobject, self.fields, rid);
        if sourceRecord is () {
            // The record was deleted between the change event and the re-fetch; a
            // delete event will follow, so there is nothing to ingest here.
            return;
        }
        _ = check self.sink->ingest(sourceRecord);
    }
}

// The change-event header as a plain JSON map. `metadata` is a top-level field of
// `EventData` (not inside `changedData`); it is serialized to JSON rather than read
// via its typed fields because the library types numeric header fields as `int`
// but delivers them as strings, so a typed access would cast and throw.
isolated function changeMetadata(salesforce:EventData payload) returns map<json> {
    salesforce:ChangeEventMetadata? metadata = payload?.metadata;
    if metadata is () {
        return {};
    }
    json meta = metadata.toJson();
    return meta is map<json> ? meta : {};
}

// A monotonic version for a change, from the commit number (delivered as a string).
isolated function commitVersion(map<json> meta) returns string {
    json commitNumber = meta["commitNumber"];
    if commitNumber is int {
        return commitNumber.toString();
    }
    if commitNumber is string {
        return commitNumber;
    }
    return "0";
}

// The commit time (epoch millis in the header) as an RFC 3339 timestamp, so a delete
// re-delivered on replay is byte-identical. Returns () to fall back to now() if absent.
isolated function commitObservedAt(map<json> meta) returns string? {
    json commitTimestamp = meta["commitTimestamp"];
    int millis;
    if commitTimestamp is int {
        millis = commitTimestamp;
    } else if commitTimestamp is string {
        int|error parsed = int:fromString(commitTimestamp);
        if parsed is error {
            return ();
        }
        millis = parsed;
    } else {
        return ();
    }
    time:Utc utc = [millis / 1000, <decimal>(millis % 1000) / 1000d];
    return time:utcToString(utc);
}
