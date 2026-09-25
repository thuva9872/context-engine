// Shared Salesforce helpers used by both the SOQL (poll) and CDC (listen)
// connectors in this module: client construction, single-record fetch, and the
// row-to-SourceRecord mapping. Kept module-private so the mapping is guaranteed
// identical across the two connectors (a record ingested by a backfill poll and
// by a CDC-triggered re-fetch must be byte-identical to deduplicate cleanly).

import ballerinax/salesforce;

import context_engine_connectors.core;

// Build an authenticated Salesforce REST client using the OAuth2 client-credentials
// flow
isolated function newClient(string baseUrl, string apiVersion, string clientId, string clientSecret)
        returns salesforce:Client|error {
    return new ({
        baseUrl,
        auth: {tokenUrl: tokenEndpoint(baseUrl), clientId, clientSecret},
        apiVersion
    });
}

// The client-credentials token endpoint is the instance's My Domain URL.
isolated function tokenEndpoint(string baseUrl) returns string {
    return baseUrl + "/services/oauth2/token";
}

// Query one record by Id and map it to a SourceRecord, or () if it no longer exists.
// Produces content identical to a backfill of the same record for the same `fields`.
function fetchRecordById(salesforce:Client sfClient, string sobject, string[] fields, string id)
        returns core:SourceRecord|error? {
    string fieldList = selectFields(fields, ["Id", "SystemModstamp"]);
    string soql = string `SELECT ${fieldList} FROM ${sobject} WHERE Id = '${id}' LIMIT 1`;
    stream<record {}, error?> rows = check sfClient->query(soql);
    record {|record {} value;|}|error? next = rows.next();
    check rows.close();
    if next is error {
        return next;
    }
    if next is () {
        return ();
    }
    return mapRecord(next.value, fields);
}

// Map a SOQL row to a SourceRecord: Id and SystemModstamp are envelope; content is
// the projection of `fields` in order, so the same record maps identically from a
// backfill poll and a by-id re-fetch.
isolated function mapRecord(record {} row, string[] fields) returns core:SourceRecord {
    map<anydata> projected = {};
    foreach string f in fields {
        projected[f] = row[f];
    }
    string modstamp = row["SystemModstamp"].toString();
    core:SourceRecord sourceRecord = {
        recordId: row["Id"].toString(),
        content: projected.toJsonString(),
        contentType: "application/json",
        sourceVersion: modstamp,
        // Deterministic observed time from the record's own modified timestamp, so
        // re-ingesting the same version (backfill re-run, CDC replay) is a clean replay.
        sourceObservedAt: toRfc3339(modstamp)
    };
    anydata name = row["Name"];
    if name is string {
        sourceRecord.title = name;
    }
    return sourceRecord;
}

// Build a SELECT field list from the business fields plus any required extras, deduped.
isolated function selectFields(string[] fields, string[] required) returns string {
    string[] selected = fields.clone();
    foreach string r in required {
        if selected.indexOf(r) is () {
            selected.push(r);
        }
    }
    return string:'join(", ", ...selected);
}

// Normalize a Salesforce REST datetime (e.g. "...+0000") to the Z form, valid both as
// a SOQL datetime literal and as an RFC 3339 `sourceObservedAt`.
isolated function toRfc3339(string datetime) returns string {
    if datetime.endsWith("+0000") {
        return datetime.substring(0, datetime.length() - 5) + "Z";
    }
    return datetime;
}
