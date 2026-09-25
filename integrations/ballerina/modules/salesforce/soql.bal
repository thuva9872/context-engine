// Salesforce SOQL poll connector: creates + initial backfill, paged by CreatedDate.
//
// Polling by CreatedDate means it only ever sees newly created records (and the
// initial backfill) — updates and deletes are the CDC listener's job, so no record
// is ingested by both. Content is the projection of `fields` only (see `mapRecord`
// in client.bal), so a record's content is identical whether it arrives via a
// backfill poll or a CDC-triggered re-fetch.

import ballerinax/salesforce;

import context_engine_connectors.core;

// The poll pages by CreatedDate, so it only surfaces creates + backfill.
const string SOQL_CURSOR_FIELD = "CreatedDate";

# Pull connector that queries an SObject for newly created records by CreatedDate.
public class SalesforceSoqlConnector {
    *core:PollConnector;

    private final SalesforceSettings settings;
    private final salesforce:Client sfClient;

    # Create a connector bound to its settings and a Salesforce client.
    #
    # + settings - the Salesforce connector settings
    # + sfClient - authenticated Salesforce REST client
    public function init(SalesforceSettings settings, salesforce:Client sfClient) {
        self.settings = settings;
        self.sfClient = sfClient;
    }

    # Fetch records created since `cursor` (a CreatedDate value).
    #
    # + cursor - the last persisted CreatedDate ("" on the first poll)
    # + return - the new records plus the newest CreatedDate seen
    public function fetch(string cursor) returns core:FetchResult|error {
        stream<record {}, error?> rows = check self.sfClient->query(self.buildSoql(cursor));
        core:SourceRecord[] records = [];
        string nextCursor = cursor;
        check from record {} row in rows
            do {
                records.push(mapRecord(row, self.settings.fields));
                string cursorValue = row[SOQL_CURSOR_FIELD].toString();
                if cursorValue > nextCursor {
                    nextCursor = cursorValue;
                }
            };
        return {records, cursor: nextCursor};
    }

    // Build the incremental SOQL query for the current cursor.
    private function buildSoql(string cursor) returns string {
        string fieldList = selectFields(self.settings.fields, ["Id", "SystemModstamp", SOQL_CURSOR_FIELD]);
        string soql = string `SELECT ${fieldList} FROM ${self.settings.sobject}`;
        if cursor != "" {
            soql = soql + string ` WHERE ${SOQL_CURSOR_FIELD} > ${toRfc3339(cursor)}`;
        }
        return soql + string ` ORDER BY ${SOQL_CURSOR_FIELD} ASC LIMIT ${self.settings.batchSize}`;
    }
}
