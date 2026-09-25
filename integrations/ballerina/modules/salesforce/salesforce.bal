// The Salesforce connector: one registration that runs BOTH a SOQL poll (creates +
// initial backfill, paged by CreatedDate) and a CDC listener (updates + deletes).
// Its `ConnectorType` sets both a poll factory and a listen factory, so the manager
// schedules the poll and attaches the listener for a single `salesforce` config —
// the user never has to know it is two connectors underneath.

import ballerinax/salesforce;

import context_engine_connectors.core;

// TODO: support all Salesforce auth flows, not just client-credentials. The
// salesforce Client and Listener both accept BearerTokenConfig, refresh-token,
// password, and client-credentials grants. Add an `authType` discriminator to
// `SalesforceSettings` (with the fields each flow needs) and build the matching
// auth config in `newClient` (client.bal) and the listener (cdc.bal). Include the
// JWT bearer flow too (mint the token via ballerina/jwt, pass BearerTokenConfig)
// for cert-based server-to-server auth.

# Registry key for the Salesforce connector.
public const SALESFORCE_TYPE = "salesforce";

# Settings for the Salesforce connector (shared by the SOQL poll and CDC listener).
#
# Uses the OAuth2 client-credentials flow: enable it on the Connected App and set a
# run-as user. No refresh token is involved, so mandatory refresh-token rotation
# does not apply.
public type SalesforceSettings record {|
    # Connected App consumer key.
    string clientId;
    # Connected App consumer secret.
    string clientSecret;
    # Salesforce instance base URL, e.g. "https://<instance>.my.salesforce.com".
    # The client-credentials token endpoint is derived from it.
    string baseUrl;
    # Salesforce REST API version.
    string apiVersion = "59.0";
    # The SObject to sync, e.g. "Account". Must have Change Data Capture enabled.
    string sobject;
    # Business fields to ingest as content.
    string[] fields;
    # CDC subscription start: -1 tip (new only), -2 last 72h, or a replayId.
    int replayFrom = -1;
    # Maximum records per backfill poll.
    int batchSize = 200;
|};

# The connector type registration to hand to `ConnectorManager.register`.
#
# Sets both factories: the manager schedules the SOQL poll AND attaches the CDC
# listener for one instance, so a single `salesforce` config runs the full sync.
#
# + return - the Salesforce connector type
public function salesforceType() returns core:ConnectorType {
    return {
        name: SALESFORCE_TYPE,
        pollFactory: createPollConnector,
        listenFactory: createListenConnector
    };
}

// Poll factory: SOQL for creates + backfill.
function createPollConnector(json settings) returns core:PollConnector|error {
    SalesforceSettings s = check settings.cloneWithType();
    salesforce:Client sfClient = check newClient(s.baseUrl, s.apiVersion, s.clientId, s.clientSecret);
    return new SalesforceSoqlConnector(s, sfClient);
}

// Listen factory: CDC for updates + deletes.
function createListenConnector(json settings) returns core:ListenConnector|error {
    SalesforceSettings s = check settings.cloneWithType();
    salesforce:Client sfClient = check newClient(s.baseUrl, s.apiVersion, s.clientId, s.clientSecret);
    return new SalesforceCdcConnector(s, sfClient);
}
