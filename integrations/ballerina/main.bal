// Connector runtime host.
//
// A single process that runs any number of configured connector instances. It
// builds one engine client, registers the connector types it enables, loads the
// instance configurations from a `ConfigProvider`, and hands them to the
// `ConnectorManager`, which schedules polls and attaches listeners.
//
// To enable a new connector type, import its submodule and register its
// `ConnectorType` below. To run more instances, add entries to the configuration
// source (the `CONNECTOR_CONFIGS` environment variable for `EnvConfigProvider`).

import ballerina/lang.runtime;
import ballerina/log;

import context_engine_connectors.core;
import context_engine_connectors.file_source;
import context_engine_connectors.manager;
import context_engine_connectors.salesforce;

# Base URL of the running Context Engine REST API.
configurable string engineBaseUrl = "http://127.0.0.1:8000";

# Optional bearer token for authenticated engine deployments.
configurable string? engineToken = ();

# Environment variable holding the JSON array of connector instance configs.
configurable string configEnvVar = "CONNECTOR_CONFIGS";

# Whether to serve the built-in file-upload endpoint (`POST /spaces/{spaceId}/files`).
configurable boolean fileUploadEnabled = true;

# Port the built-in file-upload endpoint binds to.
configurable int fileUploadPort = 9090;

# Server-controlled source identity and governance for uploaded files (the target
# space is named per request; these are not).
configurable string fileUploadSourceId = "source-files";
configurable string[] fileUploadAudience = ["source-group:uploads"];
configurable string fileUploadSourceAclVersion = "1";

# Start the connector runtime.
#
# + return - an error if the engine client, configuration, or any connector fails to start
public function main() returns error? {
    core:EngineClient engineClient = check new ({
        baseUrl: engineBaseUrl,
        bearerToken: engineToken
    });

    manager:ConnectorManager connectorManager = new (engineClient);
    // Register every connector type this host enables.
    connectorManager.register(salesforce:salesforceType());

    manager:ConfigProvider provider = new manager:EnvConfigProvider(configEnvVar);
    manager:ConnectorInstanceConfig[] configs = check provider.provide();

    check connectorManager.'start(configs);
    log:printInfo("connector runtime started", instances = configs.length(),
            scheduledPolls = connectorManager.scheduledPollCount());

    // The file-upload endpoint is a host built-in, not a managed connector: it
    // ingests into whichever space each request names, so it has no fixed
    // destination. Audience and ACL version stay server-controlled.
    if fileUploadEnabled {
        check file_source:serveUploads(engineClient, {
            sourceId: fileUploadSourceId,
            audience: fileUploadAudience,
            sourceAclVersion: fileUploadSourceAclVersion
        }, fileUploadPort);
    }

    // Registered listeners hold the runtime open; the poll scheduler also keeps
    // its worker thread alive. Park the main strand so the host stays up for a
    // poll-only deployment (no listeners) as well.
    runtime:registerListener(keepAlive);
}

// A no-op dynamic listener that keeps the process alive after main returns.
final Keepalive keepAlive = new;

isolated class Keepalive {
    *runtime:DynamicListener;
    public isolated function 'start() returns error? {
    }
    public isolated function gracefulStop() returns error? {
    }
    public isolated function immediateStop() returns error? {
    }
}
