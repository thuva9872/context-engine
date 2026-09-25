// File upload endpoint — a host built-in, not a managed connector.
//
// Unlike the poll/listen connectors, a file-upload endpoint is inherently
// multi-space: the caller names the target space per request. So it is NOT a
// registered `ConnectorType` with a fixed destination and does not go through the
// manager. The host starts it directly with an engine client and server-controlled
// governance; for each upload it builds a per-request `RecordSink` for the space
// named in the path. Only the space comes from the request — audience and ACL
// version stay server-controlled.
//
// Files are passed through unmodified: each upload is base64-encoded with its
// declared content type and submitted to the engine. Extraction (including PDF)
// happens on the engine side via the knowledge backend (M4) — the connector does
// no extraction itself, so any file type is accepted.

import ballerina/http;
import ballerina/lang.runtime;
import ballerina/log;
import ballerina/mime;

import context_engine_connectors.core;

# Server-controlled identity and governance for uploaded files. Never taken from
# the request (only the target space is).
public type UploadGovernance record {|
    # Registered source identifier for uploaded files.
    string sourceId;
    # Trusted audience used by engine policy for routing.
    string[] audience;
    # Version of the source ACL that authorizes the audience.
    string sourceAclVersion;
|};

# Start the file-upload endpoint. Called directly by the host (not the manager),
# since the target space is per-request rather than fixed.
#
# + engineClient - the shared engine client
# + governance - server-controlled source identity/authorization for uploads
# + uploadPort - port the endpoint binds to
# + return - an error if the listener cannot start
public function serveUploads(core:EngineClient engineClient, UploadGovernance governance,
        int uploadPort) returns error? {
    http:Listener uploadListener = check new (uploadPort);
    check uploadListener.attach(new UploadService(engineClient, governance), "/");
    check uploadListener.'start();
    runtime:registerListener(uploadListener);
    log:printInfo("file upload endpoint started", port = uploadPort);
}

# HTTP service that accepts file uploads for a caller-named space and ingests them.
service class UploadService {
    *http:Service;

    private final core:EngineClient engineClient;
    private final UploadGovernance governance;

    function init(core:EngineClient engineClient, UploadGovernance governance) {
        self.engineClient = engineClient;
        self.governance = governance;
    }

    # Ingest one or more uploaded files into the space named in the path.
    #
    # Accepts `multipart/form-data`; each part is a file whose name is its record id.
    # Each part is passed through as base64 content with its declared content type —
    # no extraction here; the engine's knowledge backend extracts per type. Only the
    # target space comes from the request; audience and ACL version are server-controlled.
    #
    # + spaceId - target context space (from the path)
    # + request - the multipart HTTP request carrying the uploaded files
    # + return - the accepted engine job handles, or an error
    resource function post spaces/[string spaceId]/files(http:Request request)
            returns core:JobAccepted[]|error {
        core:RecordSink sink = new (self.engineClient, {
            spaceId,
            sourceId: self.governance.sourceId,
            audience: self.governance.audience,
            sourceAclVersion: self.governance.sourceAclVersion
        });
        mime:Entity[] parts = check request.getBodyParts();
        core:JobAccepted[] accepted = [];
        foreach mime:Entity part in parts {
            mime:ContentDisposition disposition = part.getContentDisposition();
            string fileName = disposition.fileName != "" ? disposition.fileName : disposition.name;
            string contentType = part.getContentType();
            byte[] data = check part.getByteArray();
            core:SourceRecord sourceRecord = {
                recordId: fileName,
                content: data.toBase64(),
                contentType: contentType != "" ? contentType : "application/octet-stream",
                title: fileName
            };
            core:JobAccepted job = check sink->ingest(sourceRecord);
            log:printInfo("ingested uploaded file", space = spaceId, recordId = fileName,
                    contentType = contentType, jobId = job.jobId);
            accepted.push(job);
        }
        return accepted;
    }
}
