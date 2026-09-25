// Connector manager: a registry of connector types plus a scheduler that runs
// any number of configured instances.
//
// The manager holds a registry of connector *types* (salesforce, file-source,
// ...). Given a set of `ConnectorInstanceConfig` values it starts each one,
// running whichever factories the type provides: a poll factory is scheduled as a
// recurring `task:Job`, a listen factory has its listener attached. A type may
// provide both (e.g. Salesforce), so one instance can do both. Every instance
// ingests through its own `core:RecordSink`, so the envelope logic is shared.

import ballerina/log;
import ballerina/task;

import context_engine_connectors.core;

# Runs any number of configured connector instances against one engine.
public class ConnectorManager {
    private final core:EngineClient engineClient;
    private final CheckpointStore checkpointStore;
    private final map<core:ConnectorType> registry = {};
    private task:JobId[] jobs = [];

    # Create a manager bound to an engine client.
    #
    # + engineClient - the shared client every instance ingests through
    # + checkpointStore - resolves each poll instance's checkpoint; defaults to
    #   an in-memory store. Supply a durable store to resume cursors on restart.
    public function init(core:EngineClient engineClient, CheckpointStore? checkpointStore = ()) {
        self.engineClient = engineClient;
        self.checkpointStore = checkpointStore ?: new InMemoryCheckpointStore();
    }

    # Register a connector type so instances can reference it by name.
    #
    # + connectorType - the type registration (name + matching factory)
    public function register(core:ConnectorType connectorType) {
        self.registry[connectorType.name] = connectorType;
    }

    # Start every configured instance, running each factory its type provides.
    #
    # + configs - the instances to run
    # + return - an error if any instance references an unknown type or fails to start
    public function 'start(ConnectorInstanceConfig[] configs) returns error? {
        foreach ConnectorInstanceConfig config in configs {
            core:ConnectorType connectorType = check self.lookup(config.connectorType);
            core:RecordSink sink = new (self.engineClient, config.destination);
            core:PollConnectorFactory? pollFactory = connectorType.pollFactory;
            core:ListenConnectorFactory? listenFactory = connectorType.listenFactory;
            if pollFactory is core:PollConnectorFactory {
                check self.schedulePoll(connectorType.name, pollFactory, config, sink);
            }
            if listenFactory is core:ListenConnectorFactory {
                check self.attachListener(connectorType.name, listenFactory, config, sink);
            }
            if pollFactory is () && listenFactory is () {
                return error(string `connector type '${connectorType.name}' has no factory`);
            }
        }
    }

    # The number of scheduled poll jobs; useful for host liveness checks.
    #
    # + return - count of active poll jobs
    public function scheduledPollCount() returns int {
        return self.jobs.length();
    }

    private function schedulePoll(string typeName, core:PollConnectorFactory factory,
            ConnectorInstanceConfig config, core:RecordSink sink) returns error? {
        core:PollConnector connector = check factory(config.settings);
        string cursor = check self.checkpointStore.load(config.instanceId);
        PollJob job = new (connector, sink, config.instanceId, self.checkpointStore, cursor);
        task:JobId id = check task:scheduleJobRecurByFrequency(job, config.pollIntervalSeconds);
        self.jobs.push(id);
        log:printInfo("scheduled poll connector", instance = config.instanceId,
                'type = typeName, intervalSeconds = config.pollIntervalSeconds);
    }

    private function attachListener(string typeName, core:ListenConnectorFactory factory,
            ConnectorInstanceConfig config, core:RecordSink sink) returns error? {
        core:ListenConnector connector = check factory(config.settings);
        check connector.listen(sink);
        log:printInfo("attached listener connector", instance = config.instanceId, 'type = typeName);
    }

    private function lookup(string name) returns core:ConnectorType|error {
        core:ConnectorType? connectorType = self.registry[name];
        if connectorType is () {
            return error(string `unknown connector type '${name}'`);
        }
        return connectorType;
    }
}
