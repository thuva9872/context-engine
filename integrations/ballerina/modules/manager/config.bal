// Configuration sourcing for the connector runtime.
//
// The `ConnectorManager` runs whatever set of instances a `ConfigProvider`
// hands it. This decouples "which connections should run here" from "how to run
// a connection". Today the only provider reads a JSON blob from an environment
// variable. A future database-backed provider will implement the same interface
// and additionally lease connections across nodes, so exactly one instance runs
// each connection; the manager stays unaware of how configs are sourced or
// coordinated.

import ballerina/os;

import context_engine_connectors.core;

# One connector instance the manager should run.
#
# `settings` is the type-specific bag (credentials, channels, folder ids, ...)
# that the registered connector type's factory decodes into its own typed record.
public type ConnectorInstanceConfig record {|
    # Unique identity of this configuration, used in logs and (later) leasing.
    string instanceId;
    # Registry key of the connector type to run, e.g. "salesforce", "file-source".
    string connectorType;
    # The engine-side space/source this instance writes to.
    core:Destination destination;
    # Delay between polls, in seconds; used only by poll modality.
    decimal pollIntervalSeconds = 30;
    # Type-specific settings decoded by the connector factory.
    json settings = {};
|};

# Supplies the set of connector instances the manager should run.
#
# Implemented now by `EnvConfigProvider`; later by a database-backed provider
# that also coordinates single-owner leasing across nodes.
public type ConfigProvider object {
    # Return the connector instances this runtime should start.
    #
    # + return - the configured instances, or an error if they cannot be loaded
    public function provide() returns ConnectorInstanceConfig[]|error;
};

# Reads connector instances from a JSON array in an environment variable.
#
# The variable holds a JSON array of `ConnectorInstanceConfig` objects, e.g.
# `CONNECTOR_CONFIGS='[{"instanceId":"files-1","connectorType":"file-source",...}]'`.
public class EnvConfigProvider {
    *ConfigProvider;

    private final string envVar;

    # Create a provider bound to an environment variable name.
    #
    # + envVar - name of the variable holding the JSON array
    public function init(string envVar = "CONNECTOR_CONFIGS") {
        self.envVar = envVar;
    }

    # Decode and return the configured connector instances.
    #
    # + return - the parsed instances, or an error if the variable is unset or malformed
    public function provide() returns ConnectorInstanceConfig[]|error {
        string raw = os:getEnv(self.envVar);
        if raw.trim() == "" {
            return error(string `connector configuration environment variable '${self.envVar}' is not set`);
        }
        json parsed = check raw.fromJsonString();
        return parsed.cloneWithType();
    }
}
