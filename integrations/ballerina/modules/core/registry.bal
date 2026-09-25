// How a connector advertises itself for registration.
//
// A `ConnectorType` is the contract between a connector and the runtime: it names
// the type and provides a poll factory, a listen factory, or BOTH. The runtime's
// `ConnectorManager` runs whichever factories are present, so a single connector
// type can be multi-modal — e.g. Salesforce sets both (a SOQL poll for creates +
// backfill and a CDC listener for updates + deletes) and one registration runs
// both.

# Builds a pull connector instance from its type-specific settings.
public type PollConnectorFactory function (json settings) returns PollConnector|error;

# Builds a push/streaming connector instance from its type-specific settings.
public type ListenConnectorFactory function (json settings) returns ListenConnector|error;

# A connector type registered with the runtime.
#
# At least one factory must be set. A type may set both, in which case the manager
# schedules the poll connector and attaches the listen connector for one instance.
public type ConnectorType record {|
    # Registry key, e.g. "salesforce", "file-source".
    string name;
    # Poll factory; set for types the manager should schedule on an interval.
    PollConnectorFactory pollFactory?;
    # Listen factory; set for types whose listeners the manager should attach.
    ListenConnectorFactory listenFactory?;
|};
