// Checkpoint storage for pull connectors.
//
// The store is the single place that reads and writes cursors. Swap the
// implementation to change durability without touching the manager or
// connectors: `InMemoryCheckpointStore` is process-scoped; a future
// `DatabaseCheckpointStore` would `save` each cursor so polling resumes across
// restarts. Ingestion is idempotent, so at-least-once persistence is safe.

# Reads and writes the last-processed cursor for each poll instance.
#
# The manager loads one cursor per instance, keyed by its `instanceId`, so a
# durable store can restore it on restart. `InMemoryCheckpointStore` is the
# default; a future `DatabaseCheckpointStore` implements this same interface.
public type CheckpointStore object {
    # Return the persisted cursor for a key, or "" if none exists yet.
    #
    # + key - stable instance identity, typically the `instanceId`
    # + return - the persisted cursor, or an error if it cannot be read
    public function load(string key) returns string|error;

    # Persist the cursor for a key.
    #
    # + key - stable instance identity
    # + cursor - the cursor to persist
    # + return - an error if the cursor cannot be written
    public function save(string key, string cursor) returns error?;
};

# Default store that keeps cursors in memory, one per instance key.
public class InMemoryCheckpointStore {
    *CheckpointStore;

    private final map<string> cursors = {};

    # Return the instance's cursor, or "" before the first save.
    #
    # + key - stable instance identity
    # + return - the stored cursor, or ""
    public function load(string key) returns string|error {
        return self.cursors[key] ?: "";
    }

    # Store the instance's cursor in memory.
    #
    # + key - stable instance identity
    # + cursor - the cursor to store
    # + return - never fails for the in-memory implementation
    public function save(string key, string cursor) returns error? {
        self.cursors[key] = cursor;
    }
}
