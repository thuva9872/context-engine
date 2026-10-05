-- Structured-data RML mappings for a source. A source that delivers structured content
-- (database rows as JSON, CSV, XML) may carry an RML mapping (RDF Mapping Language, Turtle)
-- per content type, supplied at registration. When a record's content type has a mapping, the
-- worker materializes it to graph nodes and edges instead of indexing flattened text. The
-- value is a JSON object keyed by content type: {"application/json": "<turtle>", ...}. Null
-- means the source has no structured mappings and every record takes the text path. A mapping is
-- schema-specific, so it belongs only on a schema-homogeneous source (a database table, a
-- Salesforce object), not a multi-modal one like Drive or file upload (ADR 0016).
ALTER TABLE sources ADD COLUMN structured_mapping_json TEXT;
