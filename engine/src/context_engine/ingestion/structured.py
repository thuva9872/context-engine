"""Materialize one structured record to RDF through its source's RML mapping.

A source may register an RML mapping (RDF Mapping Language, Turtle) per content type. When a
record's content type has one, the worker calls :func:`materialize` to turn the staged bytes
into an RDF graph via morph-kgc, instead of extracting flattened text. The result is a neutral
W3C artifact (``rdflib.Graph``) that the knowledge provider converts into graph nodes and edges.

morph-kgc and rdflib ship with the knowledge-provider extra, so both are imported lazily; a
build without the provider never pays for them and this module still imports.

Performance: morph-kgc is a batch engine whose public ``materialize(config)`` re-parses and
re-plans the whole RML mapping on every call. Measured, that parse is ~95% of a single-record
materialization, so calling it per record (one Salesforce row, one Drive file) pays a large
fixed cost every time. The parse result depends only on the mapping text, not the record, so
:func:`materialize` parses each distinct mapping once, caches it, and reuses it across every
record of the source — a >10x per-record speedup. This uses morph-kgc internals pinned to
2.10.0; a test (``tests/test_structured_rml.py``) guards against drift by materializing a record
through both this fast path and morph-kgc's public API and requiring identical output, so a
version bump that moves these internals is caught in CI, not at runtime.
"""

from __future__ import annotations

import hashlib
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import rdflib


class StructuredMaterializationError(Exception):
    """A structured record could not be materialized to RDF.

    Carries a stable code so the worker can fail the record terminally rather than retry a bad
    mapping or malformed content forever.
    """

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


# The file extension morph-kgc expects for each supported structured content type. The mapping's
# reference formulation (JSONPath, CSV, XPath) drives parsing; the extension keeps inputs tidy.
_SUFFIXES = {
    "application/json": ".json",
    "text/csv": ".csv",
    "application/xml": ".xml",
    "text/xml": ".xml",
}

# Parsed-mapping cache for the fast path: sha256(mapping Turtle) -> parsed morph-kgc mapping.
# Access is from the single-threaded worker, so no lock is needed; a modest cap bounds memory if
# a source churns its mapping. The value is morph-kgc's own parse output, reused across records.
_MAPPING_CACHE: dict[str, Any] = {}
_MAPPING_CACHE_CAP = 128


def supported(content_type: str) -> bool:
    """Return whether a content type can be materialized (ignoring any charset parameter)."""

    return _base(content_type) in _SUFFIXES


def validate_mapping(rml_mapping: str) -> None:
    """Reject a malformed RML mapping at registration time by parsing it as Turtle.

    This is a syntax gate, not a guarantee: a syntactically valid mapping can still fail to
    materialize a given record (a wrong iterator, a template naming a field the data lacks), which
    only morph-kgc can tell against real bytes at ingest. When rdflib is absent — a control plane
    built without the provider extra — the check is skipped and the provider validates at ingest.
    Raises :class:`StructuredMaterializationError` with a stable code on invalid Turtle.
    """

    try:
        import rdflib
    except ImportError:  # pragma: no cover - provider extra missing
        return
    try:
        rdflib.Graph().parse(data=rml_mapping, format="turtle")
    except StructuredMaterializationError:
        raise
    except Exception as exc:
        raise StructuredMaterializationError(
            "structured_invalid_mapping", "The RML mapping is not valid Turtle"
        ) from exc


def _base(content_type: str) -> str:
    return content_type.split(";", 1)[0].strip().lower()


def materialize(content_type: str, data: bytes, rml_mapping: str) -> rdflib.Graph:
    """Return the RDF graph produced by applying ``rml_mapping`` to ``data``.

    ``content_type`` selects the input format; ``rml_mapping`` is the Turtle mapping registered on
    the source, parsed once and reused across the source's records. Raises
    :class:`StructuredMaterializationError` with a stable code when the content type is unsupported
    or morph-kgc rejects the mapping or the data.
    """

    base = _base(content_type)
    suffix = _SUFFIXES.get(base)
    if suffix is None:
        raise StructuredMaterializationError(
            "structured_unsupported", f"No RML materialization for content type {base}"
        )
    try:
        import morph_kgc
    except ImportError as exc:  # pragma: no cover - provider extra missing
        raise StructuredMaterializationError(
            "structured_unavailable", "The RML materialization dependency is not installed"
        ) from exc

    try:
        return _materialize_fast(morph_kgc, suffix, data, rml_mapping)
    except StructuredMaterializationError:
        raise
    except Exception as exc:  # morph-kgc raises many shapes for bad mappings or data.
        raise StructuredMaterializationError(
            "structured_materialization_failed",
            "The RML mapping could not be applied to the record",
        ) from exc


def _materialize_fast(morph_kgc: Any, suffix: str, data: bytes, rml_mapping: str) -> rdflib.Graph:
    """Materialize reusing a cached parse of the mapping if available.

    This replicates the body of morph-kgc's ``materialize_set`` plus its graph assembly, but feeds
    the per-record materializer a mapping parsed once and cached rather than parsing it again. All
    schema knowledge comes from the cached parse; only ``file_path`` (the record's bytes) changes.
    """

    import rdflib
    from morph_kgc.materializer import _materialize_mapping_group_to_set

    with tempfile.TemporaryDirectory(prefix="context-engine-rml-") as workdir:
        root = Path(workdir)
        data_path = root / f"source{suffix}"
        mapping_path = root / "mapping.ttl"
        data_path.write_bytes(data)
        mapping_path.write_text(rml_mapping, encoding="utf-8")
        config = morph_kgc.load_config_from_argument(
            "[CONFIGURATION]\n"
            "output_format=N-TRIPLES\n"
            "[DataSource1]\n"
            f"mappings={mapping_path}\n"
            f"file_path={data_path}\n"
        )
        # Parallelization is unsupported as a library off Linux; keep it single-process (as the
        # public path does) so the materializer runs in-process.
        if "linux" not in sys.platform:
            config.set_number_of_processes("1")

        rml_df, fnml_df, http_api_df = _parsed_mapping(morph_kgc, config, rml_mapping)
        # The parse baked the parse-time file path into the mapping rows (the file reader takes it
        # from there, not the config), so point every file source at THIS record's bytes before
        # materializing — this is what makes the cached parse reusable across records.
        rml_df.loc[rml_df["source_name"] == "DataSource1", "logical_source_value"] = str(data_path)
        # The materializer reads the (here empty) HTTP-API frame back from the config per run.
        config.set("CONFIGURATION", "http_api_df", http_api_df.to_csv())

        asserted = rml_df.loc[rml_df["triples_map_type"] == morph_kgc.RML_TRIPLES_MAP_CLASS]
        groups = [group for _, group in asserted.groupby(by="mapping_partition")]
        triples: set[str] = set()
        for group in groups:
            triples.update(_materialize_mapping_group_to_set(group, rml_df, fnml_df, config, None))

    graph = rdflib.Graph()
    if triples:
        graph.parse(data=".\n".join(triples) + ".", format="nquads")
    return graph


def _parsed_mapping(morph_kgc: Any, config: Any, rml_mapping: str) -> Any:
    """Return morph-kgc's parse of ``rml_mapping`` (rml_df, fnml_df, http_api_df), cached.

    The parse depends only on the mapping Turtle, so it is cached by its hash and reused for every
    record of the source. On a miss the given ``config`` (already pointing at this record's data)
    is used once to parse, exactly as ``materialize_set`` would. The caller rewrites the data file
    path into the returned mapping before materializing, so the cached parse serves any record.
    """

    key = hashlib.sha256(rml_mapping.encode("utf-8")).hexdigest()
    cached = _MAPPING_CACHE.get(key)
    if cached is not None:
        return cached

    from morph_kgc.mapping.mapping_parser import retrieve_mappings

    value = retrieve_mappings(config)  # (rml_df, fnml_df, http_api_df)
    if len(_MAPPING_CACHE) >= _MAPPING_CACHE_CAP:
        _MAPPING_CACHE.clear()
    _MAPPING_CACHE[key] = value
    return value


