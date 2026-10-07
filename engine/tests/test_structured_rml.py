"""Structured-data RML path: materialization and the RDF -> datapoint adapter.

The materialization test needs only morph-kgc and rdflib; the adapter test also needs the
private provider (it builds native Entity/Chunk models). Each skips when its deps are absent.
"""

from __future__ import annotations

import tempfile
from importlib.util import find_spec
from pathlib import Path

import pytest

from context_engine.ingestion import structured as _structured
from context_engine.ingestion.structured import (
    StructuredMaterializationError,
    materialize,
    supported,
    validate_mapping,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures"
JSON_BYTES = (FIXTURES / "accounts.json").read_bytes()
RML_MAPPING = (FIXTURES / "accounts.rml.ttl").read_text(encoding="utf-8")

_HAS_RDF = find_spec("morph_kgc") is not None and find_spec("rdflib") is not None
_HAS_PROVIDER = _HAS_RDF and find_spec("cognee") is not None

requires_rdf = pytest.mark.skipif(not _HAS_RDF, reason="morph-kgc/rdflib are not installed")
requires_provider = pytest.mark.skipif(
    not _HAS_PROVIDER, reason="private provider dependency is not installed"
)


def _public_materialize(data: bytes, mapping: str):
    """Materialize via morph-kgc's public API (re-parses every call) — the drift-check reference.

    The engine's :func:`materialize` uses a cached internal fast path; these tests compare it to
    this unoptimized public call to prove the fast path stays correct against the pinned morph-kgc.
    """

    import morph_kgc

    with tempfile.TemporaryDirectory() as workdir:
        root = Path(workdir)
        data_path = root / "source.json"
        mapping_path = root / "mapping.ttl"
        data_path.write_bytes(data)
        mapping_path.write_text(mapping, encoding="utf-8")
        return morph_kgc.materialize(
            "[CONFIGURATION]\n"
            "output_format=N-TRIPLES\n"
            "[DataSource1]\n"
            f"mappings={mapping_path}\n"
            f"file_path={data_path}\n"
        )


def test_supported_content_types():
    assert supported("application/json; charset=utf-8")
    assert supported("text/csv")
    assert supported("application/xml")
    assert not supported("text/plain")


def test_unsupported_content_type_is_a_stable_error():
    with pytest.raises(StructuredMaterializationError) as error:
        materialize("text/plain", b"hello", RML_MAPPING)
    assert error.value.code == "structured_unsupported"


@requires_rdf
def test_validate_mapping_accepts_the_fixture_and_rejects_bad_turtle():
    validate_mapping(RML_MAPPING)  # a well-formed mapping passes
    with pytest.raises(StructuredMaterializationError) as error:
        validate_mapping("@prefix ex: <http://example.org/> .\nex:a ex:b")  # missing object + dot
    assert error.value.code == "structured_invalid_mapping"


@requires_rdf
def test_materialize_json_fixture_produces_expected_subjects():
    import rdflib

    graph = materialize("application/json", JSON_BYTES, RML_MAPPING)
    # The Salesforce-shaped fixture has 2 accounts, 2 owners, 3 contacts, 3 opportunities.
    types: dict[str, int] = {}
    for _s, p, o in graph:
        if p == rdflib.RDF.type:
            types[str(o).rsplit("/", 1)[-1]] = types.get(str(o).rsplit("/", 1)[-1], 0) + 1
    assert types == {"Account": 2, "User": 2, "Contact": 3, "Opportunity": 3}


@requires_rdf
def test_materialize_rejects_a_broken_mapping():
    with pytest.raises(StructuredMaterializationError) as error:
        materialize("application/json", JSON_BYTES, "this is not valid turtle @@@")
    assert error.value.code == "structured_materialization_failed"


@requires_rdf
def test_morph_kgc_fast_path_matches_the_public_api():
    # Drift guard: the cached fast path uses morph-kgc internals pinned to 2.10.0. A bump that
    # moves these symbols or changes output fails here instead of at runtime. Checks the symbols
    # the fast path imports, then compares its output to the public API on a tiny record.
    import morph_kgc
    from morph_kgc.mapping.mapping_parser import retrieve_mappings  # noqa: F401
    from morph_kgc.materializer import _materialize_mapping_group_to_set  # noqa: F401

    for attr in ("load_config_from_argument", "materialize", "RML_TRIPLES_MAP_CLASS"):
        assert hasattr(morph_kgc, attr), f"pinned morph-kgc is missing {attr}"

    probe_mapping = (
        "@prefix rr: <http://www.w3.org/ns/r2rml#> .\n"
        "@prefix rml: <http://semweb.mmlab.be/ns/rml#> .\n"
        "@prefix ql: <http://semweb.mmlab.be/ns/ql#> .\n"
        "@prefix ex: <http://example.org/> .\n"
        "<#M> a rr:TriplesMap ;\n"
        '  rml:logicalSource [ rml:source "x" ; rml:referenceFormulation ql:JSONPath ;'
        ' rml:iterator "$" ] ;\n'
        '  rr:subjectMap [ rr:template "http://example.org/r/{id}" ; rr:class ex:R ] ;\n'
        '  rr:predicateObjectMap [ rr:predicate ex:v ; rr:objectMap [ rml:reference "v" ] ] .\n'
    )
    probe_data = b'{"id":"1","v":"a"}'
    fast = {
        (str(s), str(p), str(o))
        for s, p, o in materialize("application/json", probe_data, probe_mapping)
    }
    public = {
        (str(s), str(p), str(o)) for s, p, o in _public_materialize(probe_data, probe_mapping)
    }
    assert fast == public


@requires_rdf
def test_cached_fast_path_is_correct_across_distinct_records():
    # One mapping, several different records: the cached parse must be rewired to each record's
    # bytes, so materialize (the cached fast path) must match the public API every time.
    _structured._MAPPING_CACHE.clear()
    mapping = (FIXTURES / "accounts.rml.ttl").read_text(encoding="utf-8")
    variants = [
        JSON_BYTES,
        b'[{"Id":"001A","Name":"Solo Account","Owner":{"Id":"005Z","Name":"Zoe"}}]',
    ]
    for data in variants:
        fast = {
            (str(s), str(p), str(o))
            for s, p, o in materialize("application/json", data, mapping)
        }
        public = {(str(s), str(p), str(o)) for s, p, o in _public_materialize(data, mapping)}
        assert fast == public
    # A second record of the same mapping reused the cached parse rather than re-parsing.
    assert len(_structured._MAPPING_CACHE) == 1


@requires_provider
def test_rdf_to_datapoints_builds_nodes_edges_and_is_deterministic():
    from uuid import uuid4

    from context_engine.knowledge_backend.providers.rml_adapter import (
        node_id,
        rdf_to_datapoints,
    )

    graph = materialize("application/json", JSON_BYTES, RML_MAPPING)
    document_id = uuid4()
    datapoints, edges = rdf_to_datapoints(graph, "partition-a", document_id)

    # One Entity per subject, plus the broad EntityTypes and one record TextDocument whose id is
    # the document id passed in, so retrieval can attribute its chunks to the record.
    kinds = {type(dp).__name__ for dp in datapoints}
    assert "EntityType" in kinds and "TextDocument" in kinds and "DocumentChunk" in kinds
    docs = [dp for dp in datapoints if type(dp).__name__ == "TextDocument"]
    assert len(docs) == 1 and docs[0].id == document_id

    # Relationship names come straight from the mapping's predicates.
    relationships = {rel for _s, _t, rel, _p in edges}
    assert "owned_by" in relationships
    assert "is_a" in relationships

    # Node ids are deterministic and partition-namespaced: the Acme account IRI always maps to
    # the same id in one partition, and to a different id in another.
    acme = "http://example.org/account/001A"
    assert node_id("partition-a", acme) == node_id("partition-a", acme)
    assert node_id("partition-a", acme) != node_id("partition-b", acme)

    # A foreign-key edge lands on its target node with no lookup (FK template == subject IRI).
    account_id = node_id("partition-a", acme)
    owner_id = node_id("partition-a", "http://example.org/user/005X")
    assert (account_id, owner_id, "owned_by", {}) in edges
