"""Structured-data RML path: materialization and the RDF -> datapoint adapter.

The materialization test needs only morph-kgc and rdflib; the adapter test also needs the
private provider (it builds native Entity/Chunk models). Each skips when its deps are absent.
"""

from __future__ import annotations

from importlib.util import find_spec
from pathlib import Path

import pytest

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
