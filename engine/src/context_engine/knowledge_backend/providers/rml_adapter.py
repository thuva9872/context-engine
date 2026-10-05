"""Convert a materialized RDF graph into native datapoints and edges.

This is the impedance layer between RML/morph-kgc output and the knowledge provider. It is
PRIVATE to the provider: it imports native models (Entity, EntityType, DocumentChunk,
TextDocument), so it must never be imported by provider-neutral engine code. The provider-neutral
materialization (morph-kgc -> rdflib.Graph) lives in ``context_engine.ingestion.structured``.

The mapping is generic, not schema-specific — all schema knowledge lives in the source's RML
mapping:

  subject IRI   -> one Entity node (type = rdf:type local name)
  rdfs:label    -> node name (embedded for retrieval)
  literal object-> node property (predicate local name)
  IRI object    -> edge (relationship = predicate local name)
  ex:is_a IRI   -> an EntityType node and an ``is_a`` edge (broad semantic type)
  (per subject) -> a DocumentChunk, so retrieval and provenance have text to ground on

Node ids are ``uuid5(partition + subject IRI)``: deterministic, so a foreign-key IRI lands on its
target node with no lookup (the RML FK template and the subject template produce the same IRI),
and partition-namespaced, so the same IRI in two partitions never collides across datasets.

Adapted from the proof of concept at tryout-cognee-local/rml_graph/adapter.py.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import NAMESPACE_OID, UUID, uuid5

if TYPE_CHECKING:
    import rdflib

RESERVED = {
    "id", "name", "description", "is_a", "relations", "metadata", "type",
    "version", "created_at", "updated_at", "ontology_valid", "ontology_uri",
    "topological_rank", "valid_to", "belongs_to_set",
}


def _local(uri: object) -> str:
    text = str(uri)
    return text.rsplit("#", 1)[-1].rsplit("/", 1)[-1]


def node_id(partition: str, iri: object) -> UUID:
    """Return the deterministic, partition-namespaced node id for a subject or target IRI."""

    return uuid5(NAMESPACE_OID, f"{partition}\0{iri}")


def rdf_to_datapoints(
    graph: rdflib.Graph, partition: str, document_id: UUID
) -> tuple[list, list]:
    """Return ``(datapoints, custom_edges)`` for ``add_data_points`` from an RDF graph.

    ``partition`` namespaces every node id so a record's nodes stay within one dataset's identity
    space. ``document_id`` is the record's native item id: one TextDocument carries that id and
    every chunk belongs to it, so retrieval attributes the chunks to this record and the engine
    can trace evidence back to it. Imports of native models are deferred so this module loads
    without the provider extra.
    """

    import rdflib
    from cognee.modules.chunking.chunk_id import chunk_content_hash, content_chunk_id
    from cognee.modules.chunking.models.DocumentChunk import DocumentChunk
    from cognee.modules.data.processing.document_types.TextDocument import TextDocument
    from cognee.modules.engine.models.Entity import Entity
    from cognee.modules.engine.models.EntityType import EntityType
    from pydantic import create_model

    rdf_type = rdflib.RDF.type
    rdfs_label = rdflib.RDFS.label

    # 1. gather per-subject: type, label, literal props, iri edges, is_a categories
    subjects: dict = {}
    for s, p, o in graph:
        rec = subjects.setdefault(
            s, {"type": None, "label": None, "props": {}, "edges": [], "categories": []}
        )
        if p == rdf_type:
            rec["type"] = _local(o)
        elif p == rdfs_label:
            rec["label"] = str(o)
        elif _local(p) == "is_a" and isinstance(o, rdflib.URIRef):
            rec["categories"].append(_local(o))  # broad semantic type -> EntityType
        elif isinstance(o, rdflib.Literal):
            rec["props"][_local(p)] = str(o)
        elif isinstance(o, rdflib.URIRef):
            rec["edges"].append((_local(p), o))  # (relationship, target IRI)

    # 2. a dynamic Entity subclass per type, carrying the union of its property names
    prop_names_by_type: dict = {}
    for rec in subjects.values():
        t = rec["type"] or "Resource"
        names = prop_names_by_type.setdefault(t, set())
        names.update(n for n in rec["props"] if n not in RESERVED)

    models: dict = {}
    for t, names in prop_names_by_type.items():
        fields = {n: (str | None, None) for n in names}
        fields["metadata"] = (
            dict,
            {"index_fields": ["name", "description"], "identity_fields": ["name"]},
        )
        models[t] = create_model(t, __base__=Entity, **fields)

    # 3. build nodes + chunks + edges
    datapoints, edges, chunks = [], [], []
    etypes: dict = {}  # category name -> EntityType (broad semantic type, e.g. Person)

    def get_etype(name: str) -> object:
        if name not in etypes:
            etypes[name] = EntityType(
                id=uuid5(NAMESPACE_OID, f"{partition}\0etype:{name}"),
                name=name,
                description=f"Entities of broad type '{name}'.",
            )
        return etypes[name]

    # One document per record, carrying the record's native item id, so retrieval attributes the
    # chunks to this record and the engine can resolve each back to it.
    doc = TextDocument(
        id=document_id,
        name="structured_record",
        raw_data_location=f"rdf://{document_id}",
        external_metadata=None,
        mime_type="text/plain",
    )

    for idx, (s, rec) in enumerate(subjects.items()):
        t = rec["type"] or "Resource"
        label = rec["label"] or _local(s)
        props = {n: v for n, v in rec["props"].items() if n not in RESERVED}
        desc = f"{t} '{label}'. " + ", ".join(f"{k}={v}" for k, v in props.items())

        node = models[t](id=node_id(partition, s), name=label, description=desc, **props)
        datapoints.append(node)

        for rel, target in rec["edges"]:
            edges.append((node_id(partition, s), node_id(partition, target), rel, {}))

        # broad semantic type -> EntityType node + is_a edge (native typing)
        for cat in rec["categories"]:
            et = get_etype(cat)
            edges.append((node_id(partition, s), et.id, "is_a", {}))

        # provenance: one chunk per subject (the RDF "record"), so retrieval has text to ground on
        text = f"{t}: " + ", ".join(
            [f"label={label}"]
            + [f"{k}={v}" for k, v in props.items()]
            + [f"{rel}->{_local(tg)}" for rel, tg in rec["edges"]]
        )
        chash = chunk_content_hash(text)
        chunks.append(
            DocumentChunk(
                id=content_chunk_id(doc.id, chash, idx),
                text=text,
                chunk_size=len(text),
                chunk_index=idx,
                cut_type="rdf_subject",
                content_hash=chash,
                is_part_of=doc,
                contains=[node],
                document_id=str(doc.id),
                document_name=doc.name,
            )
        )

    return datapoints + list(etypes.values()) + [doc] + chunks, edges
