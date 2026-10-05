"""Materialize one structured record to RDF through its source's RML mapping.

A source may register an RML mapping (RDF Mapping Language, Turtle) per content type. When a
record's content type has one, the worker calls :func:`materialize` to turn the staged bytes
into an RDF graph via morph-kgc, instead of extracting flattened text. The result is a neutral
W3C artifact (``rdflib.Graph``) that the knowledge provider converts into graph nodes and edges.

morph-kgc and rdflib ship with the knowledge-provider extra, so both are imported lazily; a
build without the provider never pays for them and this module still imports.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

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
    the source. Raises :class:`StructuredMaterializationError` with a stable code when the content
    type is unsupported or morph-kgc rejects the mapping or the data.
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

    with tempfile.TemporaryDirectory(prefix="context-engine-rml-") as workdir:
        root = Path(workdir)
        data_path = root / f"source{suffix}"
        mapping_path = root / "mapping.ttl"
        data_path.write_bytes(data)
        mapping_path.write_text(rml_mapping, encoding="utf-8")
        config = (
            "[CONFIGURATION]\n"
            "output_format=N-TRIPLES\n"
            "[DataSource1]\n"
            f"mappings={mapping_path}\n"
            f"file_path={data_path}\n"
        )
        try:
            return morph_kgc.materialize(config)
        except StructuredMaterializationError:
            raise
        except Exception as exc:  # morph-kgc raises many shapes for bad mappings or data.
            raise StructuredMaterializationError(
                "structured_materialization_failed",
                "The RML mapping could not be applied to the record",
            ) from exc
