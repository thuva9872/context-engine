"""Validation tests for public HTTP, event, and MCP contracts."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator, ValidationError, validate
from openapi_spec_validator import validate as validate_openapi

ROOT = Path(__file__).resolve().parents[2]


def test_openapi_is_valid():
    document = yaml.safe_load((ROOT / "contracts/openapi/context-engine-v1.yaml").read_text())
    validate_openapi(document)


def test_ingestion_example_matches_schema():
    schema = json.loads((ROOT / "contracts/schemas/ingestion-event.schema.json").read_text())
    example = json.loads((ROOT / "contracts/examples/ingestion-upsert.json").read_text())
    Draft202012Validator.check_schema(schema)
    validate(example, schema)


def test_invalid_ingestion_example_is_rejected():
    schema = json.loads((ROOT / "contracts/schemas/ingestion-event.schema.json").read_text())
    example = json.loads(
        (ROOT / "contracts/examples/ingestion-invalid-missing-content.json").read_text()
    )
    with pytest.raises(ValidationError):
        validate(example, schema)


def test_upsert_accepts_inline_content_or_staged_reference():
    schema = json.loads((ROOT / "contracts/schemas/ingestion-event.schema.json").read_text())
    base = json.loads((ROOT / "contracts/examples/ingestion-upsert.json").read_text())

    inline_only = {key: value for key, value in base.items() if key != "contentHash"}
    validate(inline_only, schema)

    reference_only = {
        key: value for key, value in base.items() if key not in {"content", "contentHash"}
    }
    reference_only["contentRef"] = "staged-object-01J8M0"
    validate(reference_only, schema)

    # An upsert with neither inline content nor a staged reference is rejected.
    neither = {
        key: value
        for key, value in base.items()
        if key not in {"content", "contentHash", "contentRef"}
    }
    with pytest.raises(ValidationError):
        validate(neither, schema)


def test_mcp_contract_matches_schema():
    schema = json.loads((ROOT / "contracts/mcp/context-engine-tools.schema.json").read_text())
    contract = json.loads((ROOT / "contracts/mcp/context-engine-tools.json").read_text())
    Draft202012Validator.check_schema(schema)
    validate(contract, schema)
    assert len({tool["name"] for tool in contract["tools"]}) == len(contract["tools"])


def test_provider_boundary_lint():
    completed = subprocess.run(
        [sys.executable, str(ROOT / "scripts/check_provider_boundary.py")],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
