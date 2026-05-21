"""Smoke test for runtime dependencies in the uv-managed environment."""

from __future__ import annotations

from importlib.metadata import requires

import duckdb
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField


def test_runtime_dependencies_are_declared_and_usable() -> None:
    """Package metadata declares required floors and runtime deps execute together."""
    runtime_requirements = requires("lakehouse-health-analyzer")
    assert runtime_requirements is not None

    assert any(req.startswith("pyiceberg>=0.11.1") for req in runtime_requirements)
    assert any(req.startswith("duckdb>=1.5.3") for req in runtime_requirements)

    schema = Schema(
        NestedField(
            field_id=1, name="record_count", field_type=LongType(), required=True
        )
    )
    field_name = schema.find_field("record_count").name

    query = (
        f"SELECT SUM({field_name}) AS total_records "
        f"FROM (VALUES (10), (20), (30)) AS records({field_name})"
    )
    total_records = duckdb.connect(":memory:").sql(query).fetchone()[0]
    assert total_records == 60
