"""Smoke tests for the legacy Iceberg analyzer import surface."""

from analyzers.iceberg import IcebergAnalyzer
from catalogs.iceberg import IcebergCatalog


def test_legacy_iceberg_analyzer_imports_under_uv() -> None:
    """The uv-managed test suite can collect current analyzer modules."""
    assert IcebergAnalyzer.__name__ == "IcebergAnalyzer"
    assert IcebergCatalog.__name__ == "IcebergCatalog"
