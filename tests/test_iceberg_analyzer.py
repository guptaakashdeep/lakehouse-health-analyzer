"""Smoke tests for the Iceberg analyzer import surface."""

from analyzers.base import LiveTableMetrics, SnapshotMetrics
from analyzers.iceberg import IcebergAnalyzer
from catalogs.iceberg import IcebergCatalog


def test_iceberg_analyzer_imports_under_uv() -> None:
    """The uv-managed test suite can collect current analyzer modules."""
    assert IcebergAnalyzer.__name__ == "IcebergAnalyzer"
    assert IcebergCatalog.__name__ == "IcebergCatalog"


def test_iceberg_analyzer_does_not_print_when_snapshot_is_missing(capsys) -> None:
    analyzer = object.__new__(IcebergAnalyzer)

    metrics = analyzer.get_snapshot_metrics(None)

    assert isinstance(metrics, SnapshotMetrics)
    assert capsys.readouterr().out == ""


def test_iceberg_analyzer_get_table_metrics_does_not_print(capsys) -> None:
    analyzer = object.__new__(IcebergAnalyzer)
    analyzer.table_name = "sales.orders"
    analyzer.table = _FakeTable()
    analyzer.get_live_table_metrics = lambda: LiveTableMetrics(
        total_data_files=1,
        total_delete_files=0,
        total_files_size=1024,
        data_file_size=1024,
        delete_file_size=0,
        record_count=100,
    )
    analyzer.get_snapshot_metrics = lambda snapshot_id: SnapshotMetrics(
        added_data_files=0,
        deleted_data_files=0,
        added_delete_files=0,
        removed_delete_files=0,
        added_records=0,
        deleted_records=0,
        changed_partition_count=0,
        operation="append",
    )
    analyzer.get_partition_metrics = lambda: []
    analyzer.get_file_metrics = lambda: []
    analyzer.get_historical_snapshots = lambda limit=20: []

    metrics = analyzer.get_table_metrics()

    assert metrics.table_name == "sales.orders"
    assert capsys.readouterr().out == ""


class _FakeSnapshot:
    snapshot_id = 123


class _FakeTable:
    def current_snapshot(self):
        return _FakeSnapshot()
