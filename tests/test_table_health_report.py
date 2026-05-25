from datetime import datetime, timezone
from unittest.mock import patch

from analysis.iceberg import analyze_iceberg_metadata_file, analyze_iceberg_table
import pytest

from analysis.report import CalculationWarning, MaintenanceRecommendation
from configuration import (
    AnalyzerConfiguration,
    AnalysisPolicy,
    GlueCatalogTableSourceConfiguration,
    MetadataFileSourceConfiguration,
)


class FakeInspect:
    def __init__(self, files, partitions=(), snapshots=()):
        self._files = files
        self._partitions = partitions
        self._snapshots = snapshots

    def files(self):
        return self._files

    def partitions(self):
        return self._partitions

    def snapshots(self):
        return self._snapshots


class FakeIcebergTable:
    def __init__(
        self, name, files, partitions=(), snapshots=(), current_snapshot_id=None
    ):
        self._name = name
        self._current_snapshot_id = current_snapshot_id
        self.inspect = FakeInspect(files, partitions, snapshots)

    def name(self):
        return self._name

    def current_snapshot(self):
        if self._current_snapshot_id is not None:
            return {"snapshot_id": self._current_snapshot_id}
        return None


def test_metadata_file_analysis_produces_canonical_table_health_report():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [
            {"content": 0, "file_size_in_bytes": 120, "record_count": 10},
            {"content": 0, "file_size_in_bytes": 80, "record_count": 5},
            {"content": 1, "file_size_in_bytes": 20, "record_count": 2},
        ],
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    assert report.table_name == "warehouse.sales.orders"
    assert report.table_source.kind == "metadata_file"
    assert report.table_source.location == "/tmp/orders.metadata.json"

    assert report.health_metric("data_file_count").value == 2
    assert report.health_metric("delete_file_count").value == 1
    assert report.health_metric("total_file_size_bytes").value == 220
    assert report.health_metric("data_file_size_bytes").value == 200

    assert report.display_statistic("total_file_size").value == "220 B"
    assert report.display_statistic("average_data_file_size").value == "100 B"
    assert report.display_statistic("average_data_file_size").derived_from == (
        "data_file_size_bytes",
        "data_file_count",
    )
    assert report.health_metric("estimated_current_record_count").value is None
    assert report.calculation_warnings == (
        CalculationWarning(
            metric_key="estimated_current_record_count",
            message=(
                "Delete files are present, so current records cannot be "
                "calculated exactly from metadata-only file counts."
            ),
        ),
    )


def test_maintenance_recommendation_severity_is_limited_to_known_values():
    with pytest.raises(ValueError):
        MaintenanceRecommendation(
            recommendation_type="compaction",
            severity="urgent",
            evidence={},
            thresholds={},
            rationale="Unsupported severity should fail.",
        )


def test_metadata_file_analysis_accepts_centralized_configuration():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [{"content": 0, "file_size_in_bytes": 64, "record_count": 10}],
    )
    config = AnalyzerConfiguration(
        table_source=MetadataFileSourceConfiguration(
            location="/tmp/orders.metadata.json"
        )
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file(config)

    assert report.table_source.kind == "metadata_file"
    assert report.table_source.location == "/tmp/orders.metadata.json"
    assert report.health_metric("data_file_count").value == 1


def test_glue_catalog_table_analysis_uses_centralized_configuration():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [{"content": 0, "file_size_in_bytes": 64, "record_count": 10}],
    )
    config = AnalyzerConfiguration(
        table_source=GlueCatalogTableSourceConfiguration(
            catalog_name="analytics",
            namespace=("sales",),
            table_name="orders",
            aws_profile="dev",
            region="us-east-1",
        )
    )

    with patch("analysis.iceberg.load_catalog") as load_catalog:
        load_catalog.return_value.load_table.return_value = table
        report = analyze_iceberg_table(config)

    load_catalog.assert_called_once_with(
        "analytics",
        type="glue",
        **{"glue.profile-name": "dev", "glue.region": "us-east-1"},
    )
    load_catalog.return_value.load_table.assert_called_once_with(("sales", "orders"))
    assert report.table_name == "warehouse.sales.orders"
    assert report.table_source.kind == "glue_catalog_table"
    assert report.table_source.location == "analytics.sales.orders"
    assert report.health_metric("data_file_count").value == 1


def test_unavailable_health_metric_values_are_unknown_with_warnings():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [
            {"content": 0, "file_size_in_bytes": None, "record_count": 10},
        ],
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    total_size = report.health_metric("total_file_size_bytes")
    assert total_size.value is None
    assert total_size.is_unknown
    assert report.display_statistic("total_file_size").value is None
    assert report.calculation_warnings[0].metric_key == "total_file_size_bytes"


def test_partially_unavailable_file_sizes_keep_totals_unknown():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [
            {"content": 0, "file_size_in_bytes": 120, "record_count": 10},
            {"content": 0, "file_size_in_bytes": None, "record_count": 5},
            {"content": 1, "file_size_in_bytes": 20, "record_count": 2},
        ],
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    assert report.health_metric("total_file_size_bytes").value is None
    assert report.health_metric("data_file_size_bytes").value is None
    assert report.display_statistic("total_file_size").value is None
    assert report.display_statistic("average_data_file_size").value is None
    assert {warning.metric_key for warning in report.calculation_warnings} == {
        "total_file_size_bytes",
        "data_file_size_bytes",
        "estimated_current_record_count",
    }


def test_partitioned_metadata_file_analysis_reports_balanced_partition_health():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [
            {"content": 0, "file_size_in_bytes": 120, "record_count": 10},
            {"content": 0, "file_size_in_bytes": 80, "record_count": 5},
        ],
        partitions=[
            {
                "partition": {"region": "east"},
                "file_count": 2,
                "position_delete_file_count": 1,
                "equality_delete_file_count": 0,
                "total_data_file_size_in_bytes": 200,
            },
            {
                "partition": {"region": "west"},
                "file_count": 2,
                "position_delete_file_count": 0,
                "equality_delete_file_count": 1,
                "total_data_file_size_in_bytes": 200,
            },
        ],
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    assert report.health_metric("partition_count").value == 2
    assert report.health_metric("average_data_files_per_partition").value == 2
    assert report.health_metric("max_data_files_per_partition").value == 2
    assert report.health_metric("partition_size_skewness").value == 0

    assert len(report.partition_metrics) == 2
    assert report.partition_metrics[0].partition == {"region": "east"}
    assert report.partition_metrics[0].data_file_count == 2
    assert report.partition_metrics[0].delete_file_count == 1
    assert report.partition_metrics[0].total_data_file_size_bytes == 200
    assert report.partition_metrics[0].average_data_file_size_bytes == 100


def test_partitioned_metadata_file_analysis_reports_skewed_partition_distribution():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [{"content": 0, "file_size_in_bytes": 1, "record_count": 1}],
        partitions=[
            {
                "partition": {"region": "east"},
                "file_count": 1,
                "total_data_file_size_in_bytes": 100,
            },
            {
                "partition": {"region": "west"},
                "file_count": 10,
                "total_data_file_size_in_bytes": 1000,
            },
        ],
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    assert report.health_metric("min_partition_data_file_size_bytes").value == 100
    assert report.health_metric("max_partition_data_file_size_bytes").value == 1000
    assert report.health_metric("average_partition_data_file_size_bytes").value == 550
    assert report.health_metric("partition_size_skewness").value > 0.8


def test_empty_iceberg_table_without_snapshot_reports_no_data_metrics():
    no_snapshot_message = "Cannot get the snapshot as the table doesn't have any"

    class NoSnapshotInspect:
        def files(self):
            raise ValueError(no_snapshot_message)

        def partitions(self):
            raise ValueError(no_snapshot_message)

        def snapshots(self):
            raise ValueError(no_snapshot_message)

    table = FakeIcebergTable(("warehouse", "sales", "empty_orders"), [])
    table.inspect = NoSnapshotInspect()

    def current_snapshot():
        raise ValueError(no_snapshot_message)

    table.current_snapshot = current_snapshot

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/empty_orders.metadata.json")

    assert report.table_name == "warehouse.sales.empty_orders"
    assert report.health_metric("data_file_count").value == 0
    assert report.health_metric("delete_file_count").value == 0
    assert report.health_metric("total_file_size_bytes").value == 0
    assert report.health_metric("data_file_size_bytes").value == 0
    assert report.health_metric("data_file_record_count").value == 0
    assert report.health_metric("partition_count").value == 0
    assert report.health_metric("valid_snapshot_count").value == 0
    assert report.health_metric("expirable_snapshot_candidate_count").value == 0
    assert report.display_statistic("total_file_size").value == "0 B"


def test_empty_partition_metadata_reports_zero_partition_totals():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [],
        partitions=[],
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    assert report.health_metric("partition_count").value == 0
    assert report.health_metric("partition_total_data_file_size_bytes").value == 0
    assert report.health_metric("average_data_files_per_partition").value is None
    assert report.health_metric("partition_size_skewness").value == 0
    assert report.partition_metrics == ()


def test_high_file_count_partition_pressure_is_reported_with_evidence():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [{"content": 0, "file_size_in_bytes": 1, "record_count": 1}],
        partitions=[
            {
                "partition": {"date": "2026-05-20"},
                "file_count": 25,
                "total_data_file_size_in_bytes": 250,
            },
            {
                "partition": {"date": "2026-05-21"},
                "file_count": 125,
                "total_data_file_size_in_bytes": 1250,
            },
        ],
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    assert report.health_metric("high_file_count_partition_count").value == 1
    assert report.health_metric("high_file_count_partition_threshold").value == 100
    assert report.health_metric("max_data_files_per_partition").value == 125
    assert report.partition_metrics[1].partition == {"date": "2026-05-21"}
    assert report.partition_metrics[1].data_file_count == 125


def test_high_file_count_partition_pressure_produces_compaction_recommendation():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [{"content": 0, "file_size_in_bytes": 1, "record_count": 1}],
        partitions=[
            {
                "partition": {"date": "2026-05-20"},
                "file_count": 125,
                "total_data_file_size_in_bytes": 1250,
            },
        ],
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    recommendation = report.maintenance_recommendations[0]
    assert recommendation.recommendation_type == "compaction"
    assert recommendation.severity == "warning"
    assert recommendation.evidence["high_file_count_partition_count"] == 1
    assert recommendation.evidence["max_data_files_per_partition"] == 125
    assert recommendation.thresholds["high_file_count_partition_threshold"] == 100
    assert recommendation.thresholds["high_file_count_partition_count_warning"] == 1
    assert "compact" in recommendation.rationale.lower()


def test_compaction_recommendation_respects_configured_severity_boundaries():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [{"content": 0, "file_size_in_bytes": 1, "record_count": 1}],
        partitions=[
            {
                "partition": {"date": "2026-05-20"},
                "file_count": 125,
                "total_data_file_size_in_bytes": 1250,
            },
        ],
    )
    config = AnalyzerConfiguration(
        table_source=MetadataFileSourceConfiguration(
            location="/tmp/orders.metadata.json"
        ),
        analysis=AnalysisPolicy(
            recommendation_thresholds={
                "high_file_count_partition_count_warning": 2,
                "high_file_count_partition_count_critical": 1,
            }
        ),
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file(config)

    recommendation = report.maintenance_recommendations[0]
    assert recommendation.severity == "critical"
    assert recommendation.thresholds["high_file_count_partition_count_critical"] == 1


def test_non_partitioned_metadata_without_partition_column_analyzes_successfully():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [{"content": 0, "file_size_in_bytes": 128, "record_count": 10}],
        partitions=[
            {
                "file_count": 1,
                "position_delete_file_count": 0,
                "equality_delete_file_count": 0,
                "total_data_file_size_in_bytes": 128,
            }
        ],
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    assert report.health_metric("partition_count").value == 1
    assert report.partition_metrics[0].partition == {}
    assert report.partition_metrics[0].data_file_count == 1
    assert report.partition_metrics[0].total_data_file_size_bytes == 128


def test_data_only_files_report_exact_estimated_current_record_count():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [
            {"content": 0, "file_size_in_bytes": 120, "record_count": 10},
            {"content": 0, "file_size_in_bytes": 80, "record_count": 5},
        ],
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    assert report.health_metric("data_file_record_count").value == 15
    assert report.health_metric("delete_file_size_bytes").value == 0
    assert report.health_metric("position_delete_record_count").value == 0
    assert report.health_metric("equality_delete_record_count").value == 0
    assert report.health_metric("estimated_current_record_count").value == 15


def test_incomplete_partition_sizes_make_skewness_unknown_with_warning():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [{"content": 0, "file_size_in_bytes": 1, "record_count": 1}],
        partitions=[
            {
                "partition": {"region": "east"},
                "file_count": 1,
                "total_data_file_size_in_bytes": 100,
            },
            {
                "partition": {"region": "west"},
                "file_count": 1,
                "total_data_file_size_in_bytes": None,
            },
        ],
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    assert report.health_metric("partition_size_skewness").value is None
    assert any(
        warning.metric_key == "partition_size_skewness"
        for warning in report.calculation_warnings
    )


def test_position_delete_files_make_estimated_current_record_count_unknown():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [
            {"content": 0, "file_size_in_bytes": 120, "record_count": 10},
            {"content": 1, "file_size_in_bytes": 20, "record_count": 2},
        ],
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    assert report.health_metric("data_file_record_count").value == 10
    assert report.health_metric("position_delete_record_count").value == 2
    assert report.health_metric("equality_delete_record_count").value == 0
    assert report.health_metric("estimated_current_record_count").value is None
    assert any(
        warning.metric_key == "estimated_current_record_count"
        for warning in report.calculation_warnings
    )


def test_delete_files_produce_delete_file_cleanup_recommendation():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [
            {"content": 0, "file_size_in_bytes": 120, "record_count": 10},
            {"content": 1, "file_size_in_bytes": 20, "record_count": 2},
        ],
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    recommendation = report.maintenance_recommendations[0]
    assert recommendation.recommendation_type == "delete_file_cleanup"
    assert recommendation.severity == "info"
    assert recommendation.evidence["delete_file_count"] == 1
    assert recommendation.evidence["position_delete_record_count"] == 2
    assert recommendation.thresholds["delete_file_count_info"] == 1
    assert "delete file" in recommendation.rationale.lower()


def test_report_can_include_multiple_simultaneous_maintenance_recommendations():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [
            {"content": 0, "file_size_in_bytes": 120, "record_count": 10},
            {"content": 1, "file_size_in_bytes": 20, "record_count": 2},
        ],
        partitions=[
            {
                "partition": {"date": "2026-05-20"},
                "file_count": 125,
                "total_data_file_size_in_bytes": 1250,
            },
        ],
        snapshots=[
            {"snapshot_id": 101, "committed_at": datetime.now(timezone.utc)},
            {"snapshot_id": 102, "timestamp_ms": 0},
        ],
        current_snapshot_id=101,
    )
    config = AnalyzerConfiguration(
        table_source=MetadataFileSourceConfiguration(
            location="/tmp/orders.metadata.json"
        ),
        analysis=AnalysisPolicy(
            recommendation_thresholds={
                "valid_snapshot_count_warning": 2,
            }
        ),
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file(config)

    assert {
        recommendation.recommendation_type
        for recommendation in report.maintenance_recommendations
    } == {
        "compaction",
        "snapshot_expiration",
        "metadata_cleanup",
        "delete_file_cleanup",
    }


def test_equality_delete_files_are_counted_in_delete_file_metrics():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [
            {"content": 0, "file_size_in_bytes": 200, "record_count": 20},
            {"content": 2, "file_size_in_bytes": 40, "record_count": 5},
        ],
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    assert report.health_metric("delete_file_count").value == 1
    assert report.health_metric("delete_file_size_bytes").value == 40
    assert report.health_metric("position_delete_record_count").value == 0
    assert report.health_metric("equality_delete_record_count").value == 5


def test_mixed_delete_files_report_separate_delete_record_counts():
    table = FakeIcebergTable(
        ("warehouse", "sales", "orders"),
        [
            {"content": 0, "file_size_in_bytes": 100, "record_count": 10},
            {"content": 1, "file_size_in_bytes": 20, "record_count": 2},
            {"content": 2, "file_size_in_bytes": 30, "record_count": 3},
        ],
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    assert report.health_metric("data_file_record_count").value == 10
    assert report.health_metric("position_delete_record_count").value == 2
    assert report.health_metric("equality_delete_record_count").value == 3
    assert report.health_metric("delete_file_count").value == 2
    assert report.health_metric("delete_file_size_bytes").value == 50
    assert report.health_metric("estimated_current_record_count").value is None
