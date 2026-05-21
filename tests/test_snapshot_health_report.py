from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from analysis.iceberg import analyze_iceberg_metadata_file
from configuration import (
    AnalyzerConfiguration,
    AnalysisPolicy,
    MetadataFileSourceConfiguration,
)


class FakeInspect:
    def __init__(self, files, snapshots=()):
        self._files = files
        self._snapshots = snapshots

    def files(self):
        return self._files

    def partitions(self):
        return ()

    def snapshots(self):
        return self._snapshots


class FakeIcebergTable:
    def __init__(self, snapshots=(), current_snapshot_id=None):
        self._current_snapshot_id = current_snapshot_id
        self.inspect = FakeInspect(
            [{"content": 0, "file_size_in_bytes": 64, "record_count": 10}],
            snapshots=snapshots,
        )

    def name(self):
        return ("warehouse", "sales", "orders")

    def current_snapshot(self):
        if self._current_snapshot_id is None:
            return None
        return {"snapshot_id": self._current_snapshot_id}


def days_ago(days):
    return datetime.now(timezone.utc) - timedelta(days=days)


def test_current_only_snapshot_reports_age_without_expirable_candidate():
    table = FakeIcebergTable(
        snapshots=[
            {"snapshot_id": 101, "committed_at": days_ago(45)},
        ],
        current_snapshot_id=101,
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    assert report.health_metric("valid_snapshot_count").value == 1
    assert report.health_metric("oldest_snapshot_age_days").value == 45
    assert report.health_metric("latest_snapshot_age_days").value == 45
    assert report.health_metric("snapshot_retention_days").value == 30
    assert report.health_metric("expirable_snapshot_candidate_count").value == 0


def test_recent_snapshots_are_not_expirable_candidates():
    table = FakeIcebergTable(
        snapshots=[
            {"snapshot_id": 101, "committed_at": days_ago(7)},
            {"snapshot_id": 102, "committed_at": days_ago(0)},
        ],
        current_snapshot_id=102,
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    assert report.health_metric("valid_snapshot_count").value == 2
    assert report.health_metric("oldest_snapshot_age_days").value == 7
    assert report.health_metric("latest_snapshot_age_days").value == 0
    assert report.health_metric("expirable_snapshot_candidate_count").value == 0


def test_old_snapshots_use_configured_retention_policy_for_candidates():
    table = FakeIcebergTable(
        snapshots=[
            {"snapshot_id": 101, "committed_at": days_ago(70)},
            {"snapshot_id": 102, "committed_at": days_ago(2)},
        ],
        current_snapshot_id=102,
    )
    config = AnalyzerConfiguration(
        table_source=MetadataFileSourceConfiguration(
            location="/tmp/orders.metadata.json"
        ),
        analysis=AnalysisPolicy(snapshot_retention_days=60),
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file(config)

    assert report.health_metric("snapshot_retention_days").value == 60
    assert report.health_metric("expirable_snapshot_candidate_count").value == 1


def test_mixed_snapshots_only_count_non_current_old_candidates():
    table = FakeIcebergTable(
        snapshots=[
            {"snapshot_id": 101, "committed_at": days_ago(45)},
            {"snapshot_id": 102, "committed_at": days_ago(10)},
            {"snapshot_id": 103, "committed_at": days_ago(90)},
        ],
        current_snapshot_id=103,
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    assert report.health_metric("valid_snapshot_count").value == 3
    assert report.health_metric("oldest_snapshot_age_days").value == 90
    assert report.health_metric("latest_snapshot_age_days").value == 10
    assert report.health_metric("expirable_snapshot_candidate_count").value == 1


def test_incomplete_snapshot_metadata_is_unknown_with_calculation_warnings():
    table = FakeIcebergTable(
        snapshots=[
            {"snapshot_id": 101},
        ],
        current_snapshot_id=101,
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    assert report.health_metric("valid_snapshot_count").value is None
    assert report.health_metric("oldest_snapshot_age_days").value is None
    assert report.health_metric("latest_snapshot_age_days").value is None
    assert report.health_metric("expirable_snapshot_candidate_count").value is None
    assert {warning.metric_key for warning in report.calculation_warnings} >= {
        "valid_snapshot_count",
        "oldest_snapshot_age_days",
        "latest_snapshot_age_days",
        "expirable_snapshot_candidate_count",
    }


def test_snapshot_without_id_is_not_counted_as_expirable_candidate():
    table = FakeIcebergTable(
        snapshots=[
            {"committed_at": days_ago(45)},
        ],
        current_snapshot_id=101,
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/orders.metadata.json")

    assert report.health_metric("valid_snapshot_count").value is None
    assert report.health_metric("expirable_snapshot_candidate_count").value is None
    assert {warning.metric_key for warning in report.calculation_warnings} >= {
        "valid_snapshot_count",
        "oldest_snapshot_age_days",
        "latest_snapshot_age_days",
        "expirable_snapshot_candidate_count",
    }
