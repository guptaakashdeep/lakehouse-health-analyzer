from unittest.mock import patch

from analysis.iceberg import analyze_iceberg_metadata_file


class FakeInspect:
    def __init__(self, files):
        self._files = files

    def files(self):
        return self._files


class FakeIcebergTable:
    def __init__(self, name, files):
        self._name = name
        self.inspect = FakeInspect(files)

    def name(self):
        return self._name


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
    assert report.calculation_warnings == ()


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
    }
