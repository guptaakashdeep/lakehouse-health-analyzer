from unittest.mock import patch

from analysis.iceberg import analyze_iceberg_metadata_file


class FakeInspect:
    def files(self):
        return [{"content": 0, "file_size_in_bytes": 64, "record_count": 10}]

    def partitions(self):
        return ()

    def snapshots(self):
        return ()


class FakeIcebergTable:
    def __init__(self, metadata_history, retained_metadata_history_complete=True):
        self.inspect = FakeInspect()
        self._metadata_history = metadata_history
        self._retained_metadata_history_complete = retained_metadata_history_complete

    def name(self):
        return ("warehouse", "sales", "orders")

    def current_snapshot(self):
        return None

    def metadata_history(self):
        return self._metadata_history

    def retained_metadata_history_complete(self):
        return self._retained_metadata_history_complete


class FakePyIcebergTable:
    def __init__(self, metadata):
        self.inspect = FakeInspect()
        self.metadata = metadata

    def name(self):
        return ("warehouse", "sales", "orders")

    def current_snapshot(self):
        return None


def test_retained_metadata_schema_evolution_describes_added_columns():
    table = FakeIcebergTable(
        metadata_history=[
            {
                "metadata_file": "/tmp/v1.metadata.json",
                "schema": {
                    "fields": [
                        {"id": 1, "name": "order_id", "type": "long"},
                    ]
                },
                "properties": {},
            },
            {
                "metadata_file": "/tmp/v2.metadata.json",
                "schema": {
                    "fields": [
                        {"id": 1, "name": "order_id", "type": "long"},
                        {"id": 2, "name": "customer_id", "type": "long"},
                    ]
                },
                "properties": {},
            },
        ]
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/v2.metadata.json")

    assert len(report.table_evolution_history.schema_changes) == 1
    change = report.table_evolution_history.schema_changes[0]
    assert change.change_type == "added"
    assert change.subject == "schema.column"
    assert change.name == "customer_id"
    assert change.before is None
    assert change.after == "long"


def test_retained_metadata_schema_evolution_describes_removed_and_type_changed_columns():
    table = FakeIcebergTable(
        metadata_history=[
            {
                "metadata_file": "/tmp/v1.metadata.json",
                "schema": {
                    "fields": [
                        {"id": 1, "name": "order_id", "type": "long"},
                        {"id": 2, "name": "legacy_code", "type": "string"},
                    ]
                },
                "properties": {},
            },
            {
                "metadata_file": "/tmp/v2.metadata.json",
                "schema": {
                    "fields": [
                        {"id": 1, "name": "order_id", "type": "string"},
                    ]
                },
                "properties": {},
            },
        ]
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/v2.metadata.json")

    assert [
        (change.change_type, change.name, change.before, change.after)
        for change in report.table_evolution_history.schema_changes
    ] == [
        ("removed", "legacy_code", "string", None),
        ("type_changed", "order_id", "long", "string"),
    ]


def test_retained_metadata_property_evolution_describes_added_removed_and_changed_values():
    table = FakeIcebergTable(
        metadata_history=[
            {
                "metadata_file": "/tmp/v1.metadata.json",
                "schema": {"fields": []},
                "properties": {
                    "write.format.default": "parquet",
                    "retention.days": "30",
                },
            },
            {
                "metadata_file": "/tmp/v2.metadata.json",
                "schema": {"fields": []},
                "properties": {
                    "write.format.default": "orc",
                    "owner": "analytics",
                },
            },
        ]
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/v2.metadata.json")

    assert [
        (change.change_type, change.name, change.before, change.after)
        for change in report.table_evolution_history.property_changes
    ] == [
        ("added", "owner", None, "analytics"),
        ("removed", "retention.days", "30", None),
        ("changed", "write.format.default", "parquet", "orc"),
    ]


def test_pyiceberg_metadata_log_entries_describe_property_evolution():
    table = FakePyIcebergTable(
        metadata={
            "metadata_file": "/tmp/v2.metadata.json",
            "metadata_log": [{"metadata_file": "/tmp/v1.metadata.json"}],
            "current_schema_id": 1,
            "schemas": [{"schema_id": 1, "fields": []}],
            "properties": {
                "write.format.default": "orc",
                "owner": "analytics",
            },
        }
    )
    retained_metadata = {
        "metadata_file": "/tmp/v1.metadata.json",
        "current_schema_id": 1,
        "schemas": [{"schema_id": 1, "fields": []}],
        "properties": {
            "write.format.default": "parquet",
            "retention.days": "30",
        },
    }

    with (
        patch("analysis.iceberg._load_static_table", return_value=table),
        patch(
            "analysis.iceberg._load_retained_table_metadata",
            return_value=retained_metadata,
        ) as load_retained_metadata,
    ):
        report = analyze_iceberg_metadata_file("/tmp/v2.metadata.json")

    load_retained_metadata.assert_called_once_with("/tmp/v1.metadata.json")
    assert [
        (change.change_type, change.name, change.before, change.after)
        for change in report.table_evolution_history.property_changes
    ] == [
        ("added", "owner", None, "analytics"),
        ("removed", "retention.days", "30", None),
        ("changed", "write.format.default", "parquet", "orc"),
    ]
    assert any(
        warning.metric_key == "table_evolution_history"
        for warning in report.calculation_warnings
    )


def test_pruned_retained_metadata_history_adds_calculation_warning():
    table = FakeIcebergTable(
        metadata_history=[
            {
                "metadata_file": "/tmp/v2.metadata.json",
                "schema": {"fields": []},
                "properties": {},
            },
        ],
        retained_metadata_history_complete=False,
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/v2.metadata.json")

    assert any(
        warning.metric_key == "table_evolution_history"
        and "retained metadata history is incomplete" in warning.message
        for warning in report.calculation_warnings
    )


def test_retained_metadata_without_schema_or_property_changes_reports_empty_history():
    table = FakeIcebergTable(
        metadata_history=[
            {
                "metadata_file": "/tmp/v1.metadata.json",
                "schema": {"fields": [{"id": 1, "name": "order_id", "type": "long"}]},
                "properties": {"write.format.default": "parquet"},
            },
            {
                "metadata_file": "/tmp/v2.metadata.json",
                "schema": {"fields": [{"id": 1, "name": "order_id", "type": "long"}]},
                "properties": {"write.format.default": "parquet"},
            },
        ]
    )

    with patch("analysis.iceberg._load_static_table", return_value=table):
        report = analyze_iceberg_metadata_file("/tmp/v2.metadata.json")

    assert report.table_evolution_history.schema_changes == ()
    assert report.table_evolution_history.property_changes == ()
    assert not any(
        warning.metric_key == "table_evolution_history"
        for warning in report.calculation_warnings
    )
