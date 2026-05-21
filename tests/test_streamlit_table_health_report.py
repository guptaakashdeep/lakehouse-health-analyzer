from analysis.report import (
    CalculationWarning,
    DisplayStatistic,
    HealthMetric,
    PartitionHealthMetric,
    TableHealthReport,
    TableSource,
)
from configuration import AnalyzerConfiguration, GlueCatalogTableSourceConfiguration
from visualization.table_health_report import display_table_health_report


class FakeStreamlit:
    def __init__(self):
        self.headers = []
        self.metrics = []
        self.warnings = []

    def header(self, text):
        self.headers.append(text)

    def subheader(self, text):
        self.headers.append(text)

    def metric(self, label, value):
        self.metrics.append((label, value))

    def warning(self, text):
        self.warnings.append(text)


class FakePartitionStreamlit(FakeStreamlit):
    def __init__(self):
        super().__init__()
        self.dataframes = []

    def dataframe(self, value, **kwargs):
        self.dataframes.append(value)


def test_streamlit_report_renderer_consumes_report_values(monkeypatch):
    fake_st = FakeStreamlit()
    monkeypatch.setattr("visualization.table_health_report.st", fake_st)

    report = TableHealthReport(
        table_name="warehouse.sales.orders",
        table_source=TableSource(
            kind="metadata_file", location="/tmp/orders.metadata.json"
        ),
        health_metrics=(
            HealthMetric(
                key="total_file_size_bytes",
                label="Total File Size Bytes",
                value=999,
                unit="bytes",
                source="test",
            ),
        ),
        display_statistics=(
            DisplayStatistic(
                key="total_file_size",
                label="Total File Size",
                value="rendered by report",
                derived_from=("total_file_size_bytes",),
            ),
        ),
        calculation_warnings=(
            CalculationWarning(
                metric_key="total_file_size_bytes",
                message="size is incomplete",
            ),
        ),
    )

    display_table_health_report(report)

    assert ("Total File Size Bytes", "999 bytes") in fake_st.metrics
    assert ("Total File Size", "rendered by report") in fake_st.metrics
    assert fake_st.warnings == ["total_file_size_bytes: size is incomplete"]


def test_streamlit_report_renderer_displays_partition_report_values(monkeypatch):
    fake_st = FakePartitionStreamlit()
    monkeypatch.setattr("visualization.table_health_report.st", fake_st)
    monkeypatch.setattr("visualization.components.partition_metrics.st", fake_st)

    report = TableHealthReport(
        table_name="warehouse.sales.orders",
        table_source=TableSource(
            kind="metadata_file", location="/tmp/orders.metadata.json"
        ),
        health_metrics=(
            HealthMetric(
                key="partition_count",
                label="Partition Count",
                value=2,
                unit="partitions",
                source="iceberg.inspect.partitions",
            ),
            HealthMetric(
                key="high_file_count_partition_count",
                label="High File Count Partition Count",
                value=1,
                unit="partitions",
                source="iceberg.inspect.partitions",
            ),
        ),
        display_statistics=(),
        partition_metrics=(
            PartitionHealthMetric(
                partition={"region": "east"},
                data_file_count=4,
                delete_file_count=1,
                total_data_file_size_bytes=400,
                average_data_file_size_bytes=100,
                source="iceberg.inspect.partitions",
            ),
        ),
    )

    display_table_health_report(report)

    assert ("Partition Count", "2 partitions") in fake_st.metrics
    assert ("High File Count Partition Count", "1 partitions") in fake_st.metrics
    assert fake_st.dataframes
    assert fake_st.dataframes[0].to_dict("records") == [
        {
            "region": "east",
            "data_file_count": 4,
            "delete_file_count": 1,
            "total_data_file_size_bytes": 400,
            "average_data_file_size_bytes": 100,
        }
    ]


def test_streamlit_metadata_file_mode_uses_centralized_configuration(monkeypatch):
    import streamlit_app

    report = TableHealthReport(
        table_name="warehouse.sales.orders",
        table_source=TableSource(
            kind="metadata_file", location="/tmp/orders.metadata.json"
        ),
        health_metrics=(),
        display_statistics=(),
    )

    calls = []

    def fake_analyze(config):
        calls.append(config)
        return report

    monkeypatch.setattr(streamlit_app, "analyze_iceberg_metadata_file", fake_analyze)

    result = streamlit_app.get_table_metrics(
        use_metadata_file=True,
        metadata_location="/tmp/orders.metadata.json",
    )

    assert result is report
    assert len(calls) == 1
    assert isinstance(calls[0], AnalyzerConfiguration)
    assert calls[0].table_source.location == "/tmp/orders.metadata.json"


def test_streamlit_catalog_mode_uses_canonical_report_path(monkeypatch):
    import streamlit_app

    report = TableHealthReport(
        table_name="warehouse.sales.orders",
        table_source=TableSource(
            kind="glue_catalog_table", location="analytics.sales.orders"
        ),
        health_metrics=(),
        display_statistics=(),
    )

    calls = []

    def fake_analyze(config):
        calls.append(config)
        return report

    monkeypatch.setattr(streamlit_app, "analyze_iceberg_table", fake_analyze)

    result = streamlit_app.get_table_metrics(
        table_name="sales.orders",
        catalog_name="analytics",
        use_metadata_file=False,
    )

    assert result is report
    assert len(calls) == 1
    assert isinstance(calls[0], AnalyzerConfiguration)
    assert calls[0].table_source == GlueCatalogTableSourceConfiguration(
        catalog_name="analytics",
        namespace=("sales",),
        table_name="orders",
    )
