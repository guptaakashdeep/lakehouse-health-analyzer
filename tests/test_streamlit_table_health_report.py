from analysis.report import (
    CalculationWarning,
    DisplayStatistic,
    HealthMetric,
    TableHealthReport,
    TableSource,
)
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


def test_streamlit_metadata_file_mode_uses_canonical_report_path(monkeypatch):
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

    def fake_analyze(metadata_location):
        calls.append(metadata_location)
        return report

    monkeypatch.setattr(streamlit_app, "analyze_iceberg_metadata_file", fake_analyze)

    result = streamlit_app.get_table_metrics(
        use_metadata_file=True,
        metadata_location="/tmp/orders.metadata.json",
    )

    assert result is report
    assert calls == ["/tmp/orders.metadata.json"]
