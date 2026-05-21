from analysis.report import (
    CalculationWarning,
    DisplayStatistic,
    HealthMetric,
    MaintenanceRecommendation,
    TableHealthReport,
    TableSource,
)
from visualization import catalog_overview
from visualization.catalog_overview import CatalogOverview, build_catalog_overview


def test_catalog_overview_summarizes_analyzed_table_reports():
    overview = build_catalog_overview(
        table_names=("sales.orders",),
        analyze_table=lambda table_name: _report(
            table_name=table_name,
            warning_count=2,
            recommendation_severities=("critical", "warning", "warning", "info"),
        ),
    )

    assert overview.table_count == 1
    assert overview.failure_count == 0
    assert overview.rows == (
        {
            "table": "sales.orders",
            "status": "loaded",
            "total_file_size": "12 MiB",
            "data_file_count": 7,
            "delete_file_count": 1,
            "data_file_record_count": 1200,
            "partition_count": 3,
            "average_data_file_size": "1.7 MiB",
            "warning_count": 2,
            "info_recommendation_count": 1,
            "warning_recommendation_count": 2,
            "critical_recommendation_count": 1,
        },
    )
    assert overview.failures == ()


def test_catalog_overview_keeps_successful_tables_when_one_table_fails():
    def analyze_table(table_name):
        if table_name == "sales.missing":
            raise RuntimeError("table is unavailable")
        return _report(table_name=table_name)

    overview = build_catalog_overview(
        table_names=("sales.orders", "sales.missing"),
        analyze_table=analyze_table,
    )

    assert overview.table_count == 1
    assert overview.failure_count == 1
    assert overview.rows[0]["table"] == "sales.orders"
    assert overview.failures == (
        {
            "table": "sales.missing",
            "status": "warning",
            "message": "table is unavailable",
        },
    )


def test_catalog_overview_handles_empty_catalog_without_analysis():
    calls = []

    overview = build_catalog_overview(
        table_names=(),
        analyze_table=lambda table_name: calls.append(table_name),
    )

    assert overview.table_count == 0
    assert overview.failure_count == 0
    assert overview.rows == ()
    assert overview.failures == ()
    assert calls == []


def test_catalog_overview_renderer_shows_summary_rows_and_failure_warnings(
    monkeypatch,
):
    fake_st = FakeStreamlit()
    monkeypatch.setattr(catalog_overview, "st", fake_st)

    catalog_overview.display_catalog_overview(
        CatalogOverview(
            rows=(
                {
                    "table": "sales.orders",
                    "status": "loaded",
                    "warning_count": 2,
                    "critical_recommendation_count": 1,
                },
            ),
            failures=(
                {
                    "table": "sales.missing",
                    "status": "warning",
                    "message": "table is unavailable",
                },
            ),
        )
    )

    assert fake_st.metrics == [
        ("Analyzed Tables", 1),
        ("Table Warnings", 1),
    ]
    assert fake_st.dataframes == [
        [
            {
                "table": "sales.orders",
                "status": "loaded",
                "warning_count": 2,
                "critical_recommendation_count": 1,
            }
        ]
    ]
    assert fake_st.warnings == ["sales.missing: table is unavailable"]


class FakeStreamlit:
    def __init__(self):
        self.metrics = []
        self.dataframes = []
        self.warnings = []
        self.infos = []

    def metric(self, label, value):
        self.metrics.append((label, value))

    def dataframe(self, value, **kwargs):
        self.dataframes.append(value.to_dict("records"))

    def warning(self, text):
        self.warnings.append(text)

    def info(self, text):
        self.infos.append(text)


def _report(
    table_name: str,
    warning_count: int = 0,
    recommendation_severities: tuple[str, ...] = (),
) -> TableHealthReport:
    return TableHealthReport(
        table_name=table_name,
        table_source=TableSource(kind="glue_catalog_table", location=table_name),
        health_metrics=(
            HealthMetric(
                key="data_file_count",
                label="Data File Count",
                value=7,
                unit="files",
                source="test",
            ),
            HealthMetric(
                key="delete_file_count",
                label="Delete File Count",
                value=1,
                unit="files",
                source="test",
            ),
            HealthMetric(
                key="data_file_record_count",
                label="Data File Record Count",
                value=1200,
                unit="records",
                source="test",
            ),
            HealthMetric(
                key="partition_count",
                label="Partition Count",
                value=3,
                unit="partitions",
                source="test",
            ),
        ),
        display_statistics=(
            DisplayStatistic(
                key="total_file_size",
                label="Total File Size",
                value="12 MiB",
                derived_from=("total_file_size_bytes",),
            ),
            DisplayStatistic(
                key="average_data_file_size",
                label="Average Data File Size",
                value="1.7 MiB",
                derived_from=("data_file_size_bytes", "data_file_count"),
            ),
        ),
        calculation_warnings=tuple(
            CalculationWarning(metric_key=f"metric_{index}", message="incomplete")
            for index in range(warning_count)
        ),
        maintenance_recommendations=tuple(
            MaintenanceRecommendation(
                recommendation_type="compaction",
                severity=severity,
                evidence={},
                thresholds={},
                rationale="Review this table.",
            )
            for severity in recommendation_severities
        ),
    )
