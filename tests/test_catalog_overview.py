from datetime import datetime, timezone

from analysis.report import (
    CalculationWarning,
    DisplayStatistic,
    HealthMetric,
    MaintenanceRecommendation,
    TableHealthReport,
    TableSource,
)
from operator_cache import CatalogOverviewCache
from visualization import metrics_dashboard
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


def test_catalog_overview_refresh_bypasses_cached_scope_and_replaces_it(tmp_path):
    analyzed_at = datetime(2026, 5, 21, 8, 30, tzinfo=timezone.utc)
    cache = CatalogOverviewCache(
        tmp_path / "operator-cache.duckdb",
        ttl_seconds=900,
        now=lambda: analyzed_at,
    )
    cache.write(
        "analytics:sales",
        CatalogOverview(rows=({"table": "sales.stale", "status": "loaded"},)),
    )
    calls = []

    overview = build_catalog_overview(
        table_names=("sales.orders",),
        analyze_table=lambda table_name: (
            calls.append(table_name) or _report(table_name=table_name)
        ),
        cache=cache,
        cache_scope_key="analytics:sales",
        refresh=True,
    )

    assert calls == ["sales.orders"]
    assert overview.cache_status == "fresh"
    assert overview.last_analyzed_at == analyzed_at
    assert overview.rows[0]["table"] == "sales.orders"
    assert cache.read("analytics:sales").rows[0]["table"] == "sales.orders"


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
        ("Cache Status", "fresh"),
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


def test_catalog_overview_renderer_marks_cached_values(monkeypatch):
    fake_st = FakeStreamlit()
    monkeypatch.setattr(catalog_overview, "st", fake_st)
    analyzed_at = datetime(2026, 5, 21, 8, 30, tzinfo=timezone.utc)

    catalog_overview.display_catalog_overview(
        CatalogOverview(
            rows=({"table": "sales.orders", "status": "loaded"},),
            cache_status="cached",
            last_analyzed_at=analyzed_at,
        )
    )

    assert ("Cache Status", "cached") in fake_st.metrics
    assert ("Last Analyzed", analyzed_at.isoformat()) in fake_st.metrics


def test_streamlit_catalog_overview_uses_duckdb_cache_and_refresh(monkeypatch):
    fake_st = FakeDashboardStreamlit()
    fake_cache = FakeCache()
    monkeypatch.setattr(metrics_dashboard, "st", fake_st)
    monkeypatch.setattr(
        metrics_dashboard,
        "CatalogOverviewCache",
        lambda path, ttl_seconds: fake_cache.with_ttl(ttl_seconds),
    )
    monkeypatch.setattr(
        metrics_dashboard,
        "default_catalog_overview_cache_path",
        lambda: "/tmp/catalog-overview.duckdb",
    )
    monkeypatch.setattr(
        metrics_dashboard,
        "_catalog_overview_cache_ttl_seconds",
        lambda: 60,
    )

    metrics_dashboard.show_dashboard(
        get_table_metrics=lambda **kwargs: _report(table_name=kwargs["table_name"]),
        list_catalog_tables=lambda **kwargs: ("sales.orders",),
        catalog_name="analytics",
        catalog_namespace="sales",
        use_metadata_file=False,
    )

    assert fake_cache.invalidated_scope == "analytics|sales|||sales.orders"
    assert fake_cache.written_scope == "analytics|sales|||sales.orders"
    assert fake_cache.ttl_seconds == 60


def test_streamlit_catalog_overview_cache_scope_includes_account_and_region(
    monkeypatch,
):
    fake_st = FakeDashboardStreamlit()
    fake_cache = FakeCache()
    monkeypatch.setattr(metrics_dashboard, "st", fake_st)
    monkeypatch.setattr(
        metrics_dashboard,
        "CatalogOverviewCache",
        lambda path, ttl_seconds: fake_cache.with_ttl(ttl_seconds),
    )
    monkeypatch.setattr(
        metrics_dashboard,
        "default_catalog_overview_cache_path",
        lambda: "/tmp/catalog-overview.duckdb",
    )

    metrics_dashboard.show_dashboard(
        get_table_metrics=lambda **kwargs: _report(table_name=kwargs["table_name"]),
        list_catalog_tables=lambda **kwargs: ("sales.orders",),
        catalog_name="analytics",
        catalog_namespace="sales",
        aws_profile="dev",
        aws_region="us-east-1",
        use_metadata_file=False,
    )

    assert fake_cache.written_scope == "analytics|sales|dev|us-east-1|sales.orders"


def test_catalog_overview_cache_ttl_reads_runtime_env_without_table_source_config(
    monkeypatch,
):
    monkeypatch.setenv("LHA_TABLE_SOURCE_KIND", "glue_catalog_table")
    monkeypatch.delenv("LHA_GLUE_CATALOG_NAME", raising=False)
    monkeypatch.setenv("LHA_CACHE_TTL_SECONDS", "120")

    assert metrics_dashboard._catalog_overview_cache_ttl_seconds() == 120


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


class FakeCache:
    def __init__(self):
        self.ttl_seconds = None
        self.invalidated_scope = None
        self.written_scope = None

    def with_ttl(self, ttl_seconds):
        self.ttl_seconds = ttl_seconds
        return self

    def read(self, scope_key):
        return None

    def write(self, scope_key, overview):
        self.written_scope = scope_key
        return overview

    def invalidate(self, scope_key):
        self.invalidated_scope = scope_key


class FakeDashboardStreamlit:
    def __init__(self):
        self.session_state = FakeSessionState()

    def title(self, text):
        pass

    def markdown(self, text):
        pass

    def radio(self, label, options, index, key, help):
        return "Catalog Tables"

    def text_input(self, label, value, key=None, help=None):
        return value

    def checkbox(self, label, value=False):
        return True

    def button(self, label):
        return label == "Load Catalog Overview"

    def progress(self, value):
        return FakeProgress()

    def empty(self):
        return FakeStatus()

    def warning(self, text):
        pass

    def metric(self, label, value):
        pass

    def dataframe(self, value, **kwargs):
        pass


class FakeProgress:
    def progress(self, value):
        pass

    def empty(self):
        pass


class FakeStatus:
    def text(self, value):
        pass

    def empty(self):
        pass


class FakeSessionState(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name, value):
        self[name] = value


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
