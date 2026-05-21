import io
import time

from analysis.report import (
    CalculationWarning,
    DisplayStatistic,
    EvolutionChange,
    HealthMetric,
    MaintenanceRecommendation,
    PartitionHealthMetric,
    TableEvolutionHistory,
    TableHealthReport,
    TableSource,
)
from configuration import (
    AnalyzerConfiguration,
    GlueCatalogTableSourceConfiguration,
    RuntimePolicy,
)
from operator_tui import (
    OperatorCatalogOverview,
    OperatorCatalogWorkflow,
    configured_operator_catalog_workflow,
    list_configured_catalog_tables,
    run_operator_catalog_workflow,
)


def test_operator_catalog_overview_lists_tables_with_headline_metrics():
    workflow = OperatorCatalogWorkflow(
        list_catalog_tables=lambda: ("sales.orders", "sales.customers"),
        analyze_table=lambda table_name: _report(table_name),
        runtime=RuntimePolicy(max_concurrency=2, timeout_seconds=1),
    )

    overview = workflow.load_overview()

    assert [row["table"] for row in overview.rows] == [
        "sales.orders",
        "sales.customers",
    ]
    assert overview.rows[0]["status"] == "loaded"
    assert overview.rows[0]["cache_status"] == "fresh"
    assert overview.rows[0]["data_file_count"] == 7
    assert "sales.orders" in workflow.render_overview(overview)


def test_operator_catalog_overview_shows_snapshot_and_recommendation_counts():
    workflow = OperatorCatalogWorkflow(
        list_catalog_tables=lambda: ("sales.orders",),
        analyze_table=lambda table_name: _report(
            table_name,
            recommendation_severities=("critical", "warning", "info"),
        ),
    )

    overview = workflow.load_overview()

    assert overview.rows[0]["latest_snapshot_age_days"] == 2
    assert overview.rows[0]["valid_snapshot_count"] == 5
    assert overview.rows[0]["expirable_snapshot_candidate_count"] == 3
    assert overview.rows[0]["info_recommendation_count"] == 1
    assert overview.rows[0]["warning_recommendation_count"] == 1
    assert overview.rows[0]["critical_recommendation_count"] == 1
    rendered = workflow.render_overview(overview)
    assert "Critical Recs" in rendered
    assert "12 MiB" in rendered


def test_operator_catalog_overview_keeps_failed_table_identity_visible():
    def analyze_table(table_name):
        if table_name == "sales.missing":
            raise RuntimeError("table is unavailable")
        return _report(table_name)

    workflow = OperatorCatalogWorkflow(
        list_catalog_tables=lambda: ("sales.orders", "sales.missing"),
        analyze_table=analyze_table,
    )

    overview = workflow.load_overview()

    assert [row["table"] for row in overview.rows] == [
        "sales.orders",
        "sales.missing",
    ]
    assert overview.rows[1]["status"] == "warning"
    assert overview.rows[1]["message"] == "table is unavailable"
    assert overview.failure_count == 1
    assert "sales.missing" in workflow.render_overview(overview)


def test_operator_catalog_overview_uses_configured_concurrency_limit(tmp_path):
    events_path = tmp_path / "events.log"

    def analyze_table(table_name):
        _record_event(events_path, "start", table_name)
        time.sleep(0.05)
        _record_event(events_path, "end", table_name)
        return _report(table_name)

    workflow = OperatorCatalogWorkflow(
        list_catalog_tables=lambda: (
            "sales.orders",
            "sales.customers",
            "sales.line_items",
        ),
        analyze_table=analyze_table,
        runtime=RuntimePolicy(max_concurrency=2, timeout_seconds=1),
    )

    overview = workflow.load_overview()

    assert overview.table_count == 3
    assert _max_active_fetches(events_path) == 2


def test_operator_catalog_overview_marks_table_timeout_as_warning():
    def analyze_table(table_name):
        if table_name == "sales.slow":
            time.sleep(0.02)
        return _report(table_name)

    workflow = OperatorCatalogWorkflow(
        list_catalog_tables=lambda: ("sales.fast", "sales.slow"),
        analyze_table=analyze_table,
        runtime=RuntimePolicy(max_concurrency=2, timeout_seconds=0.001),
    )

    overview = workflow.load_overview()

    assert overview.rows[0]["status"] == "loaded"
    assert overview.rows[1]["status"] == "warning"
    assert overview.rows[1]["message"] == "table analysis timed out after 0.001s"
    assert overview.failure_count == 1


def test_operator_catalog_overview_times_table_after_worker_starts():
    def analyze_table(table_name):
        if table_name == "sales.slow":
            time.sleep(0.02)
        return _report(table_name)

    workflow = OperatorCatalogWorkflow(
        list_catalog_tables=lambda: ("sales.slow", "sales.fast"),
        analyze_table=analyze_table,
        runtime=RuntimePolicy(max_concurrency=1, timeout_seconds=0.001),
    )

    overview = workflow.load_overview()

    assert overview.rows[0]["status"] == "warning"
    assert overview.rows[1]["status"] == "loaded"
    assert overview.rows[1]["table"] == "sales.fast"


def test_operator_catalog_overview_terminates_timed_out_fetch_before_next_table(
    tmp_path,
):
    events_path = tmp_path / "events.log"

    def analyze_table(table_name):
        _record_event(events_path, "start", table_name)
        try:
            if table_name == "sales.slow":
                time.sleep(0.5)
            return _report(table_name)
        finally:
            _record_event(events_path, "end", table_name)

    workflow = OperatorCatalogWorkflow(
        list_catalog_tables=lambda: (
            "sales.slow",
            "sales.fast",
            "sales.other",
        ),
        analyze_table=analyze_table,
        runtime=RuntimePolicy(max_concurrency=1, timeout_seconds=0.01),
    )

    started_at = time.monotonic()
    overview = workflow.load_overview()

    assert time.monotonic() - started_at < 0.5
    assert overview.rows[0]["status"] == "warning"
    assert overview.rows[1]["status"] == "loaded"
    assert overview.rows[2]["status"] == "loaded"
    assert _max_active_fetches(events_path) == 1


def test_operator_catalog_overview_uses_cached_rows_when_available():
    cache = FakeCache(
        cached=OperatorCatalogOverview(
            rows=(
                {
                    "table": "sales.orders",
                    "status": "loaded",
                    "cache_status": "cached",
                    "data_file_count": 7,
                    "delete_file_count": 1,
                    "warning_count": 0,
                },
            ),
            cache_status="cached",
        )
    )
    calls = []
    workflow = OperatorCatalogWorkflow(
        list_catalog_tables=lambda: ("sales.orders",),
        analyze_table=lambda table_name: (
            calls.append(table_name) or _report(table_name)
        ),
        cache=cache,
        cache_scope_key="analytics|sales",
    )

    overview = workflow.load_overview()

    assert calls == []
    assert overview.cache_status == "cached"
    assert overview.rows[0]["cache_status"] == "cached"
    assert cache.read_scope == "analytics|sales"


def test_operator_catalog_workflow_inspects_selected_table_report():
    workflow = OperatorCatalogWorkflow(
        list_catalog_tables=lambda: ("sales.orders",),
        analyze_table=lambda table_name: _report(table_name),
    )

    report = workflow.inspect_table("sales.orders")

    assert report.table_name == "sales.orders"
    detail = workflow.render_table_health_report(report)
    assert "Table Health Report: sales.orders" in detail
    assert "Data File Count: 7 files" in detail
    assert "Delete File Count: 1 files" in detail


def test_operator_catalog_workflow_renders_full_selected_table_report():
    workflow = OperatorCatalogWorkflow(
        list_catalog_tables=lambda: ("sales.orders",),
        analyze_table=lambda table_name: _report(
            table_name,
            recommendation_severities=("critical",),
            include_detail=True,
        ),
    )

    output = io.StringIO()
    result = run_operator_catalog_workflow(
        workflow,
        inspect_table="sales.orders",
        output=output,
    )

    assert result == 0
    rendered = output.getvalue()
    assert "Table | Status | Cache" in rendered
    assert "Display Statistics" in rendered
    assert "Total File Size: 12 MiB" in rendered
    assert "Partition Metrics" in rendered
    assert "region=east" in rendered
    assert "added: schema customer_id unknown -> bigint" in rendered
    assert "Evidence: high_file_count_partition_count=5" in rendered
    assert "Thresholds: high_file_count_partition_count_critical=5" in rendered
    assert "Calculation Warnings" in rendered


def test_operator_catalog_tables_are_listed_from_configured_catalog_source(monkeypatch):
    calls = []

    class FakeCatalog:
        def list_tables(self, namespace):
            calls.append(("list_tables", namespace))
            return (("sales", "orders"), ("sales", "customers"))

    def fake_load_catalog(catalog_name, **properties):
        calls.append(("load_catalog", catalog_name, properties))
        return FakeCatalog()

    monkeypatch.setattr("operator_tui.load_catalog", fake_load_catalog)
    config = AnalyzerConfiguration(
        table_source=GlueCatalogTableSourceConfiguration(
            catalog_name="analytics",
            namespace=("sales",),
            table_name="orders",
            aws_profile="dev",
            region="us-east-1",
        )
    )

    assert list_configured_catalog_tables(config) == (
        "sales.orders",
        "sales.customers",
    )
    assert calls == [
        (
            "load_catalog",
            "analytics",
            {"type": "glue", "glue.profile-name": "dev", "glue.region": "us-east-1"},
        ),
        ("list_tables", ("sales",)),
    ]


def test_configured_operator_workflow_analyzes_each_listed_catalog_table(tmp_path):
    calls_path = tmp_path / "calls.log"
    config = AnalyzerConfiguration(
        table_source=GlueCatalogTableSourceConfiguration(
            catalog_name="analytics",
            namespace=("sales",),
            table_name="__catalog_overview__",
        ),
        runtime=RuntimePolicy(max_concurrency=1, timeout_seconds=2),
    )

    def analyze_config(configured):
        table_source = configured.table_source
        calls_path.write_text(
            "|".join(
                (
                    table_source.catalog_name,
                    ".".join(table_source.namespace),
                    table_source.table_name,
                )
            )
        )
        return _report("sales.orders")

    workflow = configured_operator_catalog_workflow(
        config,
        list_tables=lambda configured: ("sales.orders",),
        analyze_config=analyze_config,
    )

    overview = workflow.load_overview()

    assert overview.rows[0]["table"] == "sales.orders"
    assert workflow.runtime == config.runtime
    assert calls_path.read_text() == "analytics|sales|orders"


class FakeCache:
    def __init__(self, cached=None):
        self.cached = cached
        self.read_scope = None
        self.written_scope = None

    def read(self, scope_key):
        self.read_scope = scope_key
        return self.cached

    def write(self, scope_key, overview):
        self.written_scope = scope_key
        return overview

    def invalidate(self, scope_key):
        pass


def _record_event(path, event, table_name):
    with path.open("a") as events:
        events.write(f"{time.monotonic()} {event} {table_name}\n")


def _max_active_fetches(path) -> int:
    active = 0
    max_active = 0
    events = sorted(
        (
            (float(timestamp), event)
            for timestamp, event, _ in (
                line.split(maxsplit=2) for line in path.read_text().splitlines()
            )
        ),
        key=lambda item: (item[0], 0 if item[1] == "end" else 1),
    )
    for _, event in events:
        if event == "start":
            active += 1
            max_active = max(max_active, active)
        else:
            active -= 1
    return max_active


def _report(
    table_name: str,
    recommendation_severities: tuple[str, ...] = (),
    include_detail: bool = False,
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
            HealthMetric(
                key="latest_snapshot_age_days",
                label="Latest Snapshot Age",
                value=2,
                unit="days",
                source="test",
            ),
            HealthMetric(
                key="valid_snapshot_count",
                label="Valid Snapshot Count",
                value=5,
                unit="snapshots",
                source="test",
            ),
            HealthMetric(
                key="expirable_snapshot_candidate_count",
                label="Expirable Snapshot Candidate Count",
                value=3,
                unit="snapshots",
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
        maintenance_recommendations=tuple(
            MaintenanceRecommendation(
                recommendation_type="compaction",
                severity=severity,
                evidence=(
                    {"high_file_count_partition_count": 5} if include_detail else {}
                ),
                thresholds=(
                    {"high_file_count_partition_count_critical": 5}
                    if include_detail
                    else {}
                ),
                rationale="Review this table.",
            )
            for severity in recommendation_severities
        ),
        calculation_warnings=(
            (
                CalculationWarning(
                    metric_key="partition_size_skewness",
                    message="Partition sizes were incomplete.",
                ),
            )
            if include_detail
            else ()
        ),
        partition_metrics=(
            (
                PartitionHealthMetric(
                    partition={"region": "east"},
                    data_file_count=5,
                    delete_file_count=1,
                    total_data_file_size_bytes=1024,
                    average_data_file_size_bytes=204.8,
                    source="test",
                ),
            )
            if include_detail
            else ()
        ),
        table_evolution_history=(
            TableEvolutionHistory(
                schema_changes=(
                    EvolutionChange(
                        change_type="added",
                        subject="schema",
                        name="customer_id",
                        before=None,
                        after="bigint",
                        source="metadata-log",
                    ),
                )
            )
            if include_detail
            else TableEvolutionHistory()
        ),
    )
