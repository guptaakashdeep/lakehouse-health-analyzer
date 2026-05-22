from datetime import datetime, timedelta, timezone
from threading import Event

from analysis.report import HealthMetric, TableHealthReport, TableSource
from configuration import AnalyzerConfiguration, GlueCatalogTableSourceConfiguration
from operator_cache import OperatorCache
from workflows.catalog_browser import (
    CatalogBrowserWorkflow,
    CatalogNamespace,
    CatalogNamespaceListing,
    CatalogTableListing,
    CatalogTableRow,
    TableFormatClassification,
)
from workflows.table_detail import TableDetailResult, TableDetailWorkflow


def test_catalog_browser_workflow_reuses_cached_table_classification(tmp_path):
    analyzed_at = datetime(2026, 5, 21, 8, 30, tzinfo=timezone.utc)
    cache = OperatorCache(
        tmp_path / "operator-cache.duckdb",
        ttl_seconds=900,
        now=lambda: analyzed_at,
    )
    cache.write_table_classification(
        "sales.orders",
        TableFormatClassification.NON_ICEBERG,
        "analysis_failure",
    )
    workflow = CatalogBrowserWorkflow(
        catalog_access=FakeCatalogAccess(
            tables=CatalogTableListing(
                namespace=("sales",),
                rows=(_table_row("orders"),),
            )
        ),
        cache=cache,
        cache_scope_key="analytics",
    )

    listing = workflow.list_tables(("sales",))

    assert listing.cache_status == "fresh"
    assert listing.rows[0].table_format == TableFormatClassification.NON_ICEBERG
    assert listing.rows[0].classification_source == "analysis_failure"
    assert listing.rows[0].cache_status == "cached"


def test_cached_table_listing_does_not_extend_expired_classification_ttl(tmp_path):
    written_at = datetime(2026, 5, 21, 8, 30, tzinfo=timezone.utc)
    current_time = written_at
    cache = OperatorCache(
        tmp_path / "operator-cache.duckdb",
        ttl_seconds=60,
        now=lambda: current_time,
    )
    cache.write_table_classification(
        "sales.orders",
        TableFormatClassification.NON_ICEBERG,
        "analysis_failure",
    )
    access = FakeCatalogAccess(
        tables=CatalogTableListing(
            namespace=("sales",),
            rows=(_table_row("orders"),),
        )
    )
    workflow = CatalogBrowserWorkflow(
        catalog_access=access,
        cache=cache,
        cache_scope_key="analytics",
    )

    current_time = written_at + timedelta(seconds=30)
    learned_listing = workflow.list_tables(("sales",))

    assert learned_listing.rows[0].table_format == (
        TableFormatClassification.NON_ICEBERG
    )
    assert learned_listing.rows[0].cache_status == "cached"

    current_time = written_at + timedelta(seconds=61)
    cached_listing = workflow.list_tables(("sales",))

    assert cached_listing.cache_status == "cached"
    assert cached_listing.rows[0].table_format == TableFormatClassification.UNKNOWN
    assert cached_listing.rows[0].classification_source == "glue_parameters"
    assert access.requested_table_namespaces == [("sales",)]


def test_catalog_browser_workflow_caches_table_listing_until_ttl_expires(tmp_path):
    written_at = datetime(2026, 5, 21, 8, 30, tzinfo=timezone.utc)
    current_time = written_at
    cache = OperatorCache(
        tmp_path / "operator-cache.duckdb",
        ttl_seconds=60,
        now=lambda: current_time,
    )
    access = FakeCatalogAccess(
        tables=CatalogTableListing(namespace=("sales",), rows=(_table_row("orders"),))
    )
    workflow = CatalogBrowserWorkflow(
        catalog_access=access,
        cache=cache,
        cache_scope_key="analytics",
    )

    assert workflow.list_tables(("sales",)).cache_status == "fresh"
    cached = workflow.list_tables(("sales",))

    assert access.requested_table_namespaces == [("sales",)]
    assert cached.cache_status == "cached"
    assert cached.rows[0].cache_status == "cached"

    current_time = written_at + timedelta(seconds=61)
    refreshed = workflow.list_tables(("sales",))

    assert refreshed.cache_status == "fresh"
    assert access.requested_table_namespaces == [("sales",), ("sales",)]


def test_catalog_browser_refresh_preserves_unrelated_cached_scopes(tmp_path):
    cache = OperatorCache(tmp_path / "operator-cache.duckdb")
    cache.write_table_listing(
        "analytics",
        ("sales",),
        CatalogTableListing(namespace=("sales",), rows=(_table_row("old_orders"),)),
    )
    cache.write_table_listing(
        "analytics",
        ("finance",),
        CatalogTableListing(namespace=("finance",), rows=(_table_row("invoices", "finance"),)),
    )
    workflow = CatalogBrowserWorkflow(
        catalog_access=FakeCatalogAccess(
            tables=CatalogTableListing(namespace=("sales",), rows=(_table_row("orders"),))
        ),
        cache=cache,
        cache_scope_key="analytics",
    )

    refreshed = workflow.list_tables(("sales",), refresh=True)

    assert refreshed.rows[0].identifier == "sales.orders"
    finance = cache.read_table_listing("analytics", ("finance",))
    assert finance.rows[0].identifier == "finance.invoices"


def test_catalog_browser_namespace_refresh_failure_returns_stale_cached_listing(
    tmp_path,
):
    cache = OperatorCache(tmp_path / "operator-cache.duckdb")
    cache.write_namespace_listing(
        "analytics",
        CatalogNamespaceListing(
            namespaces=(CatalogNamespace(name=("sales",), display_name="sales"),)
        ),
    )
    workflow = CatalogBrowserWorkflow(
        catalog_access=FailingCatalogAccess("glue is unavailable"),
        cache=cache,
        cache_scope_key="analytics",
    )

    listing = workflow.list_namespaces(refresh=True)

    assert listing.cache_status == "stale"
    assert listing.cache_message == "glue is unavailable"
    assert listing.namespaces[0].display_name == "sales"


def test_catalog_browser_namespace_refresh_preserves_table_cache(tmp_path):
    cache = OperatorCache(tmp_path / "operator-cache.duckdb")
    cache.write_table_listing(
        "analytics",
        ("sales",),
        CatalogTableListing(namespace=("sales",), rows=(_table_row("orders"),)),
    )
    workflow = CatalogBrowserWorkflow(
        catalog_access=FakeCatalogAccess(
            namespaces=CatalogNamespaceListing(
                namespaces=(CatalogNamespace(name=("finance",), display_name="finance"),)
            )
        ),
        cache=cache,
        cache_scope_key="analytics",
    )

    listing = workflow.list_namespaces(refresh=True)

    assert listing.namespaces[0].display_name == "finance"
    assert cache.read_table_listing("analytics", ("sales",)).rows[0].identifier == (
        "sales.orders"
    )


def test_table_detail_workflow_reuses_cached_report_and_refreshes_on_request(
    tmp_path,
):
    calls = []
    cache = OperatorCache(tmp_path / "operator-cache.duckdb")
    workflow = TableDetailWorkflow(
        base_config=_base_config(),
        analyze_config=lambda config: calls.append(config) or _report("sales.orders"),
        cache=cache,
    )
    table = _table_row("orders")

    fresh = workflow.select_table(table).result(timeout=1)
    cached = workflow.select_table(table).result(timeout=1)
    refreshed = workflow.select_table(table, refresh=True).result(timeout=1)

    assert [result.cache_status for result in (fresh, cached, refreshed)] == [
        "fresh",
        "cached",
        "fresh",
    ]
    assert [call.table_source.table_name for call in calls] == ["orders", "orders"]
    assert cached.report.table_name == "sales.orders"


def test_operator_cache_caches_table_detail_report_until_ttl_expires(tmp_path):
    written_at = datetime(2026, 5, 21, 8, 30, tzinfo=timezone.utc)
    current_time = written_at
    cache = OperatorCache(
        tmp_path / "operator-cache.duckdb",
        ttl_seconds=60,
        now=lambda: current_time,
    )
    cache.write_table_detail_report(
        "sales.orders",
        TableDetailResult(
            table=_table_row("orders"),
            report=_report("sales.orders"),
        ),
    )

    cached = cache.read_table_detail_report("sales.orders")

    assert cached.cache_status == "cached"
    assert cached.report.table_name == "sales.orders"
    assert cached.detail_view.table_name == "sales.orders"

    current_time = written_at + timedelta(seconds=61)

    assert cache.read_table_detail_report("sales.orders") is None
    assert cache.read_stale_table_detail_report("sales.orders").cache_status == "stale"


def test_table_detail_refresh_failure_preserves_stale_cached_report(tmp_path):
    cache = OperatorCache(tmp_path / "operator-cache.duckdb")
    workflow = TableDetailWorkflow(
        base_config=_base_config(),
        analyze_config=lambda config: _report("sales.orders"),
        cache=cache,
    )
    table = _table_row("orders")
    workflow.select_table(table).result(timeout=1)

    failing_workflow = TableDetailWorkflow(
        base_config=_base_config(),
        analyze_config=lambda config: (_ for _ in ()).throw(RuntimeError("glue down")),
        cache=cache,
    )

    result = failing_workflow.select_table(table, refresh=True).result(timeout=1)

    assert result.cache_status == "stale"
    assert result.message == "glue down"
    assert result.report.table_name == "sales.orders"
    assert result.table.table_format == TableFormatClassification.ICEBERG


def test_table_detail_refresh_task_exposes_refreshing_cache_state(tmp_path):
    release_analysis = Event()

    def analyze_config(config):
        release_analysis.wait(0.2)
        return _report("sales.orders")

    workflow = TableDetailWorkflow(
        base_config=_base_config(),
        analyze_config=analyze_config,
        cache=OperatorCache(tmp_path / "operator-cache.duckdb"),
    )

    task = workflow.select_table(_table_row("orders"), refresh=True)

    assert task.cache_status == "refreshing"
    release_analysis.set()
    assert task.result(timeout=1).cache_status == "fresh"


class FakeCatalogAccess:
    def __init__(self, namespaces=None, tables=None):
        self.namespaces = namespaces or CatalogNamespaceListing(namespaces=())
        self.tables = tables
        self.requested_table_namespaces = []

    def list_namespaces(self):
        return self.namespaces

    def list_tables(self, namespace):
        self.requested_table_namespaces.append(namespace)
        return self.tables


class FailingCatalogAccess:
    def __init__(self, message):
        self.message = message

    def list_namespaces(self):
        raise RuntimeError(self.message)

    def list_tables(self, namespace):
        raise RuntimeError(self.message)


def _base_config() -> AnalyzerConfiguration:
    return AnalyzerConfiguration(
        table_source=GlueCatalogTableSourceConfiguration(
            catalog_name="analytics",
            namespace=("sales",),
            table_name="__placeholder__",
        )
    )


def _table_row(table_name: str, namespace: str = "sales") -> CatalogTableRow:
    return CatalogTableRow(
        namespace=(namespace,),
        name=table_name,
        identifier=f"{namespace}.{table_name}",
        table_format=TableFormatClassification.UNKNOWN,
        classification_source="glue_parameters",
    )


def _report(table_name: str) -> TableHealthReport:
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
        ),
        display_statistics=(),
    )
