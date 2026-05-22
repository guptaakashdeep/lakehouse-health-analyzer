import asyncio
import json
import time
from datetime import datetime, timezone
from threading import Event

from analysis.report import (
    HealthMetric,
    TableEvolutionHistory,
    TableHealthReport,
    TableSource,
)
from configuration import (
    AnalyzerConfiguration,
    GlueCatalogTableSourceConfiguration,
    OutputPolicy,
)
from operator_tui.app import (
    OperatorCatalogBrowserApp,
    OperatorTuiContext,
    TableAnalysisResult,
    WorkflowCatalogTableAccess,
    configured_operator_catalog_browser_app,
)
from operator_tui.widgets import CatalogTable
from textual.widgets import Input, ListView, Static
from workflows.catalog_browser import (
    CatalogBrowserFailure,
    CatalogBrowserWorkflow,
    CatalogNamespace,
    CatalogNamespaceListing,
    CatalogTableListing,
    CatalogTableRow,
    TableFormatClassification,
)
from workflows.errors import UnsupportedTableError


def test_operator_catalog_browser_shell_renders_before_namespace_loading_finishes():
    access = FakeNamespaceAccess(
        namespaces=(CatalogNamespace(("sales",), "sales"),),
        load_delay=0.2,
    )
    app = OperatorCatalogBrowserApp(
        namespace_workflow=CatalogBrowserWorkflow(access),
        table_access=FakeTableAccess(),
        context=OperatorTuiContext(catalog_name="analytics"),
    )

    async def run_app():
        started_at = time.monotonic()
        async with app.run_test() as pilot:
            assert time.monotonic() - started_at < 0.15
            assert "Loading catalog namespaces" in _text(app, "#detail-panel")
            assert "Catalog analytics" in _text(app, "#catalog-header")
            await pilot.pause(0.25)
            assert _list_labels(app, "#namespace-list") == ["sales"]

    asyncio.run(run_app())


def test_operator_catalog_browser_populates_tables_lazily_sorted_by_namespace():
    namespace_access = FakeNamespaceAccess(
        namespaces=(
            CatalogNamespace(("sales",), "sales"),
            CatalogNamespace(("finance",), "finance"),
        )
    )
    table_access = FakeTableAccess(
        tables={
            ("sales",): (
                CatalogTable(("sales",), "z_events", "sales.z_events"),
                CatalogTable(("sales",), "Orders", "sales.orders", "ICEBERG"),
                CatalogTable(("sales",), "customers", "sales.customers"),
            )
        }
    )
    app = OperatorCatalogBrowserApp(
        namespace_workflow=CatalogBrowserWorkflow(namespace_access),
        table_access=table_access,
        context=OperatorTuiContext(
            catalog_name="analytics",
            profile="dev",
            default_chain="default",
            region="us-east-1",
        ),
    )

    async def run_app():
        async with app.run_test() as pilot:
            await pilot.pause(0.05)
            assert table_access.list_calls == []

            await pilot.press("enter")
            await pilot.pause(0.05)

            assert table_access.list_calls == [(("sales",), False)]
            assert _table_names(app) == ["customers", "Orders", "z_events"]
            assert _table_formats(app) == ["UNKNOWN", "ICEBERG", "UNKNOWN"]
            header = _text(app, "#catalog-header")
            assert "Catalog analytics" in header
            assert "Profile dev" in header
            assert "Chain default" in header
            assert "Region us-east-1" in header
            assert "Namespace sales" in header
            assert "Freshness FRESH" in header
            assert "Enter Analyze" in _text(app, "#operator-footer")

    asyncio.run(run_app())


def test_operator_catalog_browser_filter_applies_to_active_panel_and_escape_clears():
    app = OperatorCatalogBrowserApp(
        namespace_workflow=CatalogBrowserWorkflow(
            FakeNamespaceAccess(namespaces=(CatalogNamespace(("sales",), "sales"),))
        ),
        table_access=FakeTableAccess(
            tables={
                ("sales",): (
                    CatalogTable(("sales",), "Orders", "sales.orders"),
                    CatalogTable(("sales",), "Customers", "sales.customers"),
                    CatalogTable(("sales",), "LineItems", "sales.line_items"),
                )
            }
        ),
    )

    async def run_app():
        async with app.run_test() as pilot:
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)

            await pilot.press("/")
            assert "Esc Clear" in _text(app, "#operator-footer")
            app.query_one("#filter-input", Input).value = "ord"
            await pilot.pause(0.05)
            assert _table_names(app) == ["Orders"]

            await pilot.press("escape")
            await pilot.pause(0.05)
            assert _table_names(app) == ["Customers", "LineItems", "Orders"]
            assert app.query_one("#filter-input", Input).has_class("hidden")

    asyncio.run(run_app())


def test_operator_catalog_browser_highlight_does_not_analyze_until_enter():
    table_access = FakeTableAccess(
        tables={
            ("sales",): (
                CatalogTable(("sales",), "customers", "sales.customers"),
                CatalogTable(("sales",), "orders", "sales.orders"),
            )
        }
    )
    app = OperatorCatalogBrowserApp(
        namespace_workflow=CatalogBrowserWorkflow(
            FakeNamespaceAccess(namespaces=(CatalogNamespace(("sales",), "sales"),))
        ),
        table_access=table_access,
    )

    async def run_app():
        async with app.run_test() as pilot:
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)

            await pilot.press("j")
            await pilot.pause(0.05)
            assert table_access.analyze_calls == []
            assert app.analysis_calls == 0

            await pilot.press("enter")
            await pilot.pause(0.05)
            assert table_access.analyze_calls == [("sales.orders", False)]
            assert app.analysis_calls == 1
            assert "Table Health Report: sales.orders" in _text(app, "#detail-panel")
            assert _table_formats(app) == ["UNKNOWN", "ICEBERG"]

    asyncio.run(run_app())


def test_operator_catalog_browser_pressing_e_offers_export_formats_for_selected_report():
    app = OperatorCatalogBrowserApp(
        namespace_workflow=CatalogBrowserWorkflow(
            FakeNamespaceAccess(namespaces=(CatalogNamespace(("sales",), "sales"),))
        ),
        table_access=FakeTableAccess(
            tables={
                ("sales",): (
                    CatalogTable(("sales",), "orders", "sales.orders"),
                )
            }
        ),
    )

    async def run_app():
        async with app.run_test() as pilot:
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)

            await pilot.press("e")
            await pilot.pause(0.05)

            detail = _text(app, "#detail-panel")
            assert "Export report for sales.orders" in detail
            assert "1 JSON" in detail
            assert "2 Markdown" in detail
            assert "3 Both" in detail

    asyncio.run(run_app())


def test_operator_catalog_browser_export_writes_selected_formats_to_configured_destination(
    tmp_path,
):
    app = OperatorCatalogBrowserApp(
        namespace_workflow=CatalogBrowserWorkflow(
            FakeNamespaceAccess(namespaces=(CatalogNamespace(("sales",), "sales"),))
        ),
        table_access=FakeTableAccess(
            tables={("sales",): (CatalogTable(("sales",), "orders", "sales.orders"),)}
        ),
        output_policy=OutputPolicy(export_directory=str(tmp_path)),
    )

    async def run_app():
        async with app.run_test() as pilot:
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)
            await pilot.press("e")
            await pilot.press("3")
            await pilot.pause(0.05)

            detail = _text(app, "#detail-panel")
            assert "Exported report to:" in detail
            assert str(tmp_path / "sales-orders.json") in detail
            assert str(tmp_path / "sales-orders.md") in detail
            assert (tmp_path / "sales-orders.json").exists()
            assert (tmp_path / "sales-orders.md").exists()

    asyncio.run(run_app())


def test_operator_catalog_browser_export_uses_cached_status_metadata_when_report_is_cached(
    tmp_path,
):
    analyzed_at = datetime(2026, 5, 22, 10, 0, tzinfo=timezone.utc)
    app = OperatorCatalogBrowserApp(
        namespace_workflow=CatalogBrowserWorkflow(
            FakeNamespaceAccess(namespaces=(CatalogNamespace(("sales",), "sales"),))
        ),
        table_access=FakeTableAccess(
            tables={("sales",): (CatalogTable(("sales",), "orders", "sales.orders"),)},
            analyze_results={
                "sales.orders": TableAnalysisResult(
                    report=_table_health_report("sales.orders"),
                    cache_status="cached",
                    analyzed_at=analyzed_at,
                )
            },
        ),
        output_policy=OutputPolicy(export_directory=str(tmp_path)),
    )

    async def run_app():
        async with app.run_test() as pilot:
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)
            await pilot.press("e")
            await pilot.press("1")
            await pilot.pause(0.05)

            payload = json.loads((tmp_path / "sales-orders.json").read_text())
            assert payload["export_metadata"]["cache_status"] == "cached"
            assert payload["export_metadata"]["analyzed_at"] == analyzed_at.isoformat()

    asyncio.run(run_app())


def test_operator_catalog_browser_export_defaults_destination_to_cwd_when_not_configured(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    app = OperatorCatalogBrowserApp(
        namespace_workflow=CatalogBrowserWorkflow(
            FakeNamespaceAccess(namespaces=(CatalogNamespace(("sales",), "sales"),))
        ),
        table_access=FakeTableAccess(
            tables={("sales",): (CatalogTable(("sales",), "orders", "sales.orders"),)}
        ),
        output_policy=OutputPolicy(),
    )

    async def run_app():
        async with app.run_test() as pilot:
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)
            await pilot.press("e")
            await pilot.press("1")
            await pilot.pause(0.05)
            assert (tmp_path / "sales-orders.json").exists()

    asyncio.run(run_app())


def test_operator_catalog_browser_export_failure_is_concise_and_keeps_tui_usable(
    tmp_path,
):
    app = OperatorCatalogBrowserApp(
        namespace_workflow=CatalogBrowserWorkflow(
            FakeNamespaceAccess(namespaces=(CatalogNamespace(("sales",), "sales"),))
        ),
        table_access=FakeTableAccess(
            tables={("sales",): (CatalogTable(("sales",), "orders", "sales.orders"),)}
        ),
        output_policy=OutputPolicy(export_directory=str(tmp_path / "missing")),
    )

    async def run_app():
        async with app.run_test() as pilot:
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)
            await pilot.press("e")
            await pilot.press("1")
            await pilot.pause(0.05)

            detail = _text(app, "#detail-panel")
            assert detail.startswith("Export failed:")
            assert "does not exist" in detail

            await pilot.press("j")
            await pilot.pause(0.05)
            assert app.analysis_calls == 1

    asyncio.run(run_app())


def test_operator_catalog_browser_unsupported_analysis_promotes_non_iceberg():
    table_access = FakeTableAccess(
        tables={
            ("sales",): (
                CatalogTable(("sales",), "legacy_view", "sales.legacy_view"),
            )
        },
        analyze_results={
            "sales.legacy_view": UnsupportedTableError("table is not an iceberg table")
        },
    )
    app = OperatorCatalogBrowserApp(
        namespace_workflow=CatalogBrowserWorkflow(
            FakeNamespaceAccess(namespaces=(CatalogNamespace(("sales",), "sales"),))
        ),
        table_access=table_access,
    )

    async def run_app():
        async with app.run_test() as pilot:
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)

            await pilot.press("enter")
            await pilot.pause(0.05)
            assert app.analysis_calls == 1
            assert _table_formats(app) == ["NON-ICEBERG"]
            assert "NON-ICEBERG" in _text(app, "#detail-panel")

    asyncio.run(run_app())


def test_operator_catalog_browser_analysis_failure_stays_recoverable_and_ui_usable():
    table_access = FakeTableAccess(
        tables={
            ("sales",): (
                CatalogTable(("sales",), "customers", "sales.customers"),
                CatalogTable(("sales",), "orders", "sales.orders"),
            )
        },
        analyze_results={"sales.orders": RuntimeError("catalog temporarily unavailable")},
    )
    app = OperatorCatalogBrowserApp(
        namespace_workflow=CatalogBrowserWorkflow(
            FakeNamespaceAccess(namespaces=(CatalogNamespace(("sales",), "sales"),))
        ),
        table_access=table_access,
    )

    async def run_app():
        async with app.run_test() as pilot:
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)

            await pilot.press("j")
            await pilot.press("enter")
            await pilot.pause(0.05)

            assert "Unable to analyze sales.orders" in _text(app, "#detail-panel")
            assert "catalog temporarily unavailable" in _text(app, "#detail-panel")
            assert _table_formats(app) == ["UNKNOWN", "UNKNOWN"]

            await pilot.press("k")
            await pilot.press("enter")
            await pilot.pause(0.05)
            assert "Table Health Report: sales.customers" in _text(app, "#detail-panel")
            assert _table_formats(app) == ["ICEBERG", "UNKNOWN"]

    asyncio.run(run_app())


def test_operator_catalog_browser_cached_non_iceberg_selection_shows_unsupported_state():
    table_access = FakeTableAccess(
        tables={
            ("sales",): (
                CatalogTable(
                    ("sales",),
                    "legacy_view",
                    "sales.legacy_view",
                    table_format="NON-ICEBERG",
                ),
            )
        }
    )
    app = OperatorCatalogBrowserApp(
        namespace_workflow=CatalogBrowserWorkflow(
            FakeNamespaceAccess(namespaces=(CatalogNamespace(("sales",), "sales"),))
        ),
        table_access=table_access,
    )

    async def run_app():
        async with app.run_test() as pilot:
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)

            await pilot.press("enter")
            await pilot.pause(0.05)
            detail = _text(app, "#detail-panel")
            assert "NON-ICEBERG" in detail
            assert "Left" in detail
            assert "r" in detail
            assert app.analysis_calls == 0
            assert table_access.analyze_calls == []

    asyncio.run(run_app())


def test_operator_catalog_browser_setup_needed_state_does_not_call_catalog():
    namespace_access = FakeNamespaceAccess(
        namespaces=(CatalogNamespace(("sales",), "sales"),)
    )
    app = OperatorCatalogBrowserApp(
        namespace_workflow=CatalogBrowserWorkflow(namespace_access),
        table_access=FakeTableAccess(),
        setup_needed_message="Setup is required before browsing the catalog.",
    )

    async def run_app():
        async with app.run_test() as pilot:
            await pilot.pause()
            assert namespace_access.list_calls == 0
            assert "Setup is required" in _text(app, "#detail-panel")
            assert "Freshness SETUP NEEDED" in _text(app, "#catalog-header")

    asyncio.run(run_app())


def test_operator_catalog_browser_surfaces_namespace_warnings_without_blocking_use():
    namespace_access = FakeNamespaceAccess(
        namespaces=(CatalogNamespace(("sales",), "sales"),),
        failures=(CatalogBrowserFailure(scope="namespaces", message="glue throttled"),),
    )
    table_access = FakeTableAccess(
        tables={
            ("sales",): (
                CatalogTable(("sales",), "orders", "sales.orders"),
            )
        }
    )
    app = OperatorCatalogBrowserApp(
        namespace_workflow=CatalogBrowserWorkflow(namespace_access),
        table_access=table_access,
    )

    async def run_app():
        async with app.run_test() as pilot:
            await pilot.pause(0.05)
            assert _list_labels(app, "#namespace-list") == ["sales"]
            assert "Catalog warnings" in _text(app, "#detail-panel")
            assert "glue throttled" in _text(app, "#detail-panel")

            await pilot.press("enter")
            await pilot.pause(0.05)
            assert _table_names(app) == ["orders"]

    asyncio.run(run_app())


def test_operator_catalog_browser_surfaces_table_warnings_without_dropping_rows():
    namespace_access = FakeNamespaceAccess(
        namespaces=(CatalogNamespace(("sales",), "sales"),),
    )
    table_access = FakeTableAccess(
        tables={
            ("sales",): CatalogTableListing(
                namespace=("sales",),
                rows=(
                    CatalogTableRow(
                        namespace=("sales",),
                        name="orders",
                        identifier="sales.orders",
                        table_format=TableFormatClassification.UNKNOWN,
                        classification_source="glue_parameters",
                    ),
                ),
                failures=(
                    CatalogBrowserFailure(scope="sales", message="glue throttled"),
                ),
            )
        }
    )
    app = OperatorCatalogBrowserApp(
        namespace_workflow=CatalogBrowserWorkflow(namespace_access),
        table_access=table_access,
    )

    async def run_app():
        async with app.run_test() as pilot:
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)

            assert _table_names(app) == ["orders"]
            assert "Table warnings" in _text(app, "#detail-panel")
            assert "glue throttled" in _text(app, "#detail-panel")

            await pilot.press("enter")
            await pilot.pause(0.05)
            assert app.analysis_calls == 1

    asyncio.run(run_app())


def test_operator_catalog_browser_refreshes_active_table_listing():
    table_access = FakeTableAccess(
        tables={("sales",): (CatalogTable(("sales",), "orders", "sales.orders"),)}
    )
    app = OperatorCatalogBrowserApp(
        namespace_workflow=CatalogBrowserWorkflow(
            FakeNamespaceAccess(namespaces=(CatalogNamespace(("sales",), "sales"),))
        ),
        table_access=table_access,
    )

    async def run_app():
        async with app.run_test() as pilot:
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)
            await pilot.press("r")
            await pilot.pause(0.05)

            assert table_access.list_calls == [
                (("sales",), False),
                (("sales",), True),
            ]

    asyncio.run(run_app())


def test_operator_catalog_browser_refreshes_visible_table_detail():
    table_access = FakeTableAccess(
        tables={("sales",): (CatalogTable(("sales",), "orders", "sales.orders"),)}
    )
    app = OperatorCatalogBrowserApp(
        namespace_workflow=CatalogBrowserWorkflow(
            FakeNamespaceAccess(namespaces=(CatalogNamespace(("sales",), "sales"),))
        ),
        table_access=table_access,
    )

    async def run_app():
        async with app.run_test() as pilot:
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)
            await pilot.press("r")
            await pilot.pause(0.05)

            assert table_access.analyze_calls == [
                ("sales.orders", False),
                ("sales.orders", True),
            ]

    asyncio.run(run_app())


def test_operator_catalog_browser_ignores_stale_analysis_after_selection_changes():
    started = Event()
    release = Event()
    table_access = FakeTableAccess(
        tables={
            ("sales",): (
                CatalogTable(("sales",), "slow", "sales.slow"),
                CatalogTable(("sales",), "current", "sales.current"),
            )
        },
        analyze_results={"sales.slow": (started, release)},
    )
    app = OperatorCatalogBrowserApp(
        namespace_workflow=CatalogBrowserWorkflow(
            FakeNamespaceAccess(namespaces=(CatalogNamespace(("sales",), "sales"),))
        ),
        table_access=table_access,
    )

    async def run_app():
        async with app.run_test() as pilot:
            await pilot.pause(0.05)
            await pilot.press("enter")
            await pilot.pause(0.05)

            tables_by_name = {
                table.name: table for table in app.tables_by_namespace[("sales",)]
            }
            slow = tables_by_name["slow"]
            current = tables_by_name["current"]
            app.selected_table = slow
            analysis = asyncio.create_task(app._analyze_selected_table(slow))
            await asyncio.to_thread(started.wait, 1)
            app.selected_table = current
            app.query_one("#detail-panel", Static).update("Current selection")

            release.set()
            await analysis

            assert app.selected_table.identifier == "sales.current"
            assert "Current selection" in _text(app, "#detail-panel")
            assert _table_formats(app) == ["UNKNOWN", "ICEBERG"]

    asyncio.run(run_app())


def test_configured_operator_catalog_browser_app_wires_cache_backed_workflows(
    monkeypatch, tmp_path
):
    config = AnalyzerConfiguration(
        table_source=GlueCatalogTableSourceConfiguration(
            catalog_name="analytics",
            namespace=("placeholder",),
            table_name="__placeholder__",
            aws_profile="dev",
            region="us-east-1",
        )
    )
    cache = object()
    browser_workflow = CatalogBrowserWorkflow(
        FakeNamespaceAccess(namespaces=(CatalogNamespace(("sales",), "sales"),))
    )
    created = {}

    class FakeDetailWorkflow:
        def __init__(self, base_config, cache=None, **kwargs):
            created["detail_config"] = base_config
            created["detail_cache"] = cache

    def fake_configured_catalog_browser_workflow(
        configured, *, cache=None, cache_scope_key
    ):
        created["browser_config"] = configured
        created["browser_cache"] = cache
        created["browser_scope"] = cache_scope_key
        return browser_workflow

    monkeypatch.setattr("operator_tui.app.OperatorCache", lambda path, ttl_seconds: cache)
    monkeypatch.setattr(
        "operator_tui.app.default_catalog_overview_cache_path",
        lambda: tmp_path / "operator-cache.duckdb",
    )
    monkeypatch.setattr(
        "operator_tui.app.configured_catalog_browser_workflow",
        fake_configured_catalog_browser_workflow,
    )
    monkeypatch.setattr("operator_tui.app.TableDetailWorkflow", FakeDetailWorkflow)

    app = configured_operator_catalog_browser_app(config)

    assert app.namespace_workflow is browser_workflow
    assert isinstance(app.table_access, WorkflowCatalogTableAccess)
    assert app.table_access.catalog_workflow is browser_workflow
    assert created == {
        "browser_config": config,
        "browser_cache": cache,
        "browser_scope": "analytics|dev|us-east-1",
        "detail_config": config,
        "detail_cache": cache,
    }


class FakeNamespaceAccess:
    def __init__(self, namespaces, failures=(), load_delay=0):
        self.namespaces = namespaces
        self.failures = failures
        self.load_delay = load_delay
        self.list_calls = 0

    def list_namespaces(self):
        self.list_calls += 1
        if self.load_delay:
            time.sleep(self.load_delay)
        return CatalogNamespaceListing(
            namespaces=self.namespaces,
            failures=self.failures,
        )


class FakeTableAccess:
    def __init__(self, tables=None, analyze_results=None):
        self.tables = tables or {}
        self.analyze_results = analyze_results or {}
        self.list_calls = []
        self.analyze_calls = []

    def list_tables(self, namespace, *, refresh=False):
        self.list_calls.append((namespace, refresh))
        return self.tables.get(namespace, ())

    def analyze_table(self, table_identifier, *, refresh=False):
        self.analyze_calls.append((table_identifier, refresh))
        if table_identifier in self.analyze_results:
            result = self.analyze_results[table_identifier]
            if isinstance(result, tuple) and len(result) == 2:
                started, release = result
                started.set()
                release.wait(1)
                return _table_health_report(table_identifier)
            if isinstance(result, Exception):
                raise result
            return result
        return _table_health_report(table_identifier)


def _text(app, selector) -> str:
    return str(app.query_one(selector, Static).content)


def _list_labels(app, selector) -> list[str]:
    return [
        getattr(item, "display_name", getattr(getattr(item, "table", None), "name", ""))
        for item in app.query_one(selector, ListView).children
    ]


def _table_names(app) -> list[str]:
    return [item.table.name for item in app.query_one("#table-list", ListView).children]


def _table_formats(app) -> list[str]:
    return [
        item.table.table_format
        for item in app.query_one("#table-list", ListView).children
    ]


def _table_health_report(table_identifier: str) -> TableHealthReport:
    return TableHealthReport(
        table_name=table_identifier,
        table_source=TableSource(kind="glue_catalog_table", location=table_identifier),
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
        table_evolution_history=TableEvolutionHistory(),
    )
