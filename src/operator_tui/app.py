from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Protocol, Sequence

from textual import events
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.widgets import Input, ListView, Static

from analysis.report import TableHealthReport
from configuration import (
    AnalyzerConfiguration,
    GlueCatalogTableSourceConfiguration,
    OutputPolicy,
)
from operator_cache import OperatorCache, default_catalog_overview_cache_path
from workflows.catalog_browser import (
    CatalogBrowserWorkflow,
    CatalogNamespace,
    CatalogTableListing,
    CatalogTableRow,
    TableFormatClassification,
    configured_catalog_browser_workflow,
)
from workflows.errors import UnsupportedTableError
from workflows.table_detail import TableDetailWorkflow

from . import _analyze_iceberg_table, _glue_catalog_properties
from .export import TableReportExportWorkflow
from .rendering import (
    render_detail_state,
    render_export_failure,
    render_export_offer,
    render_export_success,
    render_header_chrome,
    render_report_detail,
    render_unsupported_detail,
    render_warning_state,
)
from .theme import LayoutState, layout_state_for_size, semantic_status
from .widgets import CatalogTable, NamespaceListItem, TableListItem


@dataclass(frozen=True)
class OperatorTuiContext:
    catalog_name: str = "unconfigured"
    profile: str | None = None
    default_chain: str = "default"
    region: str | None = None


class CatalogTableAccess(Protocol):
    def list_tables(
        self, namespace: tuple[str, ...], *, refresh: bool = False
    ) -> Sequence[CatalogTable | CatalogTableRow | str] | CatalogTableListing: ...

    def analyze_table(
        self, table_identifier: str, *, refresh: bool = False
    ) -> TableHealthReport | "TableAnalysisResult": ...


@dataclass(frozen=True)
class TableAnalysisResult:
    report: TableHealthReport
    cache_status: str = "fresh"
    analyzed_at: datetime | None = None


class OperatorCatalogBrowserApp(App[None]):
    CSS = """
    Screen {
        background: #101316;
        color: #e7eff2;
    }

    #catalog-header {
        height: 4;
        padding: 0 1;
        margin: 1 1 0 1;
        background: #151a1e;
        border: round #2c3940;
        color: #e7eff2;
    }

    #operator-toolbar {
        height: auto;
        min-height: 3;
        margin: 1 1 0 1;
        background: #101316;
    }

    #filter-input {
        height: 3;
        width: 1fr;
        margin-right: 0;
        background: #151b20;
        border: round #2c3940;
        color: #e7eff2;
    }

    #filter-input:focus {
        border: round #42d9d1;
    }

    .toolbar-button {
        height: 3;
        width: auto;
        min-width: 8;
        margin-left: 1;
        padding: 0 1;
        content-align: center middle;
        background: #101316;
        border: none;
        color: #aab8bd;
    }

    .toolbar-button.active {
        color: #42d9d1;
        text-style: bold underline;
    }

    .hidden {
        display: none;
    }

    #workspace {
        layout: horizontal;
        height: 1fr;
        min-height: 0;
        margin: 1 1 0 1;
    }

    #catalog-column {
        width: 34%;
        height: 1fr;
        min-height: 0;
        margin-right: 1;
    }

    .browser-panel {
        height: 1fr;
        min-height: 0;
        border: round #2c3940;
        background: #171d22;
    }

    .browser-panel.active {
        border: round #42d9d1;
    }

    #namespace-panel {
        width: 100%;
        height: 32%;
        margin-bottom: 1;
    }

    #table-panel {
        width: 100%;
        height: 1fr;
    }

    #detail-column {
        width: 1fr;
        min-height: 0;
    }

    .panel-title {
        height: 2;
        padding: 0 1;
        background: #1c242a;
        content-align: left middle;
        text-style: bold;
        color: #e7eff2;
    }

    .panel-head {
        height: 2;
        background: #1c242a;
        border-bottom: solid #2c3940;
    }

    .panel-title {
        width: 1fr;
        text-style: bold;
    }

    .panel-count {
        width: auto;
        min-width: 9;
        padding: 0 1;
        color: #74868e;
        content-align: right middle;
        text-align: right;
    }

    ListView {
        height: 1fr;
        background: #171d22;
        padding: 0 1;
    }

    ListItem {
        height: 1;
        background: #171d22;
        color: #aab8bd;
    }

    ListView > ListItem.--highlight {
        background: #20282d;
        color: #e7eff2;
    }

    ListView > ListItem.--highlight .selection-rail {
        color: #42d9d1;
        text-style: bold;
    }

    ListView > ListItem.--highlight .namespace-name,
    ListView > ListItem.--highlight .table-name {
        color: #e7eff2;
        text-style: bold;
    }

    .table-row {
        height: 1;
    }

    .namespace-row {
        height: 1;
    }

    .selection-rail {
        width: 1;
        height: 1;
        color: #171d22;
        content-align: center middle;
    }

    .namespace-name {
        width: 1fr;
        height: 1;
        padding: 0 1;
        content-align: left middle;
        overflow: hidden;
        text-overflow: ellipsis;
        text-wrap: nowrap;
    }

    .row-meta {
        width: auto;
        min-width: 9;
        padding: 0 1;
        color: #74868e;
        content-align: right middle;
        text-align: right;
    }

    .table-name {
        width: 1fr;
        height: 1;
        padding: 0 1;
        content-align: left middle;
        overflow: hidden;
        text-overflow: ellipsis;
        text-wrap: nowrap;
    }

    .table-format {
        width: 9;
        padding: 0 1;
        content-align: right middle;
        text-align: right;
        color: #74868e;
    }

    .iceberg {
        color: #72dc8f;
    }

    .non-iceberg {
        color: #ff6b7d;
    }

    .semantic-critical {
        color: #ff6b7d;
        text-style: bold;
    }

    .semantic-warning {
        color: #f3c85f;
        text-style: bold;
    }

    .semantic-info {
        color: #74a7ff;
        text-style: bold;
    }

    .semantic-healthy {
        color: #72dc8f;
        text-style: bold;
    }

    .semantic-unknown {
        color: #b7c3c8;
    }

    .semantic-cached {
        color: #9fb7c3;
        text-style: bold;
    }

    .semantic-stale {
        color: #f3c85f;
        text-style: bold;
    }

    .table-cache {
        width: 7;
        padding: 0 1;
        content-align: right middle;
        text-align: right;
    }

    .empty-state {
        height: 1fr;
        padding: 1 2;
        content-align: center middle;
        text-align: center;
        color: #74868e;
        background: #171d22;
    }

    #detail-scroll {
        height: 1fr;
        min-height: 0;
        background: #171d22;
        scrollbar-color: #47606a;
        scrollbar-background: #151b20;
    }

    #detail-panel {
        height: auto;
        min-height: 0;
        padding: 1 2;
        background: #171d22;
    }

    #operator-footer {
        height: 3;
        min-height: 3;
        padding: 0 2;
        margin: 1 1;
        background: #151b20;
        border: round #2c3940;
        color: #aab8bd;
    }

    #minimum-size-message {
        height: 1fr;
        padding: 1 2;
        content-align: center middle;
        text-align: center;
        background: #171d22;
        border: solid #f3c85f;
        color: #f3c85f;
    }

    #workspace.layout-compact {
        layout: vertical;
        overflow-y: auto;
    }

    #workspace.layout-compact .browser-panel {
        width: 100%;
    }

    #workspace.layout-compact #catalog-column {
        width: 100%;
        height: 13;
        margin-right: 0;
        margin-bottom: 1;
    }

    #workspace.layout-compact #namespace-panel {
        height: 5;
    }

    #workspace.layout-compact #table-panel {
        height: 7;
    }

    #workspace.layout-wide #namespace-panel {
        height: 30%;
    }

    #workspace.layout-compact #detail-column {
        height: 1fr;
        min-height: 5;
    }

    #operator-toolbar.layout-compact #filter-databases-action,
    #operator-toolbar.layout-compact #filter-tables-action,
    #operator-toolbar.layout-compact #setup-action {
        display: none;
    }

    #workspace.layout-minimum {
        display: none;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("/", "filter", "Filter"),
        Binding("escape", "clear_filter", "Clear"),
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
        Binding("down", "cursor_down", "Down", show=False),
        Binding("up", "cursor_up", "Up", show=False),
        Binding("left", "focus_namespaces", "Namespaces", show=False),
        Binding("right", "focus_tables", "Tables", show=False),
        Binding("enter", "select", "Select"),
        Binding("r", "refresh", "Refresh"),
        Binding("e", "export", "Export"),
        Binding("1", "export_json", "JSON", show=False),
        Binding("2", "export_markdown", "Markdown", show=False),
        Binding("3", "export_both", "Both", show=False),
        Binding("s", "setup", "Setup"),
        Binding("?", "help", "Help"),
    ]

    def __init__(
        self,
        *,
        namespace_workflow: CatalogBrowserWorkflow | None = None,
        table_access: CatalogTableAccess | None = None,
        context: OperatorTuiContext | None = None,
        output_policy: OutputPolicy | None = None,
        setup_needed_message: str | None = None,
    ) -> None:
        super().__init__()
        self.namespace_workflow = namespace_workflow
        self.table_access = table_access
        self.context = context or OperatorTuiContext()
        self.output_policy = output_policy or OutputPolicy()
        self.export_workflow = TableReportExportWorkflow(
            output_policy=self.output_policy
        )
        self.setup_needed_message = setup_needed_message
        self.active_panel = "namespaces"
        self.filter_panel: str | None = None
        self.namespace_filter = ""
        self.table_filter = ""
        self.namespaces: tuple[CatalogNamespace, ...] = ()
        self.tables_by_namespace: dict[tuple[str, ...], tuple[CatalogTable, ...]] = {}
        self.selected_namespace: tuple[str, ...] | None = None
        self.selected_table: CatalogTable | None = None
        self.selected_report: TableHealthReport | None = None
        self.selected_report_result: TableAnalysisResult | None = None
        self.export_prompt_active = False
        self.freshness = "loading"
        self.loading_namespaces = False
        self.loading_tables = False
        self.analysis_calls = 0
        self.layout_state: LayoutState = layout_state_for_size(80, 24)

    def compose(self) -> ComposeResult:
        yield Static("", id="catalog-header")
        with Horizontal(id="operator-toolbar"):
            yield Input(
                placeholder="Filter active panel: databases",
                id="filter-input",
            )
            yield Static(
                "/ Filter Databases",
                id="filter-databases-action",
                classes="toolbar-button active",
            )
            yield Static(
                "/ Filter Tables",
                id="filter-tables-action",
                classes="toolbar-button",
            )
            yield Static("r Refresh", id="refresh-action", classes="toolbar-button")
            yield Static("e Export", id="export-action", classes="toolbar-button")
            yield Static("s Setup", id="setup-action", classes="toolbar-button")
        with Container(id="workspace"):
            with Vertical(id="catalog-column"):
                with Vertical(id="namespace-panel", classes="browser-panel active"):
                    with Horizontal(classes="panel-head"):
                        yield Static(
                            "DATABASES", classes="panel-title", id="namespace-title"
                        )
                        yield Static("", classes="panel-count", id="namespace-count")
                    yield ListView(id="namespace-list")
                    yield Static(
                        "Loading databases...",
                        id="namespace-empty",
                        classes="empty-state",
                    )
                with Vertical(id="table-panel", classes="browser-panel"):
                    with Horizontal(classes="panel-head"):
                        yield Static("TABLES", classes="panel-title", id="table-title")
                        yield Static("", classes="panel-count", id="table-count")
                    yield ListView(id="table-list")
                    yield Static(
                        "Select a database to load tables.",
                        id="table-empty",
                        classes="empty-state",
                    )
            with Vertical(id="detail-column", classes="browser-panel"):
                with Horizontal(classes="panel-head"):
                    yield Static("DETAILS", classes="panel-title", id="detail-title")
                    yield Static("", classes="panel-count", id="detail-count")
                with VerticalScroll(id="detail-scroll"):
                    yield Static("", id="detail-panel")
        yield Static("", id="minimum-size-message", classes="hidden")
        yield Static("", id="operator-footer")

    def on_mount(self) -> None:
        self._apply_layout_state(self.size.width, self.size.height)
        self._refresh_chrome()
        self.query_one("#namespace-list", ListView).focus()
        if self.setup_needed_message:
            self.freshness = "setup needed"
            self.query_one("#detail-panel", Static).update(
                render_detail_state(
                    "Setup required", self.setup_needed_message, status="warning"
                )
            )
            self._refresh_chrome()
            return
        self.query_one("#detail-panel", Static).update(
            render_detail_state(
                "Loading catalog",
                "Loading catalog namespaces in the background...",
                status="info",
            )
        )
        self.set_timer(0.01, self._start_namespace_load)

    def on_resize(self, event: events.Resize) -> None:
        self._apply_layout_state(event.size.width, event.size.height)
        self._refresh_chrome()

    def _start_namespace_load(self) -> None:
        asyncio.create_task(self._load_namespaces())

    async def _render_namespaces(self) -> None:
        list_view = self.query_one("#namespace-list", ListView)
        await list_view.clear()
        namespaces = self._current_namespaces()
        for namespace in namespaces:
            table_count = self.tables_by_namespace.get(namespace.name)
            meta = f"{len(table_count)} tables" if table_count is not None else ""
            await list_view.append(
                NamespaceListItem(namespace.name, namespace.display_name, meta=meta)
            )
        if namespaces:
            list_view.index = 0
        self._refresh_chrome()

    async def _load_namespaces(self, *, refresh: bool = False) -> None:
        self.freshness = "loading"
        self.loading_namespaces = True
        self._refresh_chrome()
        try:
            if self.namespace_workflow is None:
                raise RuntimeError("Catalog browser workflow is not configured.")
            listing = await asyncio.to_thread(
                self.namespace_workflow.list_namespaces, refresh=refresh
            )
            self.namespaces = tuple(listing.namespaces)
            await self._render_namespaces()
            self.freshness = listing.cache_status
            if listing.failures:
                self.query_one("#detail-panel", Static).update(
                    render_warning_state(
                        "Catalog warnings",
                        (
                            _scope_message(failure.scope, failure.message)
                            for failure in listing.failures
                        ),
                    )
                )
            elif self.namespaces:
                self.query_one("#detail-panel", Static).update(
                    render_detail_state(
                        "Databases loaded",
                        "Choose a database to load tables.",
                        status="healthy",
                    )
                )
            else:
                self.query_one("#detail-panel", Static).update(
                    render_detail_state(
                        "No databases",
                        "No catalog namespaces were found.",
                        status="unknown",
                    )
                )
        except Exception as exc:
            self.freshness = "error"
            self.query_one("#detail-panel", Static).update(
                render_detail_state(
                    "Unable to list namespaces", str(exc), status="critical"
                )
            )
        self.loading_namespaces = False
        self._refresh_chrome()

    async def _load_tables_for_selected_namespace(
        self, *, refresh: bool = False
    ) -> None:
        if self.selected_namespace is None:
            return
        namespace_label = ".".join(self.selected_namespace)
        self.freshness = "loading"
        self.loading_tables = True
        self._refresh_empty_states()
        self.query_one("#detail-panel", Static).update(
            render_detail_state(
                "Loading tables",
                f"Loading tables for {namespace_label}...",
                status="info",
            )
        )
        self._refresh_chrome()
        try:
            if self.table_access is None:
                raise RuntimeError("Catalog table access is not configured.")
            raw_listing = await asyncio.to_thread(
                self.table_access.list_tables,
                self.selected_namespace,
                refresh=refresh,
            )
            listing = _catalog_table_listing(self.selected_namespace, raw_listing)
            tables = tuple(
                sorted(
                    (
                        _catalog_table(self.selected_namespace, raw_table)
                        for raw_table in listing.rows
                    ),
                    key=lambda table: table.name.lower(),
                )
            )
            self.tables_by_namespace[self.selected_namespace] = tables
            self.freshness = listing.cache_status
            await self._render_tables()
            if listing.failures:
                self.query_one("#detail-panel", Static).update(
                    render_warning_state(
                        "Table warnings",
                        (
                            _scope_message(failure.scope, failure.message)
                            for failure in listing.failures
                        ),
                    )
                )
            else:
                self.query_one("#detail-panel", Static).update(
                    render_detail_state(
                        "Tables loaded",
                        f"{len(tables)} tables loaded for {namespace_label}.",
                        status="healthy",
                    )
                )
        except Exception as exc:
            self.freshness = "error"
            self.query_one("#detail-panel", Static).update(
                render_detail_state(
                    f"Unable to list tables for {namespace_label}",
                    str(exc),
                    status="critical",
                )
            )
        self.loading_tables = False
        self._refresh_chrome()

    async def _render_tables(self) -> None:
        list_view = self.query_one("#table-list", ListView)
        await list_view.clear()
        tables = self._current_tables()
        for table in tables:
            await list_view.append(TableListItem(table))
        if tables:
            list_view.index = 0
        self._refresh_chrome()

    def _current_tables(self) -> tuple[CatalogTable, ...]:
        if self.selected_namespace is None:
            return ()
        return tuple(
            table
            for table in self.tables_by_namespace.get(self.selected_namespace, ())
            if _contains(table.name, self.table_filter)
        )

    def _current_namespaces(self) -> tuple[CatalogNamespace, ...]:
        return tuple(
            namespace
            for namespace in self.namespaces
            if _contains(namespace.display_name, self.namespace_filter)
        )

    async def action_select(self) -> None:
        if self.active_panel == "namespaces":
            item = self.query_one("#namespace-list", ListView).highlighted_child
            if not isinstance(item, NamespaceListItem):
                return
            await self._select_namespace(item)
            return

        if self.active_panel == "tables":
            item = self.query_one("#table-list", ListView).highlighted_child
            if not isinstance(item, TableListItem):
                return
            await self._select_table(item)

    async def on_list_view_selected(self, event: ListView.Selected) -> None:
        if isinstance(event.item, NamespaceListItem):
            await self._select_namespace(event.item)
            return
        if isinstance(event.item, TableListItem):
            await self._select_table(event.item)

    async def _select_namespace(self, item: NamespaceListItem) -> None:
        self.selected_namespace = item.namespace
        self.active_panel = "tables"
        self.table_filter = ""
        self._refresh_chrome()
        await self._load_tables_for_selected_namespace()
        self.query_one("#table-list", ListView).focus()

    async def _select_table(self, item: TableListItem) -> None:
        self.active_panel = "tables"
        self.selected_table = item.table
        self.export_prompt_active = False
        if item.table.table_format == "NON-ICEBERG":
            self.query_one("#detail-panel", Static).update(
                render_unsupported_detail(item.table.identifier)
            )
            self._refresh_chrome()
            return
        await self._analyze_selected_table(item.table)

    async def _analyze_selected_table(
        self, table: CatalogTable, *, refresh: bool = False
    ) -> None:
        self.analysis_calls += 1
        self.freshness = "loading"
        self.query_one("#detail-panel", Static).update(
            render_detail_state(
                "Analyzing table",
                f"Analyzing {table.identifier}...",
                status="info",
            )
        )
        self._refresh_chrome()
        try:
            if self.table_access is None:
                raise RuntimeError("Catalog table access is not configured.")
            raw_result = await asyncio.to_thread(
                self.table_access.analyze_table, table.identifier, refresh=refresh
            )
            analysis_result = _table_analysis_result(raw_result)
            report = analysis_result.report
            updated = replace(
                table,
                table_format="ICEBERG",
                freshness=analysis_result.cache_status,
            )
            self._replace_table(updated)
            self.freshness = "fresh"
            await self._render_tables()
            if self._selected_table_identifier() == table.identifier:
                self.selected_table = updated
                self.selected_report = report
                self.selected_report_result = analysis_result
                self.export_prompt_active = False
                self.query_one("#detail-panel", Static).update(
                    render_report_detail(
                        report,
                        cache_status=analysis_result.cache_status,
                    )
                )
        except UnsupportedTableError as exc:
            updated = replace(table, table_format="NON-ICEBERG", freshness="fresh")
            self._replace_table(updated)
            self.freshness = "fresh"
            await self._render_tables()
            if self._selected_table_identifier() == table.identifier:
                self.selected_table = updated
                self.selected_report = None
                self.selected_report_result = None
                self.export_prompt_active = False
                self.query_one("#detail-panel", Static).update(
                    render_unsupported_detail(table.identifier, message=str(exc))
                )
        except Exception as exc:
            self.freshness = "fresh"
            if self._selected_table_identifier() == table.identifier:
                self.selected_report = None
                self.selected_report_result = None
                self.export_prompt_active = False
                self.query_one("#detail-panel", Static).update(
                    render_detail_state(
                        f"Unable to analyze {table.identifier}",
                        str(exc),
                        status="critical",
                    )
                )
        self._refresh_chrome()

    def _replace_table(self, updated: CatalogTable) -> None:
        if self.selected_namespace is None:
            return
        tables = self.tables_by_namespace.get(self.selected_namespace, ())
        self.tables_by_namespace[self.selected_namespace] = tuple(
            updated if table.identifier == updated.identifier else table
            for table in tables
        )

    def action_filter(self) -> None:
        self.filter_panel = self.active_panel
        filter_input = self.query_one("#filter-input", Input)
        filter_input.value = (
            self.namespace_filter
            if self.filter_panel == "namespaces"
            else self.table_filter
        )
        filter_input.focus()
        self._refresh_chrome()

    async def action_clear_filter(self) -> None:
        if self.export_prompt_active:
            self.export_prompt_active = False
            if (
                self.selected_report is not None
                and self.selected_report_result is not None
            ):
                self.query_one("#detail-panel", Static).update(
                    render_report_detail(
                        self.selected_report,
                        cache_status=self.selected_report_result.cache_status,
                    )
                )
            self._refresh_chrome()
            return
        filter_input = self.query_one("#filter-input", Input)
        filter_input.value = ""
        if self.filter_panel == "tables" or self.active_panel == "tables":
            self.table_filter = ""
            await self._render_tables()
        else:
            self.namespace_filter = ""
            await self._render_namespaces()
        self.filter_panel = None
        self._focus_active_list()
        self._refresh_chrome()

    async def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id != "filter-input":
            return
        if self.filter_panel == "tables":
            self.table_filter = event.value
            await self._render_tables()
        else:
            self.namespace_filter = event.value
            await self._render_namespaces()
        self._refresh_chrome()

    def action_cursor_down(self) -> None:
        self._active_list().action_cursor_down()

    def action_cursor_up(self) -> None:
        self._active_list().action_cursor_up()

    def action_focus_namespaces(self) -> None:
        self.active_panel = "namespaces"
        self._focus_active_list()
        self._refresh_chrome()

    def action_focus_tables(self) -> None:
        self.active_panel = "tables"
        self._focus_active_list()
        self._refresh_chrome()

    async def action_refresh(self) -> None:
        if self.active_panel == "namespaces":
            await self._load_namespaces(refresh=True)
        elif self.selected_table is not None and (
            self.selected_report_result is not None
            or self.selected_table.table_format == "NON-ICEBERG"
        ):
            await self._analyze_selected_table(self.selected_table, refresh=True)
        elif self.selected_namespace is not None:
            await self._load_tables_for_selected_namespace(refresh=True)

    def action_export(self) -> None:
        if self.selected_report_result is not None:
            self.export_prompt_active = True
            self.query_one("#detail-panel", Static).update(
                render_export_offer(
                    self.export_workflow.export_offer(
                        table_name=self.selected_report_result.report.table_name
                    )
                )
            )
            self._refresh_chrome()
            return
        self.query_one("#detail-panel", Static).update(
            render_detail_state(
                "Export unavailable",
                "Export is available after a table has been analyzed.",
                status="unknown",
            )
        )

    def action_export_json(self) -> None:
        self._export_selected_report("json")

    def action_export_markdown(self) -> None:
        self._export_selected_report("markdown")

    def action_export_both(self) -> None:
        self._export_selected_report("both")

    def _export_selected_report(self, export_choice: str) -> None:
        if not self.export_prompt_active or self.selected_report_result is None:
            return
        analysis_result = self.selected_report_result
        analyzed_at = analysis_result.analyzed_at or datetime.now(timezone.utc)
        try:
            written_paths = self.export_workflow.export_report(
                analysis_result.report,
                export_choice=export_choice,
                cache_status=analysis_result.cache_status,
                analyzed_at=analyzed_at,
            )
        except Exception as exc:
            self.query_one("#detail-panel", Static).update(
                render_export_failure(str(exc))
            )
            self.export_prompt_active = False
            self._refresh_chrome()
            return
        self.query_one("#detail-panel", Static).update(
            render_export_success(written_paths)
        )
        self.export_prompt_active = False
        self._refresh_chrome()

    def action_setup(self) -> None:
        self.query_one("#detail-panel", Static).update(
            render_detail_state(
                "Setup",
                "Open setup to configure catalog access.",
                status="info",
            )
        )

    def action_help(self) -> None:
        self.query_one("#detail-panel", Static).update(
            render_detail_state(
                "Keyboard help",
                (
                    "↑/↓ or j/k move within the active panel.",
                    "← focuses databases; → focuses tables.",
                    "Enter loads tables or analyzes the highlighted table.",
                    "/ filters the active panel; Esc clears filters or export prompt.",
                    "r refreshes visible data; e exports an analyzed report.",
                    "q quits.",
                ),
                status="info",
            )
        )

    def _active_list(self) -> ListView:
        if self.active_panel == "tables":
            return self.query_one("#table-list", ListView)
        return self.query_one("#namespace-list", ListView)

    def _focus_active_list(self) -> None:
        self._active_list().focus()

    def _selected_table_identifier(self) -> str | None:
        if self.selected_table is None:
            return None
        return self.selected_table.identifier

    def _refresh_chrome(self) -> None:
        namespace = ".".join(self.selected_namespace or ()) or "none"
        freshness = semantic_status(self.freshness)
        self.query_one("#catalog-header", Static).update(
            render_header_chrome(
                catalog_name=self.context.catalog_name,
                profile=self.context.profile or "default",
                default_chain=self.context.default_chain,
                region=self.context.region or "default",
                namespace=namespace,
                freshness=freshness.label,
            )
        )

        filter_input = self.query_one("#filter-input", Input)
        filter_scope = self.filter_panel or self.active_panel
        filter_input.placeholder = f"Filter active panel: {_panel_label(filter_scope)}"

        self.query_one("#namespace-count", Static).update(
            f"{len(self._current_namespaces())} shown"
        )
        self.query_one("#table-count", Static).update(
            f"{len(self._current_tables())} shown"
        )
        self.query_one("#detail-count", Static).update(self._detail_count_label())
        self._refresh_empty_states()

        self.query_one("#operator-footer", Static).update(self._footer_text())
        for panel_id, panel_name in (
            ("#namespace-panel", "namespaces"),
            ("#table-panel", "tables"),
        ):
            panel = self.query_one(panel_id)
            panel.set_class(self.active_panel == panel_name, "active")
        self.query_one("#filter-databases-action").set_class(
            self.active_panel == "namespaces", "active"
        )
        self.query_one("#filter-tables-action").set_class(
            self.active_panel == "tables", "active"
        )

    def _refresh_empty_states(self) -> None:
        namespaces = self._current_namespaces()
        namespace_list = self.query_one("#namespace-list", ListView)
        namespace_empty = self.query_one("#namespace-empty", Static)
        namespace_empty.update(self._namespace_empty_message(namespaces))
        namespace_list.set_class(not namespaces, "hidden")
        namespace_empty.set_class(bool(namespaces), "hidden")

        tables = self._current_tables()
        table_list = self.query_one("#table-list", ListView)
        table_empty = self.query_one("#table-empty", Static)
        table_empty.update(self._table_empty_message(tables))
        table_list.set_class(not tables, "hidden")
        table_empty.set_class(bool(tables), "hidden")

    def _namespace_empty_message(self, namespaces: tuple[CatalogNamespace, ...]) -> str:
        if self.loading_namespaces:
            return "Loading databases..."
        if self.namespace_filter and not namespaces:
            return f"No databases match '{self.namespace_filter}'."
        return "No databases found."

    def _table_empty_message(self, tables: tuple[CatalogTable, ...]) -> str:
        if self.selected_namespace is None:
            return "Select a database to load tables."
        namespace = ".".join(self.selected_namespace)
        if self.loading_tables:
            return f"Loading tables for {namespace}..."
        if self.table_filter and not tables:
            return f"No tables match '{self.table_filter}'."
        if self.selected_namespace in self.tables_by_namespace:
            return f"No tables found in {namespace}."
        return "Press Enter on a database to load tables."

    def _detail_count_label(self) -> str:
        if self.selected_report_result is not None:
            return semantic_status(self.selected_report_result.cache_status).label
        if self.selected_table is not None:
            return semantic_status(self.selected_table.table_format).label
        return ""

    def _apply_layout_state(self, width: int, height: int) -> None:
        self.layout_state = layout_state_for_size(width, height)
        workspace = self.query_one("#workspace")
        toolbar = self.query_one("#operator-toolbar")
        minimum_message = self.query_one("#minimum-size-message", Static)
        for layout_class in ("layout-wide", "layout-compact", "layout-minimum"):
            workspace.set_class(
                self.layout_state.workspace_class == layout_class, layout_class
            )
        workspace.set_class(not self.layout_state.show_workspace, "hidden")
        toolbar.set_class(not self.layout_state.show_workspace, "hidden")
        toolbar.set_class(self.layout_state.compact_chrome, "layout-compact")
        minimum_message.set_class(self.layout_state.show_workspace, "hidden")
        minimum_message.update(self.layout_state.minimum_message)

    def _footer_text(self) -> str:
        if not self.layout_state.show_workspace:
            return "Resize terminal | q Quit"
        if self.filter_panel:
            if self.layout_state.compact_chrome:
                return "Filter active panel | Esc Clear | Enter Select | q Quit"
            return "Typing filters the active panel | Esc Clear | Enter Select | q Quit"
        if self.export_prompt_active:
            return "1 JSON | 2 Markdown | 3 Both | Esc Cancel | q Quit"
        if self.active_panel == "namespaces":
            if self.layout_state.compact_chrome:
                return "Enter Load | ↑/↓ Move | → Tables | / Filter | ? Help | q Quit"
            return (
                "Enter Load tables | ↑/↓ Move | → Tables | / Filter databases | "
                "r Refresh | s Setup | ? Help | q Quit"
            )
        if self.layout_state.compact_chrome:
            return "Enter Analyze | ↑/↓ Move | ← DBs | / Filter | ? Help | q Quit"
        return (
            "Enter Analyze | ↑/↓ Move | ← Databases | / Filter tables | "
            "r Refresh | e Export | ? Help | q Quit"
        )


def _panel_label(panel: str) -> str:
    if panel == "tables":
        return "tables"
    return "databases"


class ConfiguredCatalogTableAccess:
    def __init__(self, config: AnalyzerConfiguration) -> None:
        table_source = config.table_source
        if not isinstance(table_source, GlueCatalogTableSourceConfiguration):
            raise ValueError("Operator TUI requires a Catalog Table Source.")
        self.config = config
        self.table_source = table_source
        from pyiceberg.catalog import load_catalog

        self.catalog = load_catalog(
            table_source.catalog_name,
            **_glue_catalog_properties(table_source),
        )

    def list_tables(self, namespace: tuple[str, ...]) -> Sequence[CatalogTable]:
        return tuple(
            _catalog_table(namespace, table_identifier)
            for table_identifier in self.catalog.list_tables(namespace)
        )

    def analyze_table(self, table_identifier: str) -> TableHealthReport:
        parts = tuple(part for part in table_identifier.split(".") if part)
        table_source = replace(
            self.table_source,
            namespace=parts[:-1],
            table_name=parts[-1],
        )
        try:
            return _analyze_iceberg_table(
                replace(self.config, table_source=table_source)
            )
        except Exception as exc:
            if _is_unsupported_error(exc):
                raise UnsupportedTableError(_unsupported_error_message(exc)) from exc
            raise


@dataclass(frozen=True)
class WorkflowCatalogTableAccess:
    catalog_workflow: CatalogBrowserWorkflow
    table_detail_workflow: TableDetailWorkflow

    def list_tables(
        self, namespace: tuple[str, ...], *, refresh: bool = False
    ) -> CatalogTableListing:
        return self.catalog_workflow.list_tables(namespace, refresh=refresh)

    def analyze_table(
        self, table_identifier: str, *, refresh: bool = False
    ) -> TableAnalysisResult:
        result = self.table_detail_workflow.select_table(
            _catalog_table_row_for_identifier(table_identifier),
            refresh=refresh,
        ).result()
        if result.analysis_status == "unsupported":
            raise UnsupportedTableError(
                result.message or f"{table_identifier} is not an Iceberg table"
            )
        if result.report is None:
            raise RuntimeError(
                result.message or f"Unable to analyze {table_identifier}"
            )
        return TableAnalysisResult(
            report=result.report,
            cache_status=result.cache_status,
        )


class ConfiguredCatalogBrowserAccess:
    def __init__(self, table_access: ConfiguredCatalogTableAccess) -> None:
        self.table_access = table_access

    def list_namespaces(self):
        from workflows.catalog_browser import CatalogNamespaceListing

        namespaces = tuple(
            _catalog_namespace(namespace)
            for namespace in self.table_access.catalog.list_namespaces()
        )
        return CatalogNamespaceListing(namespaces=namespaces)


def configured_operator_catalog_browser_app(
    config: AnalyzerConfiguration,
) -> OperatorCatalogBrowserApp:
    table_source = config.table_source
    if not isinstance(table_source, GlueCatalogTableSourceConfiguration):
        raise ValueError("Operator TUI requires a Catalog Table Source.")
    cache = OperatorCache(
        default_catalog_overview_cache_path(),
        ttl_seconds=config.runtime.cache_ttl_seconds,
    )
    cache_scope_key = _configured_cache_scope_key(table_source)
    namespace_workflow = configured_catalog_browser_workflow(
        config,
        cache=cache,
        cache_scope_key=cache_scope_key,
    )
    table_access = WorkflowCatalogTableAccess(
        catalog_workflow=namespace_workflow,
        table_detail_workflow=TableDetailWorkflow(
            base_config=config,
            analyze_config=_analyze_config_with_unsupported_mapping,
            cache=cache,
        ),
    )
    return OperatorCatalogBrowserApp(
        namespace_workflow=namespace_workflow,
        table_access=table_access,
        output_policy=config.output,
        context=OperatorTuiContext(
            catalog_name=table_source.catalog_name,
            profile=table_source.aws_profile,
            region=table_source.region,
        ),
    )


def _configured_cache_scope_key(source: GlueCatalogTableSourceConfiguration) -> str:
    return "|".join(
        (
            source.catalog_name,
            source.aws_profile or "",
            source.region or "",
        )
    )


def _analyze_config_with_unsupported_mapping(
    config: AnalyzerConfiguration,
) -> TableHealthReport:
    try:
        return _analyze_iceberg_table(config)
    except Exception as exc:
        if _is_unsupported_error(exc):
            raise UnsupportedTableError(_unsupported_error_message(exc)) from exc
        raise


def _catalog_namespace(raw_namespace: object) -> CatalogNamespace:
    if isinstance(raw_namespace, str):
        name = tuple(part for part in raw_namespace.split(".") if part)
    else:
        name = tuple(str(part) for part in raw_namespace)
    return CatalogNamespace(name=name, display_name=".".join(name))


def _catalog_table(
    namespace: tuple[str, ...], raw_table: CatalogTable | CatalogTableRow | str | object
) -> CatalogTable:
    if isinstance(raw_table, CatalogTable):
        return raw_table
    if isinstance(raw_table, CatalogTableRow):
        return CatalogTable(
            namespace=raw_table.namespace,
            name=raw_table.name,
            identifier=raw_table.identifier,
            table_format=str(raw_table.table_format),
            freshness=raw_table.cache_status,
        )
    if isinstance(raw_table, str):
        parts = tuple(part for part in raw_table.split(".") if part)
    else:
        parts = tuple(str(part) for part in raw_table)
    if not parts:
        name = ""
        identifier_parts = namespace
    elif len(parts) == 1:
        name = parts[0]
        identifier_parts = (*namespace, name)
    else:
        name = parts[-1]
        identifier_parts = parts
    return CatalogTable(
        namespace=tuple(identifier_parts[:-1]),
        name=name,
        identifier=".".join(identifier_parts),
    )


def _catalog_table_row_for_identifier(table_identifier: str) -> CatalogTableRow:
    parts = tuple(part for part in table_identifier.split(".") if part)
    if len(parts) < 2:
        raise ValueError(
            "Table identifier must include namespace and table name "
            "(for example: sales.orders)."
        )
    return CatalogTableRow(
        namespace=parts[:-1],
        name=parts[-1],
        identifier=".".join(parts),
        table_format=TableFormatClassification.UNKNOWN,
        classification_source="selection",
    )


def _catalog_table_listing(
    namespace: tuple[str, ...],
    raw_listing: Sequence[CatalogTable | CatalogTableRow | str] | CatalogTableListing,
) -> CatalogTableListing:
    if isinstance(raw_listing, CatalogTableListing):
        return raw_listing
    return CatalogTableListing(namespace=namespace, rows=tuple(raw_listing))


def _contains(value: str, needle: str) -> bool:
    return needle.lower() in value.lower()


def _scope_message(scope: str, message: str) -> str:
    if scope:
        return f"{scope}: {message}"
    return message


def _is_unsupported_error(exc: Exception) -> bool:
    try:
        from pyiceberg.exceptions import NoSuchIcebergTableError
    except Exception:  # pragma: no cover - fallback when pyiceberg is unavailable
        NoSuchIcebergTableError = ()  # type: ignore[assignment]
    if NoSuchIcebergTableError and isinstance(exc, NoSuchIcebergTableError):
        return True
    message = str(exc).lower()
    return (
        "not an iceberg table" in message
        or "not a valid iceberg table" in message
        or "property table_type missing" in message
        or "could not determine type" in message
    )


def _unsupported_error_message(exc: Exception) -> str:
    message = str(exc).strip()
    if message:
        return message
    return exc.__class__.__name__


def _table_analysis_result(
    value: TableHealthReport | TableAnalysisResult,
) -> TableAnalysisResult:
    if isinstance(value, TableAnalysisResult):
        if value.analyzed_at is not None:
            return value
        return replace(value, analyzed_at=datetime.now(timezone.utc))
    return TableAnalysisResult(
        report=value,
        cache_status="fresh",
        analyzed_at=datetime.now(timezone.utc),
    )
