import io
import json
from datetime import datetime, timezone

from analysis.report import HealthMetric, TableHealthReport, TableSource
from configuration import AnalyzerConfiguration, GlueCatalogTableSourceConfiguration
from workflows.errors import UnsupportedTableError
from workflows.report_command import configured_report_command_workflow
from workflows.report_command import run_report_command_workflow


def test_report_command_writes_json_to_stdout_for_namespace_qualified_identifier():
    calls = []
    workflow = configured_report_command_workflow(
        AnalyzerConfiguration(
            table_source=GlueCatalogTableSourceConfiguration(
                catalog_name="analytics",
                namespace=("placeholder",),
                table_name="__placeholder__",
            )
        ),
        analyze_config=lambda config: calls.append(config) or _report(),
    )
    output = io.StringIO()
    error = io.StringIO()

    result = run_report_command_workflow(
        workflow,
        table_identifier="sales.orders",
        formats=("json",),
        output=output,
        error=error,
    )

    assert result == 0
    assert error.getvalue() == ""
    payload = json.loads(output.getvalue())
    assert payload["table_name"] == "sales.orders"
    assert calls[0].table_source.namespace == ("sales",)
    assert calls[0].table_source.table_name == "orders"


def test_report_command_writes_markdown_to_stdout_for_single_format():
    workflow = configured_report_command_workflow(
        _base_config(),
        analyze_config=lambda config: _report(),
    )
    output = io.StringIO()
    error = io.StringIO()

    result = run_report_command_workflow(
        workflow,
        table_identifier="sales.orders",
        formats=("markdown",),
        output=output,
        error=error,
    )

    assert result == 0
    assert error.getvalue() == ""
    assert "# Table Health Report: sales.orders" in output.getvalue()


def test_report_command_writes_exact_output_file_for_single_format(tmp_path):
    workflow = configured_report_command_workflow(
        _base_config(),
        analyze_config=lambda config: _report(),
    )
    destination = tmp_path / "orders.json"

    result = run_report_command_workflow(
        workflow,
        table_identifier="sales.orders",
        formats=("json",),
        output=io.StringIO(),
        error=io.StringIO(),
        output_path=str(destination),
        now=lambda: datetime(2026, 5, 22, 7, 15, tzinfo=timezone.utc),
    )

    assert result == 0
    assert destination.exists()
    payload = json.loads(destination.read_text())
    assert payload["export_metadata"]["cache_status"] == "fresh"


def test_report_command_writes_generated_files_for_output_directory(tmp_path):
    workflow = configured_report_command_workflow(
        _base_config(),
        analyze_config=lambda config: _report(),
    )
    output = io.StringIO()
    error = io.StringIO()

    result = run_report_command_workflow(
        workflow,
        table_identifier="sales.orders",
        formats=("json", "markdown"),
        output=output,
        error=error,
        output_directory=str(tmp_path),
    )

    assert result == 0
    assert output.getvalue() == ""
    assert error.getvalue() == ""
    assert (tmp_path / "sales-orders.json").exists()
    assert (tmp_path / "sales-orders.md").exists()


def test_report_command_rejects_multiple_stdout_formats_without_directory():
    workflow = configured_report_command_workflow(
        _base_config(),
        analyze_config=lambda config: _report(),
    )
    output = io.StringIO()
    error = io.StringIO()

    result = run_report_command_workflow(
        workflow,
        table_identifier="sales.orders",
        formats=("json", "markdown"),
        output=output,
        error=error,
    )

    assert result != 0
    assert output.getvalue() == ""
    assert "--output-dir" in error.getvalue()


def test_report_command_uses_configured_export_directory_for_multiple_formats(
    tmp_path,
):
    workflow = configured_report_command_workflow(
        _base_config(),
        analyze_config=lambda config: _report(),
    )

    result = run_report_command_workflow(
        workflow,
        table_identifier="sales.orders",
        formats=("json", "markdown"),
        output=io.StringIO(),
        error=io.StringIO(),
        configured_output_directory=str(tmp_path),
    )

    assert result == 0
    assert (tmp_path / "sales-orders.json").exists()
    assert (tmp_path / "sales-orders.md").exists()


def test_report_command_rejects_bare_table_identifier_without_namespace():
    workflow = configured_report_command_workflow(
        _base_config(),
        analyze_config=lambda config: _report(),
    )
    error = io.StringIO()

    result = run_report_command_workflow(
        workflow,
        table_identifier="orders",
        formats=("json",),
        output=io.StringIO(),
        error=error,
    )

    assert result != 0
    assert "namespace and table name" in error.getvalue()


def test_report_command_rejects_table_identifier_with_empty_namespace_segment():
    workflow = configured_report_command_workflow(
        _base_config(),
        analyze_config=lambda config: _report(),
    )
    error = io.StringIO()

    result = run_report_command_workflow(
        workflow,
        table_identifier="sales..orders",
        formats=("json",),
        output=io.StringIO(),
        error=error,
    )

    assert result != 0
    assert "namespace and table name" in error.getvalue()


def test_report_command_exits_non_zero_for_unsupported_tables():
    workflow = configured_report_command_workflow(
        _base_config(),
        analyze_config=lambda config: (_ for _ in ()).throw(
            UnsupportedTableError("unsupported table format")
        ),
    )
    error = io.StringIO()

    result = run_report_command_workflow(
        workflow,
        table_identifier="sales.view_only",
        formats=("json",),
        output=io.StringIO(),
        error=error,
    )

    assert result == 1
    assert "unsupported table" in error.getvalue()


def test_report_command_reports_generic_analysis_failure_without_unsupported_label():
    workflow = configured_report_command_workflow(
        _base_config(),
        analyze_config=lambda config: (_ for _ in ()).throw(
            RuntimeError("glue throttled")
        ),
    )
    error = io.StringIO()

    result = run_report_command_workflow(
        workflow,
        table_identifier="sales.orders",
        formats=("json",),
        output=io.StringIO(),
        error=error,
    )

    assert result == 1
    assert "analysis failed" in error.getvalue()
    assert "unsupported table" not in error.getvalue()


def test_report_command_rejects_output_path_for_multiple_formats():
    workflow = configured_report_command_workflow(
        _base_config(),
        analyze_config=lambda config: _report(),
    )
    error = io.StringIO()

    result = run_report_command_workflow(
        workflow,
        table_identifier="sales.orders",
        formats=("json", "markdown"),
        output=io.StringIO(),
        error=error,
        output_path="/tmp/sales-orders.json",
    )

    assert result != 0
    assert "--output supports only one format" in error.getvalue()


def test_report_command_rejects_unknown_export_format():
    workflow = configured_report_command_workflow(
        _base_config(),
        analyze_config=lambda config: _report(),
    )

    result = run_report_command_workflow(
        workflow,
        table_identifier="sales.orders",
        formats=("yaml",),
        output=io.StringIO(),
        error=io.StringIO(),
    )

    assert result != 0


def _base_config() -> AnalyzerConfiguration:
    return AnalyzerConfiguration(
        table_source=GlueCatalogTableSourceConfiguration(
            catalog_name="analytics",
            namespace=("placeholder",),
            table_name="__placeholder__",
        )
    )


def _report() -> TableHealthReport:
    return TableHealthReport(
        table_name="sales.orders",
        table_source=TableSource(
            kind="glue_catalog_table",
            location="analytics.sales.orders",
        ),
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
