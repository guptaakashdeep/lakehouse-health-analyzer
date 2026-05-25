from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from analysis.report import HealthMetric, TableHealthReport, TableSource
from configuration import OutputPolicy
from operator_tui.export import TableReportExportWorkflow


def test_table_report_export_workflow_offer_lists_json_markdown_and_both(tmp_path):
    workflow = TableReportExportWorkflow(
        output_policy=OutputPolicy(export_directory=str(tmp_path))
    )

    offer = workflow.export_offer(table_name="sales.orders")

    assert "Export report for sales.orders" in offer
    assert "1 JSON" in offer
    assert "2 Markdown" in offer
    assert "3 Both" in offer
    assert str(tmp_path) in offer


def test_table_report_export_workflow_exports_both_formats_to_configured_destination(
    tmp_path,
):
    workflow = TableReportExportWorkflow(
        output_policy=OutputPolicy(export_directory=str(tmp_path))
    )

    written_paths = workflow.export_report(
        _report("sales.orders"),
        export_choice="both",
        cache_status="fresh",
        analyzed_at=datetime(2026, 5, 22, 10, 0, tzinfo=timezone.utc),
    )

    assert [path.name for path in written_paths] == ["sales-orders.json", "sales-orders.md"]
    assert (tmp_path / "sales-orders.json").exists()
    assert (tmp_path / "sales-orders.md").exists()


def test_table_report_export_workflow_defaults_to_cwd_destination_when_not_configured(
    tmp_path,
):
    workflow = TableReportExportWorkflow(
        output_policy=OutputPolicy(),
        resolve_cwd=lambda: tmp_path,
    )

    written_paths = workflow.export_report(
        _report("sales.orders"),
        export_choice="json",
        cache_status="fresh",
        analyzed_at=datetime(2026, 5, 22, 10, 0, tzinfo=timezone.utc),
    )

    assert written_paths == (tmp_path / "sales-orders.json",)


def test_table_report_export_workflow_includes_cached_metadata_in_json_export(tmp_path):
    workflow = TableReportExportWorkflow(
        output_policy=OutputPolicy(export_directory=str(tmp_path))
    )

    workflow.export_report(
        _report("sales.orders"),
        export_choice="json",
        cache_status="cached",
        analyzed_at=datetime(2026, 5, 22, 10, 0, tzinfo=timezone.utc),
    )

    payload = json.loads((tmp_path / "sales-orders.json").read_text())
    assert payload["export_metadata"]["cache_status"] == "cached"


def test_table_report_export_workflow_rejects_unsupported_export_choice(tmp_path):
    workflow = TableReportExportWorkflow(
        output_policy=OutputPolicy(export_directory=str(tmp_path))
    )

    with pytest.raises(ValueError, match="Unsupported export choice"):
        workflow.export_report(
            _report("sales.orders"),
            export_choice="yaml",
            cache_status="fresh",
            analyzed_at=datetime(2026, 5, 22, 10, 0, tzinfo=timezone.utc),
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
