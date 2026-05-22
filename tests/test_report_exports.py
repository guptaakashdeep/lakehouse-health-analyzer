import json
from datetime import date, datetime, timezone

import pytest

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
from configuration import OutputPolicy
from report_exports import export_table_health_report
from report_exports import export_table_health_report_for_output_policy


def test_report_can_be_exported_as_json_without_losing_structure(tmp_path):
    destination = tmp_path / "sales-orders.json"
    analyzed_at = datetime(2026, 5, 21, 8, 30, tzinfo=timezone.utc)
    report = _report()

    written_path = export_table_health_report(
        report,
        export_format="json",
        destination=destination,
        cache_status="cached",
        analyzed_at=analyzed_at,
    )

    payload = json.loads(destination.read_text())
    assert written_path == destination
    assert payload["table_name"] == "sales.orders"
    assert payload["table_source"] == {
        "kind": "glue_catalog_table",
        "location": "analytics.sales.orders",
    }
    assert payload["health_metrics"][0]["key"] == "data_file_count"
    assert payload["display_statistics"][0]["key"] == "total_file_size"
    assert payload["maintenance_recommendations"][0]["severity"] == "warning"
    assert payload["partition_metrics"][0]["partition"] == {
        "business_date": "2026-05-21"
    }
    assert payload["table_evolution_history"]["schema_changes"][0]["after"] == "date"
    assert payload["export_metadata"] == {
        "cache_status": "cached",
        "analyzed_at": analyzed_at.isoformat(),
    }


def test_report_can_be_exported_as_markdown_with_readable_sections(tmp_path):
    destination = tmp_path / "sales-orders.md"
    analyzed_at = datetime(2026, 5, 21, 8, 30, tzinfo=timezone.utc)
    report = _report(
        calculation_warnings=(
            CalculationWarning(
                metric_key="estimated_current_record_count",
                message="Delete records present in table snapshot.",
            ),
        )
    )

    export_table_health_report(
        report,
        export_format="markdown",
        destination=destination,
        cache_status="fresh",
        analyzed_at=analyzed_at,
    )

    rendered = destination.read_text()
    assert "# Table Health Report: sales.orders" in rendered
    assert "Cache Status: fresh" in rendered
    assert f"Analyzed At: {analyzed_at.isoformat()}" in rendered
    assert "## Health Metrics" in rendered
    assert "- Data File Count: 7 files" in rendered
    assert "## Calculation Warnings" in rendered
    assert (
        "- estimated_current_record_count: Delete records present in table snapshot."
        in rendered
    )
    assert "## Maintenance Recommendations" in rendered
    assert "- warning: Compaction - Review file-count pressure." in rendered


def test_export_rejects_unknown_format(tmp_path):
    with pytest.raises(ValueError, match="Unsupported export format"):
        export_table_health_report(
            _report(),
            export_format="yaml",
            destination=tmp_path / "sales-orders.yaml",
            cache_status="fresh",
            analyzed_at=datetime(2026, 5, 21, 8, 30, tzinfo=timezone.utc),
        )


def test_export_requires_existing_destination_directory(tmp_path):
    with pytest.raises(FileNotFoundError, match="does not exist"):
        export_table_health_report(
            _report(),
            export_format="json",
            destination=tmp_path / "missing" / "sales-orders.json",
            cache_status="fresh",
            analyzed_at=datetime(2026, 5, 21, 8, 30, tzinfo=timezone.utc),
        )


def test_output_policy_exports_report_to_all_configured_formats(tmp_path):
    analyzed_at = datetime(2026, 5, 21, 8, 30, tzinfo=timezone.utc)

    written_paths = export_table_health_report_for_output_policy(
        _report(),
        output_policy=OutputPolicy(
            export_formats=("json", "markdown"),
            export_directory=str(tmp_path),
        ),
        cache_status="fresh",
        analyzed_at=analyzed_at,
    )

    assert [path.name for path in written_paths] == [
        "sales-orders.json",
        "sales-orders.md",
    ]
    assert (tmp_path / "sales-orders.json").exists()
    assert (tmp_path / "sales-orders.md").exists()


def test_output_policy_rejects_unknown_configured_export_format(tmp_path):
    with pytest.raises(ValueError, match="Unsupported export format"):
        export_table_health_report_for_output_policy(
            _report(),
            output_policy=OutputPolicy(
                export_formats=("json", "yaml"),
                export_directory=str(tmp_path),
            ),
            cache_status="fresh",
            analyzed_at=datetime(2026, 5, 21, 8, 30, tzinfo=timezone.utc),
        )
    assert not (tmp_path / "sales-orders.json").exists()


def _report(
    *,
    calculation_warnings: tuple[CalculationWarning, ...] = (),
) -> TableHealthReport:
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
        display_statistics=(
            DisplayStatistic(
                key="total_file_size",
                label="Total File Size",
                value="12 MiB",
                derived_from=("total_file_size_bytes",),
            ),
        ),
        maintenance_recommendations=(
            MaintenanceRecommendation(
                recommendation_type="compaction",
                severity="warning",
                evidence={"high_file_count_partition_count": 4},
                thresholds={"high_file_count_partition_count_warning": 3},
                rationale="Review file-count pressure.",
            ),
        ),
        calculation_warnings=calculation_warnings,
        partition_metrics=(
            PartitionHealthMetric(
                partition={"business_date": date(2026, 5, 21)},
                data_file_count=2,
                delete_file_count=0,
                total_data_file_size_bytes=1024,
                average_data_file_size_bytes=512,
                source="test",
            ),
        ),
        table_evolution_history=TableEvolutionHistory(
            schema_changes=(
                EvolutionChange(
                    change_type="added",
                    subject="schema",
                    name="business_date",
                    before=None,
                    after="date",
                    source="metadata-log",
                ),
            )
        ),
    )
