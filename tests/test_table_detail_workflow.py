import time
from threading import Event

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
from configuration import AnalyzerConfiguration, GlueCatalogTableSourceConfiguration
from workflows.catalog_browser import CatalogTableRow, TableFormatClassification
from workflows.errors import UnsupportedTableError
from workflows.table_detail import TableDetailWorkflow


def test_table_detail_workflow_analyzes_only_after_explicit_selection():
    calls = []
    workflow = TableDetailWorkflow(
        base_config=_base_config(),
        analyze_config=lambda config: calls.append(config) or _report("sales.orders"),
    )
    table = _table_row("orders")

    highlighted = workflow.highlight_table(table)

    assert highlighted.table == table
    assert highlighted.analysis_status == "not_started"
    assert calls == []

    result = workflow.select_table(table).result()

    assert calls[0].table_source.namespace == ("sales",)
    assert calls[0].table_source.table_name == "orders"
    assert result.report.table_name == "sales.orders"


def test_selected_table_analysis_returns_loading_task_before_report_is_ready():
    release_analysis = Event()

    def analyze_config(config):
        release_analysis.wait(0.2)
        return _report("sales.orders")

    workflow = TableDetailWorkflow(
        base_config=_base_config(),
        analyze_config=analyze_config,
    )

    started_at = time.monotonic()
    task = workflow.select_table(_table_row("orders"))

    assert time.monotonic() - started_at < 0.05
    assert task.analysis_status == "loading"
    assert not task.done()

    release_analysis.set()
    assert task.result(timeout=1).report.table_name == "sales.orders"


def test_successful_selected_table_analysis_promotes_classification_to_iceberg():
    workflow = TableDetailWorkflow(
        base_config=_base_config(),
        analyze_config=lambda config: _report("sales.orders"),
    )

    result = workflow.select_table(_table_row("orders")).result(timeout=1)

    assert result.table.table_format == TableFormatClassification.ICEBERG
    assert result.table.classification_source == "analysis"


def test_unsupported_selected_table_analysis_promotes_classification_to_non_iceberg():
    def analyze_config(config):
        raise UnsupportedTableError("unsupported table format")

    workflow = TableDetailWorkflow(
        base_config=_base_config(),
        analyze_config=analyze_config,
    )

    result = workflow.select_table(_table_row("orders")).result(timeout=1)

    assert result.analysis_status == "unsupported"
    assert result.table.table_format == TableFormatClassification.NON_ICEBERG
    assert result.table.classification_source == "analysis"
    assert result.report is None
    assert result.message == "unsupported table format"


def test_failed_selected_table_analysis_surfaces_error_without_reclassification():
    def analyze_config(config):
        raise RuntimeError("catalog temporarily unavailable")

    workflow = TableDetailWorkflow(
        base_config=_base_config(),
        analyze_config=analyze_config,
    )

    result = workflow.select_table(_table_row("orders")).result(timeout=1)

    assert result.analysis_status == "error"
    assert result.table.table_format == TableFormatClassification.UNKNOWN
    assert result.table.classification_source == "glue_parameters"
    assert result.report is None
    assert result.message == "catalog temporarily unavailable"


def test_table_detail_result_groups_sections_and_prioritizes_recommendation_fields():
    workflow = TableDetailWorkflow(
        base_config=_base_config(),
        analyze_config=lambda config: _detailed_report(),
    )

    result = workflow.select_table(_table_row("orders")).result(timeout=1)
    detail_view = result.detail_view

    assert [section.key for section in detail_view.sections] == [
        "recommendations",
        "warnings",
        "files",
        "records",
        "partitions",
        "snapshots",
        "metadata_and_evolution",
        "source_and_runtime",
    ]
    recommendation = detail_view.section("recommendations").items[0]
    assert recommendation.primary == {
        "severity": "critical",
        "type": "compaction",
        "rationale": "Compact clustered small files.",
    }
    assert recommendation.secondary == {
        "evidence": {"high_file_count_partition_count": 2},
        "thresholds": {"high_file_count_partition_count_critical": 2},
    }


def test_table_detail_view_uses_report_display_statistics_without_recalculating():
    workflow = TableDetailWorkflow(
        base_config=_base_config(),
        analyze_config=lambda config: _detailed_report(),
    )

    result = workflow.select_table(_table_row("orders")).result(timeout=1)
    files_section = result.detail_view.section("files")

    display_item = next(
        item for item in files_section.items if item.key == "total_file_size"
    )
    assert display_item.primary == {
        "label": "Total File Size",
        "value": "12 MiB",
    }
    assert display_item.secondary == {
        "derived_from": ("total_file_size_bytes",),
    }


def test_table_detail_view_uses_only_existing_report_partition_metrics():
    report = TableHealthReport(
        table_name="sales.orders",
        table_source=TableSource(kind="glue_catalog_table", location="sales.orders"),
        health_metrics=(),
        display_statistics=(),
        partition_metrics=(
            PartitionHealthMetric(
                partition={"region": "east"},
                data_file_count=5,
                delete_file_count=1,
                total_data_file_size_bytes=1024,
                average_data_file_size_bytes=204.8,
                source="test.partitions",
            ),
        ),
    )
    workflow = TableDetailWorkflow(
        base_config=_base_config(),
        analyze_config=lambda config: report,
    )

    result = workflow.select_table(_table_row("orders")).result(timeout=1)
    partitions_section = result.detail_view.section("partitions")

    assert [item.key for item in partitions_section.items] == ["partition:0"]
    assert partitions_section.items[0].primary == {
        "partition": {"region": "east"},
        "data_file_count": 5,
        "delete_file_count": 1,
        "total_data_file_size_bytes": 1024,
        "average_data_file_size_bytes": 204.8,
    }


def _base_config() -> AnalyzerConfiguration:
    return AnalyzerConfiguration(
        table_source=GlueCatalogTableSourceConfiguration(
            catalog_name="analytics",
            namespace=("sales",),
            table_name="__placeholder__",
        )
    )


def _table_row(table_name: str) -> CatalogTableRow:
    return CatalogTableRow(
        namespace=("sales",),
        name=table_name,
        identifier=f"sales.{table_name}",
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


def _detailed_report() -> TableHealthReport:
    return TableHealthReport(
        table_name="sales.orders",
        table_source=TableSource(kind="glue_catalog_table", location="sales.orders"),
        health_metrics=(
            HealthMetric(
                key="data_file_count",
                label="Data File Count",
                value=7,
                unit="files",
                source="test.files",
            ),
            HealthMetric(
                key="delete_file_count",
                label="Delete File Count",
                value=1,
                unit="files",
                source="test.files",
            ),
            HealthMetric(
                key="data_file_record_count",
                label="Data File Record Count",
                value=1200,
                unit="records",
                source="test.files",
            ),
            HealthMetric(
                key="partition_count",
                label="Partition Count",
                value=2,
                unit="partitions",
                source="test.partitions",
            ),
            HealthMetric(
                key="valid_snapshot_count",
                label="Valid Snapshot Count",
                value=4,
                unit="snapshots",
                source="test.snapshots",
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
        calculation_warnings=(
            CalculationWarning(
                metric_key="partition_size_skewness",
                message="Partition sizes were incomplete.",
            ),
        ),
        maintenance_recommendations=(
            MaintenanceRecommendation(
                recommendation_type="compaction",
                severity="critical",
                evidence={"high_file_count_partition_count": 2},
                thresholds={"high_file_count_partition_count_critical": 2},
                rationale="Compact clustered small files.",
            ),
        ),
        partition_metrics=(
            PartitionHealthMetric(
                partition={"region": "east"},
                data_file_count=5,
                delete_file_count=1,
                total_data_file_size_bytes=1024,
                average_data_file_size_bytes=204.8,
                source="test.partitions",
            ),
        ),
        table_evolution_history=TableEvolutionHistory(
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
        ),
    )
