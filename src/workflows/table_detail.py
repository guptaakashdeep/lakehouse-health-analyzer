from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Mapping, Protocol

from analysis.iceberg import analyze_iceberg_table
from analysis.report import TableHealthReport
from configuration import AnalyzerConfiguration, GlueCatalogTableSourceConfiguration
from workflows.catalog_browser import CatalogTableRow, TableFormatClassification
from workflows.errors import UnsupportedTableError

_SECTION_ORDER = (
    ("recommendations", "Recommendations"),
    ("warnings", "Warnings"),
    ("files", "Files"),
    ("records", "Records"),
    ("partitions", "Partitions"),
    ("snapshots", "Snapshots"),
    ("metadata_and_evolution", "Metadata and Evolution"),
    ("source_and_runtime", "Source and Runtime"),
)

_FILE_METRIC_KEYS = (
    "data_file_count",
    "delete_file_count",
    "total_file_size_bytes",
    "data_file_size_bytes",
    "delete_file_size_bytes",
)

_RECORD_METRIC_KEYS = (
    "data_file_record_count",
    "position_delete_record_count",
    "equality_delete_record_count",
    "estimated_current_record_count",
)

_PARTITION_METRIC_KEYS = (
    "partition_count",
    "average_data_files_per_partition",
    "max_data_files_per_partition",
    "high_file_count_partition_count",
    "high_file_count_partition_threshold",
    "partition_total_data_file_size_bytes",
    "average_partition_data_file_size_bytes",
    "min_partition_data_file_size_bytes",
    "max_partition_data_file_size_bytes",
    "partition_size_skewness",
)

_SNAPSHOT_METRIC_KEYS = (
    "valid_snapshot_count",
    "oldest_snapshot_age_days",
    "latest_snapshot_age_days",
    "snapshot_retention_days",
    "expirable_snapshot_candidate_count",
)


@dataclass(frozen=True)
class HighlightedTableDetail:
    table: CatalogTableRow
    analysis_status: str = "not_started"


@dataclass(frozen=True)
class TableDetailItem:
    key: str
    label: str
    primary: Mapping[str, Any]
    secondary: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TableDetailSection:
    key: str
    title: str
    items: tuple[TableDetailItem, ...] = ()


@dataclass(frozen=True)
class TableDetailView:
    table_name: str
    sections: tuple[TableDetailSection, ...]

    def section(self, key: str) -> TableDetailSection:
        for section in self.sections:
            if section.key == key:
                return section
        raise KeyError(key)


@dataclass(frozen=True)
class TableDetailResult:
    table: CatalogTableRow
    report: TableHealthReport | None
    analysis_status: str = "loaded"
    message: str = ""
    detail_view: TableDetailView | None = None
    cache_status: str = "fresh"


@dataclass(frozen=True)
class TableDetailAnalysisTask:
    table: CatalogTableRow
    _future: Future[TableDetailResult]
    analysis_status: str = "loading"
    cache_status: str = "refreshing"

    def done(self) -> bool:
        return self._future.done()

    def result(self, timeout: float | None = None) -> TableDetailResult:
        return self._future.result(timeout=timeout)


class TableDetailCache(Protocol):
    def read_table_detail_report(self, table_identifier: str) -> TableDetailResult | None:
        ...

    def read_stale_table_detail_report(
        self, table_identifier: str
    ) -> TableDetailResult | None: ...

    def write_table_detail_report(
        self, table_identifier: str, result: TableDetailResult
    ) -> TableDetailResult: ...

    def write_table_classification(
        self,
        table_identifier: str,
        table_format: TableFormatClassification,
        classification_source: str,
    ) -> object: ...


@dataclass
class TableDetailWorkflow:
    base_config: AnalyzerConfiguration
    analyze_config: Callable[[AnalyzerConfiguration], TableHealthReport] = (
        analyze_iceberg_table
    )
    cache: TableDetailCache | None = None
    _executor: ThreadPoolExecutor = field(
        default_factory=lambda: ThreadPoolExecutor(max_workers=1), repr=False
    )

    def highlight_table(self, table: CatalogTableRow) -> HighlightedTableDetail:
        return HighlightedTableDetail(table=table)

    def select_table(
        self, table: CatalogTableRow, *, refresh: bool = False
    ) -> TableDetailAnalysisTask:
        if self.cache is not None and not refresh:
            cached_result = self.cache.read_table_detail_report(table.identifier)
            if cached_result is not None:
                future: Future[TableDetailResult] = Future()
                future.set_result(cached_result)
                return TableDetailAnalysisTask(
                    table=cached_result.table,
                    _future=future,
                    analysis_status=cached_result.analysis_status,
                    cache_status=cached_result.cache_status,
                )
        config = _config_for_table(self.base_config, table)
        future = self._executor.submit(_analyze_selected_table, self, table, config)
        return TableDetailAnalysisTask(table=table, _future=future)


def _analyze_selected_table(
    workflow: TableDetailWorkflow,
    table: CatalogTableRow,
    config: AnalyzerConfiguration,
) -> TableDetailResult:
    try:
        report = workflow.analyze_config(config)
    except UnsupportedTableError as exc:
        result = TableDetailResult(
            table=replace(
                table,
                table_format=TableFormatClassification.NON_ICEBERG,
                classification_source="analysis",
            ),
            report=None,
            analysis_status="unsupported",
            message=str(exc) or exc.__class__.__name__,
        )
        return _cache_table_detail_result(workflow, table.identifier, result)
    except Exception as exc:
        message = str(exc) or exc.__class__.__name__
        if workflow.cache is not None:
            stale = workflow.cache.read_stale_table_detail_report(table.identifier)
            if stale is not None:
                return replace(
                    stale,
                    analysis_status="stale",
                    cache_status="stale",
                    message=message,
                )
        return TableDetailResult(
            table=table,
            report=None,
            analysis_status="error",
            message=message,
        )
    result = TableDetailResult(
        table=replace(
            table,
            table_format=TableFormatClassification.ICEBERG,
            classification_source="analysis",
        ),
        report=report,
        detail_view=table_detail_view(report, table=table, analysis_status="loaded"),
    )
    return _cache_table_detail_result(workflow, table.identifier, result)


def _cache_table_detail_result(
    workflow: TableDetailWorkflow, table_identifier: str, result: TableDetailResult
) -> TableDetailResult:
    if workflow.cache is None:
        return result
    workflow.cache.write_table_classification(
        table_identifier,
        result.table.table_format,
        result.table.classification_source,
    )
    return workflow.cache.write_table_detail_report(table_identifier, result)


def table_detail_view(
    report: TableHealthReport,
    *,
    table: CatalogTableRow | None = None,
    analysis_status: str = "loaded",
) -> TableDetailView:
    sections = {
        "recommendations": _recommendation_items(report),
        "warnings": _warning_items(report),
        "files": (
            *_display_statistic_items(report, derived_from=_FILE_METRIC_KEYS),
            *_health_metric_items(report, _FILE_METRIC_KEYS),
        ),
        "records": _health_metric_items(report, _RECORD_METRIC_KEYS),
        "partitions": (
            *_health_metric_items(report, _PARTITION_METRIC_KEYS),
            *_partition_items(report),
        ),
        "snapshots": _health_metric_items(report, _SNAPSHOT_METRIC_KEYS),
        "metadata_and_evolution": _evolution_items(report),
        "source_and_runtime": _source_and_runtime_items(
            report,
            table=table,
            analysis_status=analysis_status,
        ),
    }
    return TableDetailView(
        table_name=report.table_name,
        sections=tuple(
            TableDetailSection(key=key, title=title, items=sections[key])
            for key, title in _SECTION_ORDER
        ),
    )


def _recommendation_items(report: TableHealthReport) -> tuple[TableDetailItem, ...]:
    severity_rank = {"critical": 0, "warning": 1, "info": 2}
    recommendations = sorted(
        report.maintenance_recommendations,
        key=lambda recommendation: (
            severity_rank.get(recommendation.severity, 99),
            recommendation.recommendation_type,
        ),
    )
    return tuple(
        TableDetailItem(
            key=f"{recommendation.severity}:{recommendation.recommendation_type}",
            label="Maintenance Recommendation",
            primary={
                "severity": recommendation.severity,
                "type": recommendation.recommendation_type,
                "rationale": recommendation.rationale,
            },
            secondary={
                "evidence": dict(recommendation.evidence),
                "thresholds": dict(recommendation.thresholds),
            },
        )
        for recommendation in recommendations
    )


def _warning_items(report: TableHealthReport) -> tuple[TableDetailItem, ...]:
    return tuple(
        TableDetailItem(
            key=warning.metric_key,
            label="Calculation Warning",
            primary={"metric_key": warning.metric_key, "message": warning.message},
        )
        for warning in report.calculation_warnings
    )


def _health_metric_items(
    report: TableHealthReport, keys: tuple[str, ...]
) -> tuple[TableDetailItem, ...]:
    metrics_by_key = {metric.key: metric for metric in report.health_metrics}
    return tuple(
        TableDetailItem(
            key=metric.key,
            label=metric.label,
            primary={
                "label": metric.label,
                "value": metric.value,
                "unit": metric.unit,
            },
            secondary={"source": metric.source},
        )
        for key in keys
        if (metric := metrics_by_key.get(key)) is not None
    )


def _display_statistic_items(
    report: TableHealthReport, *, derived_from: tuple[str, ...]
) -> tuple[TableDetailItem, ...]:
    derived_metric_keys = set(derived_from)
    return tuple(
        TableDetailItem(
            key=statistic.key,
            label=statistic.label,
            primary={
                "label": statistic.label,
                "value": statistic.value,
            },
            secondary={"derived_from": statistic.derived_from},
        )
        for statistic in report.display_statistics
        if any(key in derived_metric_keys for key in statistic.derived_from)
    )


def _partition_items(report: TableHealthReport) -> tuple[TableDetailItem, ...]:
    return tuple(
        TableDetailItem(
            key=f"partition:{index}",
            label="Partition",
            primary={
                "partition": dict(metric.partition),
                "data_file_count": metric.data_file_count,
                "delete_file_count": metric.delete_file_count,
                "total_data_file_size_bytes": metric.total_data_file_size_bytes,
                "average_data_file_size_bytes": metric.average_data_file_size_bytes,
            },
            secondary={"source": metric.source},
        )
        for index, metric in enumerate(report.partition_metrics)
    )


def _evolution_items(report: TableHealthReport) -> tuple[TableDetailItem, ...]:
    changes = (
        *report.table_evolution_history.schema_changes,
        *report.table_evolution_history.property_changes,
    )
    return tuple(
        TableDetailItem(
            key=f"{change.subject}:{change.name}:{index}",
            label="Table Evolution Change",
            primary={
                "change_type": change.change_type,
                "subject": change.subject,
                "name": change.name,
                "before": change.before,
                "after": change.after,
            },
            secondary={"source": change.source},
        )
        for index, change in enumerate(changes)
    )


def _source_and_runtime_items(
    report: TableHealthReport,
    *,
    table: CatalogTableRow | None,
    analysis_status: str,
) -> tuple[TableDetailItem, ...]:
    items = [
        TableDetailItem(
            key="table_source",
            label="Table Source",
            primary={
                "kind": report.table_source.kind,
                "location": report.table_source.location,
            },
        ),
        TableDetailItem(
            key="analysis_runtime",
            label="Analysis Runtime",
            primary={"analysis_status": analysis_status},
        ),
    ]
    if table is not None:
        items.append(
            TableDetailItem(
                key="table_format_classification",
                label="Table Format Classification",
                primary={
                    "table_format": TableFormatClassification.ICEBERG.value,
                    "classification_source": "analysis",
                },
                secondary={
                    "previous_classification_source": table.classification_source
                },
            )
        )
    return tuple(items)


def _config_for_table(
    base_config: AnalyzerConfiguration, table: CatalogTableRow
) -> AnalyzerConfiguration:
    table_source = base_config.table_source
    if not isinstance(table_source, GlueCatalogTableSourceConfiguration):
        raise ValueError(
            "Selected table analysis requires a Glue catalog table source."
        )
    return replace(
        base_config,
        table_source=replace(
            table_source,
            namespace=table.namespace,
            table_name=table.name,
        ),
    )
