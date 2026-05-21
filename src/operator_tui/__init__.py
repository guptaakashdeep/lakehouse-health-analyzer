from __future__ import annotations

import argparse
import multiprocessing
import signal
import sys
from dataclasses import dataclass, field, replace
from datetime import datetime
from queue import Empty
from time import monotonic, sleep
from typing import Callable, Mapping, Protocol, Sequence, TextIO

from pyiceberg.catalog import load_catalog

from analysis.report import TableHealthReport
from configuration import (
    AnalyzerConfiguration,
    GlueCatalogTableSourceConfiguration,
    RuntimePolicy,
)


@dataclass(frozen=True)
class OperatorCatalogOverview:
    rows: tuple[Mapping[str, object], ...]
    failures: tuple[Mapping[str, str], ...] = ()
    cache_status: str = "fresh"
    last_analyzed_at: datetime | None = None

    @property
    def table_count(self) -> int:
        return len(self.rows)

    @property
    def failure_count(self) -> int:
        return len(self.failures)


class OperatorCatalogOverviewCache(Protocol):
    def read(self, scope_key: str) -> object | None: ...

    def write(self, scope_key: str, overview: OperatorCatalogOverview) -> object: ...

    def invalidate(self, scope_key: str) -> None: ...


@dataclass
class _RunningAnalysis:
    index: int
    table_name: str
    process: multiprocessing.Process
    result_queue: multiprocessing.Queue
    started_at: multiprocessing.Value
    deadline: float | None = None


@dataclass
class OperatorCatalogWorkflow:
    list_catalog_tables: Callable[[], tuple[str, ...]]
    analyze_table: Callable[[str], TableHealthReport]
    runtime: RuntimePolicy = field(default_factory=RuntimePolicy)
    cache: OperatorCatalogOverviewCache | None = None
    cache_scope_key: str | None = None
    refresh: bool = False

    def load_overview(self) -> OperatorCatalogOverview:
        table_names = tuple(self.list_catalog_tables())
        scope_key = self.cache_scope_key or _cache_scope_key(table_names)
        if self.cache is not None:
            if self.refresh:
                self.cache.invalidate(scope_key)
            else:
                cached_overview = self.cache.read(scope_key)
                if cached_overview is not None:
                    return _operator_overview(cached_overview, cache_status="cached")

        max_workers = max(1, self.runtime.max_concurrency)
        results = []
        pending = list(enumerate(table_names))
        running: list[_RunningAnalysis] = []
        context = _process_context()

        while pending or running:
            while pending and len(running) < max_workers:
                index, table_name = pending.pop(0)
                running.append(
                    _start_analysis(
                        context,
                        index=index,
                        table_name=table_name,
                        analyze_table=self.analyze_table,
                        timeout_seconds=self.runtime.timeout_seconds,
                    )
                )

            completed = []
            now = monotonic()
            for analysis in running:
                if analysis.deadline is None and analysis.started_at.value > 0:
                    analysis.deadline = (
                        analysis.started_at.value + self.runtime.timeout_seconds
                    )
                result = _poll_completed_result(
                    analysis,
                    timeout_seconds=self.runtime.timeout_seconds,
                )
                if result is not None:
                    completed.append((analysis, result))
                    continue
                if analysis.deadline is None:
                    continue
                if analysis.process.is_alive() and now <= analysis.deadline:
                    continue
                completed.append((analysis, None))

            if not completed:
                sleep(0.01)
                continue

            for analysis, result in completed:
                running.remove(analysis)
                if result is not None:
                    analysis.process.join()
                    row, failure = result
                elif analysis.process.is_alive():
                    analysis.process.terminate()
                    analysis.process.join(1)
                    if analysis.process.is_alive():
                        analysis.process.kill()
                        analysis.process.join()
                    row, failure = _timeout_result(
                        analysis.table_name,
                        timeout_seconds=self.runtime.timeout_seconds,
                    )
                else:
                    analysis.process.join()
                    row, failure = _missing_result(
                        analysis,
                        timeout_seconds=self.runtime.timeout_seconds,
                    )
                _close_queue(analysis.result_queue)
                results.append((analysis.index, row, failure))

        rows = tuple(row for _, row, _ in sorted(results, key=lambda item: item[0]))
        failures = tuple(
            failure
            for _, _, failure in sorted(results, key=lambda item: item[0])
            if failure is not None
        )
        overview = OperatorCatalogOverview(rows=rows, failures=failures)
        if self.cache is None:
            return overview
        return _operator_overview(self.cache.write(scope_key, overview))

    def render_overview(self, overview: OperatorCatalogOverview) -> str:
        labels = tuple(label for label, _ in _OVERVIEW_COLUMNS)
        lines = [
            " | ".join(labels),
            " | ".join("---" for _ in labels),
        ]
        for row in overview.rows:
            lines.append(
                " | ".join(_format_cell(row.get(key)) for _, key in _OVERVIEW_COLUMNS)
            )
        return "\n".join(lines)

    def inspect_table(self, table_name: str) -> TableHealthReport:
        return self.analyze_table(table_name)

    def render_table_health_report(self, report: TableHealthReport) -> str:
        lines = [
            f"Table Health Report: {report.table_name}",
            f"Source: {report.table_source.kind} {report.table_source.location}",
            "",
            "Health Metrics",
        ]
        lines.extend(
            f"{metric.label}: {_format_metric_value(metric.value, metric.unit)}"
            for metric in report.health_metrics
        )
        lines.extend(("", "Display Statistics"))
        if report.display_statistics:
            lines.extend(
                f"{statistic.label}: {_format_cell(statistic.value)}"
                for statistic in report.display_statistics
            )
        else:
            lines.append("None")
        if report.partition_metrics:
            lines.extend(("", "Partition Metrics"))
            lines.append(
                "Partition | Data Files | Delete Files | Total Data Size | "
                "Average Data Size"
            )
            lines.append("--- | --- | --- | --- | ---")
            lines.extend(
                " | ".join(
                    (
                        _format_mapping(metric.partition),
                        str(metric.data_file_count),
                        str(metric.delete_file_count),
                        _format_cell(metric.total_data_file_size_bytes),
                        _format_cell(metric.average_data_file_size_bytes),
                    )
                )
                for metric in report.partition_metrics
            )
        lines.extend(("", "Table Evolution History"))
        changes = (
            *report.table_evolution_history.schema_changes,
            *report.table_evolution_history.property_changes,
        )
        if changes:
            lines.extend(
                f"{change.change_type}: {change.subject} {change.name} "
                f"{_format_cell(change.before)} -> {_format_cell(change.after)} "
                f"({change.source})"
                for change in changes
            )
        else:
            lines.append("No retained schema or table property changes found.")
        if report.maintenance_recommendations:
            lines.extend(("", "Maintenance Recommendations"))
            for recommendation in report.maintenance_recommendations:
                lines.append(
                    f"{recommendation.severity}: "
                    f"{_format_recommendation_type(recommendation.recommendation_type)} - "
                    f"{recommendation.rationale}"
                )
                lines.append(f"Evidence: {_format_mapping(recommendation.evidence)}")
                lines.append(
                    f"Thresholds: {_format_mapping(recommendation.thresholds)}"
                )
        if report.calculation_warnings:
            lines.extend(("", "Calculation Warnings"))
            lines.extend(
                f"{warning.metric_key}: {warning.message}"
                for warning in report.calculation_warnings
            )
        return "\n".join(lines)


def list_configured_catalog_tables(config: AnalyzerConfiguration) -> tuple[str, ...]:
    table_source = config.table_source
    if not isinstance(table_source, GlueCatalogTableSourceConfiguration):
        raise ValueError("Operator catalog overview requires a Catalog Table Source.")

    catalog = load_catalog(
        table_source.catalog_name,
        **_glue_catalog_properties(table_source),
    )
    return tuple(
        _format_catalog_identifier(table_identifier)
        for table_identifier in catalog.list_tables(table_source.namespace)
    )


def configured_operator_catalog_workflow(
    config: AnalyzerConfiguration,
    *,
    list_tables: Callable[[AnalyzerConfiguration], tuple[str, ...]] | None = None,
    analyze_config: Callable[[AnalyzerConfiguration], TableHealthReport] | None = None,
    cache: OperatorCatalogOverviewCache | None = None,
    refresh: bool = False,
) -> OperatorCatalogWorkflow:
    table_source = config.table_source
    if not isinstance(table_source, GlueCatalogTableSourceConfiguration):
        raise ValueError("Operator catalog overview requires a Catalog Table Source.")

    list_catalog_tables = list_tables or list_configured_catalog_tables
    analyze_catalog_config = analyze_config or _analyze_iceberg_table

    return OperatorCatalogWorkflow(
        list_catalog_tables=lambda: list_catalog_tables(config),
        analyze_table=lambda table_name: analyze_catalog_config(
            replace(
                config,
                table_source=_table_source_for_identifier(table_source, table_name),
            )
        ),
        runtime=config.runtime,
        cache=cache,
        cache_scope_key=_catalog_cache_scope_key(table_source),
        refresh=refresh,
    )


def run_operator_catalog_workflow(
    workflow: OperatorCatalogWorkflow,
    *,
    inspect_table: str | None = None,
    output: TextIO | None = None,
) -> int:
    stream = output or sys.stdout
    overview = workflow.load_overview()
    stream.write(workflow.render_overview(overview))
    stream.write("\n")
    if inspect_table is not None:
        report = workflow.inspect_table(inspect_table)
        stream.write("\n")
        stream.write(workflow.render_table_health_report(report))
        stream.write("\n")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lakehouse-health-operator",
        description="List configured catalog tables and inspect table health reports.",
    )
    parser.add_argument(
        "--inspect",
        metavar="TABLE",
        help="Render the detailed Table Health Report for a selected table.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Bypass and replace the cached catalog overview.",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable the DuckDB operator overview cache for this run.",
    )
    args = parser.parse_args(argv)

    config = AnalyzerConfiguration.from_environment()
    cache = None
    if not args.no_cache:
        from operator_cache import (
            CatalogOverviewCache,
            default_catalog_overview_cache_path,
        )

        cache = CatalogOverviewCache(
            default_catalog_overview_cache_path(),
            ttl_seconds=config.runtime.cache_ttl_seconds,
        )
    workflow = configured_operator_catalog_workflow(
        config,
        cache=cache,
        refresh=args.refresh,
    )
    return run_operator_catalog_workflow(
        workflow,
        inspect_table=args.inspect,
    )


_OVERVIEW_COLUMNS = (
    ("Table", "table"),
    ("Status", "status"),
    ("Cache", "cache_status"),
    ("Total Size", "total_file_size"),
    ("Data Files", "data_file_count"),
    ("Delete Files", "delete_file_count"),
    ("Records", "data_file_record_count"),
    ("Partitions", "partition_count"),
    ("Snapshot Age Days", "latest_snapshot_age_days"),
    ("Snapshots", "valid_snapshot_count"),
    ("Expirable Snapshots", "expirable_snapshot_candidate_count"),
    ("Warnings", "warning_count"),
    ("Info Recs", "info_recommendation_count"),
    ("Warning Recs", "warning_recommendation_count"),
    ("Critical Recs", "critical_recommendation_count"),
    ("Message", "message"),
)


def _process_context() -> multiprocessing.context.BaseContext:
    try:
        return multiprocessing.get_context("fork")
    except ValueError:
        return multiprocessing.get_context()


def _start_analysis(
    context: multiprocessing.context.BaseContext,
    *,
    index: int,
    table_name: str,
    analyze_table: Callable[[str], TableHealthReport],
    timeout_seconds: int | float,
) -> _RunningAnalysis:
    result_queue = context.Queue(maxsize=1)
    started_at = context.Value("d", 0.0)
    process = context.Process(
        target=_run_table_analysis,
        args=(analyze_table, table_name, result_queue, started_at),
    )
    process.start()
    return _RunningAnalysis(
        index=index,
        table_name=table_name,
        process=process,
        result_queue=result_queue,
        started_at=started_at,
    )


def _run_table_analysis(
    analyze_table: Callable[[str], TableHealthReport],
    table_name: str,
    result_queue: multiprocessing.Queue,
    started_at: multiprocessing.Value,
) -> None:
    signal.signal(signal.SIGTERM, _raise_terminated_analysis)
    started_at.value = monotonic()
    try:
        report = analyze_table(table_name)
        result_queue.put(("ok", (monotonic() - started_at.value, report)))
    except Exception as exc:
        result_queue.put(
            (
                "error",
                (monotonic() - started_at.value, str(exc) or exc.__class__.__name__),
            )
        )


def _raise_terminated_analysis(signum: int, frame: object) -> None:
    raise TimeoutError("table analysis terminated")


def _poll_completed_result(
    analysis: _RunningAnalysis, *, timeout_seconds: int | float
) -> tuple[Mapping[str, object], Mapping[str, str] | None] | None:
    try:
        status, payload = analysis.result_queue.get_nowait()
    except Empty:
        return None

    return _result_from_payload(
        analysis.table_name,
        status=status,
        payload=payload,
        timeout_seconds=timeout_seconds,
    )


def _result_from_payload(
    table_name: str,
    *,
    status: str,
    payload: tuple[float, object],
    timeout_seconds: int | float,
) -> tuple[Mapping[str, object], Mapping[str, str] | None]:
    elapsed_seconds, value = payload
    if elapsed_seconds > timeout_seconds:
        return _timeout_result(table_name, timeout_seconds=timeout_seconds)

    if status == "ok":
        return _overview_row(value, cache_status="fresh"), None

    message = str(value)
    return _failure_row(table_name, message=message), _failure(
        table_name, message=message
    )


def _missing_result(
    analysis: _RunningAnalysis, *, timeout_seconds: int | float
) -> tuple[Mapping[str, object], Mapping[str, str] | None]:
    try:
        status, payload = analysis.result_queue.get(timeout=0.1)
    except Empty:
        status = ""
        payload = None
    if payload is not None:
        return _result_from_payload(
            analysis.table_name,
            status=status,
            payload=payload,
            timeout_seconds=timeout_seconds,
        )

    message = (
        f"table analysis exited without a report "
        f"(exit code {analysis.process.exitcode})"
    )
    return _failure_row(analysis.table_name, message=message), _failure(
        analysis.table_name, message=message
    )


def _timeout_result(
    table_name: str, *, timeout_seconds: int | float
) -> tuple[Mapping[str, object], Mapping[str, str]]:
    message = f"table analysis timed out after {timeout_seconds:g}s"
    return _failure_row(table_name, message=message), _failure(
        table_name, message=message
    )


def _close_queue(result_queue: multiprocessing.Queue) -> None:
    result_queue.close()
    result_queue.join_thread()


def _overview_row(
    report: TableHealthReport, *, cache_status: str
) -> Mapping[str, object]:
    return {
        "table": report.table_name,
        "status": "loaded",
        "cache_status": cache_status,
        "total_file_size": _display_statistic_value(report, "total_file_size"),
        "data_file_count": _health_metric_value(report, "data_file_count"),
        "delete_file_count": _health_metric_value(report, "delete_file_count"),
        "data_file_record_count": _health_metric_value(
            report, "data_file_record_count"
        ),
        "partition_count": _health_metric_value(report, "partition_count"),
        "average_data_file_size": _display_statistic_value(
            report, "average_data_file_size"
        ),
        "latest_snapshot_age_days": _health_metric_value(
            report, "latest_snapshot_age_days"
        ),
        "valid_snapshot_count": _health_metric_value(report, "valid_snapshot_count"),
        "expirable_snapshot_candidate_count": _health_metric_value(
            report, "expirable_snapshot_candidate_count"
        ),
        "warning_count": len(report.calculation_warnings),
        "info_recommendation_count": _recommendation_count(report, "info"),
        "warning_recommendation_count": _recommendation_count(report, "warning"),
        "critical_recommendation_count": _recommendation_count(report, "critical"),
        "message": "",
    }


def _failure_row(table_name: str, *, message: str) -> Mapping[str, object]:
    return {
        "table": table_name,
        "status": "warning",
        "cache_status": "-",
        "total_file_size": None,
        "data_file_count": None,
        "delete_file_count": None,
        "data_file_record_count": None,
        "partition_count": None,
        "average_data_file_size": None,
        "latest_snapshot_age_days": None,
        "valid_snapshot_count": None,
        "expirable_snapshot_candidate_count": None,
        "warning_count": 1,
        "info_recommendation_count": 0,
        "warning_recommendation_count": 0,
        "critical_recommendation_count": 0,
        "message": message,
    }


def _failure(table_name: str, *, message: str) -> Mapping[str, str]:
    return {"table": table_name, "status": "warning", "message": message}


def _display_statistic_value(report: TableHealthReport, key: str) -> object:
    return report.display_statistic(key).value


def _health_metric_value(report: TableHealthReport, key: str) -> object:
    return report.health_metric(key).value


def _recommendation_count(report: TableHealthReport, severity: str) -> int:
    return sum(
        1
        for recommendation in report.maintenance_recommendations
        if recommendation.severity == severity
    )


def _cache_scope_key(table_names: tuple[str, ...]) -> str:
    return "\n".join(table_names)


def _operator_overview(
    overview: object, *, cache_status: str | None = None
) -> OperatorCatalogOverview:
    status = cache_status or getattr(overview, "cache_status", "fresh")
    return OperatorCatalogOverview(
        rows=tuple(
            _with_cache_status(row, status) for row in getattr(overview, "rows")
        ),
        failures=tuple(getattr(overview, "failures", ())),
        cache_status=status,
        last_analyzed_at=getattr(overview, "last_analyzed_at", None),
    )


def _with_cache_status(
    row: Mapping[str, object], cache_status: str
) -> Mapping[str, object]:
    return {**row, "cache_status": cache_status}


def _glue_catalog_properties(
    source: GlueCatalogTableSourceConfiguration,
) -> Mapping[str, str]:
    properties = {"type": "glue"}
    if source.aws_profile is not None:
        properties["glue.profile-name"] = source.aws_profile
    if source.region is not None:
        properties["glue.region"] = source.region
    return properties


def _format_catalog_identifier(table_identifier: object) -> str:
    if isinstance(table_identifier, str):
        return table_identifier
    return ".".join(str(part) for part in table_identifier)


def _analyze_iceberg_table(config: AnalyzerConfiguration) -> TableHealthReport:
    from analysis.iceberg import analyze_iceberg_table

    return analyze_iceberg_table(config)


def _table_source_for_identifier(
    base_source: GlueCatalogTableSourceConfiguration, table_identifier: str
) -> GlueCatalogTableSourceConfiguration:
    parts = tuple(part for part in table_identifier.split(".") if part)
    if len(parts) <= 1:
        return replace(base_source, table_name=table_identifier)
    return replace(base_source, namespace=parts[:-1], table_name=parts[-1])


def _catalog_cache_scope_key(source: GlueCatalogTableSourceConfiguration) -> str:
    return "|".join(
        (
            source.catalog_name,
            ".".join(source.namespace),
            source.aws_profile or "",
            source.region or "",
        )
    )


def _format_metric_value(value: object, unit: str | None) -> str:
    if value is None:
        return "unknown"
    if unit:
        return f"{value} {unit}"
    return str(value)


def _format_cell(value: object) -> str:
    if value is None:
        return "unknown"
    return str(value)


def _format_mapping(values: Mapping[str, object]) -> str:
    if not values:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in values.items())


def _format_recommendation_type(recommendation_type: str) -> str:
    return recommendation_type.replace("_", " ").title()
