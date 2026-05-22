from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Callable, Sequence, TextIO

from analysis.iceberg import analyze_iceberg_table
from analysis.report import TableHealthReport
from configuration import (
    AnalyzerConfiguration,
    GlueCatalogTableSourceConfiguration,
    OutputPolicy,
)
from report_exports import export_table_health_report
from report_exports import export_table_health_report_for_output_policy
from report_exports import render_table_health_report
from workflows.errors import UnsupportedTableError


@dataclass(frozen=True)
class ReportCommandWorkflow:
    base_config: AnalyzerConfiguration
    analyze_config: Callable[[AnalyzerConfiguration], TableHealthReport] = (
        analyze_iceberg_table
    )

    def analyze_identifier(self, table_identifier: str) -> TableHealthReport:
        table_source = self.base_config.table_source
        if not isinstance(table_source, GlueCatalogTableSourceConfiguration):
            raise ValueError("Report command requires a Catalog Table Source.")
        namespace, table_name = _namespace_and_table_name(table_identifier)
        return self.analyze_config(
            replace(
                self.base_config,
                table_source=replace(
                    table_source,
                    namespace=namespace,
                    table_name=table_name,
                ),
            )
        )


def configured_report_command_workflow(
    config: AnalyzerConfiguration,
    *,
    analyze_config: Callable[[AnalyzerConfiguration], TableHealthReport] | None = None,
) -> ReportCommandWorkflow:
    return ReportCommandWorkflow(
        base_config=config,
        analyze_config=analyze_config or analyze_iceberg_table,
    )


def run_report_command_workflow(
    workflow: ReportCommandWorkflow,
    *,
    table_identifier: str,
    formats: Sequence[str] = ("json",),
    output: TextIO,
    error: TextIO,
    output_path: str | None = None,
    output_directory: str | None = None,
    configured_output_directory: str | None = None,
    configured_formats: Sequence[str] = (),
    now: Callable[[], datetime] | None = None,
) -> int:
    normalized_formats = tuple(_normalize_format(value) for value in formats)
    if not normalized_formats:
        normalized_formats = tuple(
            _normalize_format(value) for value in configured_formats
        )
    if not normalized_formats:
        normalized_formats = ("json",)

    try:
        _validate_destination_options(
            formats=normalized_formats,
            output_path=output_path,
            output_directory=output_directory,
            configured_output_directory=configured_output_directory,
        )
        report = workflow.analyze_identifier(table_identifier)
    except ValueError as exc:
        error.write(f"{exc}\n")
        return 2
    except UnsupportedTableError as exc:
        error.write(f"unsupported table: {table_identifier}: {exc}\n")
        return 1
    except Exception as exc:
        if _is_unsupported_error(exc):
            error.write(f"unsupported table: {table_identifier}: {exc}\n")
        else:
            error.write(f"analysis failed: {table_identifier}: {exc}\n")
        return 1

    analyzed_at = (now or (lambda: datetime.now(timezone.utc)))()
    if output_path is not None:
        export_table_health_report(
            report,
            export_format=normalized_formats[0],
            destination=output_path,
            cache_status="fresh",
            analyzed_at=analyzed_at,
        )
        return 0

    destination_directory = output_directory
    if destination_directory is None and len(normalized_formats) > 1:
        destination_directory = configured_output_directory
    if destination_directory is not None:
        export_table_health_report_for_output_policy(
            report,
            output_policy=OutputPolicy(
                export_formats=normalized_formats,
                export_directory=destination_directory,
            ),
            cache_status="fresh",
            analyzed_at=analyzed_at,
        )
        return 0

    rendered = render_table_health_report(
        report,
        export_format=normalized_formats[0],
        cache_status="fresh",
        analyzed_at=analyzed_at,
    )
    output.write(rendered)
    if not rendered.endswith("\n"):
        output.write("\n")
    return 0


def parse_report_formats(raw_value: str | None) -> tuple[str, ...]:
    if raw_value is None:
        return ()
    parsed = tuple(part.strip() for part in raw_value.split(",") if part.strip())
    if not parsed:
        return ("json",)
    return parsed


def _validate_destination_options(
    *,
    formats: tuple[str, ...],
    output_path: str | None,
    output_directory: str | None,
    configured_output_directory: str | None,
) -> None:
    if output_path is not None and output_directory is not None:
        raise ValueError("--output and --output-dir cannot be used together.")
    if output_path is not None and len(formats) != 1:
        raise ValueError("--output supports only one format.")
    if len(formats) > 1 and not (
        output_directory is not None or configured_output_directory
    ):
        raise ValueError(
            "Multiple formats require --output-dir or a configured export directory."
        )
    for export_format in formats:
        if _normalize_format(export_format) not in {"json", "markdown"}:
            raise ValueError(f"Unsupported export format: {export_format}")


def _normalize_format(export_format: str) -> str:
    return export_format.strip().lower()


def _is_unsupported_error(exc: Exception) -> bool:
    try:
        from pyiceberg.exceptions import NoSuchIcebergTableError
    except Exception:  # pragma: no cover - fallback when pyiceberg is unavailable
        NoSuchIcebergTableError = ()  # type: ignore[assignment]
    if NoSuchIcebergTableError and isinstance(exc, NoSuchIcebergTableError):
        return True
    message = str(exc).lower()
    return "not an iceberg table" in message or "not a valid iceberg table" in message


def _namespace_and_table_name(table_identifier: str) -> tuple[tuple[str, ...], str]:
    raw_parts = tuple(table_identifier.split("."))
    if len(raw_parts) < 2 or any(not part for part in raw_parts):
        raise ValueError(
            "Table identifier must include namespace and table name "
            "(for example: sales.orders)."
        )
    return raw_parts[:-1], raw_parts[-1]
