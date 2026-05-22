from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Mapping, Sequence

from analysis.report import TableHealthReport
from configuration import OutputPolicy


def export_table_health_report(
    report: TableHealthReport,
    *,
    export_format: str,
    destination: str | Path,
    cache_status: str,
    analyzed_at: datetime,
) -> Path:
    normalized_format = export_format.strip().lower()
    if normalized_format not in {"json", "markdown"}:
        raise ValueError(f"Unsupported export format: {export_format}")

    output_path = Path(destination)
    parent = output_path.parent
    if not parent.exists():
        raise FileNotFoundError(
            f"Export destination directory does not exist: {parent}"
        )

    export_metadata = {
        "cache_status": cache_status,
        "analyzed_at": analyzed_at.isoformat(),
    }
    if normalized_format == "json":
        payload = _json_safe(asdict(report))
        payload["export_metadata"] = export_metadata
        output_path.write_text(json.dumps(payload, indent=2))
    else:
        output_path.write_text(_render_markdown_report(report, export_metadata))
    return output_path


def export_table_health_report_for_output_policy(
    report: TableHealthReport,
    *,
    output_policy: OutputPolicy,
    cache_status: str,
    analyzed_at: datetime,
) -> tuple[Path, ...]:
    if not output_policy.export_formats:
        return ()

    export_directory = Path(output_policy.export_directory or ".")
    export_prefix = _report_file_prefix(report.table_name)
    extension_by_format = {
        "json": ".json",
        "markdown": ".md",
    }
    planned_exports = tuple(
        (_normalize_export_format(export_format), export_format)
        for export_format in output_policy.export_formats
    )
    for normalized_format, export_format in planned_exports:
        if normalized_format not in extension_by_format:
            raise ValueError(f"Unsupported export format: {export_format}")

    written_paths: list[Path] = []
    for normalized_format, _ in planned_exports:
        extension = extension_by_format[normalized_format]
        destination = export_directory / f"{export_prefix}{extension}"
        written_paths.append(
            export_table_health_report(
                report,
                export_format=normalized_format,
                destination=destination,
                cache_status=cache_status,
                analyzed_at=analyzed_at,
            )
        )
    return tuple(written_paths)


def _render_markdown_report(
    report: TableHealthReport, export_metadata: dict[str, str]
) -> str:
    lines = [
        f"# Table Health Report: {report.table_name}",
        "",
        f"Source: {report.table_source.kind} {report.table_source.location}",
        f"Cache Status: {export_metadata['cache_status']}",
        f"Analyzed At: {export_metadata['analyzed_at']}",
        "",
        "## Health Metrics",
    ]
    lines.extend(
        f"- {metric.label}: {_format_metric_value(metric.value, metric.unit)}"
        for metric in report.health_metrics
    )
    lines.extend(("", "## Calculation Warnings"))
    if report.calculation_warnings:
        lines.extend(
            f"- {warning.metric_key}: {warning.message}"
            for warning in report.calculation_warnings
        )
    else:
        lines.append("- none")
    lines.extend(("", "## Maintenance Recommendations"))
    if report.maintenance_recommendations:
        lines.extend(
            f"- {recommendation.severity}: "
            f"{recommendation.recommendation_type.replace('_', ' ').title()} - "
            f"{recommendation.rationale}"
            for recommendation in report.maintenance_recommendations
        )
    else:
        lines.append("- none")
    return "\n".join(lines)


def _format_metric_value(value: object, unit: str | None) -> str:
    if value is None:
        return "unknown"
    if unit:
        return f"{value} {unit}"
    return str(value)


def _report_file_prefix(table_name: str) -> str:
    return table_name.replace(".", "-")


def _normalize_export_format(export_format: str) -> str:
    return export_format.strip().lower()


def _json_safe(value: object) -> object:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(nested) for key, nested in value.items()}
    if isinstance(value, tuple | list):
        return [_json_safe(nested) for nested in value]
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_json_safe(nested) for nested in value]
    return value
