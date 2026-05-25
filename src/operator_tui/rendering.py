from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from rich.console import Group, RenderableType
from rich.panel import Panel
from rich.text import Text

from analysis.report import TableHealthReport
from workflows.table_detail import (
    TableDetailItem,
    TableDetailSection,
    table_detail_view,
)

from .theme import semantic_status

BG = "#101316"
PANEL = "#171d22"
PANEL_STRONG = "#1c242a"
PANEL_SOFT = "#151b20"
LINE = "#2c3940"
LINE_BRIGHT = "#47606a"
TEXT = "#e7eff2"
TEXT_SOFT = "#aab8bd"
MUTED = "#74868e"
CYAN = "#42d9d1"
BLUE = "#74a7ff"
GREEN = "#72dc8f"
AMBER = "#f3c85f"
RED = "#ff6b7d"
ORANGE = "#ff9f43"
VIOLET = "#c99cff"
SECONDARY = "#9fb7c3"

STATUS_COLORS = {
    "semantic-critical": RED,
    "semantic-warning": AMBER,
    "semantic-info": BLUE,
    "semantic-healthy": GREEN,
    "semantic-unknown": TEXT_SOFT,
    "semantic-cached": SECONDARY,
    "semantic-stale": AMBER,
}

SECTION_COLORS = {
    "recommendations": CYAN,
    "warnings": AMBER,
    "files": BLUE,
    "records": GREEN,
    "partitions": VIOLET,
    "snapshots": ORANGE,
    "metadata_and_evolution": VIOLET,
    "source_and_runtime": SECONDARY,
}


def render_header_chrome(
    *,
    catalog_name: str,
    profile: str,
    default_chain: str,
    region: str,
    namespace: str,
    freshness: str,
) -> RenderableType:
    header = Text()
    header.append("lh", style=f"bold {CYAN} on #1e262c")
    header.append("  ")
    header.append("Lakehouse Health Analyzer", style=f"bold {TEXT}")
    header.append("  ")
    header.append(
        "AWS Glue catalog browser - explicit analysis on selection", style=TEXT_SOFT
    )
    header.append("\n")
    for label, value in (
        ("Catalog", catalog_name),
        ("Profile", profile),
        ("Chain", default_chain),
        ("Region", region),
        ("Database", namespace),
    ):
        header.append(f" {label} ", style=f"{TEXT_SOFT} on #202a30")
        header.append(f"{value} ", style=f"bold {TEXT} on #202a30")
        header.append(" ")
    header.append(_badge(freshness))
    return header


def render_report_detail(
    report: TableHealthReport,
    *,
    cache_status: str = "fresh",
) -> RenderableType:
    if report_has_no_data(report):
        return _no_data_report_detail(report, cache_status=cache_status)
    detail_view = table_detail_view(report)
    recommendations = detail_view.section("recommendations").items
    warnings = detail_view.section("warnings").items
    recommendation_count = len(recommendations)
    warning_count = len(warnings)
    metric_count = sum(
        len(section.items)
        for section in detail_view.sections
        if section.key not in {"recommendations", "warnings"}
    )
    panels: list[RenderableType] = [
        _summary_panel(
            report,
            cache_status=cache_status,
            recommendation_count=recommendation_count,
            warning_count=warning_count,
            metric_count=metric_count,
        )
    ]
    panels.extend(_section_panel(section) for section in detail_view.sections)
    group = Group(*panels)
    group.spans = _panel_text_spans(panels)  # type: ignore[attr-defined]
    return group


def report_has_no_data(report: TableHealthReport) -> bool:
    return (
        _health_metric_value(report, "valid_snapshot_count") == 0
        and _health_metric_value(report, "data_file_count") == 0
        and _health_metric_value(report, "delete_file_count") == 0
    )


def _health_metric_value(report: TableHealthReport, key: str) -> object:
    try:
        return report.health_metric(key).value
    except KeyError:
        return None


def render_detail_state(
    title: str,
    message: str | Sequence[str],
    *,
    status: str = "info",
) -> RenderableType:
    lines = _message_lines(message)
    body = Text()
    body.append(title, style=f"bold {TEXT}")
    for line in lines:
        body.append("\n")
        body.append(line, style=TEXT_SOFT)
    return _framed(body, border_style=_status_color(status))


def render_warning_state(title: str, warnings: Iterable[str]) -> RenderableType:
    rows = [title, *[f"- {warning}" for warning in warnings]]
    return render_detail_state(title, rows[1:] or "No warnings.", status="warning")


def render_unsupported_detail(
    table_identifier: str, *, message: str = ""
) -> RenderableType:
    lines = [
        f"{table_identifier} is NON-ICEBERG and unsupported.",
        "Use Left to pick another table or r to refresh and retry analysis.",
    ]
    if message:
        lines.append(message)
    return render_detail_state("Unsupported table", lines, status="critical")


def render_export_offer(offer: str) -> RenderableType:
    return render_detail_state("Export report", offer.splitlines(), status="info")


def render_export_success(written_paths: Sequence[object]) -> RenderableType:
    lines = ["Exported report to:"]
    lines.extend(f"- {path}" for path in written_paths)
    return render_detail_state("Export complete", lines, status="healthy")


def render_export_failure(message: str) -> RenderableType:
    return render_detail_state("Export failed", message, status="critical")


def _panel_text_spans(panels: Sequence[RenderableType]) -> tuple[object, ...]:
    spans = []
    for panel in panels:
        renderable = getattr(panel, "renderable", None)
        if isinstance(renderable, Text):
            spans.extend(renderable.spans)
    return tuple(spans)


def _no_data_report_detail(
    report: TableHealthReport,
    *,
    cache_status: str,
) -> RenderableType:
    body = Text()
    body.append(report.table_name, style=f"bold {TEXT}")
    body.append("  ")
    body.append(_badge("ICEBERG"))
    body.append("  ")
    body.append(_badge("NO DATA"))
    body.append("  ")
    body.append(_badge(cache_status))
    body.append("\n")
    body.append("Table has no snapshots.", style=f"bold {TEXT_SOFT}")
    body.append("\n")
    body.append(
        "This is a valid Iceberg table, but no snapshot-backed data files "
        "are available to analyze yet.",
        style=TEXT_SOFT,
    )
    return _framed(
        body,
        title="TABLE SUMMARY",
        border_style=CYAN,
        padding=(0, 1),
    )


def _summary_panel(
    report: TableHealthReport,
    *,
    cache_status: str,
    recommendation_count: int,
    warning_count: int,
    metric_count: int,
) -> Panel:
    body = Text()
    body.append(report.table_name, style=f"bold {TEXT}")
    body.append("  ")
    body.append(_badge("ICEBERG"))
    body.append("  ")
    body.append(_badge(cache_status))
    body.append("\n")
    body.append(
        f"{report.table_source.kind} {report.table_source.location}",
        style=TEXT_SOFT,
    )
    body.append("\n")
    body.append(
        f"{recommendation_count} recommendations",
        style=CYAN if recommendation_count else MUTED,
    )
    body.append(" | ", style=MUTED)
    body.append(f"{warning_count} warnings", style=AMBER if warning_count else MUTED)
    body.append(" | ", style=MUTED)
    body.append(f"{metric_count} metrics", style=GREEN if metric_count else MUTED)
    return _framed(
        body,
        title="TABLE SUMMARY",
        border_style=CYAN,
        padding=(0, 1),
    )


def _section_panel(section: TableDetailSection) -> Panel:
    section_color = _section_color(section.key)
    body = Text()
    _append_section_content(body, section, section_color=section_color)
    return _framed(
        body,
        title=f"{section.title.upper()}  {_count_label(section.items)}",
        border_style=section_color,
        padding=(0, 1),
    )


def _append_section(body: Text, section: TableDetailSection) -> None:
    section_color = _section_color(section.key)
    body.append("\n\n")
    body.append(section.title.upper(), style=f"bold {section_color}")
    body.append(f"  {_count_label(section.items)}", style=MUTED)
    body.append("\n")
    _append_section_content(body, section, section_color=section_color)


def _append_section_content(
    body: Text,
    section: TableDetailSection,
    *,
    section_color: str,
) -> None:
    if section.key == "recommendations":
        _append_recommendations(body, section.items)
        return
    if section.key == "warnings":
        _append_warnings(body, section.items)
        return
    _append_metrics(body, section.items, section_color=section_color)


def _append_recommendations(body: Text, items: tuple[TableDetailItem, ...]) -> None:
    if not items:
        body.append("No maintenance recommendations.", style=TEXT_SOFT)
        return
    for index, item in enumerate(items):
        if index:
            body.append("\n")
        severity = str(item.primary.get("severity", "info"))
        recommendation_type = _humanize(item.primary.get("type", "recommendation"))
        severity_color = _status_color(severity)
        body.append("  ")
        body.append(_badge(severity))
        body.append(" ")
        body.append(recommendation_type, style=f"bold {severity_color}")
        rationale = str(item.primary.get("rationale", ""))
        if rationale:
            body.append("\n    ")
            body.append(rationale, style=TEXT_SOFT)
        evidence = _flatten_mapping(item.secondary.get("evidence", {}))
        thresholds = _flatten_mapping(item.secondary.get("thresholds", {}))
        if evidence:
            body.append("\n    Evidence", style=f"bold {BLUE}")
            _append_pairs(body, evidence, indent=6, label_style=BLUE, value_style=TEXT)
        if thresholds:
            body.append("\n    Thresholds", style=f"bold {AMBER}")
            _append_pairs(
                body, thresholds, indent=6, label_style=AMBER, value_style=TEXT
            )


def _append_warnings(body: Text, items: tuple[TableDetailItem, ...]) -> None:
    if not items:
        body.append("No calculation warnings.", style=TEXT_SOFT)
        return
    for index, item in enumerate(items):
        if index:
            body.append("\n")
        body.append("  ")
        body.append(_badge("warning"))
        body.append(" ")
        body.append(
            _humanize(item.primary.get("metric_key", item.key)),
            style=f"bold {AMBER}",
        )
        body.append("\n    ")
        body.append(str(item.primary.get("message", "")), style=TEXT_SOFT)


def _append_metrics(
    body: Text,
    items: tuple[TableDetailItem, ...],
    *,
    section_color: str,
) -> None:
    if not items:
        body.append("No data available.", style=TEXT_SOFT)
        return
    labels = tuple(_metric_label(item, items) for item in items)
    label_width = min(max(len(label) for label in labels), 36)
    for item, raw_label in zip(items, labels):
        label = _ellipsize(raw_label, label_width)
        body.append(f"  {label:<{label_width}}  ", style=f"bold {section_color}")
        body.append(_primary_value(item), style=f"bold {_metric_value_color(item)}")
        body.append("\n")


def _metric_label(item: TableDetailItem, items: tuple[TableDetailItem, ...]) -> str:
    label = str(item.primary.get("label", item.label))
    if (
        sum(
            1
            for other in items
            if str(other.primary.get("label", other.label)) == label
        )
        == 1
    ):
        return label
    unit = item.primary.get("unit")
    if unit:
        return f"{label} ({unit})"
    return f"{label} (Readable)"


def _primary_value(item: TableDetailItem) -> str:
    primary = item.primary
    if "value" in primary:
        unit = primary.get("unit")
        return _value_with_unit(primary.get("value"), unit)
    if "location" in primary:
        return str(primary.get("location") or "unknown")
    if "analysis_status" in primary:
        return str(primary["analysis_status"]).upper()
    if "table_format" in primary:
        return str(primary["table_format"])
    if "partition" in primary:
        partition = primary.get("partition")
        files = primary.get("data_file_count")
        deletes = primary.get("delete_file_count")
        return f"{_format_value(partition)} | files {files} | deletes {deletes}"
    if "change_type" in primary:
        return " -> ".join(
            part
            for part in (
                str(primary.get("subject", "")),
                str(primary.get("name", "")),
                str(primary.get("change_type", "")),
            )
            if part
        )
    if "kind" in primary:
        return str(primary["kind"])
    return _format_value(primary)


def _metric_value_color(item: TableDetailItem) -> str:
    key = item.key.lower()
    if "warning" in key or "high" in key or "skew" in key:
        return AMBER
    if "delete" in key:
        return BLUE
    if item.label.lower().startswith("table format"):
        return GREEN
    return TEXT


def _badge(raw_status: object) -> Text:
    status = semantic_status(raw_status)
    color = STATUS_COLORS.get(status.css_class, TEXT_SOFT)
    return Text(f" {status.label} ", style=f"bold {color} on #202a30")


def _flatten_mapping(value: object) -> tuple[tuple[str, Any], ...]:
    if not isinstance(value, Mapping):
        return ()
    return tuple((_humanize(key), _format_value(item)) for key, item in value.items())


def _append_pairs(
    body: Text,
    pairs: Sequence[tuple[str, Any]],
    *,
    indent: int,
    label_style: str = MUTED,
    value_style: str = TEXT_SOFT,
) -> None:
    if not pairs:
        return
    label_width = max(len(key) for key, _value in pairs)
    prefix = " " * indent
    for key, value in pairs:
        label = _ellipsize(key, label_width)
        body.append("\n")
        body.append(f"{prefix}{label:<{label_width}}  ", style=label_style)
        body.append(str(value), style=value_style)


def _framed(
    renderable: RenderableType,
    *,
    border_style: str = LINE,
    padding: tuple[int, int] = (0, 0),
    title: str | None = None,
) -> Panel:
    return Panel(
        renderable,
        title=title,
        title_align="left",
        border_style=border_style,
        style=f"on {PANEL}",
        padding=padding,
    )


def _status_color(status: str) -> str:
    semantic = semantic_status(status)
    return STATUS_COLORS.get(semantic.css_class, TEXT_SOFT)


def _section_color(section_key: str) -> str:
    return SECTION_COLORS.get(section_key, TEXT)


def _message_lines(message: str | Sequence[str]) -> list[str]:
    if isinstance(message, str):
        return message.splitlines() or [message]
    return [str(line) for line in message]


def _count_label(items: Sequence[object]) -> str:
    count = len(items)
    if count == 1:
        return "1 ITEM"
    return f"{count} ITEMS"


def _humanize(value: object) -> str:
    return str(value or "unknown").replace("_", " ").replace("-", " ").title()


def _ellipsize(value: str, width: int) -> str:
    if width <= 1 or len(value) <= width:
        return value
    return f"{value[: width - 1]}…"


def _value_with_unit(value: object, unit: object) -> str:
    formatted = _format_value(value)
    if unit:
        return f"{formatted} {unit}"
    return formatted


def _format_value(value: object) -> str:
    if value is None:
        return "UNKNOWN"
    if isinstance(value, float):
        return f"{value:,.2f}".rstrip("0").rstrip(".")
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return ", ".join(f"{key}={item}" for key, item in value.items())
    if isinstance(value, (list, tuple, set)):
        return ", ".join(str(item) for item in value)
    return str(value)
