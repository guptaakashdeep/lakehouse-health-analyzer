"""Streamlit rendering for canonical Table Health Reports."""

try:
    import streamlit as st
except ModuleNotFoundError:
    st = None

from analysis.report import TableHealthReport
from workflows.table_detail import TableDetailItem, TableDetailSection, TableDetailView, table_detail_view


def display_table_health_report(report: TableHealthReport) -> None:
    """Render a canonical report via the shared table-detail view model."""
    display_table_detail_view(table_detail_view(report))


def display_table_detail_view(detail_view: TableDetailView) -> None:
    """Render a prepared shared table-detail view model."""
    if st is None:
        raise RuntimeError("Streamlit is required to render Table Health Reports.")

    st.header(f"Table: {detail_view.table_name}")
    for section in detail_view.sections:
        _display_section(section)


def _display_section(section: TableDetailSection) -> None:
    st.subheader(section.title)

    if section.key == "recommendations":
        _display_recommendation_items(section.items)
        return
    if section.key == "warnings":
        _display_warning_items(section.items)
        return
    if section.key == "partitions":
        _display_partition_items(section.items)
        return
    if section.key == "metadata_and_evolution":
        _display_evolution_items(section.items)
        return

    _display_metric_items(section.items)


def _display_recommendation_items(items: tuple[TableDetailItem, ...]) -> None:
    for item in items:
        severity = str(item.primary.get("severity", "info"))
        recommendation_type = str(item.primary.get("type", "recommendation"))
        rationale = str(item.primary.get("rationale", ""))
        message = (
            f"{_format_recommendation_type(recommendation_type)} "
            f"({severity}): {rationale}"
        )
        _recommendation_status(severity)(message)

        evidence = item.secondary.get("evidence")
        if evidence:
            st.caption(f"Evidence: {_format_mapping(evidence)}")
        thresholds = item.secondary.get("thresholds")
        if thresholds:
            st.caption(f"Thresholds: {_format_mapping(thresholds)}")


def _display_warning_items(items: tuple[TableDetailItem, ...]) -> None:
    for item in items:
        metric_key = item.primary.get("metric_key", item.key)
        message = item.primary.get("message", "")
        st.warning(f"{metric_key}: {message}")


def _display_metric_items(items: tuple[TableDetailItem, ...]) -> None:
    for item in items:
        label = item.primary.get("label")
        value = item.primary.get("value")
        if label is None:
            continue
        st.metric(str(label), _format_value(value, item.primary.get("unit")))


def _display_partition_items(items: tuple[TableDetailItem, ...]) -> None:
    metric_items = tuple(item for item in items if not item.key.startswith("partition:"))
    _display_metric_items(metric_items)

    partition_rows = []
    for item in items:
        if not item.key.startswith("partition:"):
            continue
        partition = item.primary.get("partition")
        row = dict(partition) if isinstance(partition, dict) else {"partition": partition}
        row.update(
            {
                "data_file_count": item.primary.get("data_file_count"),
                "delete_file_count": item.primary.get("delete_file_count"),
                "total_data_file_size_bytes": item.primary.get(
                    "total_data_file_size_bytes"
                ),
                "average_data_file_size_bytes": item.primary.get(
                    "average_data_file_size_bytes"
                ),
            }
        )
        partition_rows.append(row)
    if partition_rows:
        pd = _require_pandas()
        st.dataframe(pd.DataFrame(partition_rows, dtype=object), use_container_width=True)


def _recommendation_status(severity: str):
    if severity == "critical":
        return st.error
    if severity == "warning":
        return st.warning
    return st.info


def _format_recommendation_type(recommendation_type: str) -> str:
    return recommendation_type.replace("_", " ").title()


def _format_value(value, unit):
    if value is None:
        return "Unknown"
    if unit:
        return f"{value} {unit}"
    return str(value)


def _format_mapping(values) -> str:
    return ", ".join(f"{key}={value}" for key, value in values.items())


def _display_evolution_items(items: tuple[TableDetailItem, ...]) -> None:
    if not items:
        st.write("No retained schema or table property changes found.")
        return

    rows = []
    for item in items:
        rows.append(
            {
                "change_type": item.primary.get("change_type"),
                "subject": item.primary.get("subject"),
                "name": item.primary.get("name"),
                "before": item.primary.get("before"),
                "after": item.primary.get("after"),
                "source": item.secondary.get("source"),
            }
        )

    pd = _require_pandas()
    st.dataframe(pd.DataFrame(rows, dtype=object), use_container_width=True)


def _require_pandas():
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "pandas is required to render Table Evolution History."
        ) from exc
    return pd
