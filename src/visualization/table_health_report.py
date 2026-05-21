"""Streamlit rendering for canonical Table Health Reports."""

try:
    import streamlit as st
except ModuleNotFoundError:
    st = None

from analysis.report import TableHealthReport
from visualization.components.partition_metrics import display_partition_metrics


def display_table_health_report(report: TableHealthReport) -> None:
    """Render a canonical report without deriving health calculations."""
    if st is None:
        raise RuntimeError("Streamlit is required to render Table Health Reports.")

    st.header(f"Table: {report.table_name}")

    st.subheader("Health Metrics")
    for metric in report.health_metrics:
        st.metric(metric.label, _format_metric_value(metric.value, metric.unit))

    st.subheader("Display Statistics")
    for statistic in report.display_statistics:
        st.metric(statistic.label, _format_display_value(statistic.value))

    if report.partition_metrics:
        display_partition_metrics(report)

    if report.calculation_warnings:
        st.subheader("Calculation Warnings")
        for warning in report.calculation_warnings:
            st.warning(f"{warning.metric_key}: {warning.message}")


def _format_metric_value(value, unit):
    if value is None:
        return "Unknown"
    if unit:
        return f"{value} {unit}"
    return str(value)


def _format_display_value(value):
    if value is None:
        return "Unknown"
    return str(value)
