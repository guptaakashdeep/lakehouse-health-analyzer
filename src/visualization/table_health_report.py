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

    if report.maintenance_recommendations:
        _display_maintenance_recommendations(report)

    _display_table_evolution_history(report)

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


def _display_maintenance_recommendations(report: TableHealthReport) -> None:
    st.subheader("Maintenance Recommendations")
    for recommendation in report.maintenance_recommendations:
        message = (
            f"{_format_recommendation_type(recommendation.recommendation_type)} "
            f"({recommendation.severity}): {recommendation.rationale}"
        )
        _recommendation_status(recommendation.severity)(message)
        st.caption(f"Evidence: {_format_mapping(recommendation.evidence)}")
        st.caption(f"Thresholds: {_format_mapping(recommendation.thresholds)}")


def _recommendation_status(severity: str):
    if severity == "critical":
        return st.error
    if severity == "warning":
        return st.warning
    return st.info


def _format_recommendation_type(recommendation_type: str) -> str:
    return recommendation_type.replace("_", " ").title()


def _format_mapping(values) -> str:
    return ", ".join(f"{key}={value}" for key, value in values.items())


def _display_table_evolution_history(report: TableHealthReport) -> None:
    st.subheader("Table Evolution History")
    changes = (
        *report.table_evolution_history.schema_changes,
        *report.table_evolution_history.property_changes,
    )
    if not changes:
        st.write("No retained schema or table property changes found.")
        return

    pd = _require_pandas()
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "change_type": change.change_type,
                    "subject": change.subject,
                    "name": change.name,
                    "before": change.before,
                    "after": change.after,
                    "source": change.source,
                }
                for change in changes
            ],
            dtype=object,
        ),
        use_container_width=True,
    )


def _require_pandas():
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "pandas is required to render Table Evolution History."
        ) from exc
    return pd
