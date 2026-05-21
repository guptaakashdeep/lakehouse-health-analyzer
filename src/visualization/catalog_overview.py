from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Mapping

from analysis.report import TableHealthReport

try:
    import streamlit as st
except ModuleNotFoundError:
    st = None


@dataclass(frozen=True)
class CatalogOverview:
    rows: tuple[Mapping[str, object], ...]
    failures: tuple[Mapping[str, str], ...] = ()

    @property
    def table_count(self) -> int:
        return len(self.rows)

    @property
    def failure_count(self) -> int:
        return len(self.failures)


def build_catalog_overview(
    table_names: tuple[str, ...],
    analyze_table: Callable[[str], TableHealthReport],
) -> CatalogOverview:
    reports = []
    failures = []
    for table_name in table_names:
        try:
            reports.append(analyze_table(table_name))
        except Exception as exc:
            failures.append(
                {
                    "table": table_name,
                    "status": "warning",
                    "message": str(exc),
                }
            )
    return CatalogOverview(
        rows=tuple(_overview_row(report) for report in reports),
        failures=tuple(failures),
    )


def display_catalog_overview(overview: CatalogOverview) -> None:
    if st is None:
        raise RuntimeError("Streamlit is required to render Catalog Overview.")

    st.metric("Analyzed Tables", overview.table_count)
    st.metric("Table Warnings", overview.failure_count)

    for failure in overview.failures:
        st.warning(f"{failure['table']}: {failure['message']}")

    if not overview.rows:
        st.info("No catalog tables were found.")
        return

    pd = _require_pandas()
    st.dataframe(pd.DataFrame(overview.rows), use_container_width=True)


def _overview_row(report: TableHealthReport) -> Mapping[str, object]:
    recommendation_counts = _recommendation_counts(report)
    return {
        "table": report.table_name,
        "status": "loaded",
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
        "warning_count": len(report.calculation_warnings),
        "info_recommendation_count": recommendation_counts["info"],
        "warning_recommendation_count": recommendation_counts["warning"],
        "critical_recommendation_count": recommendation_counts["critical"],
    }


def _display_statistic_value(report: TableHealthReport, key: str) -> object:
    return report.display_statistic(key).value


def _health_metric_value(report: TableHealthReport, key: str) -> object:
    return report.health_metric(key).value


def _recommendation_counts(report: TableHealthReport) -> Mapping[str, int]:
    return {
        severity: sum(
            1
            for recommendation in report.maintenance_recommendations
            if recommendation.severity == severity
        )
        for severity in ("info", "warning", "critical")
    }


def _require_pandas():
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise RuntimeError("pandas is required to render Catalog Overview.") from exc
    return pd
