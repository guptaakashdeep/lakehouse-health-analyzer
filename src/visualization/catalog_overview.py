from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Mapping, Protocol

from analysis.report import TableHealthReport

try:
    import streamlit as st
except ModuleNotFoundError:
    st = None


@dataclass(frozen=True)
class CatalogOverview:
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


class CatalogOverviewCacheBackend(Protocol):
    def read(self, scope_key: str) -> CatalogOverview | None: ...

    def write(self, scope_key: str, overview: CatalogOverview) -> CatalogOverview: ...

    def invalidate(self, scope_key: str) -> None: ...


def build_catalog_overview(
    table_names: tuple[str, ...],
    analyze_table: Callable[[str], TableHealthReport],
    *,
    cache: CatalogOverviewCacheBackend | None = None,
    cache_scope_key: str | None = None,
    refresh: bool = False,
) -> CatalogOverview:
    if cache is not None:
        scope_key = cache_scope_key or _cache_scope_key(table_names)
        if refresh:
            cache.invalidate(scope_key)
        else:
            cached_overview = cache.read(scope_key)
            if cached_overview is not None:
                return cached_overview

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
    overview = CatalogOverview(
        rows=tuple(_overview_row(report) for report in reports),
        failures=tuple(failures),
    )
    if cache is not None:
        return cache.write(scope_key, overview)
    return overview


def display_catalog_overview(overview: CatalogOverview) -> None:
    if st is None:
        raise RuntimeError("Streamlit is required to render Catalog Overview.")

    st.metric("Analyzed Tables", overview.table_count)
    st.metric("Table Warnings", overview.failure_count)
    st.metric("Cache Status", overview.cache_status)
    if overview.last_analyzed_at is not None:
        st.metric("Last Analyzed", overview.last_analyzed_at.isoformat())

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


def _cache_scope_key(table_names: tuple[str, ...]) -> str:
    return "\n".join(table_names)


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
