from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import duckdb

from analysis.report import (
    CalculationWarning,
    DisplayStatistic,
    EvolutionChange,
    HealthMetric,
    MaintenanceRecommendation,
    PartitionHealthMetric,
    TableEvolutionHistory,
    TableHealthReport,
    TableSource,
)
from visualization.catalog_overview import CatalogOverview
from workflows.catalog_browser import (
    CatalogBrowserFailure,
    CatalogNamespace,
    CatalogNamespaceListing,
    CatalogTableListing,
    CatalogTableRow,
    TableFormatClassification,
)
from workflows.table_detail import TableDetailResult, table_detail_view


def default_catalog_overview_cache_path() -> Path:
    cache_home = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
    return cache_home / "lakehouse-health-analyzer" / "catalog-overview.duckdb"


class CatalogOverviewCache:
    def __init__(
        self,
        path: str | Path,
        *,
        ttl_seconds: int = 900,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(path)
        self.ttl_seconds = ttl_seconds
        self._now = now or (lambda: datetime.now(timezone.utc))

    def read(self, scope_key: str) -> CatalogOverview | None:
        try:
            with self._connect() as connection:
                self._ensure_schema(connection)
                result = connection.execute(
                    """
                    SELECT rows_json, failures_json, analyzed_at
                    FROM catalog_overview_cache
                    WHERE scope_key = ?
                    """,
                    [scope_key],
                ).fetchone()
            if result is None:
                return None

            rows_json, failures_json, analyzed_at = result
            last_analyzed_at = datetime.fromisoformat(analyzed_at)
            if (self._now() - last_analyzed_at).total_seconds() > self.ttl_seconds:
                return None

            return CatalogOverview(
                rows=tuple(json.loads(rows_json)),
                failures=tuple(json.loads(failures_json)),
                cache_status="cached",
                last_analyzed_at=last_analyzed_at,
            )
        except (duckdb.Error, OSError, json.JSONDecodeError, ValueError):
            return None

    def write(self, scope_key: str, overview: CatalogOverview) -> CatalogOverview:
        analyzed_at = overview.last_analyzed_at or self._now()
        fresh_overview = replace(
            overview, cache_status="fresh", last_analyzed_at=analyzed_at
        )
        try:
            with self._connect() as connection:
                self._ensure_schema(connection)
                connection.execute(
                    """
                    INSERT OR REPLACE INTO catalog_overview_cache
                        (scope_key, rows_json, failures_json, analyzed_at, written_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        scope_key,
                        json.dumps(overview.rows),
                        json.dumps(overview.failures),
                        analyzed_at.isoformat(),
                        self._now().isoformat(),
                    ],
                )
        except (duckdb.Error, OSError):
            return fresh_overview
        return fresh_overview

    def invalidate(self, scope_key: str) -> None:
        try:
            with self._connect() as connection:
                self._ensure_schema(connection)
                connection.execute(
                    "DELETE FROM catalog_overview_cache WHERE scope_key = ?",
                    [scope_key],
                )
        except (duckdb.Error, OSError):
            return

    def _connect(self) -> duckdb.DuckDBPyConnection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(self.path))

    def _ensure_schema(self, connection: duckdb.DuckDBPyConnection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS catalog_overview_cache (
                scope_key VARCHAR PRIMARY KEY,
                rows_json VARCHAR NOT NULL,
                failures_json VARCHAR NOT NULL,
                analyzed_at VARCHAR NOT NULL,
                written_at VARCHAR NOT NULL
            )
            """
        )


@dataclass(frozen=True)
class CachedTableClassification:
    table_format: TableFormatClassification
    classification_source: str
    cache_status: str
    cached_at: datetime


class OperatorCache:
    def __init__(
        self,
        path: str | Path,
        *,
        ttl_seconds: int = 900,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(path)
        self.ttl_seconds = ttl_seconds
        self._now = now or (lambda: datetime.now(timezone.utc))

    def read_table_classification(
        self, table_identifier: str
    ) -> CachedTableClassification | None:
        cached = self._read_table_classification(table_identifier)
        if cached is None or self._is_expired(cached.cached_at):
            return None
        return replace(cached, cache_status="cached")

    def read_stale_table_classification(
        self, table_identifier: str
    ) -> CachedTableClassification | None:
        cached = self._read_table_classification(table_identifier)
        if cached is None:
            return None
        return replace(cached, cache_status="stale")

    def write_table_classification(
        self,
        table_identifier: str,
        table_format: TableFormatClassification,
        classification_source: str,
    ) -> CachedTableClassification:
        cached_at = self._now()
        try:
            with self._connect() as connection:
                self._ensure_schema(connection)
                connection.execute(
                    """
                    INSERT OR REPLACE INTO table_classification_cache
                        (
                            table_identifier,
                            table_format,
                            classification_source,
                            cached_at,
                            written_at
                        )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        table_identifier,
                        table_format.value,
                        classification_source,
                        cached_at.isoformat(),
                        self._now().isoformat(),
                    ],
                )
        except (duckdb.Error, OSError):
            pass
        return CachedTableClassification(
            table_format=table_format,
            classification_source=classification_source,
            cache_status="fresh",
            cached_at=cached_at,
        )

    def read_namespace_listing(self, scope_key: str) -> CatalogNamespaceListing | None:
        cached = self._read_namespace_listing(scope_key)
        if cached is None or self._is_expired(cached.last_refreshed_at):
            return None
        return replace(cached, cache_status="cached")

    def read_stale_namespace_listing(
        self, scope_key: str
    ) -> CatalogNamespaceListing | None:
        cached = self._read_namespace_listing(scope_key)
        if cached is None:
            return None
        return replace(cached, cache_status="stale")

    def write_namespace_listing(
        self, scope_key: str, listing: CatalogNamespaceListing
    ) -> CatalogNamespaceListing:
        refreshed_at = listing.last_refreshed_at or self._now()
        fresh_listing = replace(
            listing,
            cache_status="fresh",
            cache_message="",
            last_refreshed_at=refreshed_at,
        )
        try:
            with self._connect() as connection:
                self._ensure_schema(connection)
                connection.execute(
                    """
                    INSERT OR REPLACE INTO namespace_listing_cache
                        (
                            scope_key,
                            namespaces_json,
                            failures_json,
                            refreshed_at,
                            written_at
                        )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        scope_key,
                        json.dumps([asdict(namespace) for namespace in listing.namespaces]),
                        json.dumps([asdict(failure) for failure in listing.failures]),
                        refreshed_at.isoformat(),
                        self._now().isoformat(),
                    ],
                )
        except (duckdb.Error, OSError):
            pass
        return fresh_listing

    def read_table_listing(
        self, scope_key: str, namespace: tuple[str, ...]
    ) -> CatalogTableListing | None:
        cached = self._read_table_listing(scope_key, namespace)
        if cached is None or self._is_expired(cached.last_refreshed_at):
            return None
        return replace(
            cached,
            rows=tuple(replace(row, cache_status="cached") for row in cached.rows),
            cache_status="cached",
        )

    def read_stale_table_listing(
        self, scope_key: str, namespace: tuple[str, ...]
    ) -> CatalogTableListing | None:
        cached = self._read_table_listing(scope_key, namespace)
        if cached is None:
            return None
        return replace(
            cached,
            rows=tuple(replace(row, cache_status="stale") for row in cached.rows),
            cache_status="stale",
        )

    def write_table_listing(
        self,
        scope_key: str,
        namespace: tuple[str, ...],
        listing: CatalogTableListing,
    ) -> CatalogTableListing:
        refreshed_at = listing.last_refreshed_at or self._now()
        fresh_listing = replace(
            listing,
            cache_status="fresh",
            cache_message="",
            last_refreshed_at=refreshed_at,
        )
        try:
            with self._connect() as connection:
                self._ensure_schema(connection)
                connection.execute(
                    """
                    INSERT OR REPLACE INTO table_listing_cache
                        (
                            scope_key,
                            namespace_key,
                            namespace_json,
                            rows_json,
                            failures_json,
                            refreshed_at,
                            written_at
                        )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        scope_key,
                        _namespace_key(namespace),
                        json.dumps(list(namespace)),
                        json.dumps([_table_row_payload(row) for row in listing.rows]),
                        json.dumps([asdict(failure) for failure in listing.failures]),
                        refreshed_at.isoformat(),
                        self._now().isoformat(),
                    ],
                )
        except (duckdb.Error, OSError):
            pass
        return fresh_listing

    def read_table_detail_report(
        self, table_identifier: str
    ) -> TableDetailResult | None:
        cached = self._read_table_detail_report(table_identifier)
        if cached is None or self._is_expired(cached.cached_at):
            return None
        return cached.result

    def read_stale_table_detail_report(
        self, table_identifier: str
    ) -> TableDetailResult | None:
        cached = self._read_table_detail_report(table_identifier)
        if cached is None:
            return None
        return replace(cached.result, cache_status="stale")

    def write_table_detail_report(
        self, table_identifier: str, result: TableDetailResult
    ) -> TableDetailResult:
        cached_at = self._now()
        fresh_result = replace(result, cache_status="fresh")
        try:
            with self._connect() as connection:
                self._ensure_schema(connection)
                connection.execute(
                    """
                    INSERT OR REPLACE INTO table_detail_report_cache
                        (
                            table_identifier,
                            table_json,
                            report_json,
                            analysis_status,
                            message,
                            cached_at,
                            written_at
                        )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        table_identifier,
                        json.dumps(_table_row_payload(result.table)),
                        (
                            json.dumps(_report_payload(result.report))
                            if result.report is not None
                            else None
                        ),
                        result.analysis_status,
                        result.message,
                        cached_at.isoformat(),
                        self._now().isoformat(),
                    ],
                )
        except (duckdb.Error, OSError):
            pass
        return fresh_result

    def _read_table_classification(
        self, table_identifier: str
    ) -> CachedTableClassification | None:
        try:
            with self._connect() as connection:
                self._ensure_schema(connection)
                result = connection.execute(
                    """
                    SELECT table_format, classification_source, cached_at
                    FROM table_classification_cache
                    WHERE table_identifier = ?
                    """,
                    [table_identifier],
                ).fetchone()
            if result is None:
                return None
            table_format, classification_source, cached_at = result
            return CachedTableClassification(
                table_format=TableFormatClassification(table_format),
                classification_source=classification_source,
                cache_status="cached",
                cached_at=datetime.fromisoformat(cached_at),
            )
        except (duckdb.Error, OSError, ValueError):
            return None

    def _read_namespace_listing(self, scope_key: str) -> CatalogNamespaceListing | None:
        try:
            with self._connect() as connection:
                self._ensure_schema(connection)
                result = connection.execute(
                    """
                    SELECT namespaces_json, failures_json, refreshed_at
                    FROM namespace_listing_cache
                    WHERE scope_key = ?
                    """,
                    [scope_key],
                ).fetchone()
            if result is None:
                return None
            namespaces_json, failures_json, refreshed_at = result
            return CatalogNamespaceListing(
                namespaces=tuple(
                    CatalogNamespace(
                        name=tuple(namespace["name"]),
                        display_name=namespace["display_name"],
                    )
                    for namespace in json.loads(namespaces_json)
                ),
                failures=tuple(
                    CatalogBrowserFailure(**failure)
                    for failure in json.loads(failures_json)
                ),
                cache_status="cached",
                last_refreshed_at=datetime.fromisoformat(refreshed_at),
            )
        except (duckdb.Error, OSError, json.JSONDecodeError, ValueError, TypeError):
            return None

    def _read_table_listing(
        self, scope_key: str, namespace: tuple[str, ...]
    ) -> CatalogTableListing | None:
        try:
            with self._connect() as connection:
                self._ensure_schema(connection)
                result = connection.execute(
                    """
                    SELECT namespace_json, rows_json, failures_json, refreshed_at
                    FROM table_listing_cache
                    WHERE scope_key = ? AND namespace_key = ?
                    """,
                    [scope_key, _namespace_key(namespace)],
                ).fetchone()
            if result is None:
                return None
            namespace_json, rows_json, failures_json, refreshed_at = result
            cached_namespace = tuple(json.loads(namespace_json))
            return CatalogTableListing(
                namespace=cached_namespace,
                rows=tuple(_catalog_table_row(row) for row in json.loads(rows_json)),
                failures=tuple(
                    CatalogBrowserFailure(**failure)
                    for failure in json.loads(failures_json)
                ),
                cache_status="cached",
                last_refreshed_at=datetime.fromisoformat(refreshed_at),
            )
        except (duckdb.Error, OSError, json.JSONDecodeError, ValueError, TypeError):
            return None

    def _read_table_detail_report(
        self, table_identifier: str
    ) -> _CachedTableDetailResult | None:
        try:
            with self._connect() as connection:
                self._ensure_schema(connection)
                result = connection.execute(
                    """
                    SELECT
                        table_json,
                        report_json,
                        analysis_status,
                        message,
                        cached_at
                    FROM table_detail_report_cache
                    WHERE table_identifier = ?
                    """,
                    [table_identifier],
                ).fetchone()
            if result is None:
                return None
            table_json, report_json, analysis_status, message, cached_at = result
            table = _catalog_table_row(json.loads(table_json))
            report = _table_health_report(json.loads(report_json)) if report_json else None
            detail_view = table_detail_view(report, table=table) if report else None
            return _CachedTableDetailResult(
                result=TableDetailResult(
                    table=table,
                    report=report,
                    analysis_status=analysis_status,
                    message=message,
                    detail_view=detail_view,
                    cache_status="cached",
                ),
                cached_at=datetime.fromisoformat(cached_at),
            )
        except (duckdb.Error, OSError, json.JSONDecodeError, ValueError, TypeError):
            return None

    def _is_expired(self, cached_at: datetime | None) -> bool:
        if cached_at is None:
            return True
        return (self._now() - cached_at).total_seconds() > self.ttl_seconds

    def _connect(self) -> duckdb.DuckDBPyConnection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(self.path))

    def _ensure_schema(self, connection: duckdb.DuckDBPyConnection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS table_classification_cache (
                table_identifier VARCHAR PRIMARY KEY,
                table_format VARCHAR NOT NULL,
                classification_source VARCHAR NOT NULL,
                cached_at VARCHAR NOT NULL,
                written_at VARCHAR NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS namespace_listing_cache (
                scope_key VARCHAR PRIMARY KEY,
                namespaces_json VARCHAR NOT NULL,
                failures_json VARCHAR NOT NULL,
                refreshed_at VARCHAR NOT NULL,
                written_at VARCHAR NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS table_listing_cache (
                scope_key VARCHAR NOT NULL,
                namespace_key VARCHAR NOT NULL,
                namespace_json VARCHAR NOT NULL,
                rows_json VARCHAR NOT NULL,
                failures_json VARCHAR NOT NULL,
                refreshed_at VARCHAR NOT NULL,
                written_at VARCHAR NOT NULL,
                PRIMARY KEY (scope_key, namespace_key)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS table_detail_report_cache (
                table_identifier VARCHAR PRIMARY KEY,
                table_json VARCHAR NOT NULL,
                report_json VARCHAR,
                analysis_status VARCHAR NOT NULL,
                message VARCHAR NOT NULL,
                cached_at VARCHAR NOT NULL,
                written_at VARCHAR NOT NULL
            )
            """
        )


def _namespace_key(namespace: tuple[str, ...]) -> str:
    return json.dumps(list(namespace))


def _table_row_payload(row: CatalogTableRow) -> dict[str, object]:
    return {
        "namespace": list(row.namespace),
        "name": row.name,
        "identifier": row.identifier,
        "table_format": row.table_format.value,
        "classification_source": row.classification_source,
    }


def _catalog_table_row(payload: dict[str, object]) -> CatalogTableRow:
    return CatalogTableRow(
        namespace=tuple(str(part) for part in payload["namespace"]),
        name=str(payload["name"]),
        identifier=str(payload["identifier"]),
        table_format=TableFormatClassification(str(payload["table_format"])),
        classification_source=str(payload["classification_source"]),
        cache_status="cached",
    )


@dataclass(frozen=True)
class _CachedTableDetailResult:
    result: TableDetailResult
    cached_at: datetime


def _report_payload(report: TableHealthReport) -> dict[str, object]:
    return asdict(report)


def _table_health_report(payload: dict[str, object]) -> TableHealthReport:
    evolution = payload.get("table_evolution_history", {})
    return TableHealthReport(
        table_name=str(payload["table_name"]),
        table_source=TableSource(**payload["table_source"]),
        health_metrics=tuple(
            HealthMetric(**metric) for metric in payload["health_metrics"]
        ),
        display_statistics=tuple(
            DisplayStatistic(
                key=statistic["key"],
                label=statistic["label"],
                value=statistic["value"],
                derived_from=tuple(statistic["derived_from"]),
            )
            for statistic in payload["display_statistics"]
        ),
        calculation_warnings=tuple(
            CalculationWarning(**warning)
            for warning in payload.get("calculation_warnings", ())
        ),
        partition_metrics=tuple(
            PartitionHealthMetric(**metric)
            for metric in payload.get("partition_metrics", ())
        ),
        table_evolution_history=TableEvolutionHistory(
            schema_changes=tuple(
                EvolutionChange(**change)
                for change in evolution.get("schema_changes", ())
            ),
            property_changes=tuple(
                EvolutionChange(**change)
                for change in evolution.get("property_changes", ())
            ),
        ),
        maintenance_recommendations=tuple(
            MaintenanceRecommendation(**recommendation)
            for recommendation in payload.get("maintenance_recommendations", ())
        ),
    )
