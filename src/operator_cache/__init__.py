from __future__ import annotations

import json
import os
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import duckdb

from visualization.catalog_overview import CatalogOverview


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
