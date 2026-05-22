from datetime import datetime, timedelta, timezone

from operator_cache import (
    CatalogOverviewCache,
    OperatorCache,
    default_catalog_overview_cache_path,
)
from visualization.catalog_overview import CatalogOverview
from workflows.catalog_browser import TableFormatClassification


def test_catalog_overview_cache_writes_and_reads_cached_overview(tmp_path):
    analyzed_at = datetime(2026, 5, 21, 8, 30, tzinfo=timezone.utc)
    cache = CatalogOverviewCache(
        tmp_path / "operator-cache.duckdb",
        ttl_seconds=900,
        now=lambda: analyzed_at,
    )
    overview = CatalogOverview(
        rows=(
            {
                "table": "sales.orders",
                "status": "loaded",
                "data_file_count": 7,
            },
        ),
        failures=(
            {
                "table": "sales.missing",
                "status": "warning",
                "message": "table is unavailable",
            },
        ),
    )

    cache.write("analytics:sales", overview)

    cached = cache.read("analytics:sales")

    assert cached == CatalogOverview(
        rows=overview.rows,
        failures=overview.failures,
        cache_status="cached",
        last_analyzed_at=analyzed_at,
    )


def test_catalog_overview_cache_treats_expired_overview_as_miss(tmp_path):
    analyzed_at = datetime(2026, 5, 21, 8, 30, tzinfo=timezone.utc)
    current_time = analyzed_at
    cache = CatalogOverviewCache(
        tmp_path / "operator-cache.duckdb",
        ttl_seconds=60,
        now=lambda: current_time,
    )

    cache.write(
        "analytics:sales",
        CatalogOverview(rows=({"table": "sales.orders", "status": "loaded"},)),
    )

    current_time = analyzed_at + timedelta(seconds=61)

    assert cache.read("analytics:sales") is None


def test_catalog_overview_cache_default_ttl_is_15_minutes(tmp_path):
    analyzed_at = datetime(2026, 5, 21, 8, 30, tzinfo=timezone.utc)
    current_time = analyzed_at
    cache = CatalogOverviewCache(
        tmp_path / "operator-cache.duckdb",
        now=lambda: current_time,
    )
    cache.write(
        "analytics:sales",
        CatalogOverview(rows=({"table": "sales.orders", "status": "loaded"},)),
    )

    current_time = analyzed_at + timedelta(minutes=15)
    assert cache.read("analytics:sales") is not None

    current_time = analyzed_at + timedelta(minutes=15, seconds=1)
    assert cache.read("analytics:sales") is None


def test_catalog_overview_cache_treats_missing_or_corrupted_cache_as_miss(tmp_path):
    cache_path = tmp_path / "operator-cache.duckdb"
    cache = CatalogOverviewCache(cache_path, ttl_seconds=900)

    assert cache.read("analytics:sales") is None

    cache_path.write_bytes(b"not a duckdb database")

    assert cache.read("analytics:sales") is None


def test_catalog_overview_cache_returns_fresh_overview_when_corrupted_cache_cannot_write(
    tmp_path,
):
    analyzed_at = datetime(2026, 5, 21, 8, 30, tzinfo=timezone.utc)
    cache_path = tmp_path / "operator-cache.duckdb"
    cache_path.write_bytes(b"not a duckdb database")
    cache = CatalogOverviewCache(
        cache_path,
        ttl_seconds=900,
        now=lambda: analyzed_at,
    )
    overview = CatalogOverview(rows=({"table": "sales.orders", "status": "loaded"},))

    written = cache.write("analytics:sales", overview)

    assert written == CatalogOverview(
        rows=overview.rows,
        cache_status="fresh",
        last_analyzed_at=analyzed_at,
    )


def test_default_catalog_overview_cache_path_uses_user_cache_directory(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache-home"))

    assert default_catalog_overview_cache_path() == (
        tmp_path
        / "cache-home"
        / "lakehouse-health-analyzer"
        / "catalog-overview.duckdb"
    )


def test_operator_cache_caches_table_classification_until_ttl_expires(tmp_path):
    written_at = datetime(2026, 5, 21, 8, 30, tzinfo=timezone.utc)
    current_time = written_at
    cache = OperatorCache(
        tmp_path / "operator-cache.duckdb",
        ttl_seconds=60,
        now=lambda: current_time,
    )

    cache.write_table_classification(
        "analytics:sales.orders",
        TableFormatClassification.ICEBERG,
        "glue_parameters",
    )

    cached = cache.read_table_classification("analytics:sales.orders")

    assert cached is not None
    assert cached.table_format == TableFormatClassification.ICEBERG
    assert cached.classification_source == "glue_parameters"
    assert cached.cache_status == "cached"
    assert cached.cached_at == written_at

    current_time = written_at + timedelta(seconds=61)

    assert cache.read_table_classification("analytics:sales.orders") is None
    assert (
        cache.read_stale_table_classification("analytics:sales.orders").cache_status
        == "stale"
    )
