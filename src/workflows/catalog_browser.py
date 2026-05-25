from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum
from typing import Protocol

from configuration import AnalyzerConfiguration, GlueCatalogTableSourceConfiguration


class TableFormatClassification(str, Enum):
    ICEBERG = "ICEBERG"
    UNKNOWN = "UNKNOWN"
    NON_ICEBERG = "NON-ICEBERG"

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class CatalogBrowserFailure:
    scope: str
    message: str


@dataclass(frozen=True)
class CatalogNamespace:
    name: tuple[str, ...]
    display_name: str


@dataclass(frozen=True)
class CatalogNamespaceListing:
    namespaces: tuple[CatalogNamespace, ...]
    failures: tuple[CatalogBrowserFailure, ...] = ()
    cache_status: str = "fresh"
    cache_message: str = ""
    last_refreshed_at: datetime | None = None


@dataclass(frozen=True)
class CatalogTableRow:
    namespace: tuple[str, ...]
    name: str
    identifier: str
    table_format: TableFormatClassification
    classification_source: str
    cache_status: str = "fresh"


@dataclass(frozen=True)
class CatalogTableListing:
    namespace: tuple[str, ...]
    rows: tuple[CatalogTableRow, ...]
    failures: tuple[CatalogBrowserFailure, ...] = ()
    cache_status: str = "fresh"
    cache_message: str = ""
    last_refreshed_at: datetime | None = None


class CatalogBrowserAccess(Protocol):
    def list_namespaces(self) -> CatalogNamespaceListing: ...

    def list_tables(self, namespace: tuple[str, ...]) -> CatalogTableListing: ...


class CachedTableClassification(Protocol):
    table_format: TableFormatClassification
    classification_source: str
    cache_status: str


class CatalogBrowserCache(Protocol):
    def read_namespace_listing(
        self, scope_key: str
    ) -> CatalogNamespaceListing | None: ...

    def read_stale_namespace_listing(
        self, scope_key: str
    ) -> CatalogNamespaceListing | None: ...

    def write_namespace_listing(
        self, scope_key: str, listing: CatalogNamespaceListing
    ) -> CatalogNamespaceListing: ...

    def read_table_listing(
        self, scope_key: str, namespace: tuple[str, ...]
    ) -> CatalogTableListing | None: ...

    def read_stale_table_listing(
        self, scope_key: str, namespace: tuple[str, ...]
    ) -> CatalogTableListing | None: ...

    def write_table_listing(
        self,
        scope_key: str,
        namespace: tuple[str, ...],
        listing: CatalogTableListing,
    ) -> CatalogTableListing: ...

    def read_table_classification(
        self, table_identifier: str
    ) -> CachedTableClassification | None: ...


@dataclass(frozen=True)
class CatalogBrowserWorkflow:
    catalog_access: CatalogBrowserAccess
    cache: CatalogBrowserCache | None = None
    cache_scope_key: str = "catalog-browser"

    def list_namespaces(self, *, refresh: bool = False) -> CatalogNamespaceListing:
        if self.cache is not None and not refresh:
            cached_listing = self.cache.read_namespace_listing(self.cache_scope_key)
            if cached_listing is not None and _cached_listing_is_usable(cached_listing):
                return cached_listing
        try:
            listing = _fresh_namespace_listing(self.catalog_access.list_namespaces())
        except Exception as exc:
            stale = self._stale_namespace_listing(str(exc) or exc.__class__.__name__)
            if stale is not None:
                return stale
            raise
        if listing.failures:
            stale = None
            if not listing.namespaces:
                stale = self._stale_namespace_listing(
                    _failures_message(listing.failures)
                )
            if stale is not None:
                return stale
            return listing
        if self.cache is not None:
            return self.cache.write_namespace_listing(self.cache_scope_key, listing)
        return listing

    def list_tables(
        self, namespace: tuple[str, ...], *, refresh: bool = False
    ) -> CatalogTableListing:
        if self.cache is not None and not refresh:
            cached_listing = self.cache.read_table_listing(
                self.cache_scope_key, namespace
            )
            if cached_listing is not None and _cached_listing_is_usable(cached_listing):
                return self._apply_cached_classifications(cached_listing)
        try:
            fresh_listing = _fresh_table_listing(
                self.catalog_access.list_tables(namespace)
            )
        except Exception as exc:
            stale = self._stale_table_listing(
                namespace,
                str(exc) or exc.__class__.__name__,
            )
            if stale is not None:
                return stale
            raise
        listing = fresh_listing
        if self.cache is not None and not refresh:
            listing = self._apply_cached_classifications(fresh_listing)
        if listing.failures:
            stale = None
            if not listing.rows:
                stale = self._stale_table_listing(
                    namespace,
                    _failures_message(listing.failures),
                )
            if stale is not None:
                return stale
            return listing
        if self.cache is not None:
            self.cache.write_table_listing(
                self.cache_scope_key, namespace, fresh_listing
            )
            return listing
        return listing

    def _stale_namespace_listing(
        self, cache_message: str
    ) -> CatalogNamespaceListing | None:
        if self.cache is None:
            return None
        stale = self.cache.read_stale_namespace_listing(self.cache_scope_key)
        if stale is None or not _cached_listing_is_usable(stale):
            return None
        return replace(
            stale,
            cache_status="stale",
            cache_message=cache_message,
        )

    def _stale_table_listing(
        self, namespace: tuple[str, ...], cache_message: str
    ) -> CatalogTableListing | None:
        if self.cache is None:
            return None
        stale = self.cache.read_stale_table_listing(self.cache_scope_key, namespace)
        if stale is None or not _cached_listing_is_usable(stale):
            return None
        return replace(
            stale,
            cache_status="stale",
            cache_message=cache_message,
        )

    def _apply_cached_classifications(
        self, listing: CatalogTableListing
    ) -> CatalogTableListing:
        if self.cache is None:
            return listing
        return replace(
            listing,
            rows=tuple(
                _row_with_cached_classification(self.cache, row) for row in listing.rows
            ),
        )


def configured_catalog_browser_workflow(
    config: AnalyzerConfiguration,
    *,
    catalog_access: CatalogBrowserAccess | None = None,
    cache: CatalogBrowserCache | None = None,
    cache_scope_key: str = "catalog-browser",
) -> CatalogBrowserWorkflow:
    table_source = config.table_source
    if not isinstance(table_source, GlueCatalogTableSourceConfiguration):
        raise ValueError("Catalog browsing requires a Glue catalog table source.")
    if catalog_access is None:
        from catalogs.glue import GlueCatalogBrowserAccess

        catalog_access = GlueCatalogBrowserAccess.from_table_source(table_source)
    return CatalogBrowserWorkflow(
        catalog_access=catalog_access,
        cache=cache,
        cache_scope_key=cache_scope_key,
    )


def _cached_listing_is_usable(
    listing: CatalogNamespaceListing | CatalogTableListing,
) -> bool:
    return not listing.failures


def _failures_message(failures: tuple[CatalogBrowserFailure, ...]) -> str:
    messages = tuple(
        f"{failure.scope}: {failure.message}" if failure.scope else failure.message
        for failure in failures
    )
    return "; ".join(messages) or "catalog listing returned failures"


def _fresh_namespace_listing(
    listing: CatalogNamespaceListing,
) -> CatalogNamespaceListing:
    return replace(listing, cache_status="fresh", cache_message="")


def _fresh_table_listing(listing: CatalogTableListing) -> CatalogTableListing:
    return replace(
        listing,
        rows=tuple(replace(row, cache_status="fresh") for row in listing.rows),
        cache_status="fresh",
        cache_message="",
    )


def _row_with_cached_classification(
    cache: CatalogBrowserCache, row: CatalogTableRow
) -> CatalogTableRow:
    cached = cache.read_table_classification(row.identifier)
    if cached is None:
        return row
    return replace(
        row,
        table_format=cached.table_format,
        classification_source=cached.classification_source,
        cache_status=cached.cache_status,
    )
