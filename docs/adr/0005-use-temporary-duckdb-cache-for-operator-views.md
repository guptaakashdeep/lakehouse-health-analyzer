# Use Temporary DuckDB Cache for Operator Views

Operator-facing views may cache catalog browsing and table detail data in a
local DuckDB file under the user's cache directory. Cached values have a default
15-minute TTL, are visibly marked in interfaces, and support explicit refresh
so the cache improves navigation speed without becoming the source of truth.

The operator cache may store namespace listings, table listings, table format
classifications, and selected-table detail reports. Listing cache status is not
the same as per-table analysis cache status: a namespace table list may come
from cache while individual rows still show `NOT ANALYZED` unless that table's
classification or detail result came from a per-table cached analysis result.

Stale fallback is allowed only within the configured TTL. If a fresh catalog or
analysis request fails, the workflow may return a stale cached listing or table
detail result only when that cached value is still recent enough. Once the TTL
expires, stale reads should return no result and the interface should surface
the fresh failure instead of showing day-old data. This keeps the cache from
masking issues such as expired AWS SSO sessions indefinitely.

The same temporary DuckDB cache may store table format classifications such as
`ICEBERG`, `UNKNOWN`, and `NON-ICEBERG` so unsupported tables do not need
repeated analysis attempts during normal browsing. These classifications follow
the same TTL rules as other operator cache entries.
