# Use Temporary DuckDB Cache for Operator Views

Catalog overview and other operator-facing views may cache table health reports in a local DuckDB file under the user's cache directory. Cached headline metrics should have a default 15-minute TTL, be visibly marked as cached in interfaces, and support explicit refresh so the cache improves navigation speed without becoming the source of truth. The same temporary DuckDB cache may store table format classifications such as `ICEBERG`, `UNKNOWN`, and `NON-ICEBERG` so unsupported tables do not need repeated analysis attempts during normal browsing.
