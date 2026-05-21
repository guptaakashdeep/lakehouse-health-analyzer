# Centralize Runtime Configuration

Table sources, catalog settings, analysis policies, runtime policies, and output options should be normalized through one configuration model that can be populated from CLI flags, environment variables, config files, and UI overrides. This keeps Streamlit, TUI, and tests from each inventing separate configuration rules for Glue, metadata-file analysis, recommendation thresholds, cache TTL, concurrency, and exports.

Implemented precedence for the metadata-file slice is: built-in defaults, then
environment variables, then UI overrides. Environment keys currently supported
are `LHA_METADATA_LOCATION`, `LHA_SNAPSHOT_RETENTION_DAYS`,
`LHA_RECOMMENDATION_THRESHOLDS`, `LHA_HISTORY_DEPTH`,
`LHA_CACHE_TTL_SECONDS`, `LHA_TIMEOUT_SECONDS`, and `LHA_MAX_CONCURRENCY`.
Streamlit metadata-file inputs are treated as UI overrides.
