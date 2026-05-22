# Lakehouse Health Analyzer

Analyze Apache Iceberg table metadata through one canonical `TableHealthReport` model shared by Streamlit, terminal workflows, and exports.

## Demo

![Lakehouse Health Analyzer Demo](docs/resources/Lakehouse-analyzer.gif)

## Scope in this version

- Table format: Apache Iceberg
- Table sources:
  - Metadata file source (`metadata.json` path)
  - AWS Glue catalog table source (`pyiceberg` Glue catalog loading)
- Interfaces:
  - Streamlit dashboard (`streamlit_app.py`)
  - Operator terminal workflow (`lakehouse-health-operator`)
- Outputs:
  - JSON structured report exports
  - Markdown summary report exports
- Included report sections:
  - Health metrics, display statistics, partition metrics
  - Snapshot and retained table evolution metrics
  - Maintenance recommendations with evidence and thresholds

Out of scope for this version:

- Orphan-file identification details and orphan-file cleanup actions
- Non-Iceberg table formats (for example Hudi or Delta Lake)
- Non-Glue catalog integrations

## Architecture (current implementation)

```mermaid
flowchart TD
    M[Metadata File<br/>metadata.json path]
    G[AWS Glue Catalog<br/>PyIceberg table loading]
    C[AnalyzerConfiguration.from_environment()<br/>centralized runtime/config policy]
    A[Analysis Core<br/>canonical TableHealthReport]
    D[DuckDB Cache<br/>catalog overview cache]
    S[Streamlit Dashboard<br/>overview table + metadata-file full report]
    O[Operator CLI<br/>lakehouse-health-operator --inspect]
    J[JSON Export]
    MD[Markdown Export]

    M --> C
    G --> C
    C --> A
    A --> D
    A --> S
    D --> S
    A --> O
    D --> O
    O --> J
    O --> MD
```

## `uv` workflow (supported path)

```bash
uv sync --extra dev
uv run pytest
```

Run all project commands through `uv run`.

## Runtime configuration

Configuration is loaded by `AnalyzerConfiguration.from_environment()`.

Required for metadata-file analysis:

- `LHA_METADATA_LOCATION`

Required for Glue catalog-table analysis:

- `LHA_TABLE_SOURCE_KIND=glue_catalog_table`
- `LHA_GLUE_CATALOG_NAME`
- `LHA_GLUE_NAMESPACE` (dot-separated for operator/table analysis; Streamlit catalog overview currently supports a single namespace component)
- `LHA_GLUE_TABLE_NAME`

Optional policy and runtime settings:

- `LHA_AWS_PROFILE`
- `LHA_AWS_REGION`
- `LHA_SNAPSHOT_RETENTION_DAYS` (default `30`)
- `LHA_RECOMMENDATION_THRESHOLDS` (JSON object)
- `LHA_CACHE_TTL_SECONDS` (default `900`)
- `LHA_TIMEOUT_SECONDS` (default `30`)
- `LHA_MAX_CONCURRENCY` (default `4`)
- `LHA_EXPORT_FORMATS` (comma-separated: `json`, `markdown`)
- `LHA_EXPORT_DIRECTORY`

## Run Streamlit

```bash
uv run streamlit run streamlit_app.py
```

In the UI you can:

- Analyze a single metadata file
- Load catalog overview rows for Glue tables
- View cache status for overview rows (`fresh` or `cached`)
- Review recommendations, warnings, partitions, and evolution history for metadata-file report views

The current Streamlit catalog view is an overview table. Full catalog-table drilldown is tracked separately from this version's completed scope.

## Run operator workflow

Example environment for Glue catalog workflow:

```bash
export LHA_TABLE_SOURCE_KIND=glue_catalog_table
export LHA_GLUE_CATALOG_NAME=analytics
export LHA_GLUE_NAMESPACE=sales
export LHA_GLUE_TABLE_NAME=orders
```

List catalog overview:

```bash
uv run lakehouse-health-operator
```

Inspect a selected table:

```bash
uv run lakehouse-health-operator --inspect sales.orders
```

Force refresh or bypass cache:

```bash
uv run lakehouse-health-operator --refresh
uv run lakehouse-health-operator --no-cache
```

## Cache behavior

- Backend: DuckDB
- Default path: `${XDG_CACHE_HOME:-~/.cache}/lakehouse-health-analyzer/catalog-overview.duckdb`
- Default TTL: 900 seconds (15 minutes)
- Expired, missing, or unreadable cache entries are treated as misses

## Export workflow

Enable exports through output policy environment variables:

```bash
export LHA_EXPORT_FORMATS=json,markdown
export LHA_EXPORT_DIRECTORY=/tmp/lha-exports
uv run lakehouse-health-operator --inspect sales.orders
```

Exports are written as:

- `<table-name-with-dots-replaced-by-dashes>.json`
- `<table-name-with-dots-replaced-by-dashes>.md`

For example, `sales.orders` exports to `sales-orders.json` and `sales-orders.md`.

JSON preserves the canonical report structure. Markdown is a readable summary with source, cache/analyzed metadata, health metrics, warnings, and recommendation summaries.

`LHA_EXPORT_DIRECTORY` must already exist.

## Recommendation workflow

Current recommendation types in report outputs:

- `compaction`
- `snapshot_expiration`
- `metadata_cleanup`
- `delete_file_cleanup`

Severity values are `info`, `warning`, or `critical`, with thresholds optionally overridden via `LHA_RECOMMENDATION_THRESHOLDS`.

## Testing

```bash
uv run pytest
```

## License

MIT. See [LICENSE](LICENSE).
