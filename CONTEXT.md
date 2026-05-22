# Lakehouse Health Analyzer

Lakehouse Health Analyzer evaluates table metadata so teams can understand the operational health of lakehouse tables.

## Language

**Health Metric**:
A canonical measurement of table health with a stable name, unit, source, and calculation rule.
_Avoid_: UI metric, chart metric, dashboard calculation

**Display Statistic**:
A presentation-only transformation of existing health metrics for formatting, sorting, grouping, filtering, or charting.
_Avoid_: Metric, health metric

**Table Source**:
The place from which a lakehouse table is loaded for analysis.
_Avoid_: Input, local, source

**Metadata File Source**:
A table source that loads one Iceberg table directly from an Iceberg metadata file path.
_Avoid_: Local mode, file mode

**Catalog Table Source**:
A table source that loads one or more Iceberg tables by catalog, namespace, and table name.
_Avoid_: Remote mode, AWS mode

**Catalog Namespace**:
The catalog-scoped grouping that contains catalog tables. In AWS Glue, this is commonly shown to users as a Glue database.
_Avoid_: Database, schema

**Table Health Report**:
The canonical result of analyzing one lakehouse table's operational health.
_Avoid_: Metrics object, dashboard payload, metadata metrics

**Calculation Warning**:
An explanation attached to a table health report when a health metric is unknown, estimated, skipped, or derived from incomplete source metadata.
_Avoid_: Error, log message, UI warning

**Data File Record Count**:
The number of records reported by live data files in table metadata.
_Avoid_: Current record count, row count

**Position Delete Record Count**:
The number of position-delete records reported by live delete files in table metadata.
_Avoid_: Deleted rows, removed records

**Equality Delete Record Count**:
The number of equality-delete records reported by live delete files in table metadata.
_Avoid_: Deleted rows, removed records

**Estimated Current Record Count**:
A best-effort estimate of readable records after applying known delete metadata.
_Avoid_: Current record count, exact row count

**Maintenance Action**:
A planned operational change intended to improve or preserve a table's health.
_Avoid_: Dashboard button, compaction job, fix

**Maintenance Recommendation**:
Advice from a table health report that identifies a maintenance action worth considering and the health metrics that support it.
_Avoid_: Alert, action, fix

**Table Format Adapter**:
A format-specific analyzer that turns one loaded lakehouse table into a table health report.
_Avoid_: Generic analyzer, plugin, connector

**Table Format Classification**:
A catalog-table classification that says whether a table is known to be Iceberg, known to be non-Iceberg, or not yet known.
_Avoid_: Health status, load status

**Expirable Snapshot Candidate**:
A valid snapshot that appears eligible for expiration under the configured retention policy.
_Avoid_: Expired snapshot, old snapshot

**Table Evolution History**:
A retained-history view of how a table's schema and properties changed over time.
_Avoid_: Audit log, complete history

**Compaction Recommendation**:
A maintenance recommendation to reduce small-file or file-concentration pressure.
_Avoid_: Compaction action, small-file alert

**Snapshot Expiration Recommendation**:
A maintenance recommendation to expire eligible retained snapshots under a retention policy.
_Avoid_: Expired snapshot alert, snapshot cleanup action

**Metadata Cleanup Recommendation**:
A maintenance recommendation to review retained metadata growth or metadata history retention.
_Avoid_: Metadata delete action, metadata alert

**Delete File Recommendation**:
A maintenance recommendation to review delete-file pressure on table reads.
_Avoid_: Delete cleanup action, delete-file alert

**Recommendation Severity**:
The urgency assigned to a maintenance recommendation based on configured thresholds and supporting health metrics.
_Avoid_: Priority, alert level

## Relationships

- A **Health Metric** may be shown through one or more **Display Statistics**
- A **Display Statistic** must be derived from existing **Health Metrics**
- A **Metadata File Source** is a **Table Source**
- A **Catalog Table Source** is a **Table Source**
- A **Catalog Table Source** uses a **Catalog Namespace** to locate catalog tables
- A **Table Health Report** contains **Health Metrics** for exactly one analyzed table
- A **Calculation Warning** belongs to a **Table Health Report**
- **Data File Record Count**, **Position Delete Record Count**, and **Equality Delete Record Count** may inform an **Estimated Current Record Count**
- A **Maintenance Action** may be recommended by a **Table Health Report**
- A **Maintenance Recommendation** may propose one **Maintenance Action**
- A **Table Format Adapter** produces a **Table Health Report**
- A **Catalog Table Source** may have a **Table Format Classification** before it is analyzed
- A **Table Health Report** may include **Expirable Snapshot Candidates** and **Table Evolution History**
- **Compaction Recommendation**, **Snapshot Expiration Recommendation**, **Metadata Cleanup Recommendation**, and **Delete File Recommendation** are kinds of **Maintenance Recommendation**
- A **Maintenance Recommendation** has one **Recommendation Severity**

## Example Dialogue

> **Dev:** "Should the dashboard calculate partition skewness before drawing the chart?"
> **Domain expert:** "No. Partition skewness is a **Health Metric**; the dashboard may only display it as a **Display Statistic**."
>
> **Dev:** "Is a local metadata file a different kind of analysis?"
> **Domain expert:** "No. It is just a **Metadata File Source**; the analysis should produce the same **Health Metrics** as any other **Table Source** when the metadata is available."
>
> **Dev:** "Should the Streamlit dashboard and TUI each define their own output shape?"
> **Domain expert:** "No. Both should render the same **Table Health Report**."
>
> **Dev:** "If an Iceberg summary field is missing, should we show zero?"
> **Domain expert:** "No. Zero means the source explicitly reported zero; missing values should remain unknown and produce a **Calculation Warning** when that affects a **Health Metric**."
>
> **Dev:** "Can we subtract delete-file records from data-file records and call that the current row count?"
> **Domain expert:** "No. Show the delete record counts separately, and only show an **Estimated Current Record Count** when the estimation rule is explicit."
>
> **Dev:** "Should a dashboard button directly compact a table?"
> **Domain expert:** "No. It should orchestrate a **Maintenance Action** with confirmation, status, and auditability."
>
> **Dev:** "Should the first serious version execute table maintenance?"
> **Domain expert:** "No. It should produce **Maintenance Recommendations** first; execution can come later."
>
> **Dev:** "Should Iceberg, Delta, and Hudi all be forced into the same metric set immediately?"
> **Domain expert:** "No. Start with an Iceberg **Table Format Adapter** and let future adapters report unsupported metrics as unknown with warnings."
>
> **Dev:** "Can the dashboard show how columns and properties changed?"
> **Domain expert:** "Yes, as **Table Evolution History** based on retained metadata, with warnings when older metadata files are unavailable."
>
> **Dev:** "Should the report execute compaction when small-file pressure is detected?"
> **Domain expert:** "No. It should produce a **Compaction Recommendation** with supporting health metrics."
>
> **Dev:** "Should a recommendation say how urgent it is?"
> **Domain expert:** "Yes. Use **Recommendation Severity** values of info, warning, or critical, and show the thresholds used."

## Flagged Ambiguities

- "metric" was used for both canonical table-health measurements and presentation-only dashboard values; resolved: use **Health Metric** for canonical measurements and **Display Statistic** for UI transformations.
- "local" was used to mean direct metadata-file loading; resolved: use **Metadata File Source** for direct Iceberg metadata-file loading and **Catalog Table Source** for catalog-backed loading.
- "report", "metrics object", and "dashboard payload" were used for the analyzer output; resolved: use **Table Health Report** for the canonical result of analyzing one table.
- `0` and missing metric values were previously conflated; resolved: `0` means explicitly reported zero, while unknown or unavailable values remain missing and are explained by **Calculation Warnings**.
- "current record count" was used as an exact count after subtracting delete records; resolved: keep data-file, position-delete, and equality-delete record counts separate, and label any post-delete value as **Estimated Current Record Count**.
- "run compaction" was used as a possible dashboard behavior; resolved: user interfaces may orchestrate **Maintenance Actions**, but should not directly mutate tables in the UI process.
- "maintenance" was used for both advice and execution; resolved: use **Maintenance Recommendation** for analyzer advice and **Maintenance Action** for an operational change.
- "analyzer" was used both as an Iceberg implementation and as a future multi-format boundary; resolved: use **Table Format Adapter** for the format-specific report producer.
- "expired snapshots" was used to mean snapshots worth expiring; resolved: use **Expirable Snapshot Candidate** for retained snapshots that appear eligible under policy.
- "table evolution history" was used as a complete audit log; resolved: it is a retained-history view and may be incomplete when metadata history was pruned.
- "orphan files" was discussed but not resolved; parked for a later version because metadata-only file inventories do not prove that unreferenced storage objects exist.
