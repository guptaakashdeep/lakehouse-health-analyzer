# Start with Focused Operator TUI

The default `lakehouse-health-operator` / `lh` command launches a focused
operator TUI rather than a chart-heavy dashboard clone. Explicit subcommands are
reserved for setup and scriptable report export workflows. Textual and Rich are
required dependencies because the default operator command depends on them and
uses Rich renderables and styles directly.

The operator TUI is keyboard-first, with mouse support allowed where Textual
provides it naturally. Core bindings include arrow keys and `j`/`k` for
movement, Enter for selection or analysis, `/` for search or filtering, `r` for
refresh, `e` for export, `s` for setup or settings, and `q` for quit.

Catalog navigation discovers available top-level catalog namespaces through the
shared catalog access layer, labels them as databases for Glue-facing users, and
lazily lists tables only when a namespace is selected. Hierarchical namespaces
may be shown as flat fully-qualified namespace entries in the first version
rather than as a nested tree. Initial browsing should avoid eager table analysis
or a full catalog health crawl.

The earlier eager `OperatorCatalogWorkflow` / catalog-overview direction is
retired. The supported operator model is lazy browsing plus explicit
selected-table analysis. Catalog table listing should load identities and cheap
format classifications, not headline health metrics for every table. Full
health metrics are produced only after the user explicitly selects a table for
analysis.

The table list renders each table with the table name left-aligned and a
right-aligned table format label. Format labels use `ICEBERG` when the format is
positively detected, `UNKNOWN` when it cannot be determined cheaply, and
`NON-ICEBERG` after attempted analysis proves the table is unsupported.
Highlighting a table must not trigger full table analysis. Selecting an
`UNKNOWN` table attempts Iceberg analysis, promotes the format label to
`ICEBERG` when analysis succeeds, and marks it `NON-ICEBERG` when the table is
unsupported. Selecting a cached `NON-ICEBERG` table shows an unsupported-table
detail state with options to go back or refresh the status.

Table analysis runs in a background task so users can keep navigating databases
and tables while analysis is in progress. In-progress analysis is associated
with the selected table and shown with an `ANALYZING` row status. Duplicate
analysis requests for the same in-flight table are deduplicated. When analysis
completes, it updates that table's row/cache state even if the user has
navigated elsewhere. A late result must not overwrite the detail panel for a
newer explicitly selected table. Concurrent analyses should respect
`RuntimePolicy.max_concurrency`.

The detail panel represents the last explicitly selected/analyzed table, not
necessarily the currently highlighted row. This lets users continue navigating
without losing the last analysis result. If the user explicitly analyzes a new
table, that new selection becomes the detail owner and older background results
must not steal the detail panel.

After a table is analyzed, the detail panel shows maintenance recommendations
first, grouped by severity, followed by calculation warnings and then grouped
metrics. Detail sections are ordered as recommendations, warnings, files,
records, partitions, snapshots, metadata and evolution, and source and runtime.
Metric rendering uses Rich/Textual styling and color to improve scanability,
with color carrying severity, state, and freshness cues rather than decorative
noise.

A valid Iceberg table with no snapshots is not an analysis failure. The TUI
should label it as `NO DATA` and explain that the table has no snapshots instead
of forcing the user to infer that state from zero-valued metrics. Genuine
analysis failures should be visible as `ERROR`, and rows should not remain stuck
as `ANALYZING` after a failure.

The visual theme uses a modern, colorful, easy-on-the-eyes palette across
panels, borders, focus states, and badges. Critical reads as red, warning as
amber or yellow, info as blue or cyan, healthy or success states as green,
unknown or not-analyzed states as muted gray, and cached or stale values as a
secondary accent color. Text labels such as `CRITICAL`, `UNKNOWN`, `CACHED`,
`STALE`, `NO DATA`, and `NON-ICEBERG` remain visible so the interface does not
rely on color alone.

The non-interactive report command accepts a namespace-qualified table
identifier, uses the configured catalog from setup, config file, environment,
or flags, and splits the identifier so the final segment is the table name and
earlier segments are the catalog namespace. The report command prints JSON or
Markdown to stdout by default for scriptability and writes files only when an
explicit output path or configured output policy requests it. Expirable snapshot
candidates use a configurable retention policy with a 30-day default, excluding
the current snapshot. Orphan-file details are intentionally out of scope for the
first serious version.
