# Operator TUI Modernization Notes

This note captures product and interaction decisions for the operator terminal
experience. Domain language remains in `CONTEXT.md`; durable architecture
decisions remain in `docs/adr/`.

## Architecture Direction

- TUI, CLI, and Streamlit should use shared UI-neutral workflows for catalog
  interaction, table analysis, exports, and common calculations.
- UI-neutral workflow modules should live under `src/workflows/`.
- Workflow APIs should be synchronous in the first implementation because the
  current PyIceberg, DuckDB cache, and analysis calls are synchronous. Textual
  should run workflow calls in background workers so the UI remains responsive.
- Rendering layers may format, group, filter, colorize, and lay out existing
  report values, but they should not own catalog behavior or canonical health
  calculations.
- Catalog browsing, namespace listing, table listing, table format
  classification, and catalog table loading should go through the shared
  Iceberg catalog access layer, which delegates to PyIceberg internally.
- For Glue browsing, the catalog access layer should use Boto3 Glue metadata
  APIs directly for lightweight namespace/table listing and table format
  classification.
- For explicit Iceberg table loading and analysis, the catalog access layer
  should continue to use PyIceberg.
- The catalog browser workflow may return a UI-neutral table detail view model
  alongside the canonical table health report. The view model should define
  shared grouping, section order, severity ordering, classification state, and
  cache status, while leaving visual rendering to Textual, Streamlit, CLI, or
  export-specific code.
- Legacy paths may remain during modernization, then be removed once the shared
  services cover supported workflows.
- Initial implementation may add new workflow and TUI modules beside the
  existing operator code, but cleanup is required once setup, interactive TUI,
  report command, catalog browsing, and detail rendering are covered by the new
  path.

## Command Shape

- `lakehouse-health-operator` launches the interactive Textual TUI by default.
- Add `lh` as a short alias for the same CLI entry point, while keeping
  `lakehouse-health-operator` for compatibility.
- Documentation examples may use `lh` for quick starts after introducing the
  alias, while reference sections should keep the full command visible.
- `lakehouse-health-operator setup` runs a plain Rich-powered setup prompt flow.
- `lakehouse-health-operator report <namespace.table> --format json|markdown`
  produces a non-interactive report. The `--format` value may be a single
  format or a comma-separated list such as `json,markdown`.
- Report table identifiers must include namespace/database plus table name;
  bare table names are invalid.
- The report command prints to stdout by default and writes a file only when an
  explicit output path or configured output policy requests it.
- The report command should always perform fresh analysis and should not offer a
  `--use-cache` option.
- Stdout report output should support only one requested format. If multiple
  formats are requested, the command should require `--output-dir` or a
  configured export directory.
- `--output` should mean an exact output file path and should be valid only for
  one requested format.
- `--output-dir` should mean generated filenames inside a directory and should
  work for one or many requested formats.
- For unsupported or non-Iceberg tables, the report command should exit
  non-zero and print a concise error to stderr.
- The previous one-off operator flags have been removed. The final interface
  uses default TUI launch plus explicit setup and report subcommands.

## TUI Export

- Pressing `e` from table detail should export the selected report.
- Export should offer JSON, Markdown, or both.
- The default destination should come from configuration. If no destination is
  configured, use the current working directory or prompt for a path.
- Export from the TUI should use the report currently shown, including cached
  reports, and include cache-status metadata in the exported output.
- Users who want a fresh TUI export should refresh the table detail before
  exporting.
- The TUI should show concise success or error feedback including written paths
  when applicable.
- The non-interactive report command remains the preferred scriptable export
  path.

## Setup

- Setup writes `${XDG_CONFIG_HOME:-~/.config}/lakehouse-health-analyzer/config.toml`.
- Configuration precedence is built-in defaults, setup config file, environment
  variables, then CLI flags or UI overrides.
- The generated TOML file should include supported defaults and short comments
  explaining each setting.
- Recommendation thresholds should be present in the generated config file for
  discoverability, but the first setup/TUI flow does not need a guided editor
  for threshold maps. Advanced users may edit TOML manually.
- Setup should offer named AWS profiles from `~/.aws/config` when present.
- If no named AWS profiles exist, setup should save no profile and use the
  default AWS credential chain.
- Glue setup must include an AWS region. Setup should infer the region from the
  selected AWS profile when available; otherwise it should prompt for a region.
- First-pass setup should support AWS profile plus region only, while keeping
  the model extendable for advanced authentication later.
- Setup should default the Glue catalog name to `glue`, mention that default to
  the user, and allow the user to override it.
- Setup should not ask for a default namespace/database. The TUI discovers
  namespaces, and report commands must provide namespace-qualified table
  identifiers.
- Setup should attempt lightweight catalog validation, such as listing
  namespaces, but validation failure should warn rather than block saving.
- User changes from supported setup or TUI settings flows should update the
  same config file.
- If the config file already exists, running setup should use existing config
  values as prompt defaults. If the user changes values, setup should overwrite
  the config file with the updated values.
- Launching the TUI without a config file or equivalent environment variables
  should show a friendly setup-needed state instead of crashing.
- The setup-needed state should point users to `lakehouse-health-operator setup`
  and may offer the `s` shortcut for setup or settings.

## Catalog Browsing

- The first interactive TUI is focused on AWS Glue catalog browsing. Metadata
  file analysis remains supported elsewhere, but is not part of the first
  multi-panel catalog browser.
- The first version supports one configured Glue catalog, not multiple catalog
  profiles.
- Startup should avoid heavy AWS or catalog calls before first paint. The app
  shell should render quickly, then load namespaces in the background with a
  visible loading state.
- After namespaces load, the TUI may auto-select the first namespace to populate
  the tables panel.
- After tables load, the TUI may highlight the first table, but must not
  auto-analyze it.
- The first implementation should not persist the last selected namespace or
  table between runs.
- A compact header/status area should show catalog name, AWS profile or default
  credential chain, region, selected namespace, and relevant cache/freshness
  state.
- The TUI should discover available catalog namespaces, labeling them as
  databases for Glue-facing users.
- The namespace panel should show all accessible namespaces rather than hiding
  namespaces that appear empty or contain only unsupported tables.
- Hierarchical namespaces may be shown as flat fully-qualified entries in the
  first version.
- The first version should not recursively crawl namespace trees during
  startup. It should show entries returned by the normal namespace listing call
  as flat entries.
- Tables should be listed lazily when a namespace is selected.
- Tables should sort alphabetically by table name in the first implementation.
- Highlighting a table should not trigger full analysis; analysis starts only
  after explicit selection.
- Table rows should show the table name left-aligned and the table format
  right-aligned, for example `TABLE123        ICEBERG`.
- Table analysis should run in a background worker or task so users can keep
  navigating while analysis is in progress.
- In-progress analysis should show an in-panel loading state associated with the
  selected table.
- If the user navigates away, the analysis should remain associated with that
  table and update cached/view state when it completes.
- Concurrent table analyses should respect `RuntimePolicy.max_concurrency`.

## Table Format Classification

- Table format classification lives in the catalog access layer, not in TUI
  widgets.
- For AWS Glue, lightweight classification should use Glue table metadata from
  paginated `GetTables` responses when available.
- The catalog access layer should inspect table-level parameters and classify a
  table as `ICEBERG` when `Parameters["table_type"]` is `ICEBERG`.
- Table browsing should avoid one `GetTable` call or one PyIceberg
  `load_table` call per table for initial classification. PyIceberg
  `load_table` validates the Glue table and reads Iceberg metadata, so it is
  appropriate for explicit analysis after selection, not for classifying every
  row in a namespace.
- Do not rely on Glue `TableType` for Iceberg detection because values such as
  `EXTERNAL_TABLE` describe Glue/Hive table semantics, not the open table
  format.
- Do not request only `NAME` or `TABLE_TYPE` attributes when the TUI needs
  format labels, because that omits the table parameters needed for Iceberg
  detection.
- Use `ICEBERG` when the format is positively detected.
- Use `UNKNOWN` when the format cannot be determined cheaply.
- Non-`ICEBERG` Glue metadata should remain `UNKNOWN` in the initial table list
  even if it hints at Hive, Delta, or Hudi. Only explicit analysis failure
  should promote a table to `NON-ICEBERG`.
- Views should remain visible in the table list as `UNKNOWN` unless positively
  classified otherwise. If selected and unsupported, they can become
  `NON-ICEBERG`.
- Use `NON-ICEBERG` for tables that analysis confirms are unsupported, rather
  than shorter labels like `OTHER`.
- Selecting an `UNKNOWN` table attempts Iceberg analysis.
- If analysis succeeds, promote the classification to `ICEBERG`.
- If analysis confirms the table is unsupported, mark it `NON-ICEBERG`.
- Selecting a cached `NON-ICEBERG` table should show an unsupported-table state
  with options to go back or refresh the status.
- Table format classifications may be cached in the DuckDB operator cache so
  unsupported tables are not repeatedly analyzed during normal browsing.
- The workflow/cache should keep a UI-neutral classification source such as
  Glue parameters, analysis success, or analysis failure so debugging can tell
  why a table is labeled `ICEBERG`, `UNKNOWN`, or `NON-ICEBERG`.

## Detail Panel

- After analysis, show maintenance recommendations first, grouped by severity.
- Recommendation rows should show severity, type, and rationale prominently.
- Recommendation evidence and thresholds should be available in a secondary
  detail area so recommendations remain explainable without making the first
  line noisy.
- Then show calculation warnings, then grouped metrics.
- This grouping should be prepared as UI-neutral view data so Streamlit can use
  the same structure through expanders, tabs, metric groups, or dataframes.
- Partition detail should be report-driven. The first detail view should show
  high-level partition metrics, and may show partition-level rows only when
  `TableHealthReport.partition_metrics` already contains them.
- The TUI must not add new partition calculations.
- Recommended section order:
  1. Recommendations
  2. Warnings
  3. Files
  4. Records
  5. Partitions
  6. Snapshots
  7. Metadata and Evolution
  8. Source and Runtime

## Error Handling

- Catalog-level failures should appear in the relevant namespace or table panel
  as an error row or message.
- Table analysis failures should appear in the detail panel for that table.
- Calculation uncertainty should remain represented as calculation warnings on
  the table health report.
- Unsupported non-Iceberg tables should be represented as `NON-ICEBERG`, not as
  application crashes.
- Error messages should be concise by default, with room for expanded details
  later.
- Partial failures should not stop the TUI from remaining usable.

## Visual Design

- Use Textual and Rich as required dependencies.
- Use a modern, colorful, easy-on-the-eyes theme across panels, borders, focus
  states, and badges.
- Use `docs/operator-tui-concept.html` as the visual baseline for the
  modernized TUI. The Textual implementation should preserve the concept's
  information hierarchy, semantic labels, calm colorful palette, and
  recommendations-first detail flow while adapting it to terminal constraints.
- The layout must handle terminal resizing gracefully. Panels should resize
  without text overlap, and narrow widths should prioritize navigation while
  allowing detail sections to scroll.
- If the terminal is below the minimum practical size for the core layout, show
  a friendly increase-terminal-size state instead of broken panels.
- Ship one carefully designed theme in the first implementation rather than
  adding theme configuration.
- Use semantic colors consistently:
  - Critical: red
  - Warning: amber or yellow
  - Info: blue or cyan
  - Healthy or success: green
  - Unknown or not analyzed: muted gray
  - Cached or stale: secondary accent color
- Keep text labels such as `CRITICAL`, `UNKNOWN`, `CACHED`, and `NON-ICEBERG`
  visible so color is never the only signal.
- Prefer text-first labels over icons or symbols. Subtle ASCII symbols are
  acceptable when helpful, but the TUI should not depend on Nerd Font or emoji
  support.

## Keyboard Model

- The TUI should be keyboard-first, with mouse support where Textual provides it
  naturally.
- A compact contextual footer/help bar should show active-panel actions such as
  `Enter Analyze`, `/ Filter`, `r Refresh`, `e Export`, `s Setup`, and `q Quit`.
- Initial bindings:
  - Arrows and `j`/`k`: move
  - Enter: select or analyze
  - `/`: search or filter
  - `r`: refresh
  - `e`: export
  - `s`: setup or settings
  - `q`: quit

## Search and Filtering

- `/` should open a filter input scoped to the active panel.
- When focus is on catalog namespaces, filtering applies to namespaces.
- When focus is on tables, filtering applies to tables in the selected
  namespace.
- First-pass filtering should use case-insensitive substring matching.
- `Esc` should clear or cancel the active filter.
- Enter on a filtered table still explicitly selects or analyzes that table.
- Fuzzy search and structured filter chips are deferred to future
  improvements.

## Refresh and Cache Semantics

- `r` should refresh the active scope.
- When focus is on catalog namespaces, refresh namespace discovery.
- When focus is on tables, refresh tables and table format classifications for
  the selected namespace.
- When focus is on table detail, re-run analysis for the selected table and
  update cached classification or report metadata.
- Refresh should bypass cache for the active scope only, not wipe the whole
  DuckDB cache.
- `CACHED`, `FRESH`, and `REFRESHING` states should be visibly represented.
- If refresh fails, keep the last cached value visible when available and mark
  it with a stale or error state plus a clear message.
- Interactive TUI table detail reports may be cached and reused within the
  configured TTL so returning to a recently analyzed table is fast.
- Cached detail reports must be visibly marked as `CACHED`.
- Pressing `r` on table detail forces fresh analysis for that table.
- The non-interactive report command should always perform fresh analysis.

## Future Improvements

- Deferred improvements are tracked in
  [operator-tui-future-improvements.md](operator-tui-future-improvements.md).

## Cleanup

- Analyzer and visualization code should not emit ad hoc debug prints during
  normal operation.
- User-facing uncertainty belongs in calculation warnings on the canonical table
  health report.
- Legacy operator flag paths have been removed now that setup, TUI, report,
  refresh, cache, export, and shared detail workflows are available through the
  final command structure.

## Testing

- Workflow modules should carry the heaviest behavior tests because they own
  UI-neutral use cases.
- Cover config file read/write, AWS profile parsing, catalog classification,
  cache semantics, report grouping, and report command stdout/file behavior.
- Textual tests should stay lightweight and verify key bindings, panel
  population, selection-triggered analysis, loading states, and error states.
- Avoid screenshot-perfect visual tests; prefer targeted rendering and state
  assertions that are stable across harmless styling changes.
- During development, use Textual devtools or manual screenshots to verify
  normal and narrow terminal layouts, resizing behavior, and readability.
