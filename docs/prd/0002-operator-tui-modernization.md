# PRD: Operator TUI Modernization

Intended issue label: `ready-for-agent`

## Problem Statement

Lakehouse Health Analyzer has a canonical analysis core, but the terminal
operator experience still feels like a prototype. The current operator command
prints dense tables and long report text, depends on environment variables for
catalog, namespace, and table selection, and makes users rerun commands when
they want to inspect a different table. Recommendations and warnings are easy
to miss, cache freshness is not part of a modern interactive flow, and the
terminal UI is not pleasant enough for repeated operator use.

Operators need a fast, readable, keyboard-first terminal experience for AWS
Glue catalog browsing. They should be able to configure a Glue catalog once,
discover available catalog namespaces, inspect tables, see whether a table is
positively identified as Iceberg, analyze selected tables on demand, and export
fresh or currently displayed reports without changing environment variables for
every table.

Maintainers also need a cleaner architecture. The TUI, CLI, and Streamlit
should not each reimplement catalog access, table grouping, report export
behavior, or display ordering. Shared UI-neutral workflows should orchestrate
catalog access, analysis, caching, report grouping, and export behavior, while
rendering layers stay focused on layout and styling.

## Solution

Build a modern Textual and Rich based operator TUI focused on AWS Glue catalog
browsing. The default operator command opens an interactive multi-panel app
with a compact environment header, catalog namespace panel, table list panel,
and table detail panel. Users configure profile, region, and catalog defaults
through a Rich-powered setup command that writes a commented TOML config file.

The TUI discovers Glue catalog namespaces, lists tables lazily for the selected
namespace, and shows table names left-aligned with right-aligned table format
labels such as `ICEBERG`, `UNKNOWN`, and `NON-ICEBERG`. It uses Boto3 Glue
metadata APIs for lightweight browsing and Table Format Classification, and
uses PyIceberg only for explicit Iceberg loading and analysis after user
selection.

Table analysis runs in background workers so the app remains responsive.
Selected table details show Maintenance Recommendations first, grouped by
severity, followed by Calculation Warnings and grouped report sections for
files, records, partitions, snapshots, metadata and evolution, and source and
runtime. Cached table detail reports may be reused in the TUI within TTL and
must be visibly marked as cached. The non-interactive report command always
performs fresh analysis.

Introduce shared UI-neutral workflows for catalog browsing, report generation,
setup/configuration, cache interaction, and grouped table detail view data. TUI,
CLI, and Streamlit can render the same workflow outputs without owning Health
Metric calculations or catalog-specific behavior.

## User Stories

1. As a lakehouse operator, I want to run one command and open an interactive TUI, so that I can browse table health without memorizing flags.
2. As a lakehouse operator, I want a short `lh` command alias, so that repeated terminal use feels fast.
3. As a lakehouse operator, I want the existing descriptive command to keep working, so that current usage does not break immediately.
4. As a first-time user, I want the TUI to show a friendly setup-needed state when no config exists, so that I am not greeted by missing environment variable errors.
5. As a first-time user, I want a setup command, so that I can configure AWS Glue once instead of exporting variables on every run.
6. As a first-time user, I want setup to read named AWS profiles, so that I can pick from my local AWS configuration.
7. As a first-time user, I want setup to continue when no named profile exists, so that default AWS credential chain environments still work.
8. As a Glue user, I want setup to require an AWS region, so that Glue API calls have an explicit region.
9. As a Glue user, I want setup to infer region from the selected profile when available, so that I do not retype known AWS configuration.
10. As a Glue user, I want setup to prompt for region when it cannot infer one, so that the saved config is complete.
11. As a Glue user, I want setup to default the catalog name to `glue`, so that the common path is easy.
12. As a Glue user, I want setup to let me override the catalog name, so that non-default catalog names still work.
13. As a Glue user, I do not want setup to ask for a default database, so that browsing remains dynamic and report commands stay explicit.
14. As a configuration-conscious user, I want setup to write a TOML config file, so that settings are readable and easy to edit.
15. As a configuration-conscious user, I want generated config files to include comments, so that I can discover available settings without digging through code.
16. As a returning user, I want rerunning setup to use current config values as defaults, so that editing configuration is safe and predictable.
17. As a returning user, I want changed setup values to update the same config file, so that the file remains the source of my default choices.
18. As a platform engineer, I want config precedence to be explicit, so that defaults, config file, environment variables, and UI/CLI overrides do not conflict mysteriously.
19. As a platform engineer, I want recommendation thresholds visible in config, so that advanced users can tune policy manually.
20. As a platform engineer, I do not need a guided threshold editor in the first version, so that setup stays focused.
21. As a lakehouse operator, I want setup to validate catalog connectivity when possible, so that typos are caught early.
22. As a lakehouse operator, I want setup validation failures to warn rather than block saving, so that temporary AWS issues do not prevent configuration.
23. As a lakehouse operator, I want the TUI to show catalog name, AWS profile or default credential chain, region, selected namespace, and freshness state, so that I know which environment I am inspecting.
24. As a lakehouse operator, I want the app shell to render before heavy AWS calls, so that startup feels responsive.
25. As a lakehouse operator, I want namespaces to load in the background, so that network or auth delays are visible inside the app.
26. As a Glue user, I want catalog namespaces labeled as databases in Glue-facing UI, so that the terminology matches my AWS mental model.
27. As a domain maintainer, I want Catalog Namespace to remain the canonical term, so that non-Glue catalog concepts stay precise.
28. As a lakehouse operator, I want all accessible namespaces shown, so that the TUI reflects what Glue exposes.
29. As a lakehouse operator, I do not want the first TUI to recursively crawl namespace trees, so that startup does not create many catalog calls.
30. As a lakehouse operator, I want hierarchical namespaces shown as flat fully qualified entries when present, so that nested names are still visible.
31. As a lakehouse operator, I want the first namespace auto-selected after load, so that the table panel populates quickly.
32. As a lakehouse operator, I want tables listed lazily after namespace selection, so that only relevant table metadata is loaded.
33. As a lakehouse operator, I want tables sorted alphabetically by name, so that browsing is predictable.
34. As a lakehouse operator, I want the first table highlighted after load, so that keyboard navigation starts in a useful place.
35. As a lakehouse operator, I do not want the TUI to auto-analyze highlighted tables, so that scrolling does not trigger expensive analysis.
36. As a lakehouse operator, I want full analysis to start only on explicit selection, so that I control AWS and metadata workload.
37. As a lakehouse operator, I want table rows to show name on the left and format on the right, so that I can scan table identity and format quickly.
38. As a lakehouse operator, I want `ICEBERG` shown only when positively detected, so that the TUI does not overclaim.
39. As a lakehouse operator, I want `UNKNOWN` shown when table format is not cheaply known, so that browsing remains fast.
40. As a lakehouse operator, I want `NON-ICEBERG` shown only after explicit analysis confirms unsupported format, so that unsupported state is based on evidence.
41. As a lakehouse operator, I want views to remain visible as `UNKNOWN`, so that catalog contents do not disappear unexpectedly.
42. As a lakehouse operator, I want selecting an `UNKNOWN` table to attempt Iceberg analysis, so that sparse Glue metadata does not block valid tables.
43. As a lakehouse operator, I want successful analysis to promote a table to `ICEBERG`, so that future browsing reflects what was learned.
44. As a lakehouse operator, I want failed unsupported analysis to promote a table to `NON-ICEBERG`, so that repeated unsupported attempts are avoided.
45. As a lakehouse operator, I want cached `NON-ICEBERG` selections to show an unsupported-table state with back and refresh options, so that I can recover if metadata changed.
46. As a maintainer, I want the workflow/cache to store classification source, so that debugging can explain why a table is `ICEBERG`, `UNKNOWN`, or `NON-ICEBERG`.
47. As a maintainer, I want initial format classification to use Glue table parameters, so that the TUI avoids one `GetTable` or PyIceberg load per table.
48. As a maintainer, I want Glue `TableType` ignored for Iceberg detection, so that `EXTERNAL_TABLE` is not confused with table format.
49. As a maintainer, I want PyIceberg used for explicit Iceberg loading and analysis, so that the analyzer keeps using the correct Iceberg table boundary.
50. As a maintainer, I want Boto3 Glue metadata calls centralized in the catalog access layer, so that TUI, CLI, and Streamlit do not each implement Glue browsing.
51. As a lakehouse operator, I want table analysis to run in the background, so that I can continue navigating while analysis runs.
52. As a lakehouse operator, I want in-progress analysis to show a loading state, so that I know work is happening.
53. As a lakehouse operator, I want analysis to stay associated with a table if I navigate away, so that results can update the right cached state later.
54. As a platform engineer, I want background analyses to respect configured concurrency, so that the tool does not overwhelm Glue or metadata storage.
55. As a lakehouse operator, I want recently analyzed table details reused within TTL, so that returning to a table is fast.
56. As a lakehouse operator, I want cached detail reports visibly marked as `CACHED`, so that I do not mistake cached data for fresh analysis.
57. As a lakehouse operator, I want pressing refresh on table detail to force fresh analysis, so that I can update stale results.
58. As a lakehouse operator, I want refresh to apply to the active scope, so that I do not wipe unrelated cached data.
59. As a lakehouse operator, I want refresh failures to keep last cached values visible when available, so that partial failures do not erase useful context.
60. As a lakehouse operator, I want `CACHED`, `FRESH`, and `REFRESHING` states visible, so that freshness is easy to understand.
61. As a terminal user, I want keyboard-first navigation, so that the tool is fast to operate without a mouse.
62. As a terminal user, I want arrow keys and `j`/`k` for movement, so that navigation feels familiar.
63. As a terminal user, I want Enter to select or analyze, so that explicit actions are predictable.
64. As a terminal user, I want `/` to filter the active panel, so that I can find namespaces and tables quickly.
65. As a terminal user, I want filtering to be scoped to the active panel, so that namespace and table filters do not interfere with each other.
66. As a terminal user, I want case-insensitive substring filtering first, so that search is simple and dependency-free.
67. As a terminal user, I want Esc to clear or cancel filtering, so that I can recover quickly.
68. As a terminal user, I want a compact footer showing active key bindings, so that the app is discoverable without clutter.
69. As a terminal user, I want mouse support where Textual provides it naturally, so that I can still click when convenient.
70. As a lakehouse operator, I want Maintenance Recommendations shown first in table detail, so that actionable findings are not buried.
71. As a lakehouse operator, I want recommendations grouped by severity, so that critical findings stand out.
72. As a lakehouse operator, I want recommendation severity, type, and rationale prominent, so that I can triage quickly.
73. As a lakehouse operator, I want evidence and thresholds available in secondary detail, so that recommendations remain explainable.
74. As a lakehouse operator, I want Calculation Warnings shown after recommendations, so that uncertainty remains visible.
75. As a lakehouse operator, I want metrics grouped into files, records, partitions, snapshots, metadata and evolution, and source and runtime, so that dense report data is readable.
76. As a Streamlit maintainer, I want the same grouped detail view model available to Streamlit, so that Streamlit and TUI do not drift in report structure.
77. As a CLI maintainer, I want the same grouped detail view model available to command output, so that non-interactive output can share ordering decisions.
78. As an export user, I want raw JSON exports to preserve canonical Table Health Report structure, so that tools can consume complete data.
79. As an export user, I want Markdown exports to be human-readable, so that findings can be shared.
80. As a TUI user, I want pressing `e` on table detail to export the displayed report, so that I can save what I am viewing.
81. As a TUI user, I want TUI exports to include cache-status metadata, so that recipients know whether the report was fresh or cached.
82. As a TUI user, I want to refresh before export when I need fresh output, so that freshness remains under my control.
83. As an automation user, I want `lh report namespace.table --format json` to print fresh JSON to stdout, so that scripts can pipe it.
84. As an automation user, I want `lh report namespace.table --format markdown` to print fresh Markdown to stdout, so that command output is predictable.
85. As an automation user, I want `lh report` always fresh, so that scripts do not accidentally consume stale cache.
86. As an automation user, I want multiple report formats to require an output directory, so that stdout remains single-format and scriptable.
87. As an automation user, I want `--output` to mean exact file path for one format, so that single-file export is explicit.
88. As an automation user, I want `--output-dir` to generate filenames for one or many formats, so that multi-format export is easy.
89. As an automation user, I want unsupported tables to exit non-zero, so that CI and scripts can detect failure.
90. As a terminal user, I want a colorful but calm visual theme, so that long sessions are easier on the eyes.
91. As a terminal user, I want semantic colors for critical, warning, info, healthy, unknown, and cached states, so that scanning is fast.
92. As a terminal user, I want labels visible in addition to color, so that low-color terminals remain usable.
93. As a terminal user, I want text-first labels rather than font-specific icons, so that the TUI works across terminals.
94. As a terminal user, I want the layout to handle resizing, so that panels do not overlap when my terminal changes size.
95. As a terminal user, I want a friendly minimum-size message, so that tiny terminals fail gracefully.
96. As a maintainer, I want workflow modules tested heavily, so that behavior remains stable while UI changes.
97. As a maintainer, I want Textual tests to stay lightweight, so that style changes do not create brittle failures.
98. As a maintainer, I want manual visual verification during development, so that the polished TUI is checked at realistic sizes.
99. As a maintainer, I want legacy operator paths removed after the new path is complete, so that stale CLI behavior does not linger.
100. As a maintainer, I want old debug prints removed, so that normal operation stays clean and deliberate.

## Implementation Decisions

- Use Textual and Rich as required dependencies because the default operator
  command opens the interactive TUI and will directly use Rich renderables and
  styles.
- Add `lh` as a short alias to the same operator CLI entry point while keeping
  the existing long command for compatibility.
- Make the default operator command launch the interactive Textual TUI once the
  migration is complete.
- Remove the temporary legacy operator flags once equivalent setup, refresh,
  cache, report, and TUI workflows are available through the new command shape.
- Add a setup subcommand implemented as a plain Rich-powered terminal prompt
  flow, not a Textual form in the first version.
- Persist reusable setup defaults to a commented TOML config file under the
  user config directory.
- Use configuration precedence of built-in defaults, setup config file,
  environment variables, then CLI flags or UI overrides.
- Generated config should include supported defaults, including recommendation
  threshold settings for discoverability.
- Setup should support AWS profile plus region only in the first version.
- Setup should default the Glue catalog name to `glue` while allowing override.
- Setup should not ask for a default Catalog Namespace.
- Setup should validate catalog connectivity with a lightweight namespace list
  when possible, but validation failure should warn rather than block saving.
- Rerunning setup should use existing config values as prompt defaults and
  rewrite the config file with updated values.
- Launching the TUI without config should show a setup-needed state rather than
  raising missing environment variable errors.
- Introduce a shared UI-neutral workflows package for use-case orchestration.
- Start with a catalog browser workflow as the first deep module because it
  centralizes catalog browsing, table classification, selected-table analysis,
  cache interaction, and table detail view data.
- Add a report command workflow for fresh non-interactive JSON and Markdown
  report generation.
- Add a setup/config workflow for config file read/write, AWS profile parsing,
  region handling, and validation behavior.
- Keep workflow APIs synchronous in the first version because current
  PyIceberg, DuckDB cache, and analysis calls are synchronous.
- Run synchronous workflows from Textual background workers so the UI remains
  responsive.
- Keep async workflow adapters out of scope until future dependencies provide
  real async I/O.
- Centralize Glue catalog interaction behind the project catalog access layer
  rather than calling Glue or PyIceberg directly from interfaces.
- Use Boto3 Glue metadata APIs inside the catalog access layer for lightweight
  namespace/table browsing and Table Format Classification.
- Use PyIceberg inside the catalog access layer for explicit Iceberg table
  loading and analysis.
- Use paginated Glue table metadata listings for initial table rows and inspect
  table-level parameters to identify Iceberg tables.
- Classify a table as `ICEBERG` when Glue table parameters contain
  `table_type=ICEBERG`.
- Classify tables as `UNKNOWN` when Iceberg cannot be positively detected
  cheaply.
- Do not use Glue `TableType` as an open table format signal.
- Do not use one Glue `GetTable` call or one PyIceberg table load per table
  during initial browsing.
- Promote `UNKNOWN` tables to `ICEBERG` when explicit analysis succeeds.
- Promote `UNKNOWN` tables to `NON-ICEBERG` when explicit analysis confirms
  unsupported format.
- Store a UI-neutral classification source such as Glue parameters, analysis
  success, or analysis failure with cached classification data.
- Keep views visible as `UNKNOWN` unless positively classified otherwise.
- Cache Table Format Classification in the operator DuckDB cache to avoid
  repeated unsupported analysis attempts.
- Cache interactive table detail reports within TTL so returning to recently
  analyzed tables is fast.
- Mark cached table detail reports visibly as `CACHED`.
- Make `lh report` always perform fresh analysis and never use cached reports.
- Allow TUI export to export the currently shown report, including cached
  reports, with cache-status metadata.
- Use active-scope refresh semantics: refresh namespaces, tables and
  classifications, or selected table detail depending on focus.
- Preserve last cached values on refresh failure when available and mark the
  state as stale or error.
- Build a UI-neutral table detail view model alongside the canonical Table
  Health Report.
- The table detail view model may define grouping, section order, severity
  ordering, classification state, and cache status.
- The table detail view model must not contain rendering-framework concerns
  such as Textual widgets, Streamlit containers, Rich styles, terminal colors,
  or chart definitions.
- Detail sections should be ordered as recommendations, warnings, files,
  records, partitions, snapshots, metadata and evolution, and source and
  runtime.
- Recommendation rows should prioritize severity, recommendation type, and
  rationale, with evidence and thresholds available in secondary detail.
- Partition details should be report-driven and must not introduce new TUI-side
  partition calculations.
- The first interactive TUI is Glue catalog focused. Metadata File Source
  analysis remains supported elsewhere.
- The first TUI supports one configured Glue catalog, not multiple named
  catalog profiles.
- The first TUI should not persist last selected namespace or table between
  runs.
- Namespace discovery should not recursively crawl namespace trees in the first
  version.
- Tables should be sorted alphabetically by table name in the first version.
- The TUI should auto-select the first namespace after namespaces load and may
  highlight the first table after tables load, but must not auto-analyze.
- Filtering should be scoped to the active panel and use case-insensitive
  substring matching in the first version.
- Fuzzy search, structured filter chips, interactive sorting, recursive
  namespace browsing, persisted UI state, theme customization, copy-to-clipboard
  actions, multi-catalog profiles, advanced AWS auth, and guided threshold
  editing are deferred to a future-improvements document.
- Use a keyboard-first interaction model with a compact contextual footer/help
  bar.
- Use a compact header/status area for catalog, profile, region, selected
  namespace, and freshness state.
- Ship one carefully designed colorful theme in the first implementation.
- Use the interactive operator TUI HTML concept as the visual baseline for the
  Textual implementation, preserving its information hierarchy, semantic
  labels, calm colorful palette, and recommendations-first detail flow while
  adapting it to terminal constraints.
- Use semantic colors for severity, state, freshness, and success, while
  keeping text labels visible.
- Prefer text-first labels over icons or symbols so the TUI does not depend on
  Nerd Font or emoji support.
- Handle terminal resizing gracefully and show a friendly minimum-size state
  when the terminal is too small.
- Remove ad hoc debug prints from analyzer and visualization paths during this
  modernization.
- Remove legacy operator module paths after the new setup command, TUI, report
  command, catalog browser workflow, shared detail view model, and cleanup
  tests are in place.

## Testing Decisions

- Tests should verify externally observable behavior and report semantics, not
  private implementation details.
- Workflow modules should carry the heaviest behavior test coverage because
  they own UI-neutral use cases.
- Test config file creation, config precedence, rerun setup defaults, TOML
  comments where practical, AWS profile parsing, required region behavior, and
  validation warning behavior.
- Test setup behavior without relying on real AWS calls by using fake profile
  data and fake catalog validation.
- Test catalog access behavior with fake Glue responses, including paginated
  namespace/table listing, table parameters, missing parameters, views, and
  partial failures.
- Test Table Format Classification rules: positive `ICEBERG`, conservative
  `UNKNOWN`, promotion to `ICEBERG` on successful analysis, and promotion to
  `NON-ICEBERG` on unsupported analysis.
- Test that Glue `TableType` does not drive Iceberg classification.
- Test that browsing/classification does not require per-table PyIceberg loads.
- Test cache behavior for classifications, cached detail reports, TTL expiry,
  active-scope refresh, stale/error fallback, and visible cache status.
- Test table detail view model grouping, severity ordering, section ordering,
  recommendation evidence/threshold placement, and inclusion of the canonical
  Table Health Report.
- Test that partition detail view data uses only existing report partition
  metrics.
- Test report command behavior for fresh analysis, namespace-qualified
  identifiers, single-format stdout, multiple-format output directory
  requirement, exact output path behavior, unsupported-table non-zero exit, and
  stderr messages.
- Test TUI export behavior through workflow boundaries, including cached report
  metadata and written path feedback.
- Textual tests should stay lightweight and cover key bindings, panel
  population, setup-needed state, filter state, selection-triggered background
  analysis, loading states, error states, and footer/header behavior.
- Avoid screenshot-perfect automated tests because styling and layout will
  evolve.
- Use manual or development-time Textual screenshots/devtools to verify normal
  and narrow terminal layouts, resizing behavior, color readability, and no
  text overlap.
- Existing operator workflow tests, configuration tests, catalog overview cache
  tests, report export tests, and Streamlit report renderer tests are useful
  prior art and should be migrated toward workflow-level behavior coverage.

## Out of Scope

- Executing, scheduling, or orchestrating Maintenance Actions such as compaction
  or cleanup.
- Non-Iceberg table format analysis.
- Delta Lake or Hudi adapters.
- Metadata File Source browsing in the first interactive TUI.
- Multiple configured Glue catalog profiles in the first version.
- Recursive namespace crawling in the first version.
- Persisting last selected namespace or table in the first version.
- Fuzzy search and structured filter chips.
- Interactive table or metric sorting beyond default alphabetical table order
  and fixed detail section order.
- Theme customization.
- Clipboard integration.
- Guided recommendation-threshold editor.
- Advanced AWS authentication such as role ARN, session credentials, or
  injected sessions.
- Async workflow APIs.
- Screenshot-perfect automated visual regression tests.
- Replacing Streamlit or removing existing Streamlit functionality.
- Removing legacy operator code before the new setup, TUI, report, workflow,
  and cleanup paths are in place.

## Further Notes

- This PRD builds on the existing glossary terms: Catalog Namespace, Table
  Format Classification, Table Health Report, Calculation Warning, Maintenance
  Recommendation, Recommendation Severity, and Table Format Adapter.
- The key architecture boundary is that TUI, CLI, and Streamlit render shared
  workflow outputs. They do not own catalog-specific behavior or canonical
  Health Metric calculations.
- Lightweight Glue browsing uses Boto3 metadata APIs for performance and to
  avoid per-table PyIceberg metadata loads.
- Explicit Iceberg analysis continues to use PyIceberg as the table-loading
  boundary.
- A future-improvements document tracks deferred ideas so they are visible
  without expanding the first implementation scope.
