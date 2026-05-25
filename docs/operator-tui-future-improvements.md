# Operator TUI Future Improvements

This file tracks ideas intentionally deferred from the first Textual TUI
modernization pass. Items here are candidates, not committed scope.

## Search and Filtering

- Add fuzzy search after case-insensitive substring filtering proves useful.
- Add structured filter chips such as `format:iceberg`, `format:unknown`,
  `format:non-iceberg`, `status:cached`, or `severity:critical`.
- Add saved filters for repeated operator workflows.

## Catalog Navigation

- Add named multi-catalog profiles if operators need to switch between multiple
  Glue catalogs or environments from one TUI session.
- Add async workflow adapters only if future catalog or analysis dependencies
  provide real async I/O.
- Add recursive namespace browsing if flat entries from the normal namespace
  listing are not enough for hierarchical catalog layouts.
- Add a collapsible tree for hierarchical catalog namespaces if flat
  fully-qualified namespace entries become hard to scan.
- Add richer table metadata in the table list when it can be fetched cheaply.
- Add interactive sorting controls for table lists and detail sections if
  alphabetical table ordering and fixed detail ordering are not enough.
- Persist the last selected namespace/table in a dedicated UI state store if
  returning users need faster resume behavior.

## Setup and Settings

- Add an in-app Textual settings screen that edits the same config file as
  `lakehouse-health-operator setup`.
- Add guided revalidation for changed catalog settings from inside the TUI.
- Add a guided recommendation-threshold editor if manual TOML editing is too
  awkward for operators.
- Add advanced AWS authentication support such as role ARN, session credentials,
  or injected sessions when production authentication requirements are clear.

## Visual Exploration

- Add theme customization only after the default theme has stabilized.
- Add compact terminal-native visual summaries only when they do not duplicate
  the richer Streamlit dashboard.
- Consider optional sparkline-style summaries for trends if retained metadata
  is available and the underlying values already exist in workflow/view data.

## Operator Convenience

- Add copy-to-clipboard actions for export paths, table identifiers, or expanded
  error details if terminal/OS support can be handled gracefully.

## Maintenance Workflows

- Add maintenance action orchestration only after confirmation, permissions,
  run IDs, status tracking, and auditability are designed.

## Table Formats

- Add non-Iceberg table format support only when real table format adapters
  exist. Until then, non-Iceberg tables should remain visible but unsupported.
