# Streamlit Dashboard UX Review (Post Core Migration)

Date: 2026-05-22  
Issue: #15 (`docs/issues/0014-review-streamlit-dashboard-ux-after-core-migration.md`)  
Reviewer scope: dashboard UX and report rendering surfaces only (no health-calculation changes)

## Review method

This review used behavior-first verification through existing public report/dashboard surfaces:

1. Canonical Streamlit report renderer (`src/visualization/table_health_report.py`) for report sections and warning/recommendation/history rendering.
2. Streamlit dashboard flow (`src/visualization/metrics_dashboard.py`) for source selection and catalog-vs-metadata UX.
3. Catalog overview renderer (`src/visualization/catalog_overview.py`) for table-list and summary warning behavior.
4. Existing focused tests as executable acceptance checks.

## Acceptance criteria coverage

### 1) Reviewed with metadata-file and catalog-table report examples

Covered by existing report-analysis behaviors and Streamlit wiring:

- Metadata-file path: `streamlit_app.get_table_metrics(..., use_metadata_file=True)` and report-renderer tests.
- Catalog-table path: `streamlit_app.get_table_metrics(..., use_metadata_file=False)` plus catalog overview flow and tests.

### 2) Checked source selection, warnings, recommendations, partition views, snapshots, evolution history

Status: reviewed and present, with polish gaps noted.

- Source selection:
  - `metrics_dashboard.show_dashboard` radio mode (`Catalog Tables` vs `Metadata File`) and source-specific inputs are present.
- Warnings:
  - Report-level calculation warnings render under `Calculation Warnings`.
  - Catalog failures render as warning rows in overview.
- Recommendations:
  - Maintenance recommendations render with severity (`info`/`warning`/`critical`) and evidence/threshold captions.
- Partition views:
  - Partition metrics render when `report.partition_metrics` exists.
- Snapshots:
  - Snapshot and expirable-candidate metrics are present in report data and validated in tests.
- Evolution history:
  - Retained schema/property changes render in a table under `Table Evolution History`.

### 3) Remaining dashboard polish captured as follow-up issues

Created focused follow-ups:

- `docs/issues/0016-add-streamlit-catalog-drilldown-for-table-health-report.md`
- `docs/issues/0017-polish-streamlit-error-and-source-context-ux.md`

### 4) No new health calculations introduced into Streamlit

Confirmed: this review added documentation only. No calculation/report logic changes were made.

### 5) Review result documented for future slices

This file is the review artifact for future implementation slices.

## Findings and UX polish candidates

1. Catalog overview is summary-only; there is no in-dashboard drilldown from a selected catalog table to full report sections (partitions, recommendations, warnings, evolution history) in the same session flow.
2. Metadata-mode failures show raw traceback text directly in the dashboard, which is noisy for operators and can obscure actionable guidance.
3. Source context is implicit once results are rendered; report views do not clearly restate whether data came from metadata-file input vs catalog table source.

## Verification commands run

```bash
uv run pytest tests/test_streamlit_table_health_report.py tests/test_catalog_overview.py tests/test_snapshot_health_report.py tests/test_table_evolution_history.py
```

Result: `31 passed`

## Residual risks

- This review used code/test surfaces and did not include a live AWS-backed interactive Streamlit session against real catalog credentials in this environment.
- Because #14 exports may land in parallel, additional UX alignment may be needed once export actions are visible from Streamlit/TUI surfaces.
