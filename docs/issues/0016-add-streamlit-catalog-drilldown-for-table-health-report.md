# Add Streamlit catalog drilldown for full table health report

Labels: `ready-for-agent`, `UX`

## Parent

#1

## What to build

Improve Streamlit catalog-mode ergonomics by letting operators select a table from catalog overview and open the full canonical **Table Health Report** (health metrics, warnings, recommendations, partitions, snapshots, evolution history) without switching modes or manually re-entering identifiers.

## Acceptance criteria

- [ ] Catalog overview includes an explicit per-table drilldown action.
- [ ] Selecting a table runs the same canonical report path used elsewhere (no duplicate health-calculation logic in Streamlit).
- [ ] Drilldown view renders warnings, recommendations, partition view, snapshot metrics, and table evolution history for the selected catalog table.
- [ ] The selected table context is preserved in session state for repeat navigation.
- [ ] Tests cover drilldown behavior through Streamlit-facing public surfaces.

## Blocked by

- #15
