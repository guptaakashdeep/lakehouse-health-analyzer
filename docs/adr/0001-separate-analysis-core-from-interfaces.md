# Separate Analysis Core from Interfaces

Lakehouse Health Analyzer started as a prototype where table loading, metric derivation, and dashboard rendering could be mixed together. We decided to introduce an analysis core that derives health metrics independently from table sources and user interfaces, so Streamlit and the future TUI render the same canonical metrics instead of re-implementing calculations.

As modernization stabilizes, TUI, CLI, and Streamlit should depend on shared
UI-neutral workflows for catalog interaction, table analysis, report exports,
and any calculation that has the same meaning regardless of rendering surface.
These use-case orchestration modules should live under `src/workflows/`.
Rendering layers may format, group, filter, colorize, or lay out existing
report values, but they should not own catalog-specific behavior or canonical
health calculations. Legacy paths can remain temporarily during the transition,
but the direction is to remove them once the shared workflow boundaries cover
the supported use cases.
For the operator experience, cleanup should happen after the new setup command,
interactive TUI, report command, catalog browser workflow, and shared table
detail view model are in place.

Workflows orchestrate existing layers rather than replacing them. For example,
a catalog browser workflow may receive a selected namespace and table, build the
appropriate analyzer configuration, call the analysis layer to produce a
canonical table health report, cache UI-neutral status such as table format
classification, and return view data that any interface can render. It must not
re-implement health metric calculations, directly depend on Textual widgets, or
make catalog calls that belong in the catalog access layer.

When workflows prepare table detail output, they may return a UI-neutral view
model alongside the canonical table health report. That view model can define
shared grouping, section order, severity ordering, classification state, and
cache status so TUI, CLI, and Streamlit present the same report structure. It
must remain free of rendering-framework concerns such as Textual widgets,
Streamlit containers, Rich styles, terminal colors, or chart definitions.

Analyzer and visualization code should not emit ad hoc debug prints during
normal operation. User-facing uncertainty should be represented as calculation
warnings in the canonical table health report, while terminal commands and TUIs
should control their own deliberate output.
