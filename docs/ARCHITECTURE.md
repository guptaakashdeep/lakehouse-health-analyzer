# Lakehouse Health Analyzer Architecture

This project analyzes lakehouse table metadata through a canonical
`TableHealthReport` model shared by the terminal workflow, Streamlit, exports,
and tests.

## Current component map

```mermaid
flowchart TB
    %% Arrows point from callers/orchestrators toward the layers they use.

    subgraph L0["Layer 0 — Entry points and user interfaces"]
        direction LR
        TUIENTRY["operator_tui.__init__<br/>lh / lakehouse-health-operator"]
        TUI["operator_tui.app<br/>Textual catalog browser"]
        STREAMLIT["streamlit_app.py<br/>Streamlit entrypoint"]
    end

    subgraph L1["Layer 1 — Presentation and exports"]
        direction LR
        TUIRENDER["operator_tui.rendering<br/>widgets / theme"]
        TUIEXPORT["operator_tui.export<br/>interactive export"]
        DASH["visualization.metrics_dashboard<br/>dashboard shell"]
        STREPORT["visualization.table_health_report<br/>Streamlit report rendering"]
        STOVERVIEW["visualization.catalog_overview<br/>Streamlit overview"]
        EXPORTS["report_exports<br/>JSON / Markdown"]
    end

    subgraph L2["Layer 2 — UI-neutral workflows"]
        direction LR
        SETUP["workflows.setup<br/>config.toml setup"]
        CB["workflows.catalog_browser<br/>namespaces, tables,<br/>format classification"]
        TD["workflows.table_detail<br/>selected table analysis<br/>TableDetailView"]
        RC["workflows.report_command<br/>fresh report command"]
    end

    subgraph L3["Layer 3 — Shared runtime services"]
        direction LR
        CFG["configuration<br/>AnalyzerConfiguration"]
        CACHE["operator_cache<br/>DuckDB OperatorCache"]
        GLUEBROWSE["catalogs.glue<br/>GlueCatalogBrowserAccess<br/>Boto3 metadata browsing"]
    end

    subgraph L4["Layer 4 — Table sources"]
        direction LR
        MF["Metadata File Source<br/>metadata.json"]
        GLUECAT["AWS Glue Catalog Table Source<br/>Glue database/table"]
    end

    subgraph L5["Layer 5 — Analysis core and canonical model"]
        direction LR
        ANALYSIS["analysis.iceberg<br/>PyIceberg loading + health calculations"]
        REPORT["analysis.report<br/>TableHealthReport<br/>HealthMetric / DisplayStatistic<br/>CalculationWarning / Recommendation"]
    end

    TUIENTRY --> TUI
    TUIENTRY --> SETUP
    TUIENTRY --> RC
    TUIENTRY --> CFG

    TUI --> TUIRENDER
    TUI --> TUIEXPORT
    TUI --> CB
    TUI --> TD

    STREAMLIT --> DASH
    STREAMLIT --> CFG
    DASH --> STREPORT
    DASH --> STOVERVIEW
    STREPORT --> TD
    STOVERVIEW --> CACHE

    TUIEXPORT --> EXPORTS
    RC --> EXPORTS
    EXPORTS --> REPORT

    SETUP --> CFG
    CB --> CFG
    CB --> CACHE
    CB --> GLUEBROWSE
    TD --> CFG
    TD --> CACHE
    TD --> ANALYSIS
    RC --> CFG
    RC --> ANALYSIS

    CFG --> MF
    CFG --> GLUECAT
    GLUEBROWSE --> GLUECAT
    ANALYSIS --> MF
    ANALYSIS --> GLUECAT
    ANALYSIS --> REPORT
    TUIRENDER --> REPORT
```

## Main layers

- **Configuration layer**: `configuration.AnalyzerConfiguration` normalizes table
  source, analysis policy, runtime policy, and output policy.
- **Calculation layer**: `analysis.iceberg` loads Iceberg tables with PyIceberg
  and derives canonical health metrics.
- **Common representation layer**: `analysis.report.TableHealthReport` is the
  shared report shape rendered by TUI, Streamlit, CLI, and exports.
- **Workflow layer**: `workflows/*` owns UI-neutral use cases such as catalog
  browsing, selected-table detail analysis, setup, and scriptable report
  generation.
- **Cache layer**: `operator_cache.OperatorCache` stores temporary DuckDB-backed
  namespace listings, table listings, table classifications, and table detail
  reports.
- **Interface layers**:
  - `operator_tui/*` renders and operates the Textual terminal interface.
  - `streamlit_app.py` and `visualization/*` render Streamlit views.
  - `report_exports` renders JSON and Markdown report outputs.

## Important direction

Rendering layers may format, group, filter, colorize, or lay out report values,
but they should not own catalog behavior or canonical health calculations.
Those belong in workflow, catalog-access, and analysis modules.
