# Use PyIceberg for Glue Catalog Loading

AWS Glue Catalog support will be implemented by configuring PyIceberg's native Glue catalog rather than building a separate Glue table-loading client. The project may own configuration normalization for profiles, regions, and future credential sources, but PyIceberg remains the boundary that turns a catalog table identifier into an Iceberg table.

Catalog browsing, namespace listing, table listing, table format classification,
and catalog table loading should be centralized behind the project's Iceberg
catalog access layer instead of being repeated in TUI, CLI, Streamlit, and
analysis modules. For lightweight Glue browsing, that layer should use Boto3
Glue metadata APIs directly. For explicit Iceberg table loading and analysis,
that layer should continue to use PyIceberg. Interface code should not each call
Glue or `pyiceberg.load_catalog` directly.

For lightweight Glue table browsing, the catalog access layer may use AWS Glue
metadata APIs to retrieve table definitions and classify table format from table
parameters before full Iceberg analysis. In particular, `Parameters["table_type"]
== "ICEBERG"` is a positive Iceberg signal, while Glue `TableType` values such
as `EXTERNAL_TABLE` must not be treated as table format. Initial browsing should
avoid one `GetTable` call or one PyIceberg `load_table` call per table. PyIceberg
table loading remains the boundary for explicit Iceberg analysis after
selection, where reading the Iceberg metadata is expected.
