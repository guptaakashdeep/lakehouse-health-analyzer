#!/usr/bin/env python3
"""
Streamlit entry point for the Lakehouse Health Analyzer dashboard.
Run with: streamlit run streamlit_app.py
"""

import os
import sys

# Add src to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from pyiceberg.catalog import load_catalog

from analysis.iceberg import analyze_iceberg_metadata_file, analyze_iceberg_table
from configuration import AnalyzerConfiguration, GlueCatalogTableSourceConfiguration


def get_table_metrics(
    table_name=None,
    catalog_name=None,
    use_metadata_file=False,
    metadata_location=None,
    aws_profile=None,
    aws_region=None,
):
    """Get metrics for an Iceberg table."""
    try:
        # Initialize analyzer based on mode
        if use_metadata_file:
            config = AnalyzerConfiguration.from_environment(
                ui_overrides={"metadata_location": metadata_location}
            )
            return analyze_iceberg_metadata_file(config)
        else:
            namespace, parsed_table_name = _catalog_table_parts(table_name)
            config = AnalyzerConfiguration.from_environment(
                ui_overrides={
                    "table_source_kind": "glue_catalog_table",
                    "glue_catalog_name": catalog_name,
                    "glue_namespace": namespace,
                    "glue_table_name": parsed_table_name,
                    "aws_profile": aws_profile,
                    "aws_region": aws_region,
                }
            )
            return analyze_iceberg_table(config)
    except Exception as e:
        print(f"Error getting table metrics: {str(e)}")
        raise e


def get_catalog_tables(
    catalog_name=None,
    namespace=None,
    aws_profile=None,
    aws_region=None,
):
    """List tables from a configured Glue Catalog Table Source."""
    overrides = {
        "table_source_kind": "glue_catalog_table",
        "glue_table_name": "__catalog_overview__",
    }
    if catalog_name is not None:
        overrides["glue_catalog_name"] = catalog_name
    if namespace is not None:
        overrides["glue_namespace"] = namespace
    if aws_profile is not None:
        overrides["aws_profile"] = aws_profile
    if aws_region is not None:
        overrides["aws_region"] = aws_region

    config = AnalyzerConfiguration.from_environment(ui_overrides=overrides)
    table_source = config.table_source
    if not isinstance(table_source, GlueCatalogTableSourceConfiguration):
        raise ValueError("Catalog overview requires a Catalog Table Source.")
    if len(table_source.namespace) != 1:
        raise ValueError(
            "AWS Glue catalog overview requires a single namespace component."
        )

    catalog = load_catalog(
        table_source.catalog_name,
        **_glue_catalog_properties(table_source),
    )
    return [
        _format_catalog_identifier(table_identifier)
        for table_identifier in catalog.list_tables(table_source.namespace[0])
    ]


def _catalog_table_parts(table_name):
    parts = str(table_name).split(".")
    if len(parts) < 2:
        return ("default",), str(table_name)
    return tuple(parts[:-1]), parts[-1]


def _glue_catalog_properties(source: GlueCatalogTableSourceConfiguration):
    properties = {"type": "glue"}
    if source.aws_profile is not None:
        properties["glue.profile-name"] = source.aws_profile
    if source.region is not None:
        properties["glue.region"] = source.region
    return properties


def _format_catalog_identifier(table_identifier):
    if isinstance(table_identifier, str):
        return table_identifier
    return ".".join(str(part) for part in table_identifier)


def main():
    """Run the dashboard with a function to get table metrics."""
    from visualization.metrics_dashboard import show_dashboard

    show_dashboard(
        get_table_metrics=get_table_metrics,
        list_catalog_tables=get_catalog_tables,
        catalog_name=os.environ.get("LHA_GLUE_CATALOG_NAME", "default"),
        catalog_namespace=os.environ.get("LHA_GLUE_NAMESPACE", "default"),
        aws_profile=os.environ.get("LHA_AWS_PROFILE"),
        aws_region=os.environ.get("LHA_AWS_REGION"),
        use_metadata_file=True,  # Default to metadata file mode
    )


if __name__ == "__main__":
    main()
