#!/usr/bin/env python3
"""
Streamlit entry point for the Lakehouse Health Analyzer dashboard.
Run with: streamlit run streamlit_app.py
"""

import os
import sys

# Add src to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from analysis.iceberg import analyze_iceberg_metadata_file, analyze_iceberg_table
from configuration import AnalyzerConfiguration


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


def _catalog_table_parts(table_name):
    parts = str(table_name).split(".")
    if len(parts) < 2:
        return ("default",), str(table_name)
    return tuple(parts[:-1]), parts[-1]


def main():
    """Run the dashboard with a function to get table metrics."""
    from visualization.metrics_dashboard import show_dashboard

    show_dashboard(
        get_table_metrics=get_table_metrics,
        available_tables=["example.default.table1", "example.default.table2"],
        catalog_name="default",
        use_metadata_file=True,  # Default to metadata file mode
    )


if __name__ == "__main__":
    main()
