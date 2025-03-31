#!/usr/bin/env python3
"""
Streamlit entry point for the Lakehouse Health Analyzer dashboard.
Run with: streamlit run streamlit_app.py
"""

import sys
import os

# Add src to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from visualization.metrics_dashboard import show_dashboard
from analyzers.iceberg import IcebergAnalyzer

def get_table_metrics(table_name=None, catalog_name=None, 
                     use_metadata_file=False, metadata_location=None):
    """Get metrics for an Iceberg table."""
    try:
        # Initialize analyzer based on mode
        if use_metadata_file:
            analyzer = IcebergAnalyzer(metadata_path=metadata_location)
        else:
            analyzer = IcebergAnalyzer(database="default", table_name=table_name, 
                                      catalog_name=catalog_name)
        
        # Get metrics
        return analyzer.get_table_metrics()
    except Exception as e:
        print(f"Error getting table metrics: {str(e)}")
        raise e

# Run the dashboard with a function to get table metrics
show_dashboard(
    get_table_metrics=get_table_metrics,
    available_tables=["example.default.table1", "example.default.table2"],
    catalog_name="default",
    use_metadata_file=True  # Default to metadata file mode
) 