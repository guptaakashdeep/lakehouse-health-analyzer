import time
from typing import List, Callable
import streamlit as st

from visualization.dashboard_utils import (
    display_metrics_with_tabs,
)


def show_dashboard(
    available_tables: List[str] = None,
    get_table_metrics: Callable = None,
    catalog_name: str = None,
    use_metadata_file: bool = False,
    metadata_location: str = None,
):
    """Display table metadata metrics in a Streamlit dashboard."""
    # Initialize session state for dashboard
    if "dashboard_state" not in st.session_state:
        st.session_state.dashboard_state = {
            "analysis_mode": "Metadata File" if use_metadata_file else "Catalog Tables",
            "metadata_location": metadata_location if metadata_location else "",
            "selected_tables": [],
            "metrics": None,
            "tables_metrics": {},
            "active_tabs": {},  # Store active tabs for each section
        }

    # Initialize active_tabs if it doesn't exist
    if "active_tabs" not in st.session_state.dashboard_state:
        st.session_state.dashboard_state["active_tabs"] = {}

    st.title("Iceberg Table Metadata Metrics Dashboard")

    # Add a brief description
    st.markdown(
        """
    This dashboard provides a comprehensive view of your Iceberg tables' metadata metrics,
    including snapshot statistics, partition distribution, and file metrics.
    """
    )

    # Add mode selection with session state
    analysis_mode = st.radio(
        "Select Analysis Mode",
        ["Catalog Tables", "Metadata File"],
        index=(
            1
            if st.session_state.dashboard_state["analysis_mode"] == "Metadata File"
            else 0
        ),
        key="analysis_mode_radio",
        help="Choose whether to analyze tables from a catalog or from a metadata file",
    )

    # Update session state
    st.session_state.dashboard_state["analysis_mode"] = analysis_mode
    use_metadata_file = analysis_mode == "Metadata File"

    if use_metadata_file:
        # Metadata file mode with session state
        metadata_location = st.text_input(
            "Metadata File Location",
            value=st.session_state.dashboard_state["metadata_location"],
            key="metadata_location_input",
            help="Enter the path to the metadata.json file",
        )

        # Update session state
        st.session_state.dashboard_state["metadata_location"] = metadata_location

        if not metadata_location:
            st.warning("Please provide the metadata file location")
            return

        # Add a button to trigger analysis
        if st.button("Analyze Metadata"):
            # Create a progress bar
            progress_bar = st.progress(0)
            status_text = st.empty()
            status_text.text("Analyzing metadata file, please wait...")

            try:
                # Update progress to 25%
                progress_bar.progress(0.25)

                metrics = get_table_metrics(
                    table_name=None,
                    catalog_name=None,
                    use_metadata_file=True,
                    metadata_location=metadata_location,
                )

                # Update progress to 75%
                progress_bar.progress(0.75)

                # Store metrics in session state
                st.session_state.dashboard_state["metrics"] = metrics

                # Display metrics
                status_text.text("Analysis complete!")
                progress_bar.progress(1.0)

                # Clear status elements after 1 second
                time.sleep(1)
                status_text.empty()
                progress_bar.empty()

                display_metrics_with_tabs(metrics)
            except Exception as e:
                import traceback

                progress_bar.empty()
                status_text.empty()
                st.error(f"Error analyzing metadata file: {str(e)}")
                st.error(traceback.format_exc())
        else:
            # Display cached metrics if available
            if st.session_state.dashboard_state["metrics"]:
                metrics = st.session_state.dashboard_state["metrics"]
                display_metrics_with_tabs(metrics)

    else:
        # Catalog mode with session state
        if not available_tables:
            st.warning("No tables available in the catalog")
            return

        # Allow selecting multiple tables with session state
        selected_tables = st.multiselect(
            "Select tables to analyze",
            available_tables,
            default=st.session_state.dashboard_state["selected_tables"],
            key="table_selector",
            format_func=lambda x: x.split(".")[-1] if "." in x else x,
            help="You can select multiple tables to analyze",
        )

        # Update session state
        st.session_state.dashboard_state["selected_tables"] = selected_tables

        if not selected_tables:
            st.info("Please select at least one table to analyze")
            return

        # Add a button to trigger analysis
        if st.button("Analyze Selected Tables"):
            # Create a progress bar
            progress_bar = st.progress(0)
            status_text = st.empty()

            # Dictionary to store metrics for selected tables
            tables_metrics = {}

            # Fetch metrics for each selected table
            for i, table_name in enumerate(selected_tables):
                status_text.text(f"Analyzing table: {table_name}")
                try:
                    metrics = get_table_metrics(
                        table_name=table_name,
                        catalog_name=catalog_name,
                        use_metadata_file=False,
                        metadata_location=None,
                    )
                    tables_metrics[table_name] = metrics
                except Exception as e:
                    import traceback

                    st.error(f"Error analyzing table {table_name}: {str(e)}")
                    st.error(traceback.format_exc())

                # Update progress
                progress = (i + 1) / len(selected_tables)
                progress_bar.progress(progress)

            status_text.text("Analysis complete!")

            if not tables_metrics:
                st.warning("No metrics available for the selected tables")
                return

            # Store metrics in session state
            st.session_state.dashboard_state["tables_metrics"] = tables_metrics

            # Create tabs for each table
            tabs = st.tabs(
                [
                    table_name.split(".")[-1] if "." in table_name else table_name
                    for table_name in tables_metrics.keys()
                ]
            )

            # Display metrics for each table in its respective tab
            for tab, (table_name, metrics) in zip(tabs, tables_metrics.items()):
                with tab:
                    display_metrics_with_tabs(metrics)
        else:
            # Display cached metrics if available
            if st.session_state.dashboard_state["tables_metrics"]:
                tables_metrics = st.session_state.dashboard_state["tables_metrics"]
                # Create tabs for each table
                tabs = st.tabs(
                    [
                        table_name.split(".")[-1] if "." in table_name else table_name
                        for table_name in tables_metrics.keys()
                    ]
                )

                # Display metrics for each table in its respective tab
                for tab, (table_name, metrics) in zip(tabs, tables_metrics.items()):
                    with tab:
                        display_metrics_with_tabs(metrics)

    # Add dashboard footer
    st.markdown("---")
    st.markdown("*Dashboard powered by Lakehouse Health Analyzer*")
