import time
from typing import List, Callable
import streamlit as st

from visualization.dashboard_utils import (
    display_metrics_with_tabs,
)
from visualization.catalog_overview import (
    build_catalog_overview,
    display_catalog_overview,
)


def show_dashboard(
    available_tables: List[str] = None,
    get_table_metrics: Callable = None,
    list_catalog_tables: Callable = None,
    catalog_name: str = None,
    catalog_namespace: str = None,
    aws_profile: str = None,
    aws_region: str = None,
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
            "catalog_overview": None,
            "catalog_name": catalog_name if catalog_name else "",
            "catalog_namespace": catalog_namespace if catalog_namespace else "",
            "aws_profile": aws_profile if aws_profile else "",
            "aws_region": aws_region if aws_region else "",
            "active_tabs": {},  # Store active tabs for each section
        }

    # Initialize active_tabs if it doesn't exist
    if "active_tabs" not in st.session_state.dashboard_state:
        st.session_state.dashboard_state["active_tabs"] = {}
    st.session_state.dashboard_state.setdefault("catalog_overview", None)
    st.session_state.dashboard_state.setdefault(
        "catalog_name", catalog_name if catalog_name else ""
    )
    st.session_state.dashboard_state.setdefault(
        "catalog_namespace", catalog_namespace if catalog_namespace else ""
    )
    st.session_state.dashboard_state.setdefault(
        "aws_profile", aws_profile if aws_profile else ""
    )
    st.session_state.dashboard_state.setdefault(
        "aws_region", aws_region if aws_region else ""
    )

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
        catalog_name = st.text_input(
            "Catalog Name",
            value=st.session_state.dashboard_state["catalog_name"],
            key="catalog_name_input",
        )
        catalog_namespace = st.text_input(
            "Namespace",
            value=st.session_state.dashboard_state["catalog_namespace"],
            key="catalog_namespace_input",
        )
        aws_profile = st.text_input(
            "AWS Profile",
            value=st.session_state.dashboard_state["aws_profile"],
            key="aws_profile_input",
        )
        aws_region = st.text_input(
            "AWS Region",
            value=st.session_state.dashboard_state["aws_region"],
            key="aws_region_input",
        )

        st.session_state.dashboard_state["catalog_name"] = catalog_name
        st.session_state.dashboard_state["catalog_namespace"] = catalog_namespace
        st.session_state.dashboard_state["aws_profile"] = aws_profile
        st.session_state.dashboard_state["aws_region"] = aws_region

        # Add a button to trigger analysis
        if st.button("Load Catalog Overview"):
            # Create a progress bar
            progress_bar = st.progress(0)
            status_text = st.empty()

            try:
                status_text.text("Listing catalog tables...")
                progress_bar.progress(0.1)
                table_names = _catalog_table_names(
                    available_tables=available_tables,
                    list_catalog_tables=list_catalog_tables,
                    catalog_name=catalog_name,
                    catalog_namespace=catalog_namespace,
                    aws_profile=aws_profile,
                    aws_region=aws_region,
                )
            except Exception as e:
                progress_bar.empty()
                status_text.empty()
                st.warning(f"Unable to list catalog tables: {str(e)}")
                return

            if not table_names:
                progress_bar.empty()
                status_text.empty()
                st.warning("No tables available in the catalog")
                return

            status_text.text("Analyzing catalog tables...")
            progress_bar.progress(0.5)
            overview = build_catalog_overview(
                table_names=tuple(table_names),
                analyze_table=lambda table_name: get_table_metrics(
                    table_name=table_name,
                    catalog_name=catalog_name,
                    use_metadata_file=False,
                    metadata_location=None,
                    aws_profile=aws_profile or None,
                    aws_region=aws_region or None,
                ),
            )

            st.session_state.dashboard_state["catalog_overview"] = overview
            status_text.text("Catalog overview complete!")
            progress_bar.progress(1.0)
            time.sleep(1)
            status_text.empty()
            progress_bar.empty()
            display_catalog_overview(overview)
        else:
            # Display cached metrics if available
            if st.session_state.dashboard_state["catalog_overview"]:
                display_catalog_overview(
                    st.session_state.dashboard_state["catalog_overview"]
                )

    # Add dashboard footer
    st.markdown("---")
    st.markdown("*Dashboard powered by Lakehouse Health Analyzer*")


def _catalog_table_names(
    available_tables,
    list_catalog_tables,
    catalog_name,
    catalog_namespace,
    aws_profile,
    aws_region,
):
    if list_catalog_tables is None:
        return [] if available_tables is None else available_tables
    return list_catalog_tables(
        catalog_name=catalog_name or None,
        namespace=catalog_namespace or None,
        aws_profile=aws_profile or None,
        aws_region=aws_region or None,
    )
