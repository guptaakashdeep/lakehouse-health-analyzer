"""Dashboard utility functions."""
import streamlit as st

from visualization.components.live_stats import display_live_stats
from visualization.components.snapshot_metrics import display_snapshot_metrics
from visualization.components.partition_metrics import display_partition_metrics
from visualization.components.file_metrics import display_file_metrics
from visualization.components.historical_snapshots import display_historical_snapshots


def display_metrics_with_tabs(metrics):
    """Display metrics with proper tab management."""
    table_name = metrics.table_name
    
    st.header(f"📊 Table: {table_name}")
    display_live_stats(metrics.live_table_metrics)
    st.markdown("---")
    
    # Snapshots Section
    st.header("📸 Snapshots")
    
    # Create a session state key for the snapshot tab
    snapshot_tab_key = f"snapshot_tab_{table_name}" if table_name else "snapshot_tab"
    
    # Initialize or get the selected tab from session state
    if snapshot_tab_key not in st.session_state:
        st.session_state[snapshot_tab_key] = "Latest Snapshot"
    
    # Create tabs without index
    tabs = ["Latest Snapshot", "Historical Snapshots"]
    tab1, tab2 = st.tabs(tabs)
    
    # Display content in each tab
    with tab1:
        st.session_state[snapshot_tab_key] = "Latest Snapshot" 
        display_snapshot_metrics(metrics.snapshot_metrics)
    
    with tab2:
        st.session_state[snapshot_tab_key] = "Historical Snapshots"
        display_historical_snapshots(metrics.historical_snapshots, table_name)
    
    st.markdown("---")
    display_partition_metrics(metrics.partition_metrics)
    st.markdown("---")
    display_file_metrics(metrics.file_metrics)
    st.markdown("---") 