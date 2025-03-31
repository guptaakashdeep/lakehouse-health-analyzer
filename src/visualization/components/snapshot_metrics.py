"""Snapshot metrics component."""
import streamlit as st
import plotly.express as px


def display_snapshot_metrics(snapshot_metrics):
    """Display snapshot metrics in a dashboard."""
    st.header("📸 Latest Snapshot Metrics")
    # Create two columns for metrics
    col1, col2 = st.columns(2)

    with col1:
        st.metric("Added Data Files", getattr(snapshot_metrics, "added_data_files", 0))
        st.metric("Deleted Data Files", getattr(snapshot_metrics, "deleted_data_files", 0))
        st.metric("Added Delete Files", getattr(snapshot_metrics, "added_delete_files", 0))
        st.metric("Removed Delete Files", getattr(snapshot_metrics, "removed_delete_files", 0))

    with col2:
        st.metric("Added Records", getattr(snapshot_metrics, "added_records", 0))
        st.metric("Deleted Records", getattr(snapshot_metrics, "deleted_records", 0))
        st.metric("Changed Partition Count", getattr(snapshot_metrics, "changed_partition_count", 0))
        st.metric("Operation", getattr(snapshot_metrics, "operation", ""))

    # Create a bar chart for file changes
    file_changes = {
        "Added Data Files": getattr(snapshot_metrics, "added_data_files", 0),
        "Deleted Data Files": getattr(snapshot_metrics, "deleted_data_files", 0),
        "Added Delete Files": getattr(snapshot_metrics, "added_delete_files", 0),
        "Removed Delete Files": getattr(snapshot_metrics, "removed_delete_files", 0)
    }

    fig = px.bar(
        x=list(file_changes.keys()),
        y=list(file_changes.values()),
        title="File Changes in Latest Snapshot",
        color=list(file_changes.keys()),
        color_discrete_map={
            "Added Data Files": "green",
            "Deleted Data Files": "red",
            "Added Delete Files": "orange",
            "Removed Delete Files": "purple"
        }
    )
    st.plotly_chart(fig)

    # Create a bar chart for record changes
    record_changes = {
        "Added Records": getattr(snapshot_metrics, "added_records", 0),
        "Deleted Records": getattr(snapshot_metrics, "deleted_records", 0)
    }

    fig = px.bar(
        x=list(record_changes.keys()),
        y=list(record_changes.values()),
        title="Record Changes in Latest Snapshot",
        color=list(record_changes.keys()),
        color_discrete_map={
            "Added Records": "green",
            "Deleted Records": "red"
        }
    )
    st.plotly_chart(fig) 