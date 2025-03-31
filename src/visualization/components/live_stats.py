"""Live table statistics component."""
import streamlit as st
import plotly.graph_objects as go


def display_live_stats(live_table_metrics):
    """Display live table statistics in a dashboard."""
    st.header("🔴 Live Table Stats")

    print("displaying_live_stats() -> ", live_table_metrics)

    # Create three columns for live metrics
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("No. of Data Files", getattr(live_table_metrics, "total_data_files", 0))
        st.metric("No. of Delete Files", getattr(live_table_metrics, "total_delete_files", 0))

    with col2:
        live_file_size_mb = getattr(live_table_metrics, "total_files_size", 0) / (1024 * 1024)
        st.metric("Total File Size", f"{live_file_size_mb:.2f} MB")
        live_data_file_size_mb = getattr(live_table_metrics, "data_file_size", 0) / (1024 * 1024)
        st.metric("Total Data File Size", f"{live_data_file_size_mb:.2f} MB")

    with col3:
        live_records = getattr(live_table_metrics, "record_count", 0)
        st.metric("No. of Records", f"{live_records:,}")
        live_delete_file_size_mb = getattr(live_table_metrics, "delete_file_size", 0) / (1024 * 1024)
        st.metric("Total Delete File Size", f"{live_delete_file_size_mb:.2f} MB")

    # Create a pie chart for live file distribution
    fig = go.Figure(data=[
        go.Pie(
            labels=['Data Files', 'Delete Files'],
            values=[
                getattr(live_table_metrics, "total_data_files", 0),
                getattr(live_table_metrics, "total_delete_files", 0)
            ],
            hole=.3
        )
    ])
    fig.update_layout(title="Live Files Distribution")
    st.plotly_chart(fig) 