import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any

from analyzers.base import TableHealthMetrics


def format_bytes(size_bytes: int) -> str:
    """Format bytes to human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def show_dashboard(metrics: TableHealthMetrics):
    """Display Streamlit dashboard for table health metrics."""
    st.title("Lakehouse Table Health Dashboard")
    
    # Overview Metrics
    st.header("Overview")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Size", format_bytes(metrics.total_size_bytes))
    with col2:
        st.metric("Total Files", metrics.total_files)
    with col3:
        st.metric("Total Partitions", metrics.total_partitions)
    
    # Partition Statistics
    st.header("Partition Statistics")
    
    # Files per Partition
    st.subheader("Files per Partition")
    files_df = pd.DataFrame(
        list(metrics.files_per_partition.items()),
        columns=["Partition", "File Count"]
    )
    fig = px.bar(files_df, x="Partition", y="File Count")
    st.plotly_chart(fig)
    
    # Partition Sizes
    st.subheader("Partition Sizes")
    sizes_df = pd.DataFrame(
        list(metrics.partition_sizes_bytes.items()),
        columns=["Partition", "Size (bytes)"]
    )
    sizes_df["Size (bytes)"] = sizes_df["Size (bytes)"].apply(format_bytes)
    st.dataframe(sizes_df)
    
    # Partition Skewness
    st.subheader("Partition Skewness")
    skewness_gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=metrics.partition_skewness,
        title={'text': "Skewness"},
        gauge={
            'axis': {'range': [0, 1]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 0.3], 'color': "lightgray"},
                {'range': [0.3, 0.7], 'color': "gray"},
                {'range': [0.7, 1], 'color': "darkgray"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': metrics.partition_skewness
            }
        }
    ))
    st.plotly_chart(skewness_gauge)
    
    # Orphan Files
    st.header("Orphan Files")
    if metrics.orphan_files:
        st.warning(f"Found {len(metrics.orphan_files)} orphan files")
        orphan_files_df = pd.DataFrame(metrics.orphan_files, columns=["File Path"])
        st.dataframe(orphan_files_df)
    else:
        st.success("No orphan files found")
    
    # Expirable Snapshots
    st.header("Expirable Snapshots")
    if metrics.snapshots_to_expire:
        st.warning(f"Found {len(metrics.snapshots_to_expire)} snapshots that can be expired")
        snapshots_df = pd.DataFrame(metrics.snapshots_to_expire)
        snapshots_df["timestamp"] = pd.to_datetime(snapshots_df["timestamp_ms"], unit="ms")
        st.dataframe(snapshots_df)
    else:
        st.success("No expirable snapshots found") 