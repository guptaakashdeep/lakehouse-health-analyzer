"""File metrics component."""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from typing import List, Dict, Any


def display_file_metrics(file_metrics: List[Dict[str, Any]]):
    """Display file metrics in a dashboard."""
    st.header("📁 File Metrics")

    if not file_metrics:
        st.warning("No file metrics available")
        return

    # Display metrics for each file type
    for metrics in file_metrics:
        st.subheader(getattr(metrics, "file_type", "Unknown"))

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("File Count", f"{getattr(metrics, 'file_count', 0):,}")
            st.metric(
                "Average Record Count",
                f"{getattr(metrics, 'avg_record_count', 0):,.2f}",
            )

        with col2:
            st.metric(
                "Max Record Count", f"{getattr(metrics, 'max_record_count', 0):,}"
            )
            st.metric(
                "Min Record Count", f"{getattr(metrics, 'min_record_count', 0):,}"
            )

        with col3:
            # Convert bytes to MB
            avg_size_mb = getattr(metrics, "avg_file_size", 0) / (1024 * 1024)
            max_size_mb = getattr(metrics, "max_file_size", 0) / (1024 * 1024)
            min_size_mb = getattr(metrics, "min_file_size", 0) / (1024 * 1024)
            st.metric("Average File Size", f"{avg_size_mb:.2f} MB")
            st.metric("Max File Size", f"{max_size_mb:.2f} MB")
            st.metric("Min File Size", f"{min_size_mb:.2f} MB")

    # Create comparison charts if we have multiple file types
    if len(file_metrics) > 1:
        # Convert Pydantic models to dictionaries for DataFrame
        metrics_dicts = [
            {
                "file_type": getattr(m, "file_type", "Unknown"),
                "file_count": getattr(m, "file_count", 0),
                "avg_record_count": getattr(m, "avg_record_count", 0),
                "avg_file_size_mb": getattr(m, "avg_file_size", 0) / (1024 * 1024),
            }
            for m in file_metrics
        ]
        df = pd.DataFrame(metrics_dicts)

        # Create two separate charts for better comparison

        # 1. File Count Comparison
        fig1 = go.Figure()
        fig1.add_trace(go.Bar(name="File Count", x=df["file_type"], y=df["file_count"]))
        fig1.update_layout(title="File Count by Type", yaxis_title="Number of Files")
        st.plotly_chart(fig1)

        # 2. Size and Records Comparison
        fig2 = go.Figure()
        fig2.add_trace(
            go.Bar(
                name="Avg Records per File", x=df["file_type"], y=df["avg_record_count"]
            )
        )
        fig2.add_trace(
            go.Bar(
                name="Avg File Size (MB)", x=df["file_type"], y=df["avg_file_size_mb"]
            )
        )
        fig2.update_layout(
            title="Average Records and File Size by Type", barmode="group"
        )
        st.plotly_chart(fig2) 