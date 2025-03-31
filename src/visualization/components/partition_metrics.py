"""Partition metrics component."""
import streamlit as st
import pandas as pd
import plotly.express as px
from typing import List, Dict, Any


def display_partition_metrics(partition_metrics: List[Dict[str, Any]]):
    """Display partition metrics in a dashboard."""
    st.header("📊 Partition Metrics")

    if not partition_metrics:
        st.warning("No partition metrics available")
        return

    # Display aggregated metrics from the first entry
    agg_metrics = partition_metrics[0]

    col1, col2 = st.columns(2)

    with col1:
        st.metric(
            "Average Record Count",
            f"{getattr(agg_metrics, 'avg_record_count', 0):,.2f}",
        )
        st.metric(
            "Max Record Count", f"{getattr(agg_metrics, 'max_record_count', 0):,}"
        )
        st.metric(
            "Min Record Count", f"{getattr(agg_metrics, 'min_record_count', 0):,}"
        )
        st.metric(
            "Record Count Deviation",
            f"{getattr(agg_metrics, 'deviation_record_count', 0):,.2f}",
        )
        st.metric(
            "Record Count Skew", f"{getattr(agg_metrics, 'skew_record_count', 0):,.2f}"
        )

    with col2:
        st.metric(
            "Average Files per Partition",
            f"{getattr(agg_metrics, 'avg_file_count', 0):,.2f}",
        )
        st.metric(
            "Max Files in Partition", f"{getattr(agg_metrics, 'max_file_count', 0):,}"
        )
        st.metric(
            "Min Files in Partition", f"{getattr(agg_metrics, 'min_file_count', 0):,}"
        )
        st.metric(
            "File Count Deviation",
            f"{getattr(agg_metrics, 'deviation_file_count', 0):,.2f}",
        )
        st.metric(
            "File Count Skew", f"{getattr(agg_metrics, 'skew_file_count', 0):,.2f}"
        )

    # Display per-partition data
    if hasattr(agg_metrics, "per_partition"):
        st.subheader("Per-Partition Details")
        per_partition_df = pd.DataFrame.from_dict(
            agg_metrics.per_partition, orient="index"
        ).reset_index()
        per_partition_df.columns = [
            "Partition",
            "File Count",
            "Record Count",
            "Total Size (bytes)",
        ]

        # Create scatter plot
        fig = px.scatter(
            per_partition_df,
            x="Partition",
            y="Record Count",
            size="File Count",
            hover_data=["Total Size (bytes)"],
            title="Partition Distribution",
        )
        st.plotly_chart(fig)

        # Show detailed data in expandable section
        with st.expander("View Detailed Partition Data"):
            st.dataframe(per_partition_df) 