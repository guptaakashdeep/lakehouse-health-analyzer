"""Partition metrics component."""

import streamlit as st
import pandas as pd
import plotly.express as px
from typing import List


def bytes_to_mb(bytes_value):
    """Convert bytes to MB with 2 decimal places."""
    return round(bytes_value / (1024 * 1024), 2)


def display_partition_metrics(partition_metrics: List):
    """Display partition metrics in a dashboard."""
    st.header("📊 Partition Metrics")

    if not partition_metrics:
        st.warning("No partition metrics available")
        return

    # Convert partition metrics list to a DataFrame
    partition_data = []
    for pm in partition_metrics:
        # Parse partition field which is a dictionary
        partition_dict = pm.partition

        # Create a dictionary with partition values and metrics
        partition_info = {}

        # Add partition keys and values to the dictionary FIRST
        if isinstance(partition_dict, dict):
            for key, value in partition_dict.items():
                partition_info[f"partition_{key}"] = value
        else:
            # Handle case where partition might be a string
            partition_info["partition"] = str(partition_dict)

        # Then add metrics
        partition_info.update(
            {
                "data_file_count": pm.data_file_count,
                "delete_file_count": pm.delete_file_count,
                "total_data_file_size": pm.total_data_file_size,
                "avg_file_size_per_partition": pm.avg_file_size_per_partition,
            }
        )

        partition_data.append(partition_info)

    if not partition_data:
        st.warning("No partition data available")
        return

    # Create a DataFrame from the partition data
    df = pd.DataFrame(partition_data)

    # Reorder columns to ensure partition columns come first
    partition_cols = [
        col for col in df.columns if col.startswith("partition_") or col == "partition"
    ]
    metric_cols = [col for col in df.columns if col not in partition_cols]
    df = df[partition_cols + metric_cols]

    # Compute aggregate metrics for display
    total_partitions = len(df)
    total_data_files = (
        df["data_file_count"].sum() if "data_file_count" in df.columns else 0
    )
    total_delete_files = (
        df["delete_file_count"].sum() if "delete_file_count" in df.columns else 0
    )
    total_data_size = (
        df["total_data_file_size"].sum() if "total_data_file_size" in df.columns else 0
    )
    total_data_size_mb = bytes_to_mb(total_data_size)

    # Display summary metrics in columns
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Partitions", f"{total_partitions:,}")

    with col2:
        st.metric("Total Data Files", f"{total_data_files:,}")

    with col3:
        st.metric("Total Delete Files", f"{total_delete_files:,}")

    with col4:
        st.metric("Total Data Size", f"{total_data_size_mb:,.2f} MB")

    # Create visualization tabs
    tab1, tab2 = st.tabs(["Partition Table", "Visualizations"])

    with tab1:
        # Format the dataframe for display
        display_df = df.copy()

        # Make partition columns more readable by removing the "partition_" prefix
        rename_dict = {}
        for col in display_df.columns:
            if col.startswith("partition_"):
                rename_dict[col] = col.replace("partition_", "")

        if rename_dict:
            display_df = display_df.rename(columns=rename_dict)

        # Convert size columns from bytes to MB and format
        if "total_data_file_size" in display_df.columns:
            # Create a new column for display
            display_df["total_data_size_mb"] = display_df["total_data_file_size"].apply(
                lambda x: f"{bytes_to_mb(x):,.2f} MB"
            )
            # Remove the original bytes column
            display_df = display_df.drop(columns=["total_data_file_size"])

        if "avg_file_size_per_partition" in display_df.columns:
            # Create a new column for display
            display_df["avg_file_size_mb"] = display_df[
                "avg_file_size_per_partition"
            ].apply(lambda x: f"{bytes_to_mb(x):,.2f} MB")
            # Remove the original bytes column
            display_df = display_df.drop(columns=["avg_file_size_per_partition"])

        # Display the dataframe with a search box
        st.dataframe(
            display_df, use_container_width=True, height=400  # Adjust height as needed
        )

    with tab2:
        if len(df) > 0:
            # Create distribution visualizations

            # Distribution of data files across partitions
            if "data_file_count" in df.columns:
                fig1 = px.histogram(
                    df,
                    x="data_file_count",
                    title="Distribution of Data Files Across Partitions",
                    labels={"data_file_count": "Number of Data Files"},
                    nbins=20,
                )
                st.plotly_chart(fig1, use_container_width=True)

            # Create a dataframe with MB values for visualization
            viz_df = df.copy()
            if "total_data_file_size" in viz_df.columns:
                viz_df["total_data_size_mb"] = viz_df["total_data_file_size"].apply(
                    bytes_to_mb
                )

            # File count vs size scatter plot
            if (
                "data_file_count" in viz_df.columns
                and "total_data_size_mb" in viz_df.columns
            ):
                fig2 = px.scatter(
                    viz_df,
                    x="data_file_count",
                    y="total_data_size_mb",
                    title="Partition Size vs File Count",
                    labels={
                        "data_file_count": "Number of Data Files",
                        "total_data_size_mb": "Total Size (MB)",
                    },
                    hover_data=[
                        col for col in viz_df.columns if col != "total_data_file_size"
                    ],
                )
                st.plotly_chart(fig2, use_container_width=True)

            # Top partitions by size
            if "total_data_size_mb" in viz_df.columns:
                top_partitions = viz_df.sort_values(
                    "total_data_size_mb", ascending=False
                ).head(10)

                # Create labels for partitions
                partition_labels = []
                for _, row in top_partitions.iterrows():
                    # Get partition columns (those starting with 'partition_')
                    partition_cols = [
                        col for col in row.index if col.startswith("partition_")
                    ]
                    if partition_cols:
                        # Format as key=value pairs
                        partition_str = ", ".join(
                            [
                                f"{col.replace('partition_', '')}={row[col]}"
                                for col in partition_cols
                            ]
                        )
                    else:
                        # Use the 'partition' column if available
                        partition_str = row.get("partition", f"Partition {_}")

                    partition_labels.append(partition_str)

                fig3 = px.bar(
                    top_partitions,
                    y=(
                        partition_labels
                        if partition_labels
                        else range(len(top_partitions))
                    ),
                    x="total_data_size_mb",
                    title="Top 10 Partitions by Size",
                    labels={"total_data_size_mb": "Total Size (MB)"},
                    orientation="h",
                )
                st.plotly_chart(fig3, use_container_width=True)

    # Add an expander with partition statistics
    with st.expander("Partition Statistics"):
        st.write("### Partition Size Distribution")

        if "total_data_file_size" in df.columns:
            stats = df["total_data_file_size"].apply(bytes_to_mb).describe()
            stats_df = pd.DataFrame(
                {"Statistic": stats.index, "Value (MB)": stats.values}
            )

            # Format the values to 2 decimal places
            stats_df["Value (MB)"] = stats_df["Value (MB)"].apply(lambda x: f"{x:,.2f}")

            st.dataframe(stats_df)

            # Calculate skewness
            mean_size = df["total_data_file_size"].mean()
            std_dev = df["total_data_file_size"].std()
            skewness = std_dev / mean_size if mean_size > 0 else 0

            st.write(f"**Partition Size Skewness:** {skewness:.4f}")
            st.write(
                "*Note: Higher skewness indicates more uneven distribution of data across partitions.*"
            )

            if skewness > 0.5:
                st.warning(
                    "The partition size distribution appears to be skewed. Consider rebalancing partitions."
                )
            elif skewness > 0.3:
                st.info("The partition size distribution shows moderate skewness.")
            else:
                st.success("The partition size distribution appears to be balanced.")
