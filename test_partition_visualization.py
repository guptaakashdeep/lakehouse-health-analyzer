import sys

sys.path.append("src")

import streamlit as st
from analyzers.base import (
    PartitionMetrics,
    TableMetadataMetrics,
    LiveTableMetrics,
    SnapshotMetrics,
    FileMetrics,
)
from visualization.components.partition_metrics import display_partition_metrics

# Create some sample partition metrics
partition_metrics = [
    PartitionMetrics(
        partition={"year": "2023", "month": "01"},
        data_file_count=10,
        delete_file_count=2,
        total_data_file_size=1024000,
        avg_file_size_per_partition=102400.0,
    ),
    PartitionMetrics(
        partition={"year": "2023", "month": "02"},
        data_file_count=15,
        delete_file_count=1,
        total_data_file_size=2048000,
        avg_file_size_per_partition=136533.33,
    ),
    PartitionMetrics(
        partition={"year": "2023", "month": "03"},
        data_file_count=8,
        delete_file_count=0,
        total_data_file_size=512000,
        avg_file_size_per_partition=64000.0,
    ),
    PartitionMetrics(
        partition={"year": "2023", "month": "04"},
        data_file_count=20,
        delete_file_count=5,
        total_data_file_size=3072000,
        avg_file_size_per_partition=153600.0,
    ),
    PartitionMetrics(
        partition={"year": "2023", "month": "05"},
        data_file_count=12,
        delete_file_count=3,
        total_data_file_size=1536000,
        avg_file_size_per_partition=128000.0,
    ),
    PartitionMetrics(
        partition={"year": "2023", "month": "06"},
        data_file_count=5,
        delete_file_count=0,
        total_data_file_size=256000,
        avg_file_size_per_partition=51200.0,
    ),
    PartitionMetrics(
        partition={"year": "2023", "month": "07"},
        data_file_count=30,
        delete_file_count=8,
        total_data_file_size=4096000,
        avg_file_size_per_partition=136533.33,
    ),
    PartitionMetrics(
        partition={"year": "2023", "month": "08"},
        data_file_count=25,
        delete_file_count=6,
        total_data_file_size=3584000,
        avg_file_size_per_partition=143360.0,
    ),
    PartitionMetrics(
        partition={"year": "2023", "month": "09"},
        data_file_count=18,
        delete_file_count=4,
        total_data_file_size=2560000,
        avg_file_size_per_partition=142222.22,
    ),
    PartitionMetrics(
        partition={"year": "2023", "month": "10"},
        data_file_count=22,
        delete_file_count=7,
        total_data_file_size=3072000,
        avg_file_size_per_partition=139636.36,
    ),
    # Add a partitions with different keys to test the visualization
    PartitionMetrics(
        partition={"year": "2024", "region": "us-west"},
        data_file_count=14,
        delete_file_count=3,
        total_data_file_size=1800000,
        avg_file_size_per_partition=128571.43,
    ),
    PartitionMetrics(
        partition={"year": "2024", "region": "us-east"},
        data_file_count=17,
        delete_file_count=2,
        total_data_file_size=2100000,
        avg_file_size_per_partition=123529.41,
    ),
]

# Create a simple Streamlit app for testing
st.title("Partition Metrics Visualization Test")
st.write("This is a test to verify that the partition visualization works correctly.")

# Display the partition metrics
display_partition_metrics(partition_metrics)

# Note: Run this script with the command:
# uv run streamlit run test_partition_visualization.py
