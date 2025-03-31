"""Historical snapshots component."""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from typing import List, Dict, Any


def display_historical_snapshots(historical_snapshots, table_name=None):
    """Display historical snapshots with timeline and trends.
    
    Args:
        historical_snapshots: List of snapshot dictionaries with metrics
        table_name: Name of the table being displayed
    """
    if not historical_snapshots:
        st.warning("No historical snapshots available")
        return
    
    # Create table-specific session state keys with unique identifiers
    tab_session_key = f"historical_tab_{table_name}" if table_name else "historical_tab"
    snapshot_session_key = f"selected_snapshot_{table_name}" if table_name else "selected_snapshot"
    selector_key = f'snapshot_selector_{table_name}' if table_name else 'snapshot_selector'
    snapshot_tab_key = f"snapshot_tab_{table_name}" if table_name else "snapshot_tab"
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(historical_snapshots)
    
    # Format snapshot options for dropdown
    df['formatted_time'] = df['timestamp'].apply(
        lambda x: datetime.fromisoformat(x).strftime('%Y-%m-%d %H:%M:%S') 
        if x else "Unknown"
    )
    df['display_option'] = df.apply(
        lambda x: f"Snapshot {x['snapshot_id']} - {x['formatted_time']} ({x['operation']})", 
        axis=1
    )
    
    # Initialize snapshot selection if needed
    if snapshot_session_key not in st.session_state or st.session_state[snapshot_session_key] is None:
        st.session_state[snapshot_session_key] = df['display_option'].iloc[0]
    elif st.session_state[snapshot_session_key] not in df['display_option'].values:
        st.session_state[snapshot_session_key] = df['display_option'].iloc[0]
    
    # Initialize tab selection if needed
    if tab_session_key not in st.session_state:
        # Default to Timeline the first time
        st.session_state[tab_session_key] = "Timeline"
    
    # Create tabs without index
    tabs = ["Timeline", "Trends", "Details"]
    tab1, tab2, tab3 = st.tabs(tabs)
    
    # Display content in each tab based on which tab is active
    with tab1:
        st.session_state[tab_session_key] = "Timeline"
        display_historical_timeline(df, historical_snapshots)
        
    with tab2:
        st.session_state[tab_session_key] = "Trends"
        display_historical_trends(df, historical_snapshots)
        
    with tab3:
        st.session_state[tab_session_key] = "Details"
        # Make sure we stay on the Historical Snapshots tab when viewing details
        st.session_state[snapshot_tab_key] = "Historical Snapshots"
        display_historical_details(df, historical_snapshots, table_name, snapshot_session_key, selector_key)


def display_historical_timeline(df, historical_snapshots):
    """Display the timeline visualization for historical snapshots."""
    st.subheader("Snapshot Timeline")
    
    # Format timestamp for display if not already done
    if 'formatted_time' not in df.columns:
        df['formatted_time'] = df['timestamp'].apply(
            lambda x: datetime.fromisoformat(x).strftime('%Y-%m-%d %H:%M:%S') 
            if x else "Unknown"
        )
    
    # Create a timeline visualization
    fig = go.Figure()
    
    # Add events to the timeline
    for _, row in df.iterrows():
        # Define color based on operation type
        color = "#1f77b4"  # default blue
        if row['operation'] == 'append':
            color = "#2ca02c"  # green
        elif row['operation'] == 'replace':
            color = "#d62728"  # red
        elif row['operation'] == 'overwrite':
            color = "#ff7f0e"  # orange
        elif row['operation'] == 'delete':
            color = "#9467bd"  # purple
        else:
            color = "#1f77b4"  # blue
        # Create hover text with detailed information
        hover_text = f"""
        <b>Snapshot ID:</b> {row['snapshot_id']}<br>
        <b>Time:</b> {row['formatted_time']}<br>
        <b>Operation:</b> {row['operation']}<br>
        <b>Added Data Files:</b> {row['added_data_files']}<br>
        <b>Deleted Data Files:</b> {row['deleted_data_files']}<br>
        <b>Added Records:</b> {row['added_records']}<br>
        <b>Deleted Records:</b> {row['deleted_records']}<br>
        """
        
        # Add event to timeline
        fig.add_trace(go.Scatter(
            x=[datetime.fromisoformat(row['timestamp'])],
            y=[0],
            mode="markers",
            marker=dict(
                symbol="circle",
                size=20,
                color=color
            ),
            name=f"Snapshot {row['snapshot_id']}",
            text=hover_text,
            hoverinfo="text"
        ))
        
    # Add lines connecting snapshots
    if len(df) > 1:
        fig.add_trace(go.Scatter(
            x=[datetime.fromisoformat(ts) for ts in df['timestamp']],
            y=[0] * len(df),
            mode="lines",
            line=dict(width=2, color="#cccccc"),
            hoverinfo="skip",
            showlegend=False
        ))
    
    # Layout for timeline
    fig.update_layout(
        title="Snapshot Timeline",
        xaxis=dict(
            title="Timestamp",
            type="date"
        ),
        yaxis=dict(
            visible=False,
            showticklabels=False,
            zeroline=False
        ),
        hovermode="closest",
        height=300,
        margin=dict(l=10, r=10, t=50, b=30)
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Add legend for colors
    st.markdown("""
    **Operation Types:**
    <span style="color:#2ca02c">■</span> Append &nbsp;
    <span style="color:#d62728">■</span> Replace &nbsp;
    <span style="color:#ff7f0e">■</span> Overwrite &nbsp;
    <span style="color:#9467bd">■</span> Delete &nbsp;
    <span style="color:#1f77b4">■</span> Other
    """, unsafe_allow_html=True)


def display_historical_trends(df, historical_snapshots):
    """Display trend visualizations for historical snapshots."""
    st.subheader("Metric Trends Over Time")
    
    # Ensure timestamp is datetime for plotting
    df['timestamp'] = df['timestamp'].apply(lambda x: datetime.fromisoformat(x) if x else None)
    
    # Sort by timestamp
    df = df.sort_values('timestamp')
    
    # Create line charts for different metrics
    metrics_to_plot = [
        {'name': 'Total Data Files', 'col': 'total_data_files'},
        {'name': 'Total Delete Files', 'col': 'total_delete_files'},
        {'name': 'Total File Size (MB)', 'col': 'total_files_size', 'convert_to_mb': True},
        {'name': 'Records', 'cols': ['added_records', 'deleted_records']}
    ]
    
    for metric in metrics_to_plot:
        if 'col' in metric:
            # For total metrics
            y_values = df[metric['col']]
            if metric.get('convert_to_mb', False):
                y_values = y_values / (1024 * 1024)  # Convert to MB
            
            fig = px.line(
                df,
                x='timestamp',
                y=y_values,
                markers=True,
                title=f"{metric['name']} Over Time",
                labels={'y': metric['name'], 'timestamp': 'Time'},
                hover_data=['snapshot_id', 'operation']
            )
            
            # Add hover text with formatted values
            if metric.get('convert_to_mb', False):
                fig.update_traces(
                    hovertemplate="<br>".join([
                        "<b>Time:</b> %{x}",
                        "<b>Size:</b> %{y:.2f} MB",
                        "<b>Snapshot ID:</b> %{customdata[0]}",
                        "<b>Operation:</b> %{customdata[1]}",
                        "<extra></extra>"
                    ])
                )
            else:
                fig.update_traces(
                    hovertemplate="<br>".join([
                        "<b>Time:</b> %{x}",
                        "<b>Count:</b> %{y:,}",
                        "<b>Snapshot ID:</b> %{customdata[0]}",
                        "<b>Operation:</b> %{customdata[1]}",
                        "<extra></extra>"
                    ])
                )
        else:
            # For record changes
            plot_data = pd.melt(
                df, 
                id_vars=['timestamp', 'snapshot_id', 'operation'],
                value_vars=metric['cols'],
                var_name='metric_type',
                value_name='count'
            )
            
            # Create nicer labels
            plot_data['metric_type'] = plot_data['metric_type'].apply(
                lambda x: ' '.join(word.capitalize() for word in x.split('_'))
            )
            
            fig = px.line(
                plot_data,
                x='timestamp',
                y='count',
                color='metric_type',
                markers=True,
                title=f"{metric['name']} Changes Over Time",
                labels={'count': 'Count', 'timestamp': 'Time', 'metric_type': 'Type'},
                hover_data=['snapshot_id', 'operation']
            )
        
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)


def display_historical_details(df, historical_snapshots, table_name, snapshot_session_key, selector_key):
    """Display detailed information for a selected historical snapshot."""
    st.subheader("Snapshot Details")
    
    # Ensure we're on the Historical Snapshots tab
    snapshot_tab_key = f"snapshot_tab_{table_name}" if table_name else "snapshot_tab"
    st.session_state[snapshot_tab_key] = "Historical Snapshots"
    
    # Define a callback for the selectbox change
    def on_snapshot_change():
        # Get the new selection and update session state
        st.session_state[snapshot_session_key] = st.session_state[selector_key]
        
        # Force the tab states to keep us on this view
        tab_session_key = f"historical_tab_{table_name}" if table_name else "historical_tab"
        st.session_state[tab_session_key] = "Details"
        st.session_state[snapshot_tab_key] = "Historical Snapshots"
        
        # Mark that we've made an interaction
        st.session_state["last_snapshot_change"] = True
    
    # Create the selectbox with the callback
    select_index = df['display_option'].tolist().index(st.session_state[snapshot_session_key])
    st.selectbox(
        "Select a snapshot to view details:",
        options=df['display_option'].tolist(),
        index=select_index,
        key=selector_key,
        on_change=on_snapshot_change
    )
    
    # Get the selected snapshot
    selected_snapshot_value = st.session_state[snapshot_session_key]
    selected_index = df[df['display_option'] == selected_snapshot_value].index[0]
    snapshot = historical_snapshots[selected_index]
    
    # Display details in columns
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"**Snapshot ID:** {snapshot.get('snapshot_id', 'N/A')}")
        st.markdown(f"**Parent ID:** {snapshot.get('parent_id', 'N/A')}")
        st.markdown(f"**Timestamp:** {df.loc[selected_index, 'formatted_time']}")
        st.markdown(f"**Operation:** {snapshot.get('operation', 'N/A')}")
    
    with col2:
        st.markdown(f"**Added Data Files:** {snapshot.get('added_data_files', 0)}")
        st.markdown(f"**Deleted Data Files:** {snapshot.get('deleted_data_files', 0)}")
        st.markdown(f"**Added Delete Files:** {snapshot.get('added_delete_files', 0)}")
        st.markdown(f"**Removed Delete Files:** {snapshot.get('removed_delete_files', 0)}")
        
    # Additional details
    st.markdown(f"**Added Records:** {snapshot.get('added_records', 0):,}")
    st.markdown(f"**Deleted Records:** {snapshot.get('deleted_records', 0):,}")
    st.markdown(f"**Changed Partition Count:** {snapshot.get('changed_partition_count', 0)}")
    
    # Create some visualizations for this specific snapshot
    file_changes = {
        "Added Data Files": snapshot.get('added_data_files', 0),
        "Deleted Data Files": snapshot.get('deleted_data_files', 0),
        "Added Delete Files": snapshot.get('added_delete_files', 0),
        "Removed Delete Files": snapshot.get('removed_delete_files', 0)
    }
    
    # Bar chart for file changes
    if any(file_changes.values()):
        fig = px.bar(
            x=list(file_changes.keys()),
            y=list(file_changes.values()),
            title=f"File Changes in Snapshot {snapshot.get('snapshot_id')}",
            color=list(file_changes.keys()),
            color_discrete_map={
                "Added Data Files": "green",
                "Deleted Data Files": "red",
                "Added Delete Files": "orange",
                "Removed Delete Files": "purple"
            }
        )
        st.plotly_chart(fig, use_container_width=True)
        
    # Clear the last interaction flag after displaying
    if "last_snapshot_change" in st.session_state:
        st.session_state.pop("last_snapshot_change") 