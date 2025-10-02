#!/usr/bin/env python3
"""
Streamlit Dashboard for Rail Delay Percentile Analysis
Visualizes delay patterns from the one-day rehearsal data collection.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import os

# Page configuration
st.set_page_config(
    page_title="Rail Delay Analysis Dashboard",
    page_icon="🚂",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data
def load_data():
    """Load and prepare the percentile data."""
    if not os.path.exists("data/leg_percentiles.csv"):
        st.error("No percentile data found. Run the orchestrator first!")
        st.stop()
    
    df = pd.read_csv("data/leg_percentiles.csv")
    
    # Add route labels for better display
    df['route'] = df['origin'] + ' → ' + df['dest']
    
    # Add day of week labels
    dow_labels = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 
                  4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
    df['day_name'] = df['dow'].map(dow_labels)
    
    return df

def create_heatmap(df, metric='p90'):
    """Create a heatmap showing delays by route and hour."""
    # Pivot data for heatmap
    pivot_data = df.pivot_table(
        index='route', 
        columns='hour', 
        values=metric, 
        aggfunc='mean'
    ).fillna(0)
    
    fig = px.imshow(
        pivot_data,
        title=f'Average {metric.upper()} Delays by Route and Hour',
        labels=dict(x="Hour of Day", y="Route", color=f"{metric.upper()} Delay (min)"),
        aspect="auto",
        color_continuous_scale="Reds"
    )
    
    fig.update_layout(height=400)
    return fig

def create_hourly_pattern(df, selected_routes):
    """Create hourly delay patterns for selected routes."""
    filtered_df = df[df['route'].isin(selected_routes)]
    
    # Average by hour across all days
    hourly_avg = filtered_df.groupby(['route', 'hour']).agg({
        'p80': 'mean',
        'p90': 'mean', 
        'p95': 'mean',
        'obs_count': 'sum'
    }).reset_index()
    
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Delay Percentiles by Hour', 'Observation Count by Hour'),
        vertical_spacing=0.12
    )
    
    colors = px.colors.qualitative.Set1
    
    for i, route in enumerate(selected_routes):
        route_data = hourly_avg[hourly_avg['route'] == route]
        color = colors[i % len(colors)]
        
        # Add percentile lines
        fig.add_trace(
            go.Scatter(x=route_data['hour'], y=route_data['p80'], 
                      name=f'{route} P80', line=dict(color=color, dash='dot')),
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(x=route_data['hour'], y=route_data['p90'], 
                      name=f'{route} P90', line=dict(color=color)),
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(x=route_data['hour'], y=route_data['p95'], 
                      name=f'{route} P95', line=dict(color=color, dash='dash')),
            row=1, col=1
        )
        
        # Add observation count
        fig.add_trace(
            go.Bar(x=route_data['hour'], y=route_data['obs_count'], 
                   name=f'{route} Obs', marker_color=color, opacity=0.6),
            row=2, col=1
        )
    
    fig.update_xaxes(title_text="Hour of Day", row=2, col=1)
    fig.update_yaxes(title_text="Delay (minutes)", row=1, col=1)
    fig.update_yaxes(title_text="Observations", row=2, col=1)
    
    fig.update_layout(height=600, showlegend=True)
    return fig

def create_route_comparison(df):
    """Create a comparison of routes by average delays."""
    route_summary = df.groupby('route').agg({
        'p80': 'mean',
        'p90': 'mean',
        'p95': 'mean',
        'obs_count': 'sum'
    }).reset_index()
    
    fig = go.Figure()
    
    x = route_summary['route']
    
    fig.add_trace(go.Bar(
        name='P80',
        x=x, y=route_summary['p80'],
        marker_color='lightblue'
    ))
    
    fig.add_trace(go.Bar(
        name='P90', 
        x=x, y=route_summary['p90'],
        marker_color='orange'
    ))
    
    fig.add_trace(go.Bar(
        name='P95',
        x=x, y=route_summary['p95'], 
        marker_color='red'
    ))
    
    fig.update_layout(
        title='Average Delay Percentiles by Route',
        xaxis_title='Route',
        yaxis_title='Delay (minutes)',
        barmode='group',
        height=400
    )
    
    return fig

def create_day_of_week_analysis(df):
    """Analyze patterns by day of week."""
    dow_summary = df.groupby(['day_name', 'route']).agg({
        'p90': 'mean',
        'obs_count': 'sum'
    }).reset_index()
    
    fig = px.bar(
        dow_summary,
        x='day_name',
        y='p90',
        color='route',
        title='P90 Delays by Day of Week and Route',
        labels={'p90': 'P90 Delay (minutes)', 'day_name': 'Day of Week'}
    )
    
    fig.update_layout(height=400)
    return fig

def create_coverage_matrix(df):
    """Show data coverage across routes and hours."""
    coverage = df.pivot_table(
        index='route',
        columns='hour', 
        values='obs_count',
        aggfunc='sum',
        fill_value=0
    )
    
    fig = px.imshow(
        coverage,
        title='Data Coverage: Observations by Route and Hour',
        labels=dict(x="Hour of Day", y="Route", color="Observations"),
        aspect="auto",
        color_continuous_scale="Blues"
    )
    
    fig.update_layout(height=400)
    return fig

def main():
    """Main dashboard application."""
    st.title("🚂 Rail Delay Analysis Dashboard")
    st.markdown("### Full July 2025 Analysis (31 Days)")
    
    # Load data
    df = load_data()
    
    # Sidebar controls
    st.sidebar.header("Dashboard Controls")
    
    # Route selection
    all_routes = df['route'].unique()
    selected_routes = st.sidebar.multiselect(
        "Select Routes",
        options=all_routes,
        default=all_routes
    )
    
    # Day of week filter
    all_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    selected_days = st.sidebar.multiselect(
        "Filter by Day of Week",
        options=all_days,
        default=all_days
    )
    
    # Metric selection for heatmap
    heatmap_metric = st.sidebar.selectbox(
        "Heatmap Metric",
        options=['p80', 'p90', 'p95'],
        index=1
    )
    
    # Apply filters
    filtered_df = df.copy()
    if selected_routes:
        filtered_df = filtered_df[filtered_df['route'].isin(selected_routes)]
    if selected_days:
        filtered_df = filtered_df[filtered_df['day_name'].isin(selected_days)]
    
    # Data summary
    st.sidebar.markdown("### Data Summary")
    st.sidebar.metric("Total Routes", len(df['route'].unique()))
    st.sidebar.metric("Total Observations", df['obs_count'].sum())
    st.sidebar.metric("Percentile Groups", len(df))
    st.sidebar.metric("Hours Covered", df['hour'].nunique())
    st.sidebar.metric("Days of Week", df['dow'].nunique())
    
    st.sidebar.markdown("### Filtered Data")
    st.sidebar.metric("Filtered Groups", len(filtered_df))
    st.sidebar.metric("Filtered Observations", filtered_df['obs_count'].sum())
    
    # Main dashboard layout
    col1, col2 = st.columns(2)
    
    with col1:
        st.plotly_chart(create_heatmap(filtered_df, heatmap_metric), use_container_width=True)
    
    with col2:
        st.plotly_chart(create_route_comparison(filtered_df), use_container_width=True)
    
    # Full width charts
    if selected_routes:
        st.plotly_chart(create_hourly_pattern(filtered_df, selected_routes), use_container_width=True)
    
    col3, col4 = st.columns(2)
    
    with col3:
        st.plotly_chart(create_day_of_week_analysis(filtered_df), use_container_width=True)
    
    with col4:
        st.plotly_chart(create_coverage_matrix(filtered_df), use_container_width=True)
    
    # Data table
    st.subheader("📊 Detailed Percentile Data")
    
    # Use filtered data for display
    display_df = filtered_df
    
    # Sort by route and hour
    display_df = display_df.sort_values(['route', 'hour', 'dow'])
    
    st.dataframe(
        display_df[['route', 'hour', 'day_name', 'p80', 'p90', 'p95', 'obs_count']],
        use_container_width=True
    )
    
    # Key insights (based on filtered data)
    st.subheader("🔍 Key Insights")
    
    if len(filtered_df) > 0:
        col5, col6, col7 = st.columns(3)
        
        with col5:
            busiest_route = filtered_df.groupby('route')['obs_count'].sum().idxmax()
            busiest_count = filtered_df.groupby('route')['obs_count'].sum().max()
            st.metric("Busiest Route", busiest_route, f"{int(busiest_count)} obs")
        
        with col6:
            highest_delay = filtered_df.loc[filtered_df['p90'].idxmax()]
            st.metric(
                "Highest P90 Delay", 
                f"{highest_delay['route']}", 
                f"{highest_delay['p90']:.1f} min"
            )
        
        with col7:
            peak_hour = filtered_df.groupby('hour')['obs_count'].sum().idxmax()
            st.metric("Peak Hour", f"{peak_hour:02d}:00", "Most observations")
    else:
        st.warning("No data matches the current filters. Please adjust your selection.")
    
    # Route-specific statistics (based on filtered data)
    st.subheader("📈 Route Statistics")
    
    if len(filtered_df) > 0:
        route_stats = filtered_df.groupby('route').agg({
            'p80': ['mean', 'min', 'max'],
            'p90': ['mean', 'min', 'max'], 
            'p95': ['mean', 'min', 'max'],
            'obs_count': ['sum', 'mean']
        }).round(2)
        
        # Flatten column names
        route_stats.columns = [f"{col[1]}_{col[0]}" for col in route_stats.columns]
        route_stats = route_stats.reset_index()
        
        st.dataframe(route_stats, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown("*Data collected from July 2025 (31 days) across 5 routes*")

if __name__ == "__main__":
    main()
