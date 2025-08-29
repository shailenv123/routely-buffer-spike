#!/usr/bin/env python3
"""
Streamlit app for route buffer recommendations based on delay percentiles.

Usage:
    streamlit run app.py
"""

import streamlit as st
import pandas as pd
import os
import numpy as np
import plotly.express as px


def load_data():
    """Load the percentile data, with caching."""
    data_path = "data/route_hour_p80_p90_p95.csv"
    
    if not os.path.exists(data_path):
        st.error(f"Data file not found: {data_path}")
        st.info("Please run the pipeline first: `python pipeline.py --days 1`")
        st.stop()
    
    try:
        df = pd.read_csv(data_path)
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.stop()


def interpolate_buffer(risk_level, p80, p90, p95):
    """
    Calculate buffer recommendation using piecewise linear interpolation.
    
    Args:
        risk_level: Target risk level (80-99)
        p80, p90, p95: Percentile values
    
    Returns:
        Interpolated buffer time in minutes
    """
    if risk_level <= 80:
        return p80
    elif risk_level <= 90:
        # Linear interpolation between p80 and p90
        alpha = (risk_level - 80) / (90 - 80)
        return p80 + alpha * (p90 - p80)
    elif risk_level <= 95:
        # Linear interpolation between p90 and p95
        alpha = (risk_level - 90) / (95 - 90)
        return p90 + alpha * (p95 - p90)
    else:
        # Clamp to p95 for risk > 95%
        return p95


def main():
    """Main Streamlit app."""
    st.set_page_config(
        page_title="Routely Buffer Recommendations",
        page_icon="🚄",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🚄 Routely Buffer Recommendations")
    st.markdown("*AI-powered rail delay buffer recommendations based on historical data*")
    
    # Load data
    with st.spinner("Loading data..."):
        df = load_data()
    
    st.success(f"Loaded {len(df)} route/hour/day combinations")
    
    # Sidebar controls
    st.sidebar.header("🎛️ Route Configuration")
    
    # Route dropdown
    route_options = df[['origin', 'dest']].drop_duplicates()
    route_labels = [f"{row['origin']}→{row['dest']}" for _, row in route_options.iterrows()]
    route_dict = {label: (row['origin'], row['dest']) for label, (_, row) in zip(route_labels, route_options.iterrows())}
    
    selected_route_label = st.sidebar.selectbox(
        "📍 Select Route",
        options=route_labels,
        index=0 if route_labels else None
    )
    
    if selected_route_label:
        selected_origin, selected_dest = route_dict[selected_route_label]
    else:
        st.error("No routes available in data")
        st.stop()
    
    # Hour slider
    selected_hour = st.sidebar.slider(
        "🕘 Departure Hour",
        min_value=0,
        max_value=23,
        value=9,
        help="Hour of planned departure (24-hour format)"
    )
    
    # Day of week dropdown
    dow_labels = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    dow_mapping = {label: idx for idx, label in enumerate(dow_labels)}
    
    selected_dow_label = st.sidebar.selectbox(
        "📅 Day of Week",
        options=dow_labels,
        index=0
    )
    selected_dow = dow_mapping[selected_dow_label]
    
    # Risk slider
    risk_level = st.sidebar.slider(
        "🎯 Risk Tolerance",
        min_value=80,
        max_value=99,
        value=90,
        help="Percentage confidence level for on-time arrival"
    )
    
    st.sidebar.markdown("---")
    
    # Filter data for selected parameters
    filtered_df = df[
        (df['origin'] == selected_origin) &
        (df['dest'] == selected_dest) &
        (df['hour'] == selected_hour) &
        (df['dow'] == selected_dow)
    ]
    
    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header(f"📊 Route Analysis: {selected_route_label}")
        
        if filtered_df.empty:
            st.warning("⚠️ No data available for this route/time combination")
            st.info("""
            This could mean:
            - No services run at this time
            - Insufficient historical data
            - This route/time combination wasn't captured in the data collection period
            
            Try adjusting the hour or day of week, or run the pipeline for more days.
            """)
        else:
            row = filtered_df.iloc[0]
            p80, p90, p95 = row['p80'], row['p90'], row['p95']
            obs_count = row['obs_count']
            
            # Calculate recommended buffer
            recommended_buffer = interpolate_buffer(risk_level, p80, p90, p95)
            
            # Display KPI
            st.metric(
                label="🎯 Recommended Buffer",
                value=f"{recommended_buffer:.1f} minutes",
                help=f"Buffer time for {risk_level}% confidence of on-time arrival"
            )
            
            # Show clamp notice for high risk levels
            if risk_level > 95:
                st.info("ℹ️ Risk levels >95% are clamped to p95 in this MVP")
            
            # Display percentile chart
            st.subheader("📈 Delay Percentiles")
            
            percentile_data = pd.DataFrame({
                'Percentile': ['P80', 'P90', 'P95'],
                'Delay (minutes)': [p80, p90, p95],
                'Risk Level': ['80%', '90%', '95%']
            })
            
            fig = px.bar(
                percentile_data,
                x='Percentile',
                y='Delay (minutes)',
                title=f"Historical Delay Distribution",
                color='Delay (minutes)',
                color_continuous_scale='RdYlBu_r',
                text='Risk Level'
            )
            fig.update_traces(textposition='outside')
            fig.update_layout(showlegend=False, height=400)
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Additional stats
            st.subheader("📊 Statistics")
            
            stats_col1, stats_col2, stats_col3 = st.columns(3)
            
            with stats_col1:
                st.metric("🔢 Observations", f"{obs_count:,}")
            
            with stats_col2:
                st.metric("⏱️ Selected Hour", f"{selected_hour:02d}:00")
            
            with stats_col3:
                st.metric("📅 Day Type", selected_dow_label)
    
    with col2:
        st.header("💾 Data Export")
        
        # Download button for full dataset
        csv_data = df.to_csv(index=False)
        st.download_button(
            label="📥 Download Full Dataset",
            data=csv_data,
            file_name="route_hour_percentiles.csv",
            mime="text/csv",
            help="Download the complete percentile lookup table"
        )
        
        # Show data preview
        st.subheader("🔍 Data Preview")
        st.dataframe(
            df.head(10),
            use_container_width=True
        )
        
        # Summary stats
        st.subheader("📈 Dataset Summary")
        st.write(f"**Total entries:** {len(df):,}")
        st.write(f"**Unique routes:** {df[['origin', 'dest']].drop_duplicates().shape[0]}")
        st.write(f"**Total observations:** {df['obs_count'].sum():,}")
        st.write(f"**Avg obs/entry:** {df['obs_count'].mean():.1f}")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    **About:** This tool provides AI-powered buffer time recommendations based on historical rail delay data.
    Buffer times are calculated using percentile interpolation to match your specified risk tolerance.
    """)


if __name__ == "__main__":
    main()
