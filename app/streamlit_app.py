#!/usr/bin/env python3
"""
Sports Analytics Dashboard
Interactive visualization of NBA team performance metrics
"""

import os
import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="NBA Analytics Dashboard",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main {
        padding-top: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    div[data-testid="stMetricValue"] {
        font-size: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# Configuration
WAREHOUSE = os.getenv('WAREHOUSE', 'DUCKDB')
DUCKDB_PATH = os.getenv('DUCKDB_PATH', './data/warehouse/sports.duckdb')
EXPORT_DIR = Path('./data/exports')

@st.cache_resource
def get_connection():
    """Get database connection"""
    if WAREHOUSE.upper() == 'DUCKDB':
        return duckdb.connect(DUCKDB_PATH, read_only=True)
    else:
        st.error(f"Warehouse type {WAREHOUSE} not supported in viewer. Please connect your BI tool directly.")
        return None

@st.cache_data(ttl=300)
def load_team_data():
    """Load team dimension data"""
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    
    query = """
    SELECT 
        team_id,
        team_name,
        conference,
        is_active,
        historical_win_rate,
        avg_elo_rating,
        peak_elo_rating,
        total_games
    FROM marts.dim_teams
    ORDER BY team_name
    """
    return conn.execute(query).df()

@st.cache_data(ttl=300)
def load_games_data(selected_teams=None, selected_seasons=None):
    """Load games fact data with filters"""
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    
    query = """
    SELECT 
        f.game_date,
        f.season,
        t.team_name,
        t.conference,
        f.win_flag,
        f.score_diff,
        f.elo_pre,
        f.elo_post,
        f.elo_change,
        f.game_location,
        f.playoff
    FROM marts.fct_games f
    JOIN marts.dim_teams t ON f.team_id = t.team_id
    WHERE 1=1
    """
    
    params = []
    if selected_teams:
        placeholders = ','.join(['?' for _ in selected_teams])
        query += f" AND t.team_name IN ({placeholders})"
        params.extend(selected_teams)
    
    if selected_seasons:
        placeholders = ','.join(['?' for _ in selected_seasons])
        query += f" AND f.season IN ({placeholders})"
        params.extend(selected_seasons)
    
    query += " ORDER BY f.game_date DESC"
    
    return conn.execute(query, params).df()

def load_export_data(filename):
    """Load data from export directory"""
    filepath = EXPORT_DIR / filename
    if filepath.exists():
        return pd.read_csv(filepath)
    return None

# Title and description
st.title("🏀 NBA Analytics Dashboard")
st.markdown("**Powered by FiveThirtyEight data** • Updated daily via Airflow + dbt")

# Sidebar filters
with st.sidebar:
    st.header("Filters")
    
    # Load teams for filter
    teams_df = load_team_data()
    
    if not teams_df.empty:
        # Conference filter
        conferences = ['All'] + sorted(teams_df['conference'].unique().tolist())
        selected_conference = st.selectbox("Conference", conferences)
        
        # Team filter
        if selected_conference == 'All':
            available_teams = teams_df['team_name'].tolist()
        else:
            available_teams = teams_df[teams_df['conference'] == selected_conference]['team_name'].tolist()
        
        selected_teams = st.multiselect(
            "Teams",
            options=available_teams,
            default=available_teams[:5] if len(available_teams) > 5 else available_teams
        )
        
        # Season filter
        games_df = load_games_data()
        if not games_df.empty:
            available_seasons = sorted(games_df['season'].unique(), reverse=True)
            selected_seasons = st.multiselect(
                "Seasons",
                options=available_seasons,
                default=available_seasons[:3] if len(available_seasons) > 3 else available_seasons
            )
        else:
            selected_seasons = []
    else:
        selected_teams = []
        selected_seasons = []
    
    # Data source note
    st.markdown("---")
    st.markdown("### Data Sources")
    st.markdown("- [FiveThirtyEight NBA Elo](https://github.com/fivethirtyeight/data/tree/master/nba-elo)")
    st.markdown(f"- Warehouse: {WAREHOUSE}")
    
    # Refresh button
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# Main content area with tabs
tab1, tab2, tab3, tab4 = st.tabs(["📊 Team Win Rates", "📈 ELO Trends", "📉 Point Differential", "📋 Raw Data"])

# Tab 1: Team Win Rates
with tab1:
    st.header("Team Win Rates by Season")
    
    if selected_teams and selected_seasons:
        games_df = load_games_data(selected_teams, selected_seasons)
        
        if not games_df.empty:
            # Calculate win rates
            win_rates = games_df.groupby(['team_name', 'season']).agg({
                'win_flag': ['sum', 'count']
            }).reset_index()
            win_rates.columns = ['team_name', 'season', 'wins', 'games']
            win_rates['win_rate'] = (win_rates['wins'] / win_rates['games'] * 100).round(2)
            
            # Create visualization
            fig = px.bar(
                win_rates,
                x='season',
                y='win_rate',
                color='team_name',
                title='Win Rate by Season',
                labels={'win_rate': 'Win Rate (%)', 'season': 'Season'},
                barmode='group',
                height=500
            )
            fig.update_layout(hovermode='x unified')
            st.plotly_chart(fig, use_container_width=True)
            
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                best_team = win_rates.loc[win_rates['win_rate'].idxmax()]
                st.metric(
                    "Best Season",
                    f"{best_team['team_name']} ({best_team['season']})",
                    f"{best_team['win_rate']:.1f}%"
                )
            
            with col2:
                avg_win_rate = win_rates['win_rate'].mean()
                st.metric("Average Win Rate", f"{avg_win_rate:.1f}%")
            
            with col3:
                total_games = win_rates['games'].sum()
                st.metric("Total Games", f"{total_games:,}")
            
            with col4:
                total_teams = win_rates['team_name'].nunique()
                st.metric("Teams Analyzed", total_teams)
            
            # Download button
            csv = win_rates.to_csv(index=False)
            st.download_button(
                label="📥 Download Win Rates CSV",
                data=csv,
                file_name=f"win_rates_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
    else:
        st.info("Please select teams and seasons from the sidebar to view win rates.")

# Tab 2: ELO Trends
with tab2:
    st.header("ELO Rating Trends")
    
    if selected_teams and selected_seasons:
        games_df = load_games_data(selected_teams, selected_seasons)
        
        if not games_df.empty:
            # Create ELO trend chart
            fig = go.Figure()
            
            for team in selected_teams:
                team_data = games_df[games_df['team_name'] == team].sort_values('game_date')
                if not team_data.empty:
                    fig.add_trace(go.Scatter(
                        x=team_data['game_date'],
                        y=team_data['elo_post'],
                        mode='lines',
                        name=team,
                        line=dict(width=2),
                        hovertemplate='<b>%{fullData.name}</b><br>' +
                                    'Date: %{x}<br>' +
                                    'ELO: %{y:.0f}<br>' +
                                    '<extra></extra>'
                    ))
            
            fig.update_layout(
                title='ELO Rating Over Time',
                xaxis_title='Date',
                yaxis_title='ELO Rating',
                height=500,
                hovermode='x unified'
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # ELO change summary
            st.subheader("Season ELO Changes")
            
            elo_changes = games_df.groupby(['team_name', 'season']).agg({
                'elo_pre': 'first',
                'elo_post': 'last',
                'elo_change': 'sum'
            }).reset_index()
            elo_changes['total_change'] = elo_changes['elo_post'] - elo_changes['elo_pre']
            
            # Create heatmap
            pivot_elo = elo_changes.pivot(index='team_name', columns='season', values='total_change')
            fig_heatmap = px.imshow(
                pivot_elo,
                labels=dict(x="Season", y="Team", color="ELO Change"),
                color_continuous_scale='RdBu_r',
                color_continuous_midpoint=0,
                title="Season ELO Changes by Team"
            )
            st.plotly_chart(fig_heatmap, use_container_width=True)
    else:
        st.info("Please select teams and seasons from the sidebar to view ELO trends.")

# Tab 3: Point Differential
with tab3:
    st.header("Point Differential Analysis")
    
    if selected_teams and selected_seasons:
        games_df = load_games_data(selected_teams, selected_seasons)
        
        if not games_df.empty:
            # Box plot of point differentials
            fig_box = px.box(
                games_df,
                x='team_name',
                y='score_diff',
                color='playoff',
                title='Point Differential Distribution',
                labels={'score_diff': 'Point Differential', 'playoff': 'Playoff Game'},
                height=500,
                color_discrete_map={0: '#1f77b4', 1: '#ff7f0e'}
            )
            fig_box.update_layout(showlegend=True)
            st.plotly_chart(fig_box, use_container_width=True)
            
            # Summary statistics
            st.subheader("Point Differential Statistics")
            
            diff_stats = games_df.groupby('team_name')['score_diff'].agg([
                'mean', 'std', 'min', 'max', 'median'
            ]).round(2).reset_index()
            diff_stats.columns = ['Team', 'Average', 'Std Dev', 'Worst Loss', 'Best Win', 'Median']
            
            st.dataframe(
                diff_stats.style.format({
                    'Average': '{:.1f}',
                    'Std Dev': '{:.1f}',
                    'Worst Loss': '{:.0f}',
                    'Best Win': '{:.0f}',
                    'Median': '{:.1f}'
                }).background_gradient(subset=['Average'], cmap='RdYlGn'),
                use_container_width=True
            )
            
            # Home vs Away comparison
            st.subheader("Home Court Advantage")
            
            location_stats = games_df.groupby(['team_name', 'game_location']).agg({
                'win_flag': ['sum', 'count'],
                'score_diff': 'mean'
            }).reset_index()
            location_stats.columns = ['team_name', 'location', 'wins', 'games', 'avg_diff']
            location_stats['win_rate'] = (location_stats['wins'] / location_stats['games'] * 100).round(2)
            
            # Pivot for comparison
            pivot_location = location_stats.pivot(
                index='team_name',
                columns='location',
                values='win_rate'
            ).reset_index()
            
            if 'HOME' in pivot_location.columns and 'AWAY' in pivot_location.columns:
                pivot_location['Home Advantage'] = pivot_location['HOME'] - pivot_location['AWAY']
                
                fig_home = px.bar(
                    pivot_location,
                    x='team_name',
                    y='Home Advantage',
                    title='Home Court Advantage (Home Win% - Away Win%)',
                    labels={'Home Advantage': 'Win Rate Difference (%)'},
                    color='Home Advantage',
                    color_continuous_scale='RdYlGn',
                    color_continuous_midpoint=0
                )
                st.plotly_chart(fig_home, use_container_width=True)
    else:
        st.info("Please select teams and seasons from the sidebar to view point differential analysis.")

# Tab 4: Raw Data
with tab4:
    st.header("Raw Data Export")
    
    # Check for export files
    export_files = list(EXPORT_DIR.glob("latest_*.csv")) if EXPORT_DIR.exists() else []
    
    if export_files:
        st.markdown("### Available Exports")
        
        for export_file in export_files:
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.markdown(f"**{export_file.stem.replace('latest_', '').replace('_', ' ').title()}**")
            
            with col2:
                with open(export_file, 'rb') as f:
                    st.download_button(
                        label="Download",
                        data=f,
                        file_name=export_file.name,
                        mime="text/csv",
                        key=export_file.name
                    )
        
        # Display sample data
        st.markdown("### Data Preview")
        
        selected_export = st.selectbox(
            "Select export to preview",
            options=[f.stem.replace('latest_', '') for f in export_files],
            format_func=lambda x: x.replace('_', ' ').title()
        )
        
        if selected_export:
            preview_df = load_export_data(f"latest_{selected_export}.csv")
            if preview_df is not None:
                st.dataframe(preview_df.head(100), use_container_width=True)
                st.caption(f"Showing first 100 rows of {len(preview_df):,} total rows")
    else:
        st.warning("No export files found. Please run the data pipeline first.")
    
    # Database connection info
    if WAREHOUSE.upper() != 'DUCKDB':
        st.markdown("### Direct Database Connection")
        st.info(f"For {WAREHOUSE} warehouse, connect your BI tool directly using the credentials in your .env file")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666;'>
        Sports Analytics Pipeline • Updated Daily at 3 AM UTC • 
        <a href='https://github.com/fivethirtyeight/data/tree/master/nba-elo' target='_blank'>Data Source</a>
    </div>
    """,
    unsafe_allow_html=True
)