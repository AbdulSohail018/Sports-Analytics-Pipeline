#!/usr/bin/env python3
"""
Export dbt mart tables to CSV files for BI consumption
Creates timestamped exports and stable latest symlinks
"""

import os
import sys
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
DUCKDB_PATH = os.getenv('DUCKDB_PATH', './data/warehouse/sports.duckdb')
EXPORT_DIR = Path('./data/exports')

def ensure_directory():
    """Create export directory if it doesn't exist"""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Ensured export directory: {EXPORT_DIR}")

def get_connection():
    """Get DuckDB connection"""
    return duckdb.connect(DUCKDB_PATH)

def export_team_win_rates(conn):
    """Export team win rates by season"""
    print("\nExporting team win rates...")
    
    query = """
    SELECT 
        t.team_name,
        f.season,
        COUNT(*) as games_played,
        SUM(f.win_flag) as wins,
        ROUND(SUM(f.win_flag) * 100.0 / COUNT(*), 2) as win_rate,
        ROUND(AVG(f.score_diff), 2) as avg_point_diff
    FROM marts.fct_games f
    JOIN marts.dim_teams t ON f.team_id = t.team_id
    GROUP BY t.team_name, f.season
    ORDER BY f.season DESC, win_rate DESC
    """
    
    try:
        df = conn.execute(query).df()
        
        # Save with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"team_win_rates_{timestamp}.csv"
        filepath = EXPORT_DIR / filename
        df.to_csv(filepath, index=False)
        print(f"Exported {len(df)} rows to {filepath}")
        
        # Create stable symlink
        latest_link = EXPORT_DIR / "latest_team_win_rates.csv"
        if latest_link.exists():
            latest_link.unlink()
        latest_link.symlink_to(filename)
        
        return df
    except Exception as e:
        print(f"Error exporting team win rates: {e}")
        return None

def export_elo_trends(conn):
    """Export ELO rating trends"""
    print("\nExporting ELO trends...")
    
    query = """
    WITH elo_changes AS (
        SELECT 
            t.team_name,
            f.game_date,
            f.season,
            f.elo_pre,
            f.elo_post,
            f.elo_change,
            SUM(f.elo_change) OVER (
                PARTITION BY t.team_name 
                ORDER BY f.game_date
            ) as cumulative_elo_change
        FROM marts.fct_games f
        JOIN marts.dim_teams t ON f.team_id = t.team_id
        WHERE f.game_date >= CURRENT_DATE - INTERVAL '2 years'
    )
    SELECT 
        team_name,
        game_date,
        season,
        elo_pre,
        elo_post,
        elo_change,
        cumulative_elo_change
    FROM elo_changes
    ORDER BY team_name, game_date
    """
    
    try:
        df = conn.execute(query).df()
        
        # Save with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"elo_trends_{timestamp}.csv"
        filepath = EXPORT_DIR / filename
        df.to_csv(filepath, index=False)
        print(f"Exported {len(df)} rows to {filepath}")
        
        # Create stable symlink
        latest_link = EXPORT_DIR / "latest_elo_trends.csv"
        if latest_link.exists():
            latest_link.unlink()
        latest_link.symlink_to(filename)
        
        return df
    except Exception as e:
        print(f"Error exporting ELO trends: {e}")
        return None

def export_point_diff_by_season(conn):
    """Export point differential statistics by season"""
    print("\nExporting point differential by season...")
    
    query = """
    SELECT 
        t.team_name,
        f.season,
        COUNT(*) as games,
        ROUND(AVG(f.score_diff), 2) as avg_point_diff,
        ROUND(STDDEV(f.score_diff), 2) as stddev_point_diff,
        MIN(f.score_diff) as worst_loss,
        MAX(f.score_diff) as best_win,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY f.score_diff) as q1_point_diff,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY f.score_diff) as median_point_diff,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY f.score_diff) as q3_point_diff
    FROM marts.fct_games f
    JOIN marts.dim_teams t ON f.team_id = t.team_id
    GROUP BY t.team_name, f.season
    ORDER BY f.season DESC, avg_point_diff DESC
    """
    
    try:
        df = conn.execute(query).df()
        
        # Save with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"point_diff_by_season_{timestamp}.csv"
        filepath = EXPORT_DIR / filename
        df.to_csv(filepath, index=False)
        print(f"Exported {len(df)} rows to {filepath}")
        
        # Create stable symlink
        latest_link = EXPORT_DIR / "latest_point_diff_by_season.csv"
        if latest_link.exists():
            latest_link.unlink()
        latest_link.symlink_to(filename)
        
        return df
    except Exception as e:
        print(f"Error exporting point differential: {e}")
        return None

def create_export_summary(conn):
    """Create a summary of all exports"""
    print("\nCreating export summary...")
    
    summary = {
        'export_timestamp': datetime.now().isoformat(),
        'warehouse_type': 'DuckDB',
        'database_path': DUCKDB_PATH,
        'exports': []
    }
    
    # Get table counts
    tables = ['dim_teams', 'fct_games']
    for table in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM marts.{table}").fetchone()[0]
            summary['exports'].append({
                'table': table,
                'row_count': count
            })
        except:
            pass
    
    # Save summary
    summary_df = pd.DataFrame([summary])
    summary_path = EXPORT_DIR / 'export_summary.json'
    summary_df.to_json(summary_path, orient='records', indent=2)
    print(f"Created export summary: {summary_path}")

def main():
    """Main execution function"""
    print("Starting metrics export")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Database path: {DUCKDB_PATH}")
    
    ensure_directory()
    
    try:
        # Connect to database
        with get_connection() as conn:
            # Export metrics
            export_team_win_rates(conn)
            export_elo_trends(conn)
            export_point_diff_by_season(conn)
            
            # Create summary
            create_export_summary(conn)
            
            print("\nMetrics export completed successfully!")
            print(f"Exports available in: {EXPORT_DIR}")
            
    except Exception as e:
        print(f"Error during metrics export: {e}")
        print("Ensure dbt models have been run successfully")
        sys.exit(1)

if __name__ == '__main__':
    main()