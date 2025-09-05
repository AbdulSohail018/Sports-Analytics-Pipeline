#!/usr/bin/env python3
"""
Load NBA API data into DuckDB warehouse
Creates schemas and tables for the modern API data structure
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
WAREHOUSE = os.getenv('WAREHOUSE', 'DUCKDB')
DUCKDB_PATH = os.getenv('DUCKDB_PATH', './data/warehouse/sports.duckdb')
RAW_DATA_DIR = Path('./data/raw')

def ensure_directories():
    """Ensure warehouse directory exists"""
    warehouse_dir = Path(DUCKDB_PATH).parent
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    print(f"Ensured warehouse directory: {warehouse_dir}")

def get_connection():
    """Get database connection based on warehouse type"""
    if WAREHOUSE.upper() == 'DUCKDB':
        return duckdb.connect(DUCKDB_PATH)
    else:
        raise NotImplementedError(f"Warehouse type {WAREHOUSE} not implemented in this script")

def create_schemas(conn):
    """Create necessary schemas"""
    schemas = ['raw', 'staging']
    for schema in schemas:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"Created/verified schemas: {schemas}")

def load_teams_data(conn):
    """Load teams data"""
    csv_path = RAW_DATA_DIR / 'latest_nba_teams.csv'
    if not csv_path.exists():
        print(f"Warning: Missing file {csv_path}")
        return
    
    print(f"Loading {csv_path} into raw.teams")
    
    # Drop and recreate table
    conn.execute("DROP TABLE IF EXISTS raw.teams")
    
    # Create table
    create_sql = """
    CREATE TABLE raw.teams (
        team_id INTEGER PRIMARY KEY,
        abbreviation VARCHAR(10),
        city VARCHAR(50),
        conference VARCHAR(20),
        division VARCHAR(50),
        full_name VARCHAR(100),
        name VARCHAR(50)
    )
    """
    conn.execute(create_sql)
    
    # Load data
    conn.execute(f"""
        COPY raw.teams FROM '{csv_path}' 
        WITH (HEADER TRUE, DELIMITER ',', NULL '')
    """)
    
    # Get row count
    result = conn.execute("SELECT COUNT(*) FROM raw.teams").fetchone()
    print(f"Loaded {result[0]} teams into raw.teams")

def load_games_data(conn):
    """Load games data"""
    csv_path = RAW_DATA_DIR / 'latest_nba_games.csv'
    if not csv_path.exists():
        print(f"Warning: Missing file {csv_path}")
        return
    
    print(f"Loading {csv_path} into raw.games")
    
    # Drop and recreate table
    conn.execute("DROP TABLE IF EXISTS raw.games")
    
    # Create table
    create_sql = """
    CREATE TABLE raw.games (
        game_id BIGINT PRIMARY KEY,
        date TIMESTAMP,
        home_team_id INTEGER,
        home_team_abbreviation VARCHAR(10),
        home_team_name VARCHAR(100),
        home_team_score INTEGER,
        visitor_team_id INTEGER,
        visitor_team_abbreviation VARCHAR(10),
        visitor_team_name VARCHAR(100),
        visitor_team_score INTEGER,
        period INTEGER,
        postseason BOOLEAN,
        season INTEGER,
        status VARCHAR(20),
        time VARCHAR(20)
    )
    """
    conn.execute(create_sql)
    
    # Load data
    conn.execute(f"""
        COPY raw.games FROM '{csv_path}' 
        WITH (HEADER TRUE, DELIMITER ',', NULL '')
    """)
    
    # Get row count
    result = conn.execute("SELECT COUNT(*) FROM raw.games").fetchone()
    print(f"Loaded {result[0]} games into raw.games")
    
    # Create game-level data compatible with old schema for backward compatibility
    conn.execute("DROP TABLE IF EXISTS raw.elo")
    conn.execute("""
        CREATE TABLE raw.elo AS
        SELECT 
            game_id,
            DATE(date) as date,
            season,
            0 as neutral,  -- No neutral court info in new API
            CASE WHEN postseason THEN 1 ELSE 0 END as playoff,
            home_team_abbreviation as team1,
            visitor_team_abbreviation as team2,
            1500.0 as elo1_pre,  -- Default ELO
            1500.0 as elo2_pre,
            0.5 as elo_prob1,
            0.5 as elo_prob2,
            CASE 
                WHEN home_team_score > visitor_team_score THEN 1515.0 
                ELSE 1485.0 
            END as elo1_post,
            CASE 
                WHEN visitor_team_score > home_team_score THEN 1515.0 
                ELSE 1485.0 
            END as elo2_post,
            home_team_score as score1,
            visitor_team_score as score2
        FROM raw.games
        WHERE status = 'Final'
    """)
    
    result = conn.execute("SELECT COUNT(*) FROM raw.elo").fetchone()
    print(f"Created backward-compatible elo table with {result[0]} games")

def load_player_stats_data(conn):
    """Load player stats data"""
    csv_path = RAW_DATA_DIR / 'latest_nba_player_stats.csv'
    if not csv_path.exists():
        print(f"Warning: Missing file {csv_path}")
        return
    
    print(f"Loading {csv_path} into raw.player_stats")
    
    # Drop and recreate table
    conn.execute("DROP TABLE IF EXISTS raw.player_stats")
    
    # Create table
    create_sql = """
    CREATE TABLE raw.player_stats (
        player_id INTEGER,
        season INTEGER,
        games_played INTEGER,
        min VARCHAR(10),
        fgm DOUBLE,
        fga DOUBLE,
        fg3m DOUBLE,
        fg3a DOUBLE,
        ftm DOUBLE,
        fta DOUBLE,
        oreb DOUBLE,
        dreb DOUBLE,
        reb DOUBLE,
        ast DOUBLE,
        stl DOUBLE,
        blk DOUBLE,
        turnover DOUBLE,
        pf DOUBLE,
        pts DOUBLE,
        fg_pct DOUBLE,
        fg3_pct DOUBLE,
        ft_pct DOUBLE
    )
    """
    conn.execute(create_sql)
    
    # Load data
    conn.execute(f"""
        COPY raw.player_stats FROM '{csv_path}' 
        WITH (HEADER TRUE, DELIMITER ',', NULL '')
    """)
    
    # Get row count
    result = conn.execute("SELECT COUNT(*) FROM raw.player_stats").fetchone()
    print(f"Loaded {result[0]} player stats into raw.player_stats")

def verify_data_quality(conn):
    """Run basic data quality checks"""
    print("\nRunning data quality checks...")
    
    # Check for games with null scores
    null_scores = conn.execute("""
        SELECT COUNT(*) 
        FROM raw.games 
        WHERE (home_team_score IS NULL OR visitor_team_score IS NULL)
        AND status = 'Final'
    """).fetchone()[0]
    
    if null_scores > 0:
        print(f"Warning: Found {null_scores} completed games with null scores")
    else:
        print("✓ All completed games have scores")
    
    # Check date ranges
    date_range = conn.execute("""
        SELECT 
            MIN(DATE(date)) as earliest,
            MAX(DATE(date)) as latest,
            COUNT(DISTINCT DATE(date)) as unique_dates
        FROM raw.games
    """).fetchone()
    
    print(f"✓ Game dates: {date_range[0]} to {date_range[1]} ({date_range[2]} unique dates)")
    
    # Check team counts
    team_count = conn.execute("SELECT COUNT(*) FROM raw.teams").fetchone()[0]
    print(f"✓ Found {team_count} teams")

def main():
    """Main execution function"""
    print("Starting NBA API data load to warehouse")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Warehouse type: {WAREHOUSE}")
    print(f"Database path: {DUCKDB_PATH}")
    
    ensure_directories()
    
    try:
        # Connect to database
        with get_connection() as conn:
            # Create schemas
            create_schemas(conn)
            
            # Load data tables
            load_teams_data(conn)
            load_games_data(conn)
            load_player_stats_data(conn)
            
            # Verify data quality
            verify_data_quality(conn)
            
            print("\nData load completed successfully!")
            
    except Exception as e:
        print(f"Error during data load: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()