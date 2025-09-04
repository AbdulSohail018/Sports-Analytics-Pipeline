#!/usr/bin/env python3
"""
Load raw CSV data into DuckDB warehouse
Creates schemas and tables, performs basic cleaning
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
        # For other warehouses, implement connection logic
        raise NotImplementedError(f"Warehouse type {WAREHOUSE} not implemented in this script")

def create_schemas(conn):
    """Create necessary schemas"""
    schemas = ['raw', 'staging']
    for schema in schemas:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"Created/verified schemas: {schemas}")

def load_elo_data(conn):
    """Load elo.csv into raw.elo table"""
    csv_path = RAW_DATA_DIR / 'latest_elo.csv'
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing file: {csv_path}")
    
    print(f"Loading {csv_path} into raw.elo")
    
    # Drop and recreate table for idempotency
    conn.execute("DROP TABLE IF EXISTS raw.elo")
    
    # Create table with appropriate types
    create_sql = """
    CREATE TABLE raw.elo (
        date DATE,
        season INTEGER,
        neutral INTEGER,
        playoff INTEGER,
        team1 VARCHAR,
        team2 VARCHAR,
        elo1_pre DOUBLE,
        elo2_pre DOUBLE,
        elo_prob1 DOUBLE,
        elo_prob2 DOUBLE,
        elo1_post DOUBLE,
        elo2_post DOUBLE,
        score1 INTEGER,
        score2 INTEGER
    )
    """
    conn.execute(create_sql)
    
    # Load data
    conn.execute(f"""
        COPY raw.elo FROM '{csv_path}' 
        WITH (HEADER TRUE, DELIMITER ',', NULL '')
    """)
    
    # Get row count
    result = conn.execute("SELECT COUNT(*) FROM raw.elo").fetchone()
    print(f"Loaded {result[0]} rows into raw.elo")
    
    # Basic cleaning - handle null team codes
    conn.execute("""
        UPDATE raw.elo 
        SET team1 = 'UNK' WHERE team1 IS NULL OR team1 = '';
        UPDATE raw.elo 
        SET team2 = 'UNK' WHERE team2 IS NULL OR team2 = '';
    """)

def load_nbaallelo_data(conn):
    """Load nbaallelo.csv into raw.nbaallelo table"""
    csv_path = RAW_DATA_DIR / 'latest_nbaallelo.csv'
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing file: {csv_path}")
    
    print(f"Loading {csv_path} into raw.nbaallelo")
    
    # Drop and recreate table
    conn.execute("DROP TABLE IF EXISTS raw.nbaallelo")
    
    # Create table with full schema
    create_sql = """
    CREATE TABLE raw.nbaallelo (
        gameorder INTEGER,
        game_id VARCHAR,
        lg_id VARCHAR,
        date DATE,
        franch_id VARCHAR,
        opp_franch VARCHAR,
        elo_i DOUBLE,
        elo_n DOUBLE,
        win_equiv DOUBLE,
        opp_id VARCHAR,
        opp_elo_i DOUBLE,
        opp_elo_n DOUBLE,
        game_location VARCHAR,
        game_result VARCHAR,
        forecast DOUBLE,
        notes VARCHAR
    )
    """
    conn.execute(create_sql)
    
    # Load data
    conn.execute(f"""
        COPY raw.nbaallelo FROM '{csv_path}' 
        WITH (HEADER TRUE, DELIMITER ',', NULL '')
    """)
    
    # Get row count
    result = conn.execute("SELECT COUNT(*) FROM raw.nbaallelo").fetchone()
    print(f"Loaded {result[0]} rows into raw.nbaallelo")
    
    # Basic cleaning
    conn.execute("""
        UPDATE raw.nbaallelo 
        SET franch_id = 'UNK' WHERE franch_id IS NULL OR franch_id = '';
        UPDATE raw.nbaallelo 
        SET opp_franch = 'UNK' WHERE opp_franch IS NULL OR opp_franch = '';
    """)

def create_team_lookup(conn):
    """Create a basic team lookup if seeds exist"""
    seed_path = Path('./dbt/seeds/team_aliases.csv')
    if seed_path.exists():
        print(f"Loading team aliases from {seed_path}")
        conn.execute("DROP TABLE IF EXISTS staging.team_aliases")
        conn.execute(f"""
            CREATE TABLE staging.team_aliases AS 
            SELECT * FROM read_csv_auto('{seed_path}')
        """)

def verify_data_quality(conn):
    """Run basic data quality checks"""
    print("\nRunning data quality checks...")
    
    # Check for duplicate games in elo
    duplicates = conn.execute("""
        SELECT date, team1, team2, COUNT(*) as cnt
        FROM raw.elo
        GROUP BY date, team1, team2
        HAVING COUNT(*) > 1
        LIMIT 5
    """).fetchall()
    
    if duplicates:
        print(f"Warning: Found {len(duplicates)} duplicate game entries")
    else:
        print("✓ No duplicate games found")
    
    # Check date ranges
    date_range = conn.execute("""
        SELECT 
            MIN(date) as earliest,
            MAX(date) as latest,
            COUNT(DISTINCT date) as unique_dates
        FROM raw.elo
    """).fetchone()
    
    print(f"✓ Date range: {date_range[0]} to {date_range[1]} ({date_range[2]} unique dates)")
    
    # Check team counts
    team_count = conn.execute("""
        SELECT COUNT(DISTINCT team) as unique_teams
        FROM (
            SELECT DISTINCT team1 as team FROM raw.elo
            UNION
            SELECT DISTINCT team2 as team FROM raw.elo
        )
    """).fetchone()
    
    print(f"✓ Found {team_count[0]} unique teams")

def main():
    """Main execution function"""
    print("Starting data load to warehouse")
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
            load_elo_data(conn)
            load_nbaallelo_data(conn)
            
            # Create lookups if available
            create_team_lookup(conn)
            
            # Verify data quality
            verify_data_quality(conn)
            
            print("\nData load completed successfully!")
            
    except Exception as e:
        print(f"Error during data load: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()