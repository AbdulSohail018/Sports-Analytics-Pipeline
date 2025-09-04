#!/usr/bin/env python3
"""
Fetch FiveThirtyEight NBA data from configured URLs
Validates content and saves to data/raw with timestamped filenames
"""

import os
import sys
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
NBA_ALLELO_URL = os.getenv('NBA_ALLELO_URL', 'https://raw.githubusercontent.com/fivethirtyeight/data/master/nba-elo/nbaallelo.csv')
RAW_DATA_DIR = Path('./data/raw')
MIN_ROW_COUNT = 1000

# Expected columns for validation
EXPECTED_COLUMNS = {
    'nbaallelo': ['gameorder', 'game_id', 'lg_id', 'date', 'franch_id', 'opp_franch', 'elo_i', 'elo_n', 'win_equiv', 'opp_id', 'opp_elo_i', 'opp_elo_n', 'game_location', 'game_result', 'forecast', 'notes']
}

def ensure_directory():
    """Create raw data directory if it doesn't exist"""
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Ensured directory exists: {RAW_DATA_DIR}")

def validate_csv_content(df, expected_columns, min_rows=MIN_ROW_COUNT):
    """Validate CSV content meets requirements"""
    # Check row count
    if len(df) < min_rows:
        raise ValueError(f"CSV has only {len(df)} rows, expected at least {min_rows}")
    
    # Check for expected columns (subset check)
    missing_columns = set(expected_columns) - set(df.columns)
    if missing_columns:
        print(f"Warning: Expected columns missing: {missing_columns}")
        print(f"Available columns: {list(df.columns)}")
    
    return True

def fetch_and_save_csv(url, dataset_name, expected_cols):
    """Fetch CSV from URL and save with validation"""
    print(f"\nFetching {dataset_name} from {url}")
    
    try:
        # Make request with headers
        headers = {
            'User-Agent': 'Sports Analytics Pipeline/1.0'
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Validate content type
        content_type = response.headers.get('Content-Type', '')
        if 'text' not in content_type and 'csv' not in content_type:
            print(f"Warning: Unexpected content type: {content_type}")
        
        # Parse CSV
        df = pd.read_csv(pd.io.common.StringIO(response.text))
        print(f"Fetched {len(df)} rows, {len(df.columns)} columns")
        
        # Validate content
        validate_csv_content(df, expected_cols)
        
        # Save with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{dataset_name}_{timestamp}.csv"
        filepath = RAW_DATA_DIR / filename
        df.to_csv(filepath, index=False)
        print(f"Saved to: {filepath}")
        
        # Create stable symlink for latest version
        latest_link = RAW_DATA_DIR / f"latest_{dataset_name}.csv"
        if latest_link.exists():
            latest_link.unlink()
        latest_link.symlink_to(filename)
        print(f"Created symlink: {latest_link} -> {filename}")
        
        return filepath
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {dataset_name}: {e}")
        print("Troubleshooting: Check if the URL is correct and accessible")
        raise
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV for {dataset_name}: {e}")
        print("Troubleshooting: The data format may have changed")
        raise
    except Exception as e:
        print(f"Unexpected error for {dataset_name}: {e}")
        raise

def process_nbaallelo_to_games(df):
    """Process nbaallelo data to create game-level data"""
    print("\nProcessing nbaallelo data to create game-level dataset...")
    
    # Sort by game_id and date
    df = df.sort_values(['game_id', 'date'])
    
    # Create games by pairing rows with same game_id
    games = []
    
    # Group by game_id
    for game_id, group in df.groupby('game_id'):
        if len(group) == 2:  # Valid game with both teams
            row1, row2 = group.iloc[0], group.iloc[1]
            
            # Determine home/away based on game_location
            if row1['game_location'] == 'H':
                home_row, away_row = row1, row2
            else:
                home_row, away_row = row2, row1
            
            # Extract scores from game_result (format: 'W 110-95' or 'L 95-110')
            def extract_scores(result_str):
                if pd.isna(result_str):
                    return None, None
                parts = result_str.split()
                if len(parts) >= 2:
                    scores = parts[1].split('-')
                    if len(scores) == 2:
                        return int(scores[0]), int(scores[1])
                return None, None
            
            home_scores = extract_scores(home_row['game_result'])
            away_scores = extract_scores(away_row['game_result'])
            
            # Create game record
            game = {
                'date': home_row['date'],
                'season': pd.to_datetime(home_row['date']).year if pd.to_datetime(home_row['date']).month >= 10 else pd.to_datetime(home_row['date']).year - 1,
                'neutral': 1 if home_row['game_location'] == 'N' else 0,
                'playoff': 1 if 'playoff' in str(home_row.get('notes', '')).lower() else 0,
                'team1': home_row['franch_id'],
                'team2': away_row['franch_id'],
                'elo1_pre': home_row['elo_i'],
                'elo2_pre': away_row['elo_i'],
                'elo_prob1': home_row['forecast'],
                'elo_prob2': away_row['forecast'],
                'elo1_post': home_row['elo_n'],
                'elo2_post': away_row['elo_n'],
                'score1': home_scores[0] if home_scores else None,
                'score2': home_scores[1] if home_scores else None
            }
            
            games.append(game)
    
    games_df = pd.DataFrame(games)
    print(f"Created {len(games_df)} games from {len(df)} team records")
    
    return games_df

def main():
    """Main execution function"""
    print("Starting FiveThirtyEight data fetch")
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    ensure_directory()
    
    # Fetch nbaallelo dataset
    try:
        # Fetch the main dataset
        nbaallelo_path = fetch_and_save_csv(
            NBA_ALLELO_URL, 
            'nbaallelo', 
            EXPECTED_COLUMNS['nbaallelo']
        )
        
        # Load and process to create games dataset
        df = pd.read_csv(nbaallelo_path)
        games_df = process_nbaallelo_to_games(df)
        
        # Save games dataset
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        games_filename = f"elo_{timestamp}.csv"
        games_filepath = RAW_DATA_DIR / games_filename
        games_df.to_csv(games_filepath, index=False)
        print(f"Saved games data to: {games_filepath}")
        
        # Create stable symlink
        games_link = RAW_DATA_DIR / "latest_elo.csv"
        if games_link.exists():
            games_link.unlink()
        games_link.symlink_to(games_filename)
        print(f"Created symlink: {games_link} -> {games_filename}")
        
        print("\nSuccessfully fetched and processed all data")
        print("Data fetch completed successfully!")
        
    except Exception as e:
        print(f"Failed to fetch or process data: {e}")
        sys.exit(1)
    
if __name__ == '__main__':
    main()