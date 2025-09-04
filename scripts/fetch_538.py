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
NBA_ELO_URL = os.getenv('NBA_ELO_URL', 'https://raw.githubusercontent.com/fivethirtyeight/data/master/nba-elo/elo.csv')
NBA_ALLELO_URL = os.getenv('NBA_ALLELO_URL', 'https://raw.githubusercontent.com/fivethirtyeight/data/master/nba-elo/nbaallelo.csv')
RAW_DATA_DIR = Path('./data/raw')
MIN_ROW_COUNT = 1000

# Expected columns for validation
EXPECTED_COLUMNS = {
    'elo': ['date', 'season', 'team1', 'team2', 'elo1_pre', 'elo2_pre', 'elo1_post', 'elo2_post'],
    'nbaallelo': ['gameorder', 'date', 'team', 'elo', 'game_id']
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

def main():
    """Main execution function"""
    print("Starting FiveThirtyEight data fetch")
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    ensure_directory()
    
    # Fetch both datasets
    datasets = [
        (NBA_ELO_URL, 'elo', EXPECTED_COLUMNS['elo']),
        (NBA_ALLELO_URL, 'nbaallelo', EXPECTED_COLUMNS['nbaallelo'])
    ]
    
    fetched_files = []
    for url, name, expected_cols in datasets:
        try:
            filepath = fetch_and_save_csv(url, name, expected_cols)
            fetched_files.append(filepath)
        except Exception as e:
            print(f"Failed to fetch {name}: {e}")
            sys.exit(1)
    
    print(f"\nSuccessfully fetched {len(fetched_files)} files")
    print("Data fetch completed successfully!")
    
if __name__ == '__main__':
    main()