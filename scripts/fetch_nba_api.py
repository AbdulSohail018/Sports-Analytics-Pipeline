#!/usr/bin/env python3
"""
Fetch NBA data from balldontlie.io API
Free API with current season data, updated regularly
"""

import os
import sys
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Configuration
NBA_API_BASE_URL = os.getenv('NBA_API_BASE_URL', 'https://www.balldontlie.io/api/v1')
RAW_DATA_DIR = Path('./data/raw')
DAYS_TO_FETCH = int(os.getenv('DAYS_TO_FETCH', '30'))  # Fetch last 30 days by default

def ensure_directory():
    """Create raw data directory if it doesn't exist"""
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Ensured directory exists: {RAW_DATA_DIR}")

def fetch_teams():
    """Fetch all NBA teams"""
    print("\nFetching NBA teams...")
    url = f"{NBA_API_BASE_URL}/teams"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        teams = []
        for team in data['data']:
            teams.append({
                'team_id': team['id'],
                'abbreviation': team['abbreviation'],
                'city': team['city'],
                'conference': team['conference'],
                'division': team['division'],
                'full_name': team['full_name'],
                'name': team['name']
            })
        
        teams_df = pd.DataFrame(teams)
        print(f"Fetched {len(teams_df)} teams")
        return teams_df
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching teams: {e}")
        raise

def fetch_games(start_date, end_date):
    """Fetch games for a date range"""
    print(f"\nFetching games from {start_date} to {end_date}...")
    
    games = []
    page = 1
    per_page = 100
    
    while True:
        url = f"{NBA_API_BASE_URL}/games"
        params = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'page': page,
            'per_page': per_page
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data['data']:
                break
                
            for game in data['data']:
                games.append({
                    'game_id': game['id'],
                    'date': game['date'],
                    'home_team_id': game['home_team']['id'],
                    'home_team_abbreviation': game['home_team']['abbreviation'],
                    'home_team_name': game['home_team']['full_name'],
                    'home_team_score': game['home_team_score'],
                    'visitor_team_id': game['visitor_team']['id'],
                    'visitor_team_abbreviation': game['visitor_team']['abbreviation'],
                    'visitor_team_name': game['visitor_team']['full_name'],
                    'visitor_team_score': game['visitor_team_score'],
                    'period': game['period'],
                    'postseason': game['postseason'],
                    'season': game['season'],
                    'status': game['status'],
                    'time': game.get('time', '')
                })
            
            # Check if there are more pages
            if data['meta']['current_page'] >= data['meta']['total_pages']:
                break
                
            page += 1
            time.sleep(0.1)  # Be nice to the API
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching games: {e}")
            raise
    
    games_df = pd.DataFrame(games)
    print(f"Fetched {len(games_df)} games")
    return games_df

def fetch_player_stats(season):
    """Fetch player stats for a season"""
    print(f"\nFetching player stats for season {season}...")
    
    stats = []
    page = 1
    per_page = 100
    
    while True:
        url = f"{NBA_API_BASE_URL}/season_averages"
        params = {
            'season': season,
            'page': page,
            'per_page': per_page
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data['data']:
                break
                
            for stat in data['data']:
                stats.append({
                    'player_id': stat['player_id'],
                    'season': stat['season'],
                    'games_played': stat['games_played'],
                    'min': stat['min'],
                    'fgm': stat['fgm'],
                    'fga': stat['fga'],
                    'fg3m': stat['fg3m'],
                    'fg3a': stat['fg3a'],
                    'ftm': stat['ftm'],
                    'fta': stat['fta'],
                    'oreb': stat['oreb'],
                    'dreb': stat['dreb'],
                    'reb': stat['reb'],
                    'ast': stat['ast'],
                    'stl': stat['stl'],
                    'blk': stat['blk'],
                    'turnover': stat['turnover'],
                    'pf': stat['pf'],
                    'pts': stat['pts'],
                    'fg_pct': stat['fg_pct'],
                    'fg3_pct': stat['fg3_pct'],
                    'ft_pct': stat['ft_pct']
                })
            
            page += 1
            time.sleep(0.1)  # Be nice to the API
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching player stats: {e}")
            # Continue even if player stats fail
            break
    
    if stats:
        stats_df = pd.DataFrame(stats)
        print(f"Fetched stats for {len(stats_df)} players")
        return stats_df
    else:
        print("No player stats available")
        return pd.DataFrame()

def save_data(df, filename_prefix):
    """Save dataframe with timestamp"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{filename_prefix}_{timestamp}.csv"
    filepath = RAW_DATA_DIR / filename
    df.to_csv(filepath, index=False)
    print(f"Saved to: {filepath}")
    
    # Create stable symlink
    latest_link = RAW_DATA_DIR / f"latest_{filename_prefix}.csv"
    if latest_link.exists():
        latest_link.unlink()
    latest_link.symlink_to(filename)
    print(f"Created symlink: {latest_link} -> {filename}")
    
    return filepath

def main():
    """Main execution function"""
    print("Starting NBA data fetch from balldontlie.io API")
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    ensure_directory()
    
    try:
        # 1. Fetch teams
        teams_df = fetch_teams()
        save_data(teams_df, 'nba_teams')
        
        # 2. Fetch games for the last N days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=DAYS_TO_FETCH)
        games_df = fetch_games(start_date, end_date)
        
        if not games_df.empty:
            save_data(games_df, 'nba_games')
            
            # 3. Fetch player stats for current season
            current_season = games_df['season'].max() if not games_df.empty else 2024
            player_stats_df = fetch_player_stats(current_season)
            if not player_stats_df.empty:
                save_data(player_stats_df, 'nba_player_stats')
        else:
            print("No games found in the specified date range")
            
        print("\nData fetch completed successfully!")
        
    except Exception as e:
        print(f"Failed to fetch data: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()