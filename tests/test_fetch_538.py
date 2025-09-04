#!/usr/bin/env python3
"""
Tests for the FiveThirtyEight data fetching script
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import sys
import os

# Add scripts to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

import fetch_538

class TestFetch538:
    """Test cases for fetch_538.py"""
    
    @pytest.fixture
    def mock_response(self):
        """Create a mock response object"""
        response = Mock()
        response.status_code = 200
        response.headers = {'Content-Type': 'text/csv'}
        return response
    
    @pytest.fixture
    def sample_elo_csv(self):
        """Sample ELO CSV data"""
        return """date,season,neutral,playoff,team1,team2,elo1_pre,elo2_pre,elo_prob1,elo_prob2,elo1_post,elo2_post,score1,score2
2023-10-24,2024,0,0,LAL,DEN,1500.0,1600.0,0.36,0.64,1485.2,1614.8,107,119
2023-10-24,2024,0,0,PHX,GSW,1550.0,1520.0,0.54,0.46,1560.5,1509.5,108,104"""
    
    @pytest.fixture
    def sample_nbaallelo_csv(self):
        """Sample NBA all ELO CSV data"""
        return """gameorder,game_id,lg_id,date,franch_id,opp_franch,elo_i,elo_n,win_equiv,opp_id,opp_elo_i,opp_elo_n,game_location,game_result,forecast,notes
1,202310240LAL,NBA,2023-10-24,LAL,DEN,1500.0,1485.2,0.0,DEN,1600.0,1614.8,H,L 107-119,0.36,
2,202310240DEN,NBA,2023-10-24,DEN,LAL,1600.0,1614.8,0.0,LAL,1500.0,1485.2,A,W 119-107,0.64,"""
    
    def test_ensure_directory_creates_path(self, tmp_path, monkeypatch):
        """Test that ensure_directory creates the raw data directory"""
        test_dir = tmp_path / "data" / "raw"
        monkeypatch.setattr(fetch_538, 'RAW_DATA_DIR', test_dir)
        
        fetch_538.ensure_directory()
        
        assert test_dir.exists()
        assert test_dir.is_dir()
    
    def test_validate_csv_content_success(self, sample_elo_csv):
        """Test successful CSV validation"""
        df = pd.read_csv(pd.io.common.StringIO(sample_elo_csv))
        expected_cols = ['date', 'season', 'team1', 'team2']
        
        result = fetch_538.validate_csv_content(df, expected_cols, min_rows=1)
        
        assert result is True
    
    def test_validate_csv_content_insufficient_rows(self, sample_elo_csv):
        """Test CSV validation fails with insufficient rows"""
        df = pd.read_csv(pd.io.common.StringIO(sample_elo_csv))
        expected_cols = ['date', 'season']
        
        with pytest.raises(ValueError, match="expected at least 1000"):
            fetch_538.validate_csv_content(df, expected_cols, min_rows=1000)
    
    @patch('fetch_538.requests.get')
    def test_fetch_and_save_csv_success(self, mock_get, mock_response, sample_elo_csv, tmp_path, monkeypatch):
        """Test successful CSV fetch and save"""
        # Setup
        mock_response.text = sample_elo_csv
        mock_get.return_value = mock_response
        
        test_dir = tmp_path / "data" / "raw"
        test_dir.mkdir(parents=True)
        monkeypatch.setattr(fetch_538, 'RAW_DATA_DIR', test_dir)
        monkeypatch.setattr(fetch_538, 'MIN_ROW_COUNT', 1)
        
        # Execute
        result = fetch_538.fetch_and_save_csv(
            'http://example.com/elo.csv',
            'elo',
            ['date', 'season']
        )
        
        # Verify
        assert result.exists()
        assert result.suffix == '.csv'
        assert 'elo_' in result.name
        
        # Check symlink
        latest_link = test_dir / 'latest_elo.csv'
        assert latest_link.exists()
        assert latest_link.is_symlink()
    
    @patch('fetch_538.requests.get')
    def test_fetch_and_save_csv_http_error(self, mock_get):
        """Test handling of HTTP errors"""
        mock_get.side_effect = Exception("Connection error")
        
        with pytest.raises(Exception, match="Connection error"):
            fetch_538.fetch_and_save_csv(
                'http://example.com/elo.csv',
                'elo',
                ['date', 'season']
            )
    
    @patch('fetch_538.fetch_and_save_csv')
    @patch('fetch_538.ensure_directory')
    def test_main_success(self, mock_ensure_dir, mock_fetch):
        """Test main function executes successfully"""
        mock_fetch.return_value = Path('/tmp/test.csv')
        
        # Should not raise any exceptions
        fetch_538.main()
        
        # Verify calls
        mock_ensure_dir.assert_called_once()
        assert mock_fetch.call_count == 2  # Called for both datasets
    
    @patch('fetch_538.fetch_and_save_csv')
    @patch('fetch_538.ensure_directory')
    def test_main_failure_exits(self, mock_ensure_dir, mock_fetch):
        """Test main function exits on failure"""
        mock_fetch.side_effect = Exception("Fetch failed")
        
        with pytest.raises(SystemExit) as exc_info:
            fetch_538.main()
        
        assert exc_info.value.code == 1