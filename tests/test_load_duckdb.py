#!/usr/bin/env python3
"""
Tests for the DuckDB data loading script
"""

import pytest
import duckdb
import pandas as pd
from pathlib import Path
import tempfile
import sys
import os

# Add scripts to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

import load_duckdb

class TestLoadDuckDB:
    """Test cases for load_duckdb.py"""
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary DuckDB database"""
        with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as f:
            db_path = f.name
        yield db_path
        # Cleanup
        if Path(db_path).exists():
            Path(db_path).unlink()
    
    @pytest.fixture
    def sample_elo_data(self, tmp_path):
        """Create sample ELO CSV file"""
        data = pd.DataFrame({
            'date': ['2023-10-24', '2023-10-25'],
            'season': [2024, 2024],
            'neutral': [0, 0],
            'playoff': [0, 0],
            'team1': ['LAL', 'BOS'],
            'team2': ['DEN', 'NYK'],
            'elo1_pre': [1500.0, 1550.0],
            'elo2_pre': [1600.0, 1520.0],
            'elo_prob1': [0.36, 0.54],
            'elo_prob2': [0.64, 0.46],
            'elo1_post': [1485.2, 1560.5],
            'elo2_post': [1614.8, 1509.5],
            'score1': [107, 108],
            'score2': [119, 104]
        })
        
        csv_path = tmp_path / 'latest_elo.csv'
        data.to_csv(csv_path, index=False)
        return csv_path
    
    @pytest.fixture
    def sample_nbaallelo_data(self, tmp_path):
        """Create sample NBA all ELO CSV file"""
        data = pd.DataFrame({
            'gameorder': [1, 2, 3, 4],
            'date': ['2023-10-24', '2023-10-24', '2023-10-25', '2023-10-25'],
            'team': ['LAL', 'DEN', 'BOS', 'NYK'],
            'elo': [1500.0, 1600.0, 1550.0, 1520.0],
            'game_id': ['202310240LAL', '202310240DEN', '202310250BOS', '202310250NYK']
        })
        
        csv_path = tmp_path / 'latest_nbaallelo.csv'
        data.to_csv(csv_path, index=False)
        return csv_path
    
    def test_ensure_directories(self, tmp_path, monkeypatch):
        """Test directory creation"""
        db_path = tmp_path / 'warehouse' / 'test.duckdb'
        monkeypatch.setattr(load_duckdb, 'DUCKDB_PATH', str(db_path))
        
        load_duckdb.ensure_directories()
        
        assert db_path.parent.exists()
        assert db_path.parent.is_dir()
    
    def test_get_connection_duckdb(self, temp_db, monkeypatch):
        """Test DuckDB connection creation"""
        monkeypatch.setattr(load_duckdb, 'WAREHOUSE', 'DUCKDB')
        monkeypatch.setattr(load_duckdb, 'DUCKDB_PATH', temp_db)
        
        conn = load_duckdb.get_connection()
        
        assert conn is not None
        # Test connection works
        result = conn.execute("SELECT 1").fetchone()
        assert result[0] == 1
        conn.close()
    
    def test_get_connection_unsupported(self, monkeypatch):
        """Test unsupported warehouse type raises error"""
        monkeypatch.setattr(load_duckdb, 'WAREHOUSE', 'MYSQL')
        
        with pytest.raises(NotImplementedError, match="MYSQL not implemented"):
            load_duckdb.get_connection()
    
    def test_create_schemas(self, temp_db):
        """Test schema creation"""
        conn = duckdb.connect(temp_db)
        
        load_duckdb.create_schemas(conn)
        
        # Verify schemas exist
        schemas = conn.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('raw', 'staging')
        """).fetchall()
        
        schema_names = [s[0] for s in schemas]
        assert 'raw' in schema_names
        assert 'staging' in schema_names
        conn.close()
    
    def test_load_elo_data(self, temp_db, sample_elo_data, monkeypatch):
        """Test loading ELO data"""
        conn = duckdb.connect(temp_db)
        monkeypatch.setattr(load_duckdb, 'RAW_DATA_DIR', sample_elo_data.parent)
        
        # Create schema first
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        
        # Load data
        load_duckdb.load_elo_data(conn)
        
        # Verify data loaded
        count = conn.execute("SELECT COUNT(*) FROM raw.elo").fetchone()[0]
        assert count == 2
        
        # Verify data content
        teams = conn.execute("SELECT DISTINCT team1 FROM raw.elo ORDER BY team1").fetchall()
        assert len(teams) == 2
        assert teams[0][0] == 'BOS'
        assert teams[1][0] == 'LAL'
        
        conn.close()
    
    def test_load_nbaallelo_data(self, temp_db, sample_nbaallelo_data, monkeypatch):
        """Test loading NBA all ELO data"""
        conn = duckdb.connect(temp_db)
        monkeypatch.setattr(load_duckdb, 'RAW_DATA_DIR', sample_nbaallelo_data.parent)
        
        # Create schema first
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        
        # Load data
        load_duckdb.load_nbaallelo_data(conn)
        
        # Verify data loaded
        count = conn.execute("SELECT COUNT(*) FROM raw.nbaallelo").fetchone()[0]
        assert count == 4
        
        # Verify unique teams
        teams = conn.execute("SELECT COUNT(DISTINCT team) FROM raw.nbaallelo").fetchone()[0]
        assert teams == 4
        
        conn.close()
    
    def test_verify_data_quality(self, temp_db, sample_elo_data, monkeypatch):
        """Test data quality verification"""
        conn = duckdb.connect(temp_db)
        monkeypatch.setattr(load_duckdb, 'RAW_DATA_DIR', sample_elo_data.parent)
        
        # Setup data
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        load_duckdb.load_elo_data(conn)
        
        # Run verification (should not raise exceptions)
        load_duckdb.verify_data_quality(conn)
        
        conn.close()
    
    @pytest.mark.integration
    def test_main_integration(self, temp_db, sample_elo_data, sample_nbaallelo_data, monkeypatch):
        """Test full main execution"""
        monkeypatch.setattr(load_duckdb, 'DUCKDB_PATH', temp_db)
        monkeypatch.setattr(load_duckdb, 'RAW_DATA_DIR', sample_elo_data.parent)
        monkeypatch.setattr(load_duckdb, 'WAREHOUSE', 'DUCKDB')
        
        # Create empty team aliases file
        aliases_path = sample_elo_data.parent.parent / 'dbt' / 'seeds' / 'team_aliases.csv'
        aliases_path.parent.mkdir(parents=True, exist_ok=True)
        aliases_path.write_text('team_code,team_name,current_team_code\nLAL,Lakers,LAL\n')
        
        # Run main
        load_duckdb.main()
        
        # Verify tables exist
        conn = duckdb.connect(temp_db)
        tables = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'raw'
        """).fetchall()
        
        table_names = [t[0] for t in tables]
        assert 'elo' in table_names
        assert 'nbaallelo' in table_names
        
        conn.close()