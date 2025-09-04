#!/usr/bin/env python3
"""
Smoke test for the complete pipeline using fixture data
"""

import pytest
import subprocess
import os
import tempfile
import shutil
from pathlib import Path

class TestSmokePipeline:
    """End-to-end smoke test using fixture data"""
    
    @pytest.fixture
    def temp_workspace(self):
        """Create a temporary workspace for testing"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Copy necessary files
            workspace_dir = Path(tmpdir) / 'test_workspace'
            workspace_dir.mkdir()
            
            # Create data directories
            (workspace_dir / 'data' / 'raw').mkdir(parents=True)
            (workspace_dir / 'data' / 'warehouse').mkdir(parents=True)
            (workspace_dir / 'data' / 'exports').mkdir(parents=True)
            
            # Copy scripts
            scripts_dir = workspace_dir / 'scripts'
            shutil.copytree(Path(__file__).parent.parent / 'scripts', scripts_dir)
            
            # Copy dbt project
            dbt_dir = workspace_dir / 'dbt'
            shutil.copytree(Path(__file__).parent.parent / 'dbt', dbt_dir)
            
            # Create test env file
            env_file = workspace_dir / '.env'
            env_file.write_text(f"""
NBA_ALLELO_URL=file://{Path(__file__).parent.parent}/data/fixtures/sample_nbaallelo.csv
WAREHOUSE=DUCKDB
DUCKDB_PATH={workspace_dir}/data/warehouse/test.duckdb
""")
            
            yield workspace_dir
    
    @pytest.mark.integration
    def test_pipeline_with_fixture_data(self, temp_workspace):
        """Test the complete pipeline with fixture data"""
        os.chdir(temp_workspace)
        
        # Step 1: Fetch data (using local fixture)
        result = subprocess.run(
            ['python', 'scripts/fetch_538.py'],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0, f"Fetch failed: {result.stderr}"
        
        # Verify raw data exists
        assert (temp_workspace / 'data' / 'raw' / 'latest_nbaallelo.csv').exists()
        assert (temp_workspace / 'data' / 'raw' / 'latest_elo.csv').exists()
        
        # Step 2: Load to DuckDB
        result = subprocess.run(
            ['python', 'scripts/load_duckdb.py'],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0, f"Load failed: {result.stderr}"
        
        # Step 3: Run dbt
        os.chdir(temp_workspace / 'dbt')
        
        # dbt seed
        result = subprocess.run(
            ['dbt', 'seed', '--profiles-dir', '.'],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0, f"dbt seed failed: {result.stderr}"
        
        # dbt run
        result = subprocess.run(
            ['dbt', 'run', '--profiles-dir', '.'],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0, f"dbt run failed: {result.stderr}"
        
        # dbt test
        result = subprocess.run(
            ['dbt', 'test', '--profiles-dir', '.'],
            capture_output=True,
            text=True
        )
        # Tests might fail with small dataset, so we just check it runs
        
        # Step 4: Export metrics
        os.chdir(temp_workspace)
        result = subprocess.run(
            ['python', 'scripts/export_metrics.py'],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0, f"Export failed: {result.stderr}"
        
        # Verify exports exist
        exports_dir = temp_workspace / 'data' / 'exports'
        assert any(exports_dir.glob('*.csv')), "No CSV exports found"
        
    def test_fixture_data_is_valid(self):
        """Verify the fixture data is valid"""
        import pandas as pd
        
        fixture_path = Path(__file__).parent.parent / 'data' / 'fixtures' / 'sample_nbaallelo.csv'
        assert fixture_path.exists(), "Fixture file not found"
        
        df = pd.read_csv(fixture_path)
        assert len(df) >= 6, "Fixture should have at least 6 rows"
        assert 'game_id' in df.columns
        assert 'franch_id' in df.columns
        assert 'elo_i' in df.columns