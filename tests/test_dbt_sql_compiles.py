#!/usr/bin/env python3
"""
Tests to verify dbt SQL models compile correctly
"""

import pytest
import subprocess
import os
from pathlib import Path

class TestDbtCompilation:
    """Test cases for dbt model compilation"""
    
    @pytest.fixture
    def dbt_project_dir(self):
        """Get dbt project directory"""
        return Path(__file__).parent.parent / 'dbt'
    
    def test_dbt_project_exists(self, dbt_project_dir):
        """Test that dbt project files exist"""
        assert dbt_project_dir.exists()
        assert (dbt_project_dir / 'dbt_project.yml').exists()
        assert (dbt_project_dir / 'models').exists()
    
    def test_dbt_parse(self, dbt_project_dir):
        """Test that dbt can parse the project"""
        # Change to dbt directory
        original_dir = os.getcwd()
        os.chdir(dbt_project_dir)
        
        try:
            # Run dbt parse
            result = subprocess.run(
                ['dbt', 'parse', '--profiles-dir', '.'],
                capture_output=True,
                text=True
            )
            
            # Check for success
            assert result.returncode == 0, f"dbt parse failed: {result.stderr}"
            
        finally:
            # Change back to original directory
            os.chdir(original_dir)
    
    def test_model_sql_files_exist(self, dbt_project_dir):
        """Test that all expected model files exist"""
        expected_models = [
            'models/staging/stg_games.sql',
            'models/staging/stg_teams.sql',
            'models/marts/dim_teams.sql',
            'models/marts/fct_games.sql'
        ]
        
        for model_path in expected_models:
            full_path = dbt_project_dir / model_path
            assert full_path.exists(), f"Model file missing: {model_path}"
    
    def test_schema_yml_files_exist(self, dbt_project_dir):
        """Test that schema.yml files exist"""
        expected_schemas = [
            'models/staging/schema.yml',
            'models/tests/schema.yml',
            'models/marts/metrics.yml'
        ]
        
        for schema_path in expected_schemas:
            full_path = dbt_project_dir / schema_path
            assert full_path.exists(), f"Schema file missing: {schema_path}"
    
    def test_seed_files_exist(self, dbt_project_dir):
        """Test that seed files exist"""
        seeds_dir = dbt_project_dir / 'seeds'
        assert seeds_dir.exists()
        
        team_aliases = seeds_dir / 'team_aliases.csv'
        assert team_aliases.exists()
        
        # Verify seed has content
        content = team_aliases.read_text()
        assert 'team_code,team_name,current_team_code' in content
        assert len(content.splitlines()) > 1  # Header + at least one row
    
    def test_macro_files_exist(self, dbt_project_dir):
        """Test that macro files exist"""
        macros_dir = dbt_project_dir / 'macros'
        assert macros_dir.exists()
        
        date_helpers = macros_dir / 'date_helpers.sql'
        assert date_helpers.exists()
        
        # Verify macro has content
        content = date_helpers.read_text()
        assert 'macro extract_season_from_date' in content
    
    @pytest.mark.slow
    def test_dbt_compile(self, dbt_project_dir):
        """Test that all models compile successfully"""
        # This test requires a full dbt setup and may be slow
        original_dir = os.getcwd()
        os.chdir(dbt_project_dir)
        
        try:
            # Create minimal profiles.yml if not exists
            profiles_path = dbt_project_dir / 'profiles.yml'
            if not profiles_path.exists():
                profiles_path.write_text("""
sports_analytics:
  outputs:
    dev:
      type: duckdb
      path: ":memory:"
      threads: 1
  target: dev
""")
            
            # Run dbt compile
            result = subprocess.run(
                ['dbt', 'compile', '--profiles-dir', '.'],
                capture_output=True,
                text=True
            )
            
            # Check for success
            if result.returncode != 0:
                print(f"STDOUT: {result.stdout}")
                print(f"STDERR: {result.stderr}")
            
            assert result.returncode == 0, f"dbt compile failed: {result.stderr}"
            
        finally:
            os.chdir(original_dir)