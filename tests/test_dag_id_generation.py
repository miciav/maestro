"""
Test cases for DAG ID generation and validation functionality.
"""

import pytest
import re
from unittest.mock import Mock, patch, MagicMock
from maestro.core.status_manager import StatusManager, DOCKER_ADJECTIVES, DOCKER_NOUNS
import tempfile
import os
import sqlite3


class TestDockerLikeNameGeneration:
    """Test Docker-like name generation functionality."""
    
    def setup_method(self):
        """Set up a temporary database for each test."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.temp_db.close()
        self.sm = StatusManager(self.temp_db.name)
    
    def teardown_method(self):
        """Clean up the temporary database."""
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)
    
    def test_generate_docker_like_name_format(self):
        """Test that generated names follow the adjective_noun format."""
        with self.sm as sm:
            name = sm.generate_unique_dag_id()
            assert "_" in name
            parts = name.split("_")
            # Should have at least adjective and noun (might have suffix if collision)
            assert len(parts) >= 2
            adjective = parts[0]
            noun = parts[1]
            assert adjective in DOCKER_ADJECTIVES
            assert noun in DOCKER_NOUNS
    
    def test_generate_docker_like_name_randomness(self):
        """Test that generated names are different (mostly)."""
        with self.sm as sm:
            names = [sm.generate_unique_dag_id() for _ in range(10)]
            # Most names should be different (allowing for some duplicates due to randomness)
            unique_names = set(names)
            assert len(unique_names) >= 7  # Allow for some duplicates
    
    def test_generate_docker_like_name_valid_characters(self):
        """Test that generated names only contain valid characters."""
        with self.sm as sm:
            name = sm.generate_unique_dag_id()
            # Should only contain alphanumeric, underscores, and hyphens
            assert re.match(r'^[a-zA-Z0-9_-]+$', name)


class TestDAGIDValidation:
    """Test DAG ID validation functionality."""
    
    def setup_method(self):
        """Set up a temporary database for each test."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.temp_db.close()
        self.sm = StatusManager(self.temp_db.name)
    
    def teardown_method(self):
        """Clean up the temporary database."""
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)
    
    def test_validate_dag_id_valid_cases(self):
        """Test validation with valid DAG IDs."""
        valid_ids = [
            "simple_dag",
            "dag-with-hyphens",
            "dag_with_123_numbers",
            "CamelCaseDAG",
            "simple",
            "a1b2c3",
            "test-dag_123"
        ]
        
        with self.sm as sm:
            for dag_id in valid_ids:
                assert sm.validate_dag_id(dag_id), f"'{dag_id}' should be valid"
    
    def test_validate_dag_id_invalid_cases(self):
        """Test validation with invalid DAG IDs."""
        invalid_ids = [
            "",  # Empty string
            "dag with spaces",  # Contains spaces
            "dag@with@symbols",  # Contains special characters
            "dag.with.dots",  # Contains dots
            "dag/with/slashes",  # Contains slashes
            "dag#with#hash",  # Contains hash
            "dag%with%percent",  # Contains percent
            None,  # None value
        ]
        
        with self.sm as sm:
            for dag_id in invalid_ids:
                assert not sm.validate_dag_id(dag_id), f"'{dag_id}' should be invalid"


class TestDAGIDUniquenessCheck:
    """Test DAG ID uniqueness checking functionality."""
    
    def setup_method(self):
        """Set up a temporary database for each test."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.temp_db.close()
        self.sm = StatusManager(self.temp_db.name)
    
    def teardown_method(self):
        """Clean up the temporary database."""
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)
    
    def test_check_dag_id_uniqueness_unique(self):
        """Test uniqueness check when DAG ID is unique."""
        with self.sm as sm:
            # Add some existing DAGs
            sm._create_dag_if_not_exists('existing_dag_1')
            sm._create_dag_if_not_exists('existing_dag_2')
            
            # Test with a new DAG ID
            assert sm.check_dag_id_uniqueness('new_dag_id') is True
    
    def test_check_dag_id_uniqueness_duplicate(self):
        """Test uniqueness check when DAG ID already exists."""
        with self.sm as sm:
            # Add some existing DAGs
            sm._create_dag_if_not_exists('existing_dag_1')
            sm._create_dag_if_not_exists('existing_dag_2')
            
            # Test with an existing DAG ID
            assert sm.check_dag_id_uniqueness('existing_dag_1') is False
    
    def test_check_dag_id_uniqueness_error_handling(self):
        """Test uniqueness check error handling."""
        # Create a temporary file for the database that we'll remove
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        temp_db.close()
        sm = StatusManager(temp_db.name)
        
        # Remove the database file to simulate an error during operation
        os.unlink(temp_db.name)
        
        # Now trying to connect should fail
        with pytest.raises(sqlite3.OperationalError):
            with sm as s:
                s.check_dag_id_uniqueness('test_dag')
    
    def test_check_dag_id_uniqueness_empty_database(self):
        """Test uniqueness check with empty database."""
        with self.sm as sm:
            # Any DAG ID should be unique in empty database
            assert sm.check_dag_id_uniqueness('any_dag_id') is True


class TestUniqueDAGIDGeneration:
    """Test unique DAG ID generation functionality."""
    
    def setup_method(self):
        """Set up a temporary database for each test."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.temp_db.close()
        self.sm = StatusManager(self.temp_db.name)
    
    def teardown_method(self):
        """Clean up the temporary database."""
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)
    
    def test_generate_unique_dag_id_first_attempt(self):
        """Test successful generation on first attempt."""
        with self.sm as sm:
            result = sm.generate_unique_dag_id()
            
            # Should be a valid format
            assert "_" in result
            parts = result.split("_")
            assert len(parts) >= 2
            assert parts[0] in DOCKER_ADJECTIVES
            assert parts[1] in DOCKER_NOUNS
    
    def test_generate_unique_dag_id_retry_logic(self):
        """Test retry logic when first attempts fail."""
        with self.sm as sm:
            # Pre-populate database with many DAGs to increase chance of collision
            for adj in DOCKER_ADJECTIVES[:10]:  # Use first 10 adjectives
                for noun in DOCKER_NOUNS[:10]:  # Use first 10 nouns
                    sm._create_dag_if_not_exists(f"{adj}_{noun}")
            
            # Generate a new unique ID
            result = sm.generate_unique_dag_id()
            
            # Should still get a unique ID
            assert sm.check_dag_id_uniqueness(result)
            assert "_" in result
    
    @patch('maestro.core.status_manager.random.choices')
    def test_generate_unique_dag_id_fallback_suffix(self, mock_choices):
        """Test fallback to suffix when max attempts reached."""
        mock_choices.return_value = ['a', 'b', 'c', 'd', 'e', 'f']
        
        with self.sm as sm:
            # Pre-populate database with ALL possible combinations
            # This is impractical in reality but simulates the worst case
            # Instead, we'll mock the check_dag_id_uniqueness method
            original_check = sm.check_dag_id_uniqueness
            call_count = 0
            
            def mock_check(dag_id):
                nonlocal call_count
                call_count += 1
                # Return False for first 100 calls, then True
                if call_count <= 100:
                    return False
                return original_check(dag_id)
            
            sm.check_dag_id_uniqueness = mock_check
            
            result = sm.generate_unique_dag_id()
            
            # Should have a suffix
            assert result.endswith("_abcdef")
            assert call_count >= 100  # Should have tried at least 100 times
    
    def test_generate_unique_dag_id_valid_format(self):
        """Test that generated unique DAG ID has valid format."""
        with self.sm as sm:
            result = sm.generate_unique_dag_id()
            
            assert sm.validate_dag_id(result)


class TestDockerListContents:
    """Test that Docker name lists contain expected content."""
    
    def test_docker_adjectives_not_empty(self):
        """Test that adjectives list is not empty."""
        assert len(DOCKER_ADJECTIVES) > 0
        assert all(isinstance(adj, str) for adj in DOCKER_ADJECTIVES)
    
    def test_docker_nouns_not_empty(self):
        """Test that nouns list is not empty."""
        assert len(DOCKER_NOUNS) > 0
        assert all(isinstance(noun, str) for noun in DOCKER_NOUNS)
    
    def test_docker_names_valid_format(self):
        """Test that all names in lists are valid for DAG IDs."""
        # Test adjectives
        for adj in DOCKER_ADJECTIVES:
            assert re.match(r'^[a-zA-Z0-9_-]+$', adj), f"Invalid adjective: {adj}"
        
        # Test nouns
        for noun in DOCKER_NOUNS:
            assert re.match(r'^[a-zA-Z0-9_-]+$', noun), f"Invalid noun: {noun}"
