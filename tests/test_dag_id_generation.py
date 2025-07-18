"""
Test cases for DAG ID generation and validation functionality.
"""

import pytest
import re
from unittest.mock import Mock, patch, MagicMock
from maestro.server.app import (
    generate_docker_like_name,
    validate_dag_id,
    check_dag_id_uniqueness,
    generate_unique_dag_id,
    DOCKER_ADJECTIVES,
    DOCKER_NOUNS
)


class TestDockerLikeNameGeneration:
    """Test Docker-like name generation functionality."""
    
    def test_generate_docker_like_name_format(self):
        """Test that generated names follow the adjective_noun format."""
        name = generate_docker_like_name()
        assert "_" in name
        adjective, noun = name.split("_", 1)
        assert adjective in DOCKER_ADJECTIVES
        assert noun in DOCKER_NOUNS
    
    def test_generate_docker_like_name_randomness(self):
        """Test that generated names are different (mostly)."""
        names = [generate_docker_like_name() for _ in range(10)]
        # Most names should be different (allowing for some duplicates due to randomness)
        unique_names = set(names)
        assert len(unique_names) >= 7  # Allow for some duplicates
    
    def test_generate_docker_like_name_valid_characters(self):
        """Test that generated names only contain valid characters."""
        name = generate_docker_like_name()
        # Should only contain alphanumeric and underscores
        assert re.match(r'^[a-zA-Z0-9_]+$', name)


class TestDAGIDValidation:
    """Test DAG ID validation functionality."""
    
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
        
        for dag_id in valid_ids:
            assert validate_dag_id(dag_id), f"'{dag_id}' should be valid"
    
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
        
        for dag_id in invalid_ids:
            assert not validate_dag_id(dag_id), f"'{dag_id}' should be invalid"


class TestDAGIDUniquenessCheck:
    """Test DAG ID uniqueness checking functionality."""
    
    @patch('maestro.server.app.orchestrator')
    def test_check_dag_id_uniqueness_unique(self, mock_orchestrator):
        """Test uniqueness check when DAG ID is unique."""
        mock_sm = Mock()
        mock_sm.get_all_dags.return_value = [
            {'dag_id': 'existing_dag_1'},
            {'dag_id': 'existing_dag_2'}
        ]
        mock_orchestrator.status_manager.__enter__.return_value = mock_sm
        
        # Test with a new DAG ID
        assert check_dag_id_uniqueness('new_dag_id') is True
    
    @patch('maestro.server.app.orchestrator')
    def test_check_dag_id_uniqueness_duplicate(self, mock_orchestrator):
        """Test uniqueness check when DAG ID already exists."""
        mock_sm = Mock()
        mock_sm.get_all_dags.return_value = [
            {'dag_id': 'existing_dag_1'},
            {'dag_id': 'existing_dag_2'}
        ]
        mock_orchestrator.status_manager.__enter__.return_value = mock_sm
        
        # Test with an existing DAG ID
        assert check_dag_id_uniqueness('existing_dag_1') is False
    
    @patch('maestro.server.app.orchestrator')
    def test_check_dag_id_uniqueness_error_handling(self, mock_orchestrator):
        """Test uniqueness check error handling."""
        mock_orchestrator.status_manager.__enter__.side_effect = Exception("Database error")
        
        # Should return False when there's an error
        assert check_dag_id_uniqueness('test_dag') is False
    
    @patch('maestro.server.app.orchestrator')
    def test_check_dag_id_uniqueness_empty_database(self, mock_orchestrator):
        """Test uniqueness check with empty database."""
        mock_sm = Mock()
        mock_sm.get_all_dags.return_value = []
        mock_orchestrator.status_manager.__enter__.return_value = mock_sm
        
        # Any DAG ID should be unique in empty database
        assert check_dag_id_uniqueness('any_dag_id') is True


class TestUniqueDAGIDGeneration:
    """Test unique DAG ID generation functionality."""
    
    @patch('maestro.server.app.check_dag_id_uniqueness')
    @patch('maestro.server.app.generate_docker_like_name')
    def test_generate_unique_dag_id_first_attempt(self, mock_generate, mock_check):
        """Test successful generation on first attempt."""
        mock_generate.return_value = "amazing_tesla"
        mock_check.return_value = True
        
        result = generate_unique_dag_id()
        
        assert result == "amazing_tesla"
        mock_generate.assert_called_once()
        mock_check.assert_called_once_with("amazing_tesla")
    
    @patch('maestro.server.app.check_dag_id_uniqueness')
    @patch('maestro.server.app.generate_docker_like_name')
    def test_generate_unique_dag_id_retry_logic(self, mock_generate, mock_check):
        """Test retry logic when first attempts fail."""
        mock_generate.side_effect = ["duplicate_name", "another_duplicate", "unique_name"]
        mock_check.side_effect = [False, False, True]
        
        result = generate_unique_dag_id()
        
        assert result == "unique_name"
        assert mock_generate.call_count == 3
        assert mock_check.call_count == 3
    
    @patch('maestro.server.app.check_dag_id_uniqueness')
    @patch('maestro.server.app.generate_docker_like_name')
    @patch('maestro.server.app.random.choices')
    def test_generate_unique_dag_id_fallback_suffix(self, mock_choices, mock_generate, mock_check):
        """Test fallback to suffix when max attempts reached."""
        # Make all attempts fail uniqueness check
        mock_check.return_value = False
        mock_generate.return_value = "always_duplicate"
        mock_choices.return_value = ['a', 'b', 'c', 'd', 'e', 'f']
        
        result = generate_unique_dag_id()
        
        assert result == "always_duplicate_abcdef"
        assert mock_generate.call_count == 101  # 100 attempts + 1 for fallback
        assert mock_check.call_count == 100
        mock_choices.assert_called_once()
    
    @patch('maestro.server.app.check_dag_id_uniqueness')
    @patch('maestro.server.app.generate_docker_like_name')
    def test_generate_unique_dag_id_valid_format(self, mock_generate, mock_check):
        """Test that generated unique DAG ID has valid format."""
        mock_generate.return_value = "amazing_tesla"
        mock_check.return_value = True
        
        result = generate_unique_dag_id()
        
        assert validate_dag_id(result)


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
