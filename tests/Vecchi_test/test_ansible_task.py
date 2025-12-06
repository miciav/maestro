import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock, call
from maestro.server.tasks.ansible_task import AnsibleTask


class TestAnsibleTask:
    """Test suite for AnsibleTask."""

    def test_ansible_task_creation(self):
        """Test basic AnsibleTask creation."""
        task = AnsibleTask(
            task_id='test_ansible_task',
            playbook='test.yml',
            inventory='inventory.ini'
        )
        
        assert task.task_id == 'test_ansible_task'
        assert task.playbook == 'test.yml'
        assert task.inventory == 'inventory.ini'
        assert task.private_data_dir == './'
        assert task.verbosity == 1
        assert task.extra_vars == {}
        assert task.become_user is None

    def test_ansible_task_with_all_fields(self):
        """Test AnsibleTask creation with all fields."""
        task = AnsibleTask(
            task_id='test_ansible_task',
            playbook='test.yml',
            inventory='inventory.ini',
            private_data_dir='/tmp/ansible',
            verbosity=2,
            extra_vars={'env': 'prod', 'version': '1.0'},
            become_user='root'
        )
        
        assert task.task_id == 'test_ansible_task'
        assert task.playbook == 'test.yml'
        assert task.inventory == 'inventory.ini'
        assert task.private_data_dir == '/tmp/ansible'
        assert task.verbosity == 2
        assert task.extra_vars == {'env': 'prod', 'version': '1.0'}
        assert task.become_user == 'root'

    def test_get_absolute_paths_with_dag_file(self):
        """Test get_absolute_paths when dag_file_path is provided."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dag_file = Path(temp_dir) / 'test.yml'
            dag_file.touch()
            
            task = AnsibleTask(
                task_id='test_ansible_task',
                playbook='playbook.yml',
                inventory='inventory.ini',
                private_data_dir='./ansible',
                dag_file_path=str(dag_file)
            )
            
            playbook_path, inventory_path, data_dir = task.get_absolute_paths()
            
            assert playbook_path == str(Path(temp_dir) / 'playbook.yml')
            assert inventory_path == str(Path(temp_dir) / 'inventory.ini')
            assert data_dir == str(Path(temp_dir) / 'ansible')

    def test_get_absolute_paths_without_dag_file(self):
        """Test get_absolute_paths when dag_file_path is not provided."""
        task = AnsibleTask(
            task_id='test_ansible_task',
            playbook='playbook.yml',
            inventory='inventory.ini',
            private_data_dir='./ansible'
        )
        
        playbook_path, inventory_path, data_dir = task.get_absolute_paths()
        
        # Should resolve relative to current directory
        assert playbook_path == str(Path('playbook.yml').resolve())
        assert inventory_path == str(Path('inventory.ini').resolve())
        assert data_dir == str(Path('./ansible').resolve())

    def test_get_absolute_paths_with_absolute_paths(self):
        """Test get_absolute_paths when paths are already absolute."""
        with tempfile.TemporaryDirectory() as temp_dir:
            playbook_abs = Path(temp_dir) / 'playbook.yml'
            inventory_abs = Path(temp_dir) / 'inventory.ini'
            data_dir_abs = Path(temp_dir) / 'ansible'
            
            task = AnsibleTask(
                task_id='test_ansible_task',
                playbook=str(playbook_abs),
                inventory=str(inventory_abs),
                private_data_dir=str(data_dir_abs),
                dag_file_path='/some/other/path/dag.yml'
            )
            
            playbook_path, inventory_path, data_dir = task.get_absolute_paths()
            
            # Should use absolute paths as-is
            assert playbook_path == str(playbook_abs)
            assert inventory_path == str(inventory_abs)
            assert data_dir == str(data_dir_abs)

    @patch('maestro.server.tasks.ansible_task.ansible_runner.run')
    @patch('maestro.server.tasks.ansible_task.os.path.exists')
    @patch('maestro.server.tasks.ansible_task.get_console')
    def test_execute_local_success(self, mock_get_console, mock_exists, mock_ansible_run):
        """Test successful execution of ansible task."""
        mock_console = MagicMock()
        mock_get_console.return_value = mock_console
        mock_exists.return_value = True
        
        # Mock successful ansible run
        mock_result = MagicMock()
        mock_result.status = 'successful'
        mock_ansible_run.return_value = mock_result
        
        task = AnsibleTask(
            task_id='test_ansible_task',
            playbook='playbook.yml',
            inventory='inventory.ini'
        )
        
        task.execute_local()
        
        # Verify console output
        mock_console.print.assert_any_call("[AnsibleTask] Executing 'test_ansible_task'")
        mock_console.print.assert_any_call("[AnsibleTask] Task 'test_ansible_task' completed successfully.")
        
        # Verify ansible_runner was called correctly
        mock_ansible_run.assert_called_once()

    @patch('maestro.server.tasks.ansible_task.ansible_runner.run')
    @patch('maestro.server.tasks.ansible_task.os.path.exists')
    @patch('maestro.server.tasks.ansible_task.get_console')
    def test_execute_local_failure(self, mock_get_console, mock_exists, mock_ansible_run):
        """Test failed execution of ansible task."""
        mock_console = MagicMock()
        mock_get_console.return_value = mock_console
        mock_exists.return_value = True
        
        # Mock failed ansible run
        mock_result = MagicMock()
        mock_result.status = 'failed'
        mock_ansible_run.return_value = mock_result
        
        task = AnsibleTask(
            task_id='test_ansible_task',
            playbook='playbook.yml',
            inventory='inventory.ini'
        )
        
        with pytest.raises(Exception) as exc_info:
            task.execute_local()
        
        assert "failed with status: failed" in str(exc_info.value)
        
        # Verify console output
        mock_console.print.assert_any_call("[AnsibleTask] Executing 'test_ansible_task'")
        mock_console.print.assert_any_call(
            "[AnsibleTask] Task 'test_ansible_task' failed with status: failed",
            style="red"
        )

    @patch('maestro.server.tasks.ansible_task.os.path.exists')
    @patch('maestro.server.tasks.ansible_task.get_console')
    def test_execute_local_playbook_not_found(self, mock_get_console, mock_exists):
        """Test execution when playbook file doesn't exist."""
        mock_console = MagicMock()
        mock_get_console.return_value = mock_console
        
        # Mock playbook not existing
        def mock_exists_func(path):
            return 'playbook.yml' not in path
        
        mock_exists.side_effect = mock_exists_func
        
        task = AnsibleTask(
            task_id='test_ansible_task',
            playbook='playbook.yml',
            inventory='inventory.ini'
        )
        
        with pytest.raises(FileNotFoundError) as exc_info:
            task.execute_local()
        
        assert "Playbook not found" in str(exc_info.value)

    @patch('maestro.server.tasks.ansible_task.os.path.exists')
    @patch('maestro.server.tasks.ansible_task.get_console')
    def test_execute_local_inventory_not_found(self, mock_get_console, mock_exists):
        """Test execution when inventory file doesn't exist."""
        mock_console = MagicMock()
        mock_get_console.return_value = mock_console
        
        # Mock inventory not existing
        def mock_exists_func(path):
            return 'inventory.ini' not in path
        
        mock_exists.side_effect = mock_exists_func
        
        task = AnsibleTask(
            task_id='test_ansible_task',
            playbook='playbook.yml',
            inventory='inventory.ini'
        )
        
        with pytest.raises(FileNotFoundError) as exc_info:
            task.execute_local()
        
        assert "Inventory not found" in str(exc_info.value)

    @patch('maestro.server.tasks.ansible_task.ansible_runner.run')
    @patch('maestro.server.tasks.ansible_task.os.path.exists')
    @patch('maestro.server.tasks.ansible_task.get_console')
    def test_execute_local_with_extra_vars(self, mock_get_console, mock_exists, mock_ansible_run):
        """Test execution with extra variables."""
        mock_console = MagicMock()
        mock_get_console.return_value = mock_console
        mock_exists.return_value = True
        
        # Mock successful ansible run
        mock_result = MagicMock()
        mock_result.status = 'successful'
        mock_ansible_run.return_value = mock_result
        
        task = AnsibleTask(
            task_id='test_ansible_task',
            playbook='playbook.yml',
            inventory='inventory.ini',
            extra_vars={'env': 'test', 'debug': True},
            verbosity=2
        )
        
        task.execute_local()
        
        # Verify ansible_runner was called with correct parameters
        mock_ansible_run.assert_called_once()
        call_args = mock_ansible_run.call_args
        assert call_args[1]['verbosity'] == 2
        assert call_args[1]['quiet'] is False

    @patch('maestro.server.tasks.ansible_task.ansible_runner.run')
    @patch('maestro.server.tasks.ansible_task.os.path.exists')
    @patch('maestro.server.tasks.ansible_task.get_console')
    def test_execute_local_with_become_user(self, mock_get_console, mock_exists, mock_ansible_run):
        """Test execution with become_user option."""
        mock_console = MagicMock()
        mock_get_console.return_value = mock_console
        mock_exists.return_value = True
        
        # Mock successful ansible run
        mock_result = MagicMock()
        mock_result.status = 'successful'
        mock_ansible_run.return_value = mock_result
        
        task = AnsibleTask(
            task_id='test_ansible_task',
            playbook='playbook.yml',
            inventory='inventory.ini',
            become_user='root'
        )
        
        task.execute_local()
        
        # Verify ansible_runner was called
        mock_ansible_run.assert_called_once()

    @patch('maestro.server.tasks.ansible_task.ansible_runner.run')
    @patch('maestro.server.tasks.ansible_task.os.path.exists')
    @patch('maestro.server.tasks.ansible_task.get_console')
    def test_execute_local_console_output(self, mock_get_console, mock_exists, mock_ansible_run):
        """Test that console output shows correct paths."""
        mock_console = MagicMock()
        mock_get_console.return_value = mock_console
        mock_exists.return_value = True
        
        # Mock successful ansible run
        mock_result = MagicMock()
        mock_result.status = 'successful'
        mock_ansible_run.return_value = mock_result
        
        task = AnsibleTask(
            task_id='test_ansible_task',
            playbook='playbook.yml',
            inventory='inventory.ini'
        )
        
        task.execute_local()
        
        # Verify console output includes path information
        assert mock_console.print.call_count >= 4
        mock_console.print.assert_any_call("[AnsibleTask] Executing 'test_ansible_task'")
        
        # Check that playbook and inventory paths are printed
        calls = mock_console.print.call_args_list
        playbook_call = any("[AnsibleTask] Using playbook:" in str(call) for call in calls)
        inventory_call = any("[AnsibleTask] Using inventory:" in str(call) for call in calls)
        
        assert playbook_call, "Playbook path should be printed"
        assert inventory_call, "Inventory path should be printed"

    def test_ansible_task_field_defaults(self):
        """Test that default field values are set correctly."""
        task = AnsibleTask(
            task_id='test_ansible_task',
            playbook='playbook.yml',
            inventory='inventory.ini'
        )
        
        # Test default values
        assert task.private_data_dir == './'
        assert task.verbosity == 1
        assert task.extra_vars == {}
        assert task.become_user is None

    def test_ansible_task_with_complex_paths(self):
        """Test ansible task with complex relative and absolute paths."""
        with tempfile.TemporaryDirectory() as temp_dir:
            dag_file = Path(temp_dir) / 'dag.yml'
            dag_file.touch()
            
            # Create subdirectories
            playbook_dir = Path(temp_dir) / 'playbooks'
            playbook_dir.mkdir()
            inventory_dir = Path(temp_dir) / 'inventories'
            inventory_dir.mkdir()
            
            task = AnsibleTask(
                task_id='test_ansible_task',
                playbook='playbooks/site.yml',
                inventory='inventories/prod.ini',
                private_data_dir='./data',
                dag_file_path=str(dag_file)
            )
            
            playbook_path, inventory_path, data_dir = task.get_absolute_paths()
            
            assert playbook_path == str(Path(temp_dir) / 'playbooks' / 'site.yml')
            assert inventory_path == str(Path(temp_dir) / 'inventories' / 'prod.ini')
            assert data_dir == str(Path(temp_dir) / 'data')
