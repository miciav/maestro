import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock, call
import shutil

# Import with proper error handling
try:
    from maestro.tasks.extended_terraform_task import (
        ExtendedTerraformTask,
        check_command_exists,
        print_status,
        print_success,
        print_warning,
        print_error,
        print_header
    )
except ImportError:
    pytest.skip("ExtendedTerraformTask not available", allow_module_level=True)


class TestExtendedTerraformTask:

    def setup_method(self):
        """Setup method to ensure clean state for each test."""
        self.original_cwd = os.getcwd()

    def teardown_method(self):
        """Cleanup method to restore original state."""
        os.chdir(self.original_cwd)

    @pytest.mark.skipif(
        not shutil.which("terraform") and not shutil.which("tofu"),
        reason="Neither terraform nor tofu available in PATH"
    )
    @patch('maestro.tasks.extended_terraform_task.subprocess.run')
    @patch('maestro.tasks.terraform_task.shutil.which')
    def test_full_workflow_execution(self, mock_which, mock_subprocess):
        """Test executing the full Terraform workflow."""
        # Mock terraform being available
        mock_which.return_value = '/usr/bin/terraform'

        task = ExtendedTerraformTask(
            task_id='test_task',
            working_dir='.',
            workflow_mode=True
        )

        mock_subprocess.return_value = MagicMock(returncode=0, stdout='', stderr='')

        result = task.execute_local()

        # Verify the workflow executed successfully
        assert result is not None
        # Adjusted number based on: init, validate, fmt (check), fmt, plan, show, apply
        assert mock_subprocess.call_count == 7

    @pytest.mark.skipif(
        not shutil.which("terraform") and not shutil.which("tofu"),
        reason="Neither terraform nor tofu available in PATH"
    )
    @patch('maestro.tasks.extended_terraform_task.subprocess.run')
    @patch('maestro.tasks.terraform_task.shutil.which')
    def test_single_command_execution(self, mock_which, mock_subprocess):
        """Test executing a single Terraform command."""
        # Mock terraform being available
        mock_which.return_value = '/usr/bin/terraform'

        task = ExtendedTerraformTask(
            task_id='test_task',
            working_dir='.',
            command='init'
        )
        mock_subprocess.return_value = MagicMock(returncode=0, stdout='', stderr='')

        task.execute_local()

        mock_subprocess.assert_called_once()

    def test_missing_workflow_and_command(self):
        """Test handling of missing workflow_mode and command."""
        with patch('maestro.tasks.terraform_task.shutil.which', return_value='/usr/bin/terraform'):
            task = ExtendedTerraformTask(
                task_id='test_task',
                working_dir='.'
            )

            with pytest.raises(ValueError) as exc_info:
                task.execute_local()
            assert "either 'workflow_mode' must be true or a 'command' must be provided" in str(exc_info.value)

    @pytest.mark.skipif(
        not shutil.which("terraform") and not shutil.which("tofu"),
        reason="Neither terraform nor tofu available in PATH"
    )
    @patch('maestro.tasks.extended_terraform_task.subprocess.run')
    @patch('maestro.tasks.extended_terraform_task.os.path.exists')
    @patch('maestro.tasks.terraform_task.shutil.which')
    def test_full_workflow_with_existing_terraform_dir(self, mock_which, mock_exists, mock_subprocess):
        """Test full workflow when .terraform directory exists."""
        # Mock terraform being available
        mock_which.return_value = '/usr/bin/terraform'

        task = ExtendedTerraformTask(
            task_id='test_task',
            working_dir='.',
            workflow_mode=True
        )
        mock_subprocess.return_value = MagicMock(returncode=0, stdout='', stderr='')
        mock_exists.return_value = True

        with patch('maestro.tasks.extended_terraform_task.Path.is_dir', return_value=True):
            task.execute_local()

        # Should call init with -upgrade flag
        assert mock_subprocess.call_count == 7

    @pytest.mark.skipif(
        not shutil.which("terraform") and not shutil.which("tofu"),
        reason="Neither terraform nor tofu available in PATH"
    )
    @patch('maestro.tasks.extended_terraform_task.subprocess.run')
    @patch('maestro.tasks.extended_terraform_task.os.environ.get')
    @patch('maestro.tasks.terraform_task.shutil.which')
    def test_full_workflow_with_skip_plan(self, mock_which, mock_environ, mock_subprocess):
        """Test full workflow with SKIP_PLAN environment variable."""
        # Mock terraform being available
        mock_which.return_value = '/usr/bin/terraform'

        task = ExtendedTerraformTask(
            task_id='test_task',
            working_dir='.',
            workflow_mode=True
        )
        mock_subprocess.return_value = MagicMock(returncode=0, stdout='', stderr='')
        mock_environ.return_value = "true"

        task.execute_local()

        # Should call fewer commands when SKIP_PLAN is true
        assert mock_subprocess.call_count == 4  # init, validate, fmt (check), fmt

    @pytest.mark.skipif(
        not shutil.which("terraform") and not shutil.which("tofu"),
        reason="Neither terraform nor tofu available in PATH"
    )
    @patch('maestro.tasks.extended_terraform_task.subprocess.run')
    @patch('maestro.tasks.extended_terraform_task.os.chdir')
    @patch('maestro.tasks.terraform_task.shutil.which')
    def test_full_workflow_directory_change(self, mock_which, mock_chdir, mock_subprocess):
        """Test that the workflow changes directories correctly."""
        # Mock terraform being available
        mock_which.return_value = '/usr/bin/terraform'

        task = ExtendedTerraformTask(
            task_id='test_task',
            working_dir='/tmp/test',
            workflow_mode=True
        )
        mock_subprocess.return_value = MagicMock(returncode=0, stdout='', stderr='')

        task.execute_local()

        # Should change to the working directory and back
        assert mock_chdir.call_count == 2

    @patch('maestro.tasks.extended_terraform_task.subprocess.run')
    @patch('maestro.tasks.terraform_task.shutil.which')
    def test_full_workflow_exception_handling(self, mock_which, mock_subprocess):
        """Test that exceptions in workflow are handled properly."""
        # Mock terraform being available
        mock_which.return_value = '/usr/bin/terraform'

        task = ExtendedTerraformTask(
            task_id='test_task',
            working_dir='.',
            workflow_mode=True
        )
        mock_subprocess.side_effect = Exception("Test error")

        with pytest.raises(Exception) as exc_info:
            task.execute_local()
        assert "Test error" in str(exc_info.value)

    def test_workflow_mode_field(self):
        """Test that workflow_mode field is properly set."""
        with patch('maestro.tasks.terraform_task.shutil.which', return_value='/usr/bin/terraform'):
            task = ExtendedTerraformTask(
                task_id='test_task',
                working_dir='.',
                workflow_mode=True
            )
            assert task.workflow_mode is True

            task_no_workflow = ExtendedTerraformTask(
                task_id='test_task2',
                working_dir='.'
            )
            assert task_no_workflow.workflow_mode is False

    @pytest.mark.skipif(
        not shutil.which("terraform") and not shutil.which("tofu"),
        reason="Neither terraform nor tofu available in PATH"
    )
    @patch('maestro.tasks.extended_terraform_task.subprocess.run')
    @patch('maestro.tasks.terraform_task.shutil.which')
    def test_single_command_with_workspace(self, mock_which, mock_subprocess):
        """Test single command execution with workspace."""
        # Mock terraform being available
        mock_which.return_value = '/usr/bin/terraform'

        task = ExtendedTerraformTask(
            task_id='test_task',
            working_dir='.',
            command='plan',
            workspace='test-workspace'
        )
        mock_subprocess.return_value = MagicMock(returncode=0, stdout='', stderr='')

        task.execute_local()

        # Should call workspace select first, then the command
        assert mock_subprocess.call_count == 2

    @pytest.mark.skipif(
        not shutil.which("terraform") and not shutil.which("tofu"),
        reason="Neither terraform nor tofu available in PATH"
    )
    @patch('maestro.tasks.extended_terraform_task.subprocess.run')
    @patch('maestro.tasks.terraform_task.shutil.which')
    def test_single_command_with_variables(self, mock_which, mock_subprocess):
        """Test single command execution with variables."""
        # Mock terraform being available
        mock_which.return_value = '/usr/bin/terraform'

        task = ExtendedTerraformTask(
            task_id='test_task',
            working_dir='.',
            command='plan',
            vars={'env': 'test', 'region': 'us-east-1'}
        )
        mock_subprocess.return_value = MagicMock(returncode=0, stdout='', stderr='')

        task.execute_local()

        mock_subprocess.assert_called_once()

    def test_print_functions(self):
        """Test the print utility functions."""
        with patch('maestro.tasks.extended_terraform_task.logging.getLogger') as mock_logger:
            mock_log = MagicMock()
            mock_logger.return_value = mock_log

            print_status("Test status")
            print_success("Test success")
            print_warning("Test warning")
            print_error("Test error")
            print_header("Test header")

            # Verify logging calls were made
            assert mock_log.info.call_count == 3  # status, success, header
            assert mock_log.warning.call_count == 1  # warning
            assert mock_log.error.call_count == 1  # error

    def test_check_command_exists(self):
        """Test the check_command_exists function."""
        with patch('maestro.tasks.extended_terraform_task.shutil.which') as mock_which:
            mock_which.return_value = '/usr/bin/terraform'
            assert check_command_exists('terraform') is True

            mock_which.return_value = None
            assert check_command_exists('nonexistent') is False

    @pytest.mark.skipif(
        not shutil.which("terraform") and not shutil.which("tofu"),
        reason="Neither terraform nor tofu available in PATH"
    )
    @patch('maestro.tasks.extended_terraform_task.subprocess.run')
    @patch('maestro.tasks.terraform_task.shutil.which')
    def test_workflow_returns_value(self, mock_which, mock_subprocess):
        """Test that the workflow returns the expected value."""
        # Mock terraform being available
        mock_which.return_value = '/usr/bin/terraform'

        task = ExtendedTerraformTask(
            task_id='test_task',
            working_dir='.',
            workflow_mode=True
        )
        mock_subprocess.return_value = MagicMock(returncode=0, stdout='', stderr='')

        # The _run_full_workflow method should return 1
        result = task._run_full_workflow()
        assert result == 1

    @pytest.mark.skipif(
        not shutil.which("terraform") and not shutil.which("tofu"),
        reason="Neither terraform nor tofu available in PATH"
    )
    @patch('maestro.tasks.extended_terraform_task.subprocess.run')
    @patch('maestro.tasks.terraform_task.shutil.which')
    @patch('maestro.tasks.extended_terraform_task.os.makedirs')
    def test_workflow_with_relative_path(self, mock_makedirs, mock_which, mock_subprocess):
        """Test workflow execution with relative working directory."""
        # Mock terraform being available
        mock_which.return_value = '/usr/bin/terraform'

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create terraform directory for the test
            terraform_dir = Path(temp_dir) / 'terraform'
            terraform_dir.mkdir()

            task = ExtendedTerraformTask(
                task_id='test_task',
                working_dir='./terraform',
                workflow_mode=True,
                dag_file_path=str(Path(temp_dir) / 'test.yaml')
            )
            mock_subprocess.return_value = MagicMock(returncode=0, stdout='', stderr='')

            task.execute_local()

            assert mock_subprocess.call_count == 7


# Alternative approach: Create a separate test configuration for environments without terraform
class TestExtendedTerraformTaskMocked:
    """Tests that run entirely with mocks, regardless of terraform availability."""

    @patch('maestro.tasks.terraform_task.shutil.which')
    @patch('maestro.tasks.extended_terraform_task.subprocess.run')
    def test_mock_full_workflow_execution(self, mock_subprocess, mock_which):
        """Test executing the full Terraform workflow with complete mocking."""
        # Mock terraform being available
        mock_which.return_value = '/usr/bin/terraform'
        mock_subprocess.return_value = MagicMock(returncode=0, stdout='', stderr='')

        task = ExtendedTerraformTask(
            task_id='test_task',
            working_dir='.',
            workflow_mode=True
        )

        task.execute_local()

        # Verify the workflow executed successfully
        assert mock_subprocess.call_count == 7

    @patch('maestro.tasks.terraform_task.shutil.which')
    @patch('maestro.tasks.extended_terraform_task.subprocess.run')
    def test_mock_terraform_not_available(self, mock_subprocess, mock_which):
        """Test behavior when terraform is not available."""
        # Mock terraform NOT being available
        mock_which.return_value = None

        task = ExtendedTerraformTask(
            task_id='test_task',
            working_dir='.',
            workflow_mode=True
        )

        with pytest.raises(FileNotFoundError) as exc_info:
            task.execute_local()

        assert "Neither 'tofu' nor 'terraform' command found" in str(exc_info.value)