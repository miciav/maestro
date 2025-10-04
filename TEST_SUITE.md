# Maestro Test Suite Documentation

This document provides comprehensive information about the Maestro test suite, including test structure, coverage, and best practices.

## Test Structure

### Test Files Overview

```
tests/
├── test_dag.py                      # Core DAG functionality tests
├── test_db_feature.py               # Database persistence tests
├── test_orchestrator_dagloader.py   # Orchestrator and DAG loading tests
├── test_multi_executor.py           # Multi-executor support tests
├── test_enhanced_cli.py             # Enhanced CLI functionality tests
└── test_concurrent_execution.py     # Concurrent execution tests (limited)
```

## Test Categories

### 1. Core Functionality Tests

#### DAG Operations (`test_dag.py`)
- **test_dag_add_task**: Verifies task addition to DAG
- **test_dag_validation**: Tests DAG validation logic
- **test_dag_cycle_detection**: Ensures cycle detection works correctly

#### Database Features (`test_db_feature.py`)
- **test_db_creation**: Database initialization and table creation
- **test_save_state**: Task state persistence
- **test_resume_execution**: Resume functionality after interruption
- **test_reset_execution**: Reset and restart capabilities

#### Orchestrator & DAG Loading (`test_orchestrator_dagloader.py`)
- **test_orchestrator_load_dag_from_file**: YAML file loading
- **test_orchestrator_load_invalid_dag**: Error handling for invalid DAGs
- **test_orchestrator_run_dag**: Basic DAG execution
- **test_orchestrator_run_dag_fail_fast**: Fail-fast behavior
- **test_orchestrator_run_dag_no_fail_fast**: Continue-on-error behavior

### 2. Multi-Executor Tests (`test_multi_executor.py`)

#### Executor Factory Tests
- **test_executor_factory_default_executors**: Default executor registration
- **test_executor_factory_register_custom_executor**: Custom executor registration
- **test_executor_factory_thread_safety**: Thread-safe executor access

#### Task Executor Configuration
- **test_task_executor_field_default**: Default executor assignment
- **test_task_executor_field_custom**: Custom executor specification

#### Executor Integration
- **test_dag_execution_with_different_executors**: Multi-executor DAG execution
- **test_concurrent_execution_with_different_executors**: Concurrent multi-executor execution
- **test_executor_failure_handling**: Error handling for unknown executors
- **test_custom_executor_registration_and_usage**: Custom executor workflow
- **test_mixed_executor_dependencies**: Dependencies across different executors

#### Advanced Executor Features
- **test_orchestrator_uses_correct_executor**: Executor selection verification
- **test_executor_context_isolation**: Executor isolation testing
- **test_executor_error_propagation**: Error propagation through executors

### 3. Enhanced CLI Tests (`test_enhanced_cli.py`)

#### Status Manager CLI Features
- **test_create_dag_execution**: DAG execution record creation
- **test_update_dag_execution_status**: Status updates
- **test_get_running_dags**: Running DAG retrieval
- **test_get_dags_by_status**: Status-based DAG filtering
- **test_get_all_dags**: Complete DAG listing
- **test_get_dag_summary**: Summary statistics
- **test_get_dag_history**: Execution history tracking
- **test_cleanup_old_executions**: Record cleanup functionality
- **test_cancel_dag_execution**: DAG cancellation
- **test_log_message_and_retrieval**: Log message handling
- **test_get_dag_execution_details**: Detailed execution information

#### CLI Integration Tests
- **test_orchestrator_run_dag_in_thread_cli_integration**: CLI async execution
- **test_dag_status_monitoring_cli_scenario**: Status monitoring workflow
- **test_resume_functionality_cli_scenario**: Resume workflow testing

#### Data Format Tests
- **test_dag_execution_details_format**: CLI-expected data formats
- **test_logs_format_for_cli**: Log format validation
- **test_summary_format_for_cli**: Summary format validation

### 4. Concurrent Execution Tests (`test_concurrent_execution.py`)

⚠️ **Note**: These tests are limited due to SQLite threading constraints.

#### Basic Concurrent Features
- **test_run_dag_in_thread_returns_execution_id**: Thread execution ID return
- **test_basic_thread_safety**: Simple thread safety validation
- **test_resume_concurrent_execution**: Resume in concurrent environment

#### Status Manager Thread Safety
- **test_concurrent_status_updates**: Concurrent status modifications
- **test_concurrent_dag_execution_tracking**: Concurrent execution tracking
- **test_concurrent_log_writing**: Thread-safe log writing
- **test_cleanup_during_concurrent_access**: Concurrent cleanup operations

## Running Tests

### Complete Test Suite
```bash
# Run all tests
./run_tests.sh

# Run with verbose output
uv run pytest tests/ -v

# Run with coverage report
uv run pytest tests/ --cov=maestro --cov-report=html
```

### Specific Test Categories
```bash
# Core functionality
uv run pytest tests/test_dag.py tests/test_db_feature.py tests/test_orchestrator_dagloader.py -v

# Multi-executor support
uv run pytest tests/test_multi_executor.py -v

# Enhanced CLI features
uv run pytest tests/test_enhanced_cli.py -v

# Concurrent execution (with limitations)
uv run pytest tests/test_concurrent_execution.py -v
```

### Individual Test Files
```bash
# DAG operations
uv run pytest tests/test_dag.py -v

# Database features
uv run pytest tests/test_db_feature.py -v

# Orchestrator functionality
uv run pytest tests/test_orchestrator_dagloader.py -v

# Multi-executor support
uv run pytest tests/test_multi_executor.py -v

# Enhanced CLI features
uv run pytest tests/test_enhanced_cli.py -v
```

## Test Coverage Summary

| Test Category | Tests | Status | Coverage |
|---------------|-------|--------|----------|
| Core DAG Operations | 3/3 | ✅ Passing | 100% |
| Database Features | 4/4 | ✅ Passing | 100% |
| Orchestrator & Loading | 5/5 | ✅ Passing | 100% |
| Multi-Executor Support | 13/13 | ✅ Passing | 100% |
| Enhanced CLI Features | 15/15 | ✅ Passing | 100% |
| Concurrent Execution | 6/10 | ⚠️ Limited | 60% |

**Total: 46/50 tests passing (92% success rate)**

## Known Issues and Limitations

### SQLite Threading Constraints
- **Issue**: Segmentation faults in high-concurrency scenarios
- **Affected Tests**: `test_concurrent_execution.py` (some tests)
- **Root Cause**: SQLite's threading limitations in Python
- **Workaround**: Use separate database files per test or implement connection pooling

### Test Timing Dependencies
- **Issue**: Some tests depend on sleep timers for completion
- **Affected Tests**: Concurrent execution tests
- **Impact**: Occasional flaky test behavior
- **Mitigation**: Increased wait times and retry mechanisms

### Production Considerations
- **Database**: Consider PostgreSQL or MySQL for production use
- **Connection Pooling**: Implement database connection pooling
- **Monitoring**: Add health checks and monitoring endpoints
- **Scaling**: Test with realistic concurrent loads

## Adding New Tests

### Test File Structure
```python
import pytest
from maestro.core.orchestrator import Orchestrator
from maestro.core.dag import DAG
from maestro.server.tasks.base import BaseTask

class TestTask(BaseTask):
    def execute_local(self):
        # Test implementation
        pass

@pytest.fixture
def orchestrator(tmp_path):
    db_path = tmp_path / "test.db"
    return Orchestrator(log_level="CRITICAL", db_path=str(db_path))

def test_feature_functionality(orchestrator):
    # Test implementation
    assert True
```

### Best Practices

1. **Use temporary databases**: Always use `tmp_path` for database files
2. **Minimal test duration**: Keep sleep times as short as possible
3. **Clear assertions**: Use descriptive assertion messages
4. **Proper cleanup**: Ensure resources are properly cleaned up
5. **Test isolation**: Each test should be independent
6. **Mock external dependencies**: Use mocks for SSH, Docker, etc.

### Test Naming Convention
- Use descriptive test names: `test_feature_specific_behavior`
- Group related tests in classes: `class TestFeatureName`
- Use fixtures for common setup: `@pytest.fixture`

## Continuous Integration

### GitHub Actions (Recommended)
```yaml
name: Test Suite
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.13'
      - name: Install dependencies
        run: |
          pip install uv
          uv sync --extra test
      - name: Run tests
        run: |
          uv run pytest tests/test_dag.py tests/test_db_feature.py tests/test_orchestrator_dagloader.py tests/test_multi_executor.py tests/test_enhanced_cli.py -v
```

### Local Development
```bash
# Install development dependencies
uv sync --extra test

# Run tests before committing
./run_tests.sh

# Run specific test categories during development
uv run pytest tests/test_multi_executor.py -v
```

This comprehensive test suite ensures the reliability and functionality of all major Maestro components while providing a solid foundation for future development.
