"""Tests for DAG structure and integrity."""

import pytest
import sys
import os
from unittest.mock import patch, MagicMock

# Add dags directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))


@pytest.fixture
def mock_airflow_variables():
    """Mock Airflow Variables to avoid database connection."""
    with patch("airflow.models.Variable.get") as mock_get:
        mock_get.side_effect = lambda key, default_var=None: default_var or f"mock_{key}"
        yield mock_get


@pytest.fixture
def dag(mock_airflow_variables):
    """Load the DAG for testing."""
    # Mock environment variables
    env_vars = {
        "MINIO_ACCESS_KEY": "test_access_key",
        "MINIO_SECRET_KEY": "test_secret_key",
        "MINIO_ENDPOINT": "http://localhost:9000",
        "SPARK_MASTER_URL": "spark://localhost:7077",
    }

    with patch.dict(os.environ, env_vars):
        # Import the dag module
        from dag import dag_

        return dag_


class TestDAGStructure:
    """Tests for DAG structure."""

    def test_dag_loaded(self, dag):
        """Test that DAG loads without errors."""
        assert dag is not None

    def test_dag_has_correct_id(self, dag):
        """Test that DAG has the correct ID."""
        assert dag.dag_id == "flights_data_pipeline"

    def test_dag_has_correct_schedule(self, dag):
        """Test that DAG has the correct schedule."""
        assert dag.schedule_interval == "0 0 * * *"

    def test_dag_has_no_import_errors(self, dag):
        """Test that DAG has no import errors."""
        # If we got here, the DAG imported successfully
        assert dag.fileloc is not None

    def test_dag_has_required_tasks(self, dag):
        """Test that DAG has all required tasks."""
        task_ids = [task.task_id for task in dag.tasks]

        required_tasks = [
            "start",
            "end",
            "create_minio_bucket",
            "upload_raw_to_minio",
            "dbt_transform",
        ]

        for task_id in required_tasks:
            assert task_id in task_ids, f"Missing task: {task_id}"

    def test_dag_has_correct_tags(self, dag):
        """Test that DAG has correct tags."""
        expected_tags = {"spark", "minio", "aviationstack", "etl"}
        assert set(dag.tags) == expected_tags

    def test_dag_has_retries_configured(self, dag):
        """Test that DAG has retries configured."""
        assert dag.default_args.get("retries") == 3

    def test_dag_has_failure_callback(self, dag):
        """Test that DAG has on_failure_callback configured."""
        assert dag.default_args.get("on_failure_callback") is not None

    def test_dag_catchup_is_disabled(self, dag):
        """Test that catchup is disabled."""
        assert dag.catchup is False


class TestDAGTaskDependencies:
    """Tests for DAG task dependencies."""

    def test_start_has_no_upstream(self, dag):
        """Test that start task has no upstream dependencies."""
        start_task = dag.get_task("start")
        assert len(start_task.upstream_list) == 0

    def test_end_has_no_downstream(self, dag):
        """Test that end task has no downstream dependencies."""
        end_task = dag.get_task("end")
        assert len(end_task.downstream_list) == 0

    def test_create_bucket_follows_start(self, dag):
        """Test that create_bucket follows start."""
        start_task = dag.get_task("start")
        create_bucket_task = dag.get_task("create_minio_bucket")

        assert create_bucket_task in start_task.downstream_list


class TestDAGConfiguration:
    """Tests for DAG configuration."""

    def test_default_args_owner(self, dag):
        """Test that default_args has correct owner."""
        owner = dag.default_args.get("owner")
        assert owner is not None
        # Owner should be a valid email format or string
        assert "@" in owner or len(owner) > 0

    def test_default_args_retry_delay(self, dag):
        """Test that retry_delay is configured."""
        from datetime import timedelta

        retry_delay = dag.default_args.get("retry_delay")
        assert retry_delay is not None
        assert isinstance(retry_delay, timedelta)


class TestTaskGroups:
    """Tests for task groups."""

    def test_extract_raw_task_group_exists(self, dag):
        """Test that extract_raw task group exists."""
        task_ids = [task.task_id for task in dag.tasks]
        # Task groups create tasks with prefixed names
        extract_tasks = [t for t in task_ids if "extract" in t.lower()]
        assert len(extract_tasks) > 0, "No extract tasks found"

    def test_etl_bronze_task_group_exists(self, dag):
        """Test that etl_bronze task group exists."""
        task_ids = [task.task_id for task in dag.tasks]
        etl_tasks = [t for t in task_ids if "etl_" in t.lower()]
        assert len(etl_tasks) > 0, "No ETL tasks found"
