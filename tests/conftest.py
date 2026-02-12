"""Pytest configuration and fixtures."""

import os
import sys
import pytest

# Add project directories to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "dags"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "spark_jobs"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "include"))


@pytest.fixture(scope="session")
def project_root():
    """Return the project root directory."""
    return PROJECT_ROOT


@pytest.fixture
def mock_env_vars():
    """Mock environment variables for testing."""
    env_vars = {
        "MINIO_ACCESS_KEY": "test_access_key",
        "MINIO_SECRET_KEY": "test_secret_key",
        "MINIO_ENDPOINT": "http://localhost:9000",
        "SPARK_MASTER_URL": "spark://localhost:7077",
        "AVIATIONSTACK_API_KEY": "test_api_key",
        "AVIATIONSTACK_BASE_URL": "http://api.aviationstack.com/v1",
        "AVIATIONSTACK_LIMIT": "100",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_USER": "test",
        "POSTGRES_PASSWORD": "test",
        "POSTGRES_DB": "test",
    }

    original_env = os.environ.copy()
    os.environ.update(env_vars)
    yield env_vars
    os.environ.clear()
    os.environ.update(original_env)
