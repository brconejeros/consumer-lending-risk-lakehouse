"""Fixtures for tests that hit a real serverless Databricks cluster via
Databricks Connect. Only meant to run inside `.venv-dbconnect` (see
`scripts/setup_dbconnect_env.sh` and CLAUDE.md "Working locally") - skips
rather than errors if `databricks-connect` isn't installed, so an
accidental `pytest` run in the default env doesn't fail loudly here.
"""

import os

import pytest


@pytest.fixture(scope="session")
def spark():
    from src.lakehouse.session import DatabricksConnectSession

    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    try:
        session = DatabricksConnectSession.get(profile=profile)
    except ModuleNotFoundError as exc:
        pytest.skip(f"databricks-connect not installed - run via .venv-dbconnect ({exc})")

    yield session
