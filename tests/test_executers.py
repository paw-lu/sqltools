"""Test if functions in ``executers.py`` are properly connecting to the database."""
import pandas as pd

from sqltools import executers


def test_run_query() -> None:
    """Test if ``run_query`` connects to database."""
    expected_data = pd.DataFrame({"test": [1]})
    query = "SELECT 1 test"
    actual_data = executers.run_query(query)
    pd.testing.assert_frame_equal(expected_data, actual_data)


def test_run_command() -> None:
    """Test if ``run_command`` connects to database."""
    command = "SELECT 1"
    assert executers.run_command(command) is None
