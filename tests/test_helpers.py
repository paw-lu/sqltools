"""Test the helper functions are working correctly."""
import pandas as pd
import pytest

from sqltools import executers, helpers


def test_temp_table() -> None:
    """Test that ``TempTable`` can create a table that can be queried."""
    expected_data = pd.DataFrame({"test": [1]})
    tt = """
    --sql
    SELECT
        1 test INTO ##one;
    """
    one = helpers.TempTable(tt)
    query = """
        --sql
        SELECT
            *
        FROM
            ##one;
    """
    actual_data = executers.run_query(query)
    one.close()
    pd.testing.assert_frame_equal(expected_data, actual_data)


def test_show_temp() -> None:
    """Test that ``show_temp`` is showing all temporary tables."""
    expected_table = "##one"
    tt = """
        --sql
        SELECT
            1 test INTO ##one;
    """
    one = helpers.TempTable(tt)
    table_names = helpers.show_temp()["name"].values
    one.close()
    assert expected_table in table_names


def test_find_cols() -> None:
    """Test that ``find_cols`` runs."""
    expected_columns = ["column_name", "table_name"]
    actual_columns = helpers.find_cols("test").columns.tolist()
    assert expected_columns == actual_columns


def test_find_tables() -> None:
    """Test that ``find_tables`` runs."""
    expected_columns = ["table_name"]
    actual_columns = helpers.find_tables("test").columns.tolist()
    assert expected_columns == actual_columns


@pytest.mark.parametrize(["n", "expected_rows"], [(1, 1), (5, 5)])
def test_head(n: int, expected_rows: int) -> None:
    """Test that ``head`` retruns the top ``n`` rows of a ``sys.tables``."""
    actual_rows = helpers.head("sys.tables", n=n).shape[0]
    assert actual_rows == expected_rows
