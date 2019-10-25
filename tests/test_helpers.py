"""Test the helper functions are working correctly."""
import pandas as pd

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
