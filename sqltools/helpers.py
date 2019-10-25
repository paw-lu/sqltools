"""A collection of helper functions related to SQL."""
import pandas as pd
import pyodbc
import re
from typing import Optional

from sqltools import executers


class TempTable:
    r"""
    Create a temporary table.

    If a table already exists with the same name as the table that is
    attempting to be created, it will be dropped first.

    Parameters
    ----------
    command : SQL str
        A SQL command that creates a temporary table.
    database : str, optional
        The database to connect to. By default is "QuantDB".
    server : str, optional
        The server to connect to. By default is "DC1Q2PSQLGE1V".
    username : str in the form of "FRB\\pcosta", optional
        SQL database username. By default None, uses Kerberos authentication if
        on Windows or environmental variable ``SQLUSERNAME`` if on Linux or
        macOS.
    password : str, optional
        SQL database password. By default None, uses Kerberos authentication
        if on Windows or environmental variable ``SQLPASSWORD`` if on Linux or
        macOS.
    dsn : str, optional
        Server connection object for macOS if using unixODBC. By default set to
        "MYMSSQL".

    Example
    -------
    Create a temporary table with 10 customers.

    >>> import sqltools
    >>> tt = '''
    ...     --sql
    ...     SELECT
    ...         TOP 10 * INTO ##ten_customers
    ...     FROM
    ...         customer;
    ... '''
    >>> ten_customers = sqltools.TempTable(tt)

    The table may now be queried.

    >>> query = '''
    ...     --sql
    ...     SELECT
    ...         TOP 1 *
    ...     FROM
    ...         ##ten_customers;
    ... '''
    >>> sqltools.run_query(queryd)

    The temporary table may be closed when no longer in use.

    >>> ten_customers.close()
    """

    def __init__(
        self,
        command: str,
        database: str = "QuantDB",
        server: str = "DC1Q2PSQLGE1V",
        username: Optional[str] = None,
        password: Optional[str] = None,
        dsn: str = "MYMSSQL",
    ):
        # Automatically find table name
        temp_table_name = re.findall(r"INTO\s##\w+", command, re.IGNORECASE)[0].split(
            " "
        )[1]
        # Check if table already exists and drop it if it does
        executers.run_command(
            f"IF OBJECT_ID('tempdb..{temp_table_name}','U') IS NOT NULL DROP TABLE {temp_table_name};"
        )
        connection_string = executers._get_connection_string(
            database=database,
            server=server,
            username=username,
            password=password,
            dsn=dsn,
        )
        self.conn = pyodbc.connect(connection_string, autocommit=True)
        crsr = self.conn.cursor()
        crsr.execute(command)
        crsr.close()

    def close(self) -> None:
        """Remove the created temporary table."""
        self.conn.close()


def show_temp(
    database: str = "QuantDB",
    server: str = "DC1Q2PSQLGE1V",
    username: Optional[str] = None,
    password: Optional[str] = None,
    dsn: str = "MYMSSQL",
) -> pd.DataFrame:
    r"""
    Show all temporary tables in the database.

    Parameters
    ----------
    database : str, optional
        The database to connect to. By default is "QuantDB".
    server : str, optional
        The server to connect to. By default is "DC1Q2PSQLGE1V".
    username : str in the form of "FRB\\pcosta", optional
        SQL database username. By default None, uses Kerberos authentication if
        on Windows or environmental variable ``SQLUSERNAME`` if on Linux or
        macOS.
    password : str, optional
        SQL database password. By default None, uses Kerberos authentication
        if on Windows or environmental variable ``SQLPASSWORD`` if on Linux or
        macOS.
    dsn : str, optional
        Server connection object for macOS if using unixODBC. By default set to
        "MYMSSQL".
    params : List or tuple of strings, optional
        Any parameters to fill in. They will fill any "?" characters found in
        query, by default None.

    Returns
    -------
    temp_table_names : pd.DataFrame
        Query results are returned as a Pandas DataFrame.
    """
    query = """
        --sql
        SELECT
            name
        FROM
            tempdb.sys.objects
        WHERE
            name LIKE '##%';
    """
    return executers.run_query(
        query,
        database=database,
        server=server,
        username=username,
        password=password,
        dsn=dsn,
    )
