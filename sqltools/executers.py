"""Functions that execute commands and queries."""
import os
import sys
from typing import List, NamedTuple, Optional, Tuple, Union

import pandas as pd
import pyodbc

pyodbc.lowercase = True


def _get_connection_string(
    database: str,
    server: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    dsn: str = "MYMSSQL",
    platform: str = sys.platform,
) -> str:
    r"""
    Return the proper connection string for SQL Server.

    Parameters
    ----------
    database : str
        The database name to establish a connection to.
    server : str
        The server name to establish a connection to.
    platform : str, optional
        [description], by default detects platform. Expects "darwin" (macOS),
        "win32", or "linux".
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

    Returns
    -------
    connection_string : str
        String to be used as ``str`` argument in ``pyodbc.connect()``.
    """
    if platform == "darwin" or platform == "linux":
        if username is None:
            username = os.environ["SQLUSERNAME"]
        if password is None:
            password = os.environ["SQLPASSWORD"]
    connection_strings = {
        "win32": "Driver={ODBC Driver 13 for SQL Server};"
        f"Server={server};"
        f"Database={database};"
        "Trusted_Connection=yes;",
        "lin": "Driver={ODBC Driver 13 for SQL Server};"
        f"Server={server};"
        f"UID={username};"
        f"PWD={password};"
        f"Database={database};"
        "Trusted_Connection=yes;",
        "darwin": f"DSN={dsn};"
        f"UID={username};"
        f"PWD={password};"
        f"Database={database};",
    }
    return connection_strings[platform]


def run_query(
    query: str,
    database: str = "QuantDB",
    server: str = "DC1Q2PSQLGE1V",
    username: Optional[str] = None,
    password: Optional[str] = None,
    dsn: str = "MYMSSQL",
    params: Optional[Union[Tuple[str, ...], List[str], NamedTuple]] = None,
) -> pd.DataFrame:
    r"""
    Run query through Pandas.

    Parameters
    ----------
    query : str
        The sql query to execute.
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
    DataFrame
        Query results are returned as a Pandas DataFrame.

    Example
    -------
    Return the information of 10 customers.

    >>> import sqltools
    >>> query = '''
    ...     --sql
    ...     SELECT
    ...         TOP 10 *
    ...     FROM
    ...         customer;
    ... '''
    >>> sqltools.run_query(q)
    """
    connection_string = _get_connection_string(
        database, server, username=username, password=password, dsn=dsn
    )
    with pyodbc.connect(connection_string, autocommit=True) as conn:
        return pd.read_sql(query, conn, params=params)


def run_command(
    command: str,
    database: str = "QuantDB",
    server: str = "DC1Q2PSQLGE1V",
    username: Optional[str] = None,
    password: Optional[str] = None,
    dsn: str = "MYMSSQL",
) -> None:
    r"""
    Run sql command.

    Parameters
    ----------
    command : str
        The sql command to execute.
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

    Examples
    --------
    Drop the temporary table named "temp_table".

    >>> import sqltools
    >>> command = '''
    ...     IF OBJECT_ID('tempdb..temp_table', 'U') IS NOT NULL DROP TABLE temp_table
    ... '''
    >>> sqltools.run_command(command)

    Set the default schema of user FRB\\abby to "gb".

    >>> import sqltools
    >>> command = '''
    ...     ALTER USER [FRB\\abby] WITH DEFAULT_SCHEMA = "gb";
    ... '''
    >>> sqltoo.run_command(command)
    """
    connection_string = _get_connection_string(
        database, server, username=username, password=password, dsn=dsn
    )
    with pyodbc.connect(connection_string, autocommit=True) as conn:
        crsr = conn.cursor()
        crsr.execute(command)
