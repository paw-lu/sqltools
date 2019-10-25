"""A collection of helper functions related to SQL."""
import re
from typing import List, Optional, Tuple, Union

import jinja2
import pandas as pd
import pyodbc

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


def find_cols(
    search_terms: Union[str, List[str], Tuple[str, ...]],
    database: str = "QuantDB",
    server: str = "DC1Q2PSQLGE1V",
    username: Optional[str] = None,
    password: Optional[str] = None,
    dsn: str = "MYMSSQL",
) -> pd.DataFrame:
    r"""
    Return the tables whose columns containt the inputted search terms.

    Parameters
    ----------
    search_terms : Union[str, List[str], Tuple[str, ...]]
        Terms of list of terms that will be searched for. If multiple terms are
        inputted, returned columns must match each term.
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

    Returns
    -------
    column_locations : pd.DataFrame
        DataFrame with columns "column_name" and "table_name".
    """
    if isinstance(search_terms, str):
        search_terms = [search_terms]

    query_template = """
        --sql
        SELECT
            c.name column_name,
            CASE
                WHEN t.name IS NOT NULL THEN t.name
                ELSE v.name
            END table_name
        FROM
            sys.columns c
            LEFT JOIN sys.tables t ON c.object_id = t.object_id
            LEFT JOIN sys.views v ON v.object_id = c.object_id
        WHERE
            {%- for term in search_terms -%}
                {%- if loop.first %}\n\tc.name LIKE '%{{ term }}%'
                {%- else %}\n\tAND c.name LIKE '%{{ term }}%'
                {%- endif %}
            {%- endfor %};
    """

    query = jinja2.Template(query_template).render(search_terms=search_terms)
    return executers.run_query(
        query,
        database=database,
        server=server,
        username=username,
        password=password,
        dsn=dsn,
    )


def find_tables(
    search_terms: Union[str, List[str], Tuple[str, ...]],
    database: str = "QuantDB",
    server: str = "DC1Q2PSQLGE1V",
    username: Optional[str] = None,
    password: Optional[str] = None,
    dsn: str = "MYMSSQL",
) -> pd.DataFrame:
    r"""
    Return the tables whose names contain the inputted search terms.

    Parameters
    ----------
    search_terms : Union[str, List[str], Tuple[str, ...]]
        Terms of list of terms that will be searched for. If multiple terms are
        inputted, returned table names must match each term.
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

    Returns
    -------
    tables : pd.DataFrame
        DataFrame containing all matching tables. Column name is "table_name".
    """
    if isinstance(search_terms, str):
        search_terms = [search_terms]

    query_template = """
        --sql
        SELECT
            name table_name
        FROM
            sys.tables
        WHERE
            {%- for term in search_terms -%}
                {%- if loop.first %}\n\tname LIKE '%{{ term }}%'
                {%- else %}\n\tAND name LIKE '%{{ term }}%'
                {%- endif %}
            {%- endfor %}
        UNION ALL
        SELECT
            name table_name
        FROM
            sys.views
        WHERE
            {%- for term in search_terms -%}
                {%- if loop.first %}\n\tname LIKE '%{{ term }}%'
                {%- else %}\n\tAND name LIKE '%{{ term }}%'
                {%- endif %}
            {%- endfor %};
    """
    query = jinja2.Template(query_template).render(search_terms=search_terms)
    return executers.run_query(
        query,
        database=database,
        server=server,
        username=username,
        password=password,
        dsn=dsn,
    )


def get_def(
    search_term,
    database="RDM",
    server="vdbedcisandbox",
    username: Optional[str] = None,
    password: Optional[str] = None,
    dsn: str = "MYMSSQL",
) -> pd.DataFrame:
    r"""
    Find the definition of a column name from the data dictionary.

    Parameters
    ----------
    search_terms : Union[str, List[str], Tuple[str, ...]]
        Terms of list of terms that will be searched for in data dictionary.
    database : str, optional
        The database to connect to. By default is "RDM".
    server : str, optional
        The server to connect to. By default is "vdbedcisandbox".
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
    tables : pd.DataFrame
        DataFrame containing all matching tables. Column name is "table_name".
    """
    query = f"""
        --sql
        SELECT
            RDM_COLUMN_NAME column_name,
            RDM_TABLE_NAME table_name,
            RDM_BUSINESS_DEFINITION definition
        FROM
            V_Data_Dictionary
        WHERE
            RDM_COLUMN_NAME LIKE '%{search_term}%';
    """
    return executers.run_query(
        query,
        database=database,
        server=server,
        username=username,
        password=password,
        dsn=dsn,
    )


def head(
    table_name: str,
    n: int = 5,
    database: str = "QuantDB",
    server: str = "DC1Q2PSQLGE1V",
    username: Optional[str] = None,
    password: Optional[str] = None,
    dsn: str = "MYMSSQL",
) -> pd.DataFrame:
    r"""
    Return the first ``n`` rows from a table.

    Parameters
    ----------
    table_name : str
        Name of the table from which the top ``n`` rows will be returned from.
    n : int, optional
        The number of rows to return. By default is 5.
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

    Returns
    -------
    table_head : pd.DataFrame
        The top ``n`` rows of the selected table.
    """
    query = f"""
        --sql
        SELECT
            TOP {n} *
        FROM
            {table_name};
    """
    return executers.run_query(
        query,
        database=database,
        server=server,
        username=username,
        password=password,
        dsn=dsn,
    )


def get_cols(
    table_name: str,
    database: str = "QuantDB",
    server: str = "DC1Q2PSQLGE1V",
    username: Optional[str] = None,
    password: Optional[str] = None,
    dsn: str = "MYMSSQL",
) -> List[str]:
    r"""
    Return all of a table's column names.

    Parameters
    ----------
    table_name : str
        Name of the table whose columns will be returned.
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

    Returns
    -------
    column_names : List[str]
        The column names of the inputted table.
    """
    query = f"""
        --sql
        SELECT
            TOP 0 *
        FROM
            {table_name};
    """
    return executers.run_query(
        query,
        database=database,
        server=server,
        username=username,
        password=password,
        dsn=dsn,
    ).columns.tolist()


def to_sql_list(
    python_list: Union[str, int, float, List[Union[str, int, float]]],
    force_string: bool = False,
) -> str:
    """
    Turn a Python list into a SQL list.

    Parameters
    ----------
    python_list : str, int, or List of floats, ints, or strings
        Python list to convert to a SQL list.

    force_string : bool, optional
        Determine whether to force SQL list to be a list of strings. By default
        is False, and will detect the type of the first element in the list.

    Returns
    -------
    sql_list : str
        Return string in the form of (a,b,c,d) for insertion into sql
        query.

    Example
    -------
    Find all customers with an id of 1, 4, or 5.
    >>> import sqltools
    >>> cust_ids = [1, 4, 5]
    >>> query = f'''
    ...     --sql
    ...     SELECT
    ...         *
    ...     FROM
    ...         customer
    ...     WHERE
    ...         id IN {sqltools.to_sql_list(cust_ids)}
    ... '''
    >>> sqltools.run_query(query)
    """
    # If a single element in inputted, turn into list
    if isinstance(python_list, str):
        python_list = [python_list]
    elif isinstance(python_list, int):
        python_list = [python_list]
    elif isinstance(python_list, float):
        python_list = [python_list]

    if isinstance(python_list[0], str) or force_string:
        quote = "'"
    else:
        quote = ""

    sql_list = "("
    for item in python_list:
        sql_list += f"{quote}{item}{quote}, "
    sql_list = sql_list.strip(", ")
    sql_list += ")"
    return sql_list


def unique(
    column: str,
    table: str,
    database: str = "QuantDB",
    server: str = "DC1Q2PSQLGE1V",
    username: Optional[str] = None,
    password: Optional[str] = None,
    dsn: str = "MYMSSQL",
) -> List[str]:
    r"""
    Return unique values of ``column`` from ``table``.

    Parameters
    ----------
    column : str
        Name of column for which to return the unique values of.
    table : str
        Name of the table for which ``column`` belongs to.
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

    Returns
    -------
    column_names : List[str]
        The column names of the inputted table.
    """
    query = f"""
        --sql
        SELECT
            DISTINCT({column})
        FROM
            {table};
    """
    return executers.run_query(
        query,
        database=database,
        server=server,
        username=username,
        password=password,
        dsn=dsn,
    )
