# sqltools

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

Basic library to help with data exploration in SQL when using Jupyter/iPython.

## Quick start

Quick peak at columns.

```python
import sqltools

sqltools.head("customers")
```

|     | customer_id | name    |
| --- | ----------- | ------- |
| 0   | 11111       | Abby    |
| 1   | 22222       | Austin  |
| 2   | 33333       | Vinny   |
| 3   | 44444       | Alex    |
| 4   | 55555       | Natalie |

Find columns by their names in all tables.

```python
sqltools.find_cols("id")
```

|     | column_name | table_name |
| --- | ----------- | ---------- |
| 0   | customer_id | customers  |
| 1   | album_id    | albums     |
| 2   | song_id     | songs      |

Run queries and return their outputs as Pandas DataFrame

```python
query = """
    --sql
    SELECT
        *
    FROM
        customers
    WHERE
        id IN (5, 3, 4);
"""
interesting_customers = sqltools.run_query(query)
```

Create temporary tables that you can query.

```python
tt = """
    --sql
    SELECT
        TOP 1000 * INTO ##customer_sample
    FROM
        customers;
"""
customer_sample = TempTable(tt)

query = """
    --sql
    SELECT
        *
    FROM
        ##customer_sample
    WHERE
        birthday < '2019-01-01';
"""
young_sample_customers = run_query(query)
customer_sample.close()  # Close when done with temporary table
```

## Documentation

More functionality and accompanying examples may be found in the docs.
