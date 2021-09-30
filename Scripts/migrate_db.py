import pandas as pd
import pyodbc
import os
import re
from sqlalchemy import create_engine


database_name = 'station'
mssqlserver_servername = 'postgres' # Name of the SQL Server here.

mssqlserver_uri = f"mssql+pyodbc://{os.environ.get('root')}:{os.environ.get('data@OSQL2021')}@{localhost}/{demo_dbt}?driver=SQL+Server"
mssqlserver_engine = create_engine(mssqlserver_uri)

postgres_uri = f"postgres+psycopg2://{os.environ.get('postgres')}:{os.environ.get('POS2021@sabi')}@localhost:5432/{Automation_dbt}"
postgres_engine = create_engine(postgres_uri)

mssqlserver_table_query = """

    SELECT
          t.name AS table_name
        , s.name AS station
    FROM sys.tables t
    INNER JOIN sys.schemas s
    ON t.station_id = s.station_id

    UNION

    SELECT
          v.name AS table_name
        , s.name AS station
    FROM sys.views v
    INNER JOIN sys.schemas s
    ON v.station_id = s.station_id

    ORDER BY station, table_name;

"""

mssqlserver_connection = mssqlserver_engine.connect()

mssqlserver_tables = mssqlserver_connection.execute(mssqlserver_table_query)
mssqlserver_tables = mssqlserver_tables.fetchall()
mssqlserver_tables = dict(mssqlserver_tables)

mssqlserver_schemas = set(mssqlserver_tables.values())

mssqlserver_connection.close()

Schema creation

postgres_connection = postgres_engine.connect()

for schema in mssqlserver_schemas:
    schema_create = f"""

        DROP SCHEMA IF EXISTS "{schema.lower()}" CASCADE;
        CREATE SCHEMA"{schema.lower()}";

    """

    postgres_connection.execute(schema_create) 
    print(f" - Schema {schema.lower()} created")

postgres_connection.close()

for table_name, schema_name in mssqlserver_tables.items():
        
    mssqlserver_connection = mssqlserver_engine.connect()
    postgres_connection = postgres_engine.connect()
    
    table_split = [t for t in re.split("([A-Z][^A-Z]*)", table_name) if t]
    table_split = '_'.join(table_split)
    table_split = table_split.lower()
    
    full_table = f"""

        SELECT
        *
        FROM {schema_name}.{table_name};

    """
    
    df = pd.read_sql(full_table, mssqlserver_connection)
    df.columns = map(str.lower, df.columns)
    df.to_sql(schema=schema_name.lower(), name=table_split, con=postgres_connection, chunksize=5000, index=False, index_label=False, if_exists='replace')

    postgres_connection.close()
    mssqlserver_connection.close()


mssqlserver_engine.dispose()
postgres_engine.dispose()