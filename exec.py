import clickhouse_driver
import psycopg2

from base import settings


def execute_clickhouse_query(
    query: str,
    params: dict,
    tenant_id: str,
    echo_query: bool = False,
    echo_params: bool = False,
) -> None:
    """
    Execute a query on ClickHouse databases.

    Args:
    - query (str): The SQL query to execute.
    - params (dict): The parameters to pass to the query.
    -tenant_id(str):

    """
    if echo_query:
        print(query)
    if echo_params:
        print(params)

    with clickhouse_driver.connect(
        host=settings.CH_DB_HOST,
        port=9000,
        user=settings.CH_DB_USER,
        password=settings.CH_DB_PASSWORD,
        database=tenant_id,
    ) as ch_conn:
        with ch_conn.cursor() as ch_cursor:
            ch_cursor.execute(query, params)
            ch_result = ch_cursor.fetchall()
            return ch_result


def execute_postgres_query(
    query: str,
    params: dict,
    tenant_id: str,
    echo_query: bool = False,
    echo_params: bool = False,
) -> None:
    """
    Execute a query on PostgreSQL databases.

    Args:
    - query (str): The SQL query to execute.
    - params (dict): The parameters to pass to the query.
    -tenant_id(str):
    """
    if echo_query:
        print(query)
    if echo_params:
        print(params)

    with psycopg2.connect(
        host=settings.POSTGRES_HOST,
        port=settings.DATABASE_PORT,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        dbname=settings.POSTGRES_DB,
        options=f"-c search_path={tenant_id},public",
    ) as pg_conn:
        with pg_conn.cursor() as pg_cursor:
            pg_cursor.execute(query, params)
            pg_result = pg_cursor.fetchall()
            return pg_result
