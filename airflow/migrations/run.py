import argparse
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
from psycopg2 import ProgrammingError, OperationalError


def run_sql(sql: str, connection_id: str) -> None:
    """Executa instruções SQL em uma conexão airflow"""

    hook = PostgresHook(postgres_conn_id=connection_id)

    try:
        hook.get_conn()
    except OperationalError:
        logging.info("Connection `%s` is not configured.", connection_id)
        return

    try:
        hook.run(sql)
    except (AirflowException, ProgrammingError, OperationalError) as exc:
        logging.error(exc)


def main():
    parser = argparse.ArgumentParser(description="SQL import tool")
    parser.add_argument("sql", help="SQL file", type=argparse.FileType("r"))
    parser.add_argument(
        "connection", help="Airflow connection id, e.g: postgres_report_connection"
    )
    parsed = parser.parse_args()
    sql = "".join(parsed.sql.readlines())

    run_sql(sql, parsed.connection)

    print(os.listdir("./migrations"))


if __name__ == "__main__":
    main()
