from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

import yfinance as yf


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


@task
def get_historical_prices(symbol):
    ticket = yf.Ticker(symbol)
    data = ticket.history()
    records = []

    for index, row in data.iterrows():
        date = index.strftime('%Y-%m-%d %H:%M:%S')
        records.append([date, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]])

    return records


def _create_table(cur, schema, table):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            date date,
            "open" float,
            high float,
            low float,
            close float,
            volume bigint,
            created_date timestamp default GETDATE()
        );""")


@task
def load(schema, table, records):
    cur = get_Redshift_connection()
    # 만약 테이블이 없다면 생성
    _create_table(cur, schema, table)
    # 트랜잭션 시작
    try:
        cur.execute("BEGIN;")
        # 임시 테이블로 원본 테이블을 복사
        cur.execute(f"CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};")
        for r in records:
            sql = f"INSERT INTO t VALUES ('{r[0]}', {r[1]}, {r[2]}, {r[3]}, {r[4]}, {r[5]});"
            cur.execute(sql)

        # 임시 테이블 내용을 원본 테이블로 복사
        alter_sql = """
            DELETE FROM kyg8821.stock_info_row;
            INSERT INTO kyg8821.stock_info_row
            SELECT date, "open", high, low, close, volume
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) seq
                FROM t
            )
            WHERE seq = 1;"""
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;") 
        raise


with DAG(
    dag_id = 'UpdateSymbol_ROW',
    start_date = datetime(2023,5,30),
    catchup=False,
    tags=['DevCourse-Homework'],
    schedule = '0 10 * * *'
) as dag:

    results = get_historical_prices("AAPL")
    load("kyg8821", "stock_info_row", results)