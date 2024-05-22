from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

import logging
import requests

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def get_countries_info():
    url = 'https://restcountries.com/v3/all'
    response = requests.get(url)
    records = []

    if response.status_code == 200:
        data = response.json()
        for d in data:
            records.append([d["name"]["official"], d["population"], d["area"]])
    else:
        raise Exception(f"Status code: {response.status_code}")

    return records


@task
def load_countries_info(schema, table, records):
    logging.info("load countries info")
    cur = get_Redshift_connection()
    # 트랜잭션 실행
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
                country varchar(100),
                population bigint,
                area float
        );""")

        for record in records:
            sql = f"INSERT INTO {schema}.{table} VALUES (%s, %s, %s);"
            cur.execute(sql, (record[0], record[1], record[2]))
        cur.execute("COMMIT;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load completed")


with DAG(
    dag_id="RestCountries",
    start_date= datetime(2024, 5, 21),
    catchup=False,
    tags=['DevCourse-Homework'],
    schedule='30 6 * * 6'
) as dag:
    records = get_countries_info()
    load_countries_info("kyg8821", "rest_countries_info", records)