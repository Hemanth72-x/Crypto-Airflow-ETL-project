from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import json
import requests

# Cryptocurrency IDs and Currency
ids = 'bitcoin,ethereum,dogecoin'
vs_currencies = 'usd'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_crypto_api'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 10,
}

# DAG definition
with DAG(
    dag_id='cryptodag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    @task()
    def extract_crypto_data():
        """Extract cryptocurrency data from CoinGecko API."""
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        endpoint = f'/api/v3/simple/price?ids={ids}&vs_currencies={vs_currencies}'
        
        response = http_hook.run(endpoint)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch crypto data: {response.status_code}")

    @task()
    def transform_crypto_data(crypto_data):
        """Transform the extracted cryptocurrency data."""
        transformed_data = {
            'bitcoin': crypto_data['bitcoin']['usd'],
            'ethereum': crypto_data['ethereum']['usd'],
            'dogecoin': crypto_data['dogecoin']['usd'],
        }
        return transformed_data

    @task()
    def load_crypto_data(transformed_data):
        """Load transformed data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS crypto_data (
            currency VARCHAR(50) PRIMARY KEY,
            price FLOAT NOT NULL
        );
        """)

        for currency, price in transformed_data.items():
            cursor.execute("""
            INSERT INTO crypto_data (currency, price)
            VALUES (%s, %s)
            ON CONFLICT (currency) DO UPDATE
            SET price = EXCLUDED.price;
            """, (currency, price))

        conn.commit()
        cursor.close()

    # DAG Workflow: ETL Pipelines
    crypto_data = extract_crypto_data()
    transformed_data = transform_crypto_data(crypto_data)
    load_crypto_data(transformed_data)
