# Imports
import os
import pendulum

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Set variables
TICKERS = ['FB', 'GOOG', "MSFT"]

# Get variables
API_KEY = os.environ['API_KEY']

# Set arguments
us_east_tz = pendulum.timezone('America/Toronto')
default_args = {
    'owner': 'vardhan',
    'start_date': datetime(2022, 1, 7, 7, 30, tzinfo=us_east_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def create_table_historic_data(ticker):
    pg_hook = PostgresHook(postgres_conn_id='postgres_optionsdata')
    pg_hook.run(f"""
        CREATE TABLE IF NOT EXISTS historic__{ticker}_ (
            datetime BIGINT,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            open DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume INTEGER,
            symbol VARCHAR(32) NOT NULL,
            PRIMARY KEY (symbol, datetime)
        )
    """)

def extract_historic_data_from_tda(ticker, ti):
    import json 
    import requests

    # Configure dates
    # start_date 1 years back 
    TIMEDELTA = timedelta(days=365)
    # start_date = datetime.today(tzinfo=us_east_tz)
    end_date = datetime.utcnow().replace(tzinfo=us_east_tz)
    start_date = end_date - TIMEDELTA

    # Configure request
    headers = {
        'Authorization': '',
    }

    params = (
        ('apikey', API_KEY),
        ('startDate', int(start_date.timestamp())),
    )

    url = "https://api.tdameritrade.com/v1/marketdata/"+ ticker +"/pricehistory".format(ticker=ticker)

    # Get data
    response = requests.get(
        url,
        headers=headers,
        params=params
    )
    data = json.loads(response.content)
    print(data)
    # Push XCOM
    ti.xcom_push(key='raw_data', value=data)

# transform data into a format that can be loaded into postgres
def transform_historic_data(ticker, ti):
    
    # Import modules
    import pandas as pd

    # Get data
    data = ti.xcom_pull(key='raw_data', task_ids=["extract_historic_data_from_tda"])[0]

    # Define Columns
    columns = [
        'datetime',
        'high',
        'low',
        'open',
        'close',
        'volume'
    ]

    # extract data
    extract_data = []
    # extract_data.append(data['symbol'])
    for item in data['candles']: 
        extract_data.append(item)
    print(extract_data)
    
    # Convert to dataframe
    df = pd.DataFrame(extract_data, columns=columns)
    df['symbol'] = ticker
    print(df)
    # Convert floats
    def conv_num(x):
        return pd.to_numeric(x.astype(str).str.replace('NaN|nan', '', regex=True))
    
    for col in ["high", "low", "open", "close", "volume"]:
        df[col] = conv_num(df[col])
    
    # Convert strings
    def conv_str(x):
        return x.astype(str)

    for col in ['symbol']:
        df[col] = conv_str(df[col])

    # Convert integers
    def conv_int(x):
        return x.astype(int)

    for col in ['datetime']:
        df[col] = conv_int(df[col])

    # Fill missing values
    df = df.fillna(-99)

    # Push XCOM
    ti.xcom_push(key='transformed_historic_data', value=df.to_dict('records'))


# Create DAG
def load_historic_data_into_postgres(ticker, ti):

    # Import modules
    import pandas as pd

    # Define Postgres hook
    postgres_hook = PostgresHook(postgres_conn_id='postgres_optionsdata')

    # Pull XCOM
    data = ti.xcom_pull(key='transformed_historic_data', task_ids=['transform_historic_data'])[0]
    historic_data = pd.DataFrame(data)

    print(historic_data.head())
    # prepare insert query
    col_str = ', '.join(historic_data.columns.to_list())
    print(col_str)
    query_insert = f"INSERT INTO historic__{ticker}_ ({col_str}) VALUES %s ON CONFLICT DO NOTHING"

    # Convert to rows 
    historic_data_rows = list(historic_data.itertuples(index=False, name=None))
    for row in historic_data_rows:
        postgres_hook.run(query_insert % str(row))

# Function to create a DAG for historic data 
def create_historic_data_dag(ticker, default_args):
    dag = DAG(
        dag_id=f'{ticker}_historic_data',
        default_args=default_args,
        description=f'ETL for historic data for {ticker}',
        schedule_interval='*/30 8-21 * * 1-5',
        catchup=False,
        tags=['finance', 'historic', ticker]
    )

    with dag:
        task0_create_historic_table = PythonOperator(
            task_id='create_historic_table',
            python_callable=create_table_historic_data,
            op_kwargs={'ticker': ticker},
        )

        task1_historic_extract = PythonOperator(
            task_id='extract_historic_data_from_tda',
            python_callable=extract_historic_data_from_tda,
            op_kwargs={'ticker': ticker},
        )

        task2_transform_historic_data = PythonOperator(
            task_id='transform_historic_data',
            python_callable=transform_historic_data,
            op_kwargs={'ticker': ticker},
        )

        task3_load_historic_data_into_postgres = PythonOperator(
            task_id='load_historic_data_into_postgres',
            python_callable=load_historic_data_into_postgres,
            op_kwargs={'ticker': ticker},
        )

        task0_create_historic_table >> task1_historic_extract >> task2_transform_historic_data >> task3_load_historic_data_into_postgres

        return dag

# Create DAGs
for ticker in TICKERS:
    globals()[f'{ticker}_historic_data'] = create_historic_data_dag(ticker, default_args)