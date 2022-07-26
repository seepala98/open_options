# + -------------------- +
# | COLLECT OPTIONS DATA |
# + -------------------- +

# Imports
import os
import pendulum

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Set variables
TICKERS = ['FB', 'GOOG']

# Get variables
# API_KEY = os.environ['API_KEY']
# DB_PASSWORD = os.environ['APP_DB_PASS']
API_KEY="W2KCGVWHV34K0OZT74Q2ZDZ751FGRXUT"

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

# Function to create table
def create_table(ticker):
    # Define Postgres hook
    pg_hook = PostgresHook(postgres_conn_id='postgres_optionsdata')

    # Create table if it doesn't exist
    pg_hook.run(f"""
CREATE TABLE IF NOT EXISTS {ticker} (
    put_call VARCHAR(5) NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    description VARCHAR(64) NOT NULL,
    bid DOUBLE PRECISION,
    ask DOUBLE PRECISION,
    last DOUBLE PRECISION,
    bid_size INTEGER,
    ask_size INTEGER,
    last_size INTEGER,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    open_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    total_volume INTEGER,
    quote_time BIGINT,
    volatility DOUBLE PRECISION,
    delta DOUBLE PRECISION,
    gamma DOUBLE PRECISION,
    theta DOUBLE PRECISION,
    vega DOUBLE PRECISION,
    rho DOUBLE PRECISION,
    open_interest INTEGER,
    time_value DOUBLE PRECISION,
    theoretical_value DOUBLE PRECISION,
    strike_price DOUBLE PRECISION,
    expiration_date BIGINT,
    dte INTEGER,
    PRIMARY KEY (symbol, quote_time)
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


# Function to get data from TDA API
def extract_options_data_from_tda(ticker, ti):
    # Import modules
    import json
    import requests

    # Configure dates
    TIMEDELTA = timedelta(days=45)
    # start_date = datetime.today(tzinfo=us_east_tz)
    start_date = datetime.utcnow().replace(tzinfo=us_east_tz)
    end_date = start_date + TIMEDELTA
    
    # Configure request
    headers = {
        'Authorization': '',
    }

    params = (
        ('apikey', API_KEY),
        ('symbol', ticker),
        ('contractType', 'ALL'),
        ('strikeCount', '50'),
        ('range', 'ALL'),
        ('fromDate', start_date),
        ('toDate', end_date),
    )

    # Get data
    response = requests.get(
        'https://api.tdameritrade.com/v1/marketdata/chains',
        headers=headers,
        params=params
    )
    data = json.loads(response.content)
    print(data)
    # Push XCOM
    ti.xcom_push(key='raw_data', value=data)

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


# ---- PARSE DATA ---- #
def transform_options_data(ti):
    
    # Import modules
    import pandas as pd

    # Pull XCOM
    data = ti.xcom_pull(key='raw_data', task_ids=['extract_options_data_from_tda'])[0]

    # Define columns
    columns = ['putCall', 'symbol', 'description', 'exchangeName', 'bid', 'ask',
        'last', 'mark', 'bidSize', 'askSize', 'bidAskSize', 'lastSize',
        'highPrice', 'lowPrice', 'openPrice', 'closePrice', 'totalVolume',
        'tradeDate', 'tradeTimeInLong', 'quoteTimeInLong', 'netChange',
        'volatility', 'delta', 'gamma', 'theta', 'vega', 'rho', 'openInterest',
        'timeValue', 'theoreticalOptionValue', 'theoreticalVolatility',
        'optionDeliverablesList', 'strikePrice', 'expirationDate',
        'daysToExpiration', 'expirationType', 'lastTradingDay', 'multiplier',
        'settlementType', 'deliverableNote', 'isIndexOption', 'percentChange',
        'markChange', 'markPercentChange', 'mini', 'inTheMoney', 'nonStandard']

    # Extract puts data
    puts = []
    dates = list(data['putExpDateMap'].keys())

    for date in dates:

        strikes = data['putExpDateMap'][date]

        for strike in strikes:
            puts += data['putExpDateMap'][date][strike]

    print(puts)

    # Convert to dataframe
    puts = pd.DataFrame(puts, columns=columns)

    # Select columns
    puts = puts[['putCall', 'symbol', 'description', 'bid', 'ask', 'last', 'bidSize',
        'askSize', 'lastSize', 'highPrice', 'lowPrice', 'openPrice',
        'closePrice', 'totalVolume', 'quoteTimeInLong', 'volatility', 'delta',
        'gamma', 'theta', 'vega', 'rho', 'openInterest', 'timeValue',
        'theoreticalOptionValue', 'strikePrice', 'expirationDate',
        'daysToExpiration']]

    # Convert floats
    def conv_num(x):
        return pd.to_numeric(x.astype(str).str.replace('NaN|nan', '', regex=True))

    for col in ['bid', 'ask', 'last', 'highPrice', 'lowPrice', 'openPrice',
                'closePrice', 'volatility', 'delta', 'gamma', 'theta', 'vega',
                'rho', 'timeValue', 'theoreticalOptionValue', 'strikePrice']:
        puts[col] = conv_num(puts[col])

    # Specifically for puts delta: make it positive
    puts['delta'] = -puts['delta']

    # Convert strings
    def conv_str(x):
        return x.astype(str)

    for col in ['putCall', 'symbol', 'description']:
        puts[col] = conv_str(puts[col])

    # Convert integers
    def conv_int(x):
        return x.astype(int)

    for col in ['bidSize', 'askSize', 'lastSize', 'totalVolume', 'quoteTimeInLong',
                'openInterest', 'expirationDate', 'daysToExpiration']:
        puts[col] = conv_int(puts[col])

    # Fill missing values
    puts = puts.fillna(-99)

    # Rename columns
    puts = puts.rename(columns={
        'putCall': 'put_call',
        'bidSize': 'bid_size',
        'askSize': 'ask_size',
        'lastSize': 'last_size',
        'highPrice': 'high_price',
        'lowPrice': 'low_price',
        'openPrice': 'open_price',
        'closePrice': 'close_price',
        'totalVolume': 'total_volume',
        'quoteTimeInLong': 'quote_time',
        'openInterest': 'open_interest',
        'timeValue': 'time_value',
        'theoreticalOptionValue': 'theoretical_value',
        'strikePrice': 'strike_price',
        'expirationDate': 'expiration_date',
        'daysToExpiration': 'dte',
    })

    # Push XCOM
    ti.xcom_push(key='transformed_data', value=puts.to_dict('records'))

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

# ---- LOG INTO POSTGRES ---- #
def load_data_into_postgres(ticker, ti):
    
    # Import modules
    import pandas as pd

    # Define Postgres hook
    pg_hook = PostgresHook(postgres_conn_id='postgres_optionsdata')

    # Pull XCOM
    puts = ti.xcom_pull(key='transformed_data', task_ids=['transform_options_data'])[0]
    puts = pd.DataFrame(puts)

    # Prepare insert query
    col_str = ', '.join(puts.columns.tolist())
    query_insert = f"INSERT INTO {ticker} ({col_str}) VALUES %s ON CONFLICT DO NOTHING"

    # Convert to rows
    rows = list(puts.itertuples(index=False, name=None))
    for row in rows:
        pg_hook.run(query_insert % str(row))

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

# Function to create DAG
def create_dag(ticker, default_args):
    dag = DAG(
        dag_id=f'get_options_data_{ticker}',
        default_args=default_args,
        description=f'ETL for {ticker} options data',
        schedule_interval='*/30 8-21 * * 1-5',
        catchup=False,
        tags=['finance', 'options', ticker]
    )

    with dag:

        # Define operators
        task0_create_table = PythonOperator(
            task_id='create_table',
            python_callable=create_table,
            op_kwargs={'ticker': ticker}
        )

        task1_extract = PythonOperator(
            task_id='extract_options_data_from_tda',
            python_callable=extract_options_data_from_tda,
            op_kwargs={'ticker': ticker}
        )

        task2_transform = PythonOperator(
            task_id = 'transform_options_data',
            python_callable=transform_options_data
        )

        task3_load = PythonOperator(
            task_id='load_data_into_postgres',
            python_callable=load_data_into_postgres,
            op_kwargs={'ticker': ticker}
        )

        # Set up dependencies
        task0_create_table >> task1_extract >> task2_transform >> task3_load

        return dag


# Create DAGs
for ticker in TICKERS:
    globals()[f'get_options_data_{ticker}'] = create_dag(ticker, default_args)
    globals()[f'{ticker}_historic_data'] = create_historic_data_dag(ticker, default_args)
    

    
    

