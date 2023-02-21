import pyodbc
from pathlib import Path
import pandas as pd
import os
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import logging
import pyarrow
import config

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename="crypto_api_log.log",
    filemode="w"
)

logger = logging.getLogger(__name__)


# Extract
def api_runner():
    
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'

    # Coinmarketcap API Documentation: https://coinmarketcap.com/api/documentation/v1/

    parameters = {
        'start':'1',
        'limit':'5000',
        'convert':'USD'
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': config.api_key,
    }

    session = Session()
    session.headers.update(headers)

    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        logger.exception(e)


    df = pd.json_normalize(data['data'])
    df['timestamp'] = pd.to_datetime('now', utc=True)

    if not os.path.isfile(str(Path.cwd()) + 'crypto_data_raw.parquet'):
        df.to_parquet('crypto_data_raw.parquet', index=False)
    else:
        df.to_parquet('crypto_data_raw.parquet', mode='a')


# Transform
def transform_data():

    df = pd.read_parquet('crypto_data_raw.parquet')

    categoryCols = ['name', 'symbol', 'slug', 'platform.name', 'platform.symbol', 'platform.slug']
    df[categoryCols] = df[categoryCols].astype('category')
    floatCols = df.select_dtypes(include=[float]).columns
    df[floatCols] = df[floatCols].astype('float32')
    df[['id','num_market_pairs']] = df[['id','num_market_pairs']].astype('int32')
    df['cmc_rank'] = df['cmc_rank'].astype('int16')
    df[['date_added','last_updated','quote.USD.last_updated','timestamp']] = df[['date_added','last_updated','quote.USD.last_updated','timestamp']].apply(pd.to_datetime)

    dim_cols = ['id', 'name', 'symbol', 'circulating_supply', 'total_supply',
       'cmc_rank', 'quote.USD.price', 'quote.USD.volume_24h','timestamp']

    df = df[dim_cols]

    if not os.path.isfile(str(Path.cwd()) + 'crypto_data_clean.parquet'):
        df.to_parquet('crypto_data_clean.parquet', index=False)
    else:
        df.to_parquet('crypto_data_clean.parquet', mode='a')


# Load
def load_data():

    df = pd.read_parquet('crypto_data_clean.parquet')

    try:
        conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost\SQLEXPRESS;'
                      'Database=Crypto;'
                      'Trusted_Connection=yes;')

        cursor = conn.cursor()

        for index, row in df.iterrows():
            cursor.execute("Insert into quotes (CoinID,Name,Symbol,Circulating_Supply,Total_Supply,CMC_Rank,Quote_USD_Price,Quote_USD_Volume_24h,API_TimeStamp) values (?,?,?,?,?,?,?,?,?)", 
            row['id'],row['name'],row['symbol'],row['circulating_supply'],row['total_supply'],row['cmc_rank'],row['quote.USD.price'],row['quote.USD.volume_24h'],row['timestamp'])

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        logger.exception(e)


# Perform tasks
api_runner()
transform_data()
load_data()