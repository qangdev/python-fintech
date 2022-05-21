import json
import boto3
import sys
import yfinance as yf

import time
import random
import datetime


# Your goal is to get per-hour stock price data for a time range for the ten stocks specified in the doc. 
# Further, you should call the static info api for the stocks to get their current 52WeekHigh and 52WeekLow values.
# You should craft individual data records with information about the stockid, price, price timestamp, 52WeekHigh and 52WeekLow values and push them individually on the Kinesis stream


kinesis = boto3.client(
    'kinesis',
    region_name="us-east-1",
    aws_access_key_id='AKIAYAXNR4SSPWXTD4WP',
    aws_secret_access_key='3XoY/pGpmc/6hXXHxffkJjR3B5RiD4NDU/jxMuQT'
)

now = datetime.datetime.now()
end = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start = end - datetime.timedelta(days=2)

## Add code to pull the data for the stocks specified in the doc
stock_names = [
    # 'MSFT',
    # 'MVIS',
    # 'GOOG',
    # 'SPOT',
    # 'INO',
    # 'OCGN',
    # 'ABML',
    # 'RLLCF',
    # 'JNJ',
    'PSFE'
]
for name in stock_names:
    print('-' * 50)
    stock = yf.Ticker(name)

    # Getting ClosingPrices
    data = yf.download(name, start=start, end=end, interval='1h')
    closing_prices = [row['Close'] for idx, row in data.iterrows()]

    # Getting stockid, price, price timestamp, 52WeekHigh and 52WeekLow values
    info = stock.info

    # Print out
    print(
        f"Stock: {info['symbol']}\n"
        f"Price (at {now.timestamp()}): {info['currentPrice']}\n"
        f"52WeekHigh: {info['fiftyTwoWeekHigh']}\n"
        f"52WeekLow: {info['fiftyTwoWeekLow']}\n"
        f"Closing Price ({str(start.date())} - {str(end.date())}): {closing_prices}"
    )
    # Add your code here to push data records to Kinesis stream.
    kinesis.put_record()
