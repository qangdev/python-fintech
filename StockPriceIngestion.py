import json
import boto3
import datetime

import yfinance as yf


# Your goal is to get per-hour stock price data for a time range for the ten stocks specified in the doc. 
# Further, you should call the static info api for the stocks to get their current 52WeekHigh and 52WeekLow values.
# You should craft individual data records with information about the stockid, price, price timestamp, 52WeekHigh and 52WeekLow values and push them individually on the Kinesis stream

AWS_KEY = 'AKIAYAXNR4SSPWXTD4WP'
AWS_SCR = '3XoY/pGpmc/6hXXHxffkJjR3B5RiD4NDU/jxMuQT'
AWS_REG = 'us-east-1'

KINESIS_NAME = 'pj5'
kinesis_client = boto3.client(
    'kinesis',
    region_name=AWS_REG,
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SCR
)

now = datetime.datetime.utcnow()
end = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
start = end - datetime.timedelta(days=2)

## Add code to pull the data for the stocks specified in the doc
stock_names = [
    'MSFT',
    'MVIS',
    'GOOG',
    'SPOT',
    'INO',
    'OCGN',
    'ABML',
    'RLLCF',
    'JNJ',
    'PSFE'
]
stock_data = {}
for idx, name in enumerate(stock_names):
    stock = yf.Ticker(name)

    # Getting ClosingPrices
    records = yf.download(name, start=start, end=end, interval='1h')
    info = stock.info
    stock_data[name] = []
    for row in records.iterrows():
        print(row[0])
        data = {
            'stock': info['symbol'],
            'ts': row[0].timestamp(),
            'closePrice': row[1]['Close'],
            'price': info['currentPrice'],
            '52WeekHigh': row[1]['High'],
            '52WeekLow': row[1]['Low'],
            'current52WeekHigh': info['fiftyTwoWeekHigh'],
            'current52WeekLow': info['fiftyTwoWeekLow'],
        }
        stock_data[name].append(data)

        # Print out
        print('-' * 50)
        print(
            f'[{idx+1}/{len(stock_names)}]\n'
            f"Stock: {info['symbol']}\n"
            f"TS ({str(row[0])}): {row[0].timestamp()}\n"
            f"Closing Price ({row[0].timestamp()}): {row[1]['Close']}\n"
            f"Price (at {now.timestamp()}): {info['currentPrice']}\n"
            f"52WeekHigh: {row[1]['High']}\n"
            f"52WeekLow: {row[1]['Low']}\n"
            f"current52WeekHigh: {info['fiftyTwoWeekHigh']}\n"
            f"current52WeekLow: {info['fiftyTwoWeekLow']}\n"
        )
print(json.dumps(stock_data, indent=4))
counter = 1
for name, sub_info in stock_data.items():
    # Send data to Kinesis DataStream
    for item in sub_info:
        response = kinesis_client.put_record(
            StreamName=KINESIS_NAME,
            Data=json.dumps(item),
            PartitionKey=str(hash(item['ts']))
        )
        # If the message was not successfully sent print an error message
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            print(f'Error on {name}: {response}')

    print(f'-'*50)
    print(
        f'[Kinesis][{counter}/{len(stock_data)}] Pushed stock {name} done\n'
        f'Data: {json.dumps(sub_info, indent=4)}'
            )
    counter += 1
