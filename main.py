#!/usr/bin/env python
# coding: utf-8

"""Download historical candlestick data for all trading pairs on Binance.com.
All trading pair data is checked for integrity, sorted and saved as both a CSV
and a Parquet file. The CSV files act as a raw buffer on every update round.
The Parquet files are much more space efficient (~50GB vs ~10GB) and are
therefore the files used to upload to Kaggle after each run.
"""

__author__ = 'GOSUTO.AI'

import json
import os
import random
import subprocess
import time
from datetime import date, datetime, timedelta

import requests
import pandas as pd

import preprocessing as pp

API_BASE = 'https://api.binance.com/api/v3/'

LABELS = [
    'open_time',
    'open',
    'high',
    'low',
    'close',
    'volume',
    'close_time',
    'quote_asset_volume',
    'number_of_trades',
    'taker_buy_base_asset_volume',
    'taker_buy_quote_asset_volume',
    'ignore'
]

METADATA = {
    'id': 'jorijnsmit/binance-full-history',
    'title': 'Binance Full History',
    'isPrivate': False,
    'licenses': [{'name': 'other'}],
    'keywords': [
        'business',
        'finance',
        'investing',
        'currencies and foreign exchange'
    ],
    'collaborators': [],
    'data': []
}

def get_batch(symbol, interval='1m', start_time=0, limit=1000):
    """Use a GET request to retrieve a batch of candlesticks. Process the JSON into a pandas
    dataframe and return it. If not successful, return an empty dataframe.
    """

    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': start_time,
        'limit': limit
    }
    try:
        # timeout should also be given as a parameter to the function
        response = requests.get(f'{API_BASE}klines', params, timeout=30)
    except requests.exceptions.ConnectionError:
        print('Connection error, Cooling down for 5 mins...')
        time.sleep(5 * 60)
        return get_batch(symbol, interval, start_time, limit)
    
    except requests.exceptions.Timeout:
        print('Timeout, Cooling down for 5 min...')
        time.sleep(5 * 60)
        return get_batch(symbol, interval, start_time, limit)
    
    except requests.exceptions.ConnectionResetError:
        print('Connection reset by peer, Cooling down for 5 min...')
        time.sleep(5 * 60)
        return get_batch(symbol, interval, start_time, limit)

    if response.status_code == 200:
        return pd.DataFrame(response.json(), columns=LABELS)
    print(f'Got erroneous response back: {response}')
    return pd.DataFrame([])


def all_candles_to_csv(base, quote, interval='1m'):
    """Collect a list of candlestick batches with all candlesticks of a trading pair,
    concat into a dataframe and write it to CSV.
    """

    # see if there is any data saved on disk already
    try:
        batches = [pd.read_csv(f'data/{base}-{quote}.csv')]
        last_timestamp = batches[-1]['open_time'].max()
    except FileNotFoundError:
        batches = [pd.DataFrame([], columns=LABELS)]
        last_timestamp = 0
    old_lines = len(batches[-1].index)

    now = (time.time())*1000
    last_timestamp = last_timestamp
    # print(f'{base}--{quote} last timestamp: {last_timestamp}. Now: {now}')

    # gather all candlesticks available, starting from the last timestamp loaded from disk or 0
    # stop if the timestamp that comes back from the api is the same as the last one
    previous_timestamp = None

    while previous_timestamp != last_timestamp:
        # stop if we reached data from today
        if last_timestamp >= now:
            break

        previous_timestamp = last_timestamp

        new_batch = get_batch(
            symbol=base+quote,
            interval=interval,
            start_time=last_timestamp+1
        )

        # requesting candles from the future returns empty
        # also stop in case response code was not 200
        if new_batch.empty:
            break

        last_timestamp = new_batch['open_time'].max()

        # sometimes no new trades took place yet on date.today();
        # in this case the batch is nothing new
        if previous_timestamp == last_timestamp:
            break

        batches.append(new_batch)
        last_datetime = datetime.fromtimestamp(last_timestamp / 1000)

        covering_spaces = 20 * ' '
        print(datetime.now(), base, quote, interval, str(last_datetime)+covering_spaces, end='\r', flush=True)

    # write clean version of csv to parquet
    df = pd.concat(batches, ignore_index=True)
    df = pp.quick_clean(df)
 
    # in the case that new data was gathered write it to disk
    if len(batches) > 1:
        df.to_csv(f'data/{base}-{quote}.csv', index=False)
        return len(df.index) - old_lines
    return 0


def main():
    """Main loop; loop over all currency pairs that exist on the exchange. Once done upload the
    compressed (Parquet) dataset to Kaggle.
    """

    # get all pairs currently available
    all_symbols = pd.DataFrame(requests.get(f'{API_BASE}exchangeInfo').json()['symbols'])
    all_pairs = [tuple(x) for x in all_symbols[['baseAsset', 'quoteAsset']].to_records(index=False)]

    # randomising order helps during testing and doesn't make any difference in production
    random.shuffle(all_pairs)

    # make sure data folders exist
    os.makedirs('data', exist_ok=True)
    os.makedirs('compressed', exist_ok=True)

    # do a full update on all pairs
    n_count = len(all_pairs)
    for n, pair in enumerate(all_pairs, 1):
        base, quote = pair
        bf, qf = False, False

        if base == "BTC" or base == "USDT":
            bf = True

        if quote == "BTC" or quote == "USDT":
            qf = True

        if bf or qf:
            new_lines = all_candles_to_csv(base=base, quote=quote)
            if new_lines > 0:
                print(f'{datetime.now()} {n}/{n_count} Wrote {new_lines} new lines to file for {base}-{quote}')
        #else:
        #    print(f'Ignoring {base}-{quote}')

    # clean the data folder and upload a new version of the dataset to kaggle
    try:
        os.remove('compressed/.DS_Store')
    except FileNotFoundError:
        pass


if __name__ == '__main__':
    main()
