import json
import pandas as pd
import csv
import numpy as np
from functools import reduce
import operator

def extract_fields_orig(inpath, outpath, fields):
    '''
    Extracts field from json documents in inpath (one json object per line) 
    and stores them in csv in outpath
    '''
    with open(inpath, encoding='utf-8') as infile, open(outpath, 'w', encoding='utf-8') as outfile:
        writer = csv.writer(outfile, delimiter=',', 
                            quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
        writer.writerow(fields.keys()) 

        for i,line in enumerate(infile):
            try:
                tweet = json.loads(line)
            except json.decoder.JSONDecodeError:
                print(f"Read error in line {i}. Skipping.")
                continue

            # skip retweets
            if 'retweeted_status' in tweet:
                continue
            # skip tweets with truncated: True
            if tweet.get('truncated', False):
                continue

            if 'full_text' in tweet:
                text = tweet['full_text']
            else:
                text = tweet.get('text', np.nan)

            row = []
            for field, path in fields.items():
                try:
                    if field == 'text':
                        value = text
                    else:
                        value = reduce(operator.getitem, path, tweet) 
                except KeyError:
                    value = np.nan
                row.append(value)
            writer.writerow(row)
            if i % 10000 == 0:
                print(f"Processed {i} lines")

        #print(f"Processed {i} lines")


if __name__ == "__main__":

    RAW_DATA_FILE = '/data/tweets_final.json'
    OUT_FILE = '/data/ORIG_TWEETS_notruncation_kayla.csv'
    
    # Extract tweet metadata
    data_fields = {'tweet_id': ['id_str'], 'user_id': ['user', 'id_str'],
                   'created_at': ['created_at'], 
                   'user_screen_name': ['user', 'screen_name'],
                   'language': ['lang'], 'favorite_count': ['favorite_count'],
                   'retweet_count': ['retweet_count'], 
                   'text': [],
                   'truncated': ['truncated']}
    extract_fields_orig(RAW_DATA_FILE, OUT_FILE, data_fields)

