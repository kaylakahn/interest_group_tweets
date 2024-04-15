import json
import pandas as pd
import csv
import numpy as np
from functools import reduce
import operator

# Function to load tweet IDs from a CSV file
def load_tweet_ids(file_path):
    tweet_ids = set()
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            tweet_ids.add(row['tweet_id'])
    return tweet_ids

# extract fields from non-truncated retweets 
def extract_fields(inpath, outpath, fields, excluded_tweet_ids):
    #Extracts field from json documents in inpath (one json object per line) 
    #and stores them in csv in outpath
    
    with open(inpath, encoding='utf-8') as infile, open(outpath, 'w', encoding='utf-8') as outfile:
        writer = csv.writer(outfile, delimiter=',', 
                            quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
        writer.writerow(fields.keys()) 

        # load line
        for i,line in enumerate(infile):
            try:
                tweet = json.loads(line)
            except json.decoder.JSONDecodeError:
                print(f"Read error in line {i}. Skipping.")
                continue
            
            # skip non-RTs
            if 'retweeted_status' not in tweet:
                continue
            # skip truncation
            if tweet.get('truncated', False):
                continue
            # skip if tweet ID is in excluded_tweet_ids
            if tweet['id_str'] in excluded_tweet_ids:
                continue
            
            # if RT
            if 'full_text' in tweet:
                tweet_text = tweet['full_text']
            else:
                tweet_text = tweet.get('text', np.nan)
                
            row = []
            for field, path in fields.items():
                try:
                    if field == 'text':
                        value = tweet_text
                    else:
                        value = reduce(operator.getitem, path, tweet) 
                except KeyError:
                    value = np.nan
                row.append(value)
            writer.writerow(row)
            if i % 10000 == 0:
                print(f"Processed {i} lines")

if __name__ == "__main__":
    # Path to the CSV file containing tweet IDs to exclude
    EXCLUDED_TWEET_IDS_FILE = '/data/RETWEETS_notruncation_kayla_apr5.csv'
    # Load tweet IDs from the excluded file
    excluded_tweet_ids = load_tweet_ids(EXCLUDED_TWEET_IDS_FILE)

    RAW_DATA_FILE = '/data/tweets_final.json'
    OUT_FILE = '/data/RTS_NOTRUNC_CLAIM.csv'
    
    # Extract tweet metadata
    data_fields = {'tweet_id': ['id_str'], 'user_id': ['user', 'id_str'],
                   'created_at': ['created_at'], 
                   'user_screen_name': ['user', 'screen_name'],
                   'language': ['lang'], 'favorite_count': ['favorite_count'],
                   'retweet_count': ['retweet_count'], 
                   'text': [],
                   'truncated': ['truncated']}
    extract_fields(RAW_DATA_FILE, OUT_FILE, data_fields, excluded_tweet_ids)
