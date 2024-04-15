import json
import pandas as pd
import csv
import numpy as np
from functools import reduce
import operator

# extract fields from non-truncated retweets 

def extract_fields(inpath, outpath, fields):
    
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
            
            # if RT
            if 'retweeted_status' in tweet:
                retweet = tweet['retweeted_status']
                # if full_text exists, use it 
                if 'full_text' in retweet:
                    tweet_text = retweet['full_text']
                    retweeted_user_id = retweet['user']['id_str']
                    retweeted_user_screen_name = retweet['user']['screen_name']
                    retweet_id = retweet['id_str']
                # otherwise, check first for truncation, then use 'text' if NOT truncated (inside the RT)
                elif not retweet.get('truncated', False):
                        tweet_text = retweet.get('text', np.nan)
                        retweeted_user_id = retweet['user']['id_str']
                        retweeted_user_screen_name = retweet['user']['screen_name']
                        retweet_id = retweet['id_str']
                else:
                    continue
                

            # initialize empty list (for data_fields down below)
            row = []
            for field, path in fields.items():
                try:
                    if field == 'text':
                        value = tweet_text
                    elif field == 'retweeted_user_id':
                        value = retweeted_user_id
                    elif field == 'retweeted_user_screen_name':
                        value = retweeted_user_screen_name
                    elif field == 'retweet_id':
                        value = retweet_id
                    elif field == 'truncated':
                        value = retweet.get('truncated')
                    elif field == 'RT':  # Check if the field is 'RT'
                        value = 1
                    else:
                        value = reduce(operator.getitem, path, tweet)
                except KeyError:
                    value = np.nan
                row.append(value)
            writer.writerow(row)

            if i % 10000 == 0:
                print(f"Processed {i} lines")
    
    print(f"Processing completed. Total {i} lines processed.")
    



if __name__ == "__main__":

    RAW_DATA_FILE = '/data/tweets_final.json'
    OUT_FILE = '/data/RETWEETS_notruncation_kayla.csv'
    
    # Modify the fields dictionary to reflect the desired output fields
    data_fields = {
        'tweet_id': ['id_str'],
        'user_id': ['user', 'id_str'],
        'created_at': ['created_at'], 
        'user_screen_name': ['user', 'screen_name'],
        'language': ['lang'],
        'favorite_count': ['favorite_count'],
        'retweet_count': ['retweet_count'], 
        'text': [],  
        'retweeted_user_id': [],  
        'retweeted_user_screen_name': [], 
        'retweet_id': [], 
        'truncated': [],  
        'RT': []
    }
    
    extract_fields(RAW_DATA_FILE, OUT_FILE, data_fields)


