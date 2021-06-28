import time
import tweepy
import pandas as pd

access_token = '1238373367041966080-iJsTgVPqil2euJhkI6sbjrW6dhNhIJ'
access_token_secret = 'PuF9Rqy46CqcSrJ12AAbhYgRG1qajsEwIdISmnCEbhRBe'
consumer_key = 'FqGQo6KVRhDTRsLFSshSxQ8Mq'
consumer_secret = 'IbYlB7vzWjg3JbuCSDMbnZm2ilNa5NLyOeVowL4VMx3OXpjnyN'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

username = 'ICICIBank'
count = 100
try:
    # Creation of query method using parameters
    tweets = tweepy.Cursor(api.user_timeline, id=username, tweet_mode='extended').items(count)

    # Pulling information from tweets iterable object
    tweets_list = [[tweet.created_at, tweet.id, tweet.full_text] for tweet in tweets]

    # Creation of dataframe from tweets list
    # Add or remove columns as you remove tweet information
    tweets_df = pd.DataFrame(tweets_list)
    tweets_df.columns = ["Created_Date", "Twitter_ID", "Text"]
    tweets_df = tweets_df[~tweets_df.Text.str.contains("RT")].reset_index(drop=True)
except BaseException as e:
    print('failed on_status,', str(e))
    time.sleep(3)

tweets_df.to_csv("Result.csv", encoding="utf-8", index=False)
