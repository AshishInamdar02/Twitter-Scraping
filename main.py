import time
import tweepy
import pandas as pd
import datetime
import pytz
import numpy as np
from matplotlib import pyplot as plt
import pypyodbc as py

access_token = '#########################'
access_token_secret = '#########################'
consumer_key = '#########################'
consumer_secret = '#########################e'

cnxn = py.connect('DRIVER={SQL Server};SERVER=#########################;DATABASE=#########################;UID=#########################;PWD=#########################')
cursor = cnxn.cursor()

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)
username = 'ICICIBank' 
count = 100
df=pd.read_csv('Users.csv')
df.rename(columns=df.iloc[0])
l=df['User ID'].tolist()
try:
    # Creation of query method using parameters
    tweets = tweepy.Cursor(api.user_timeline, id=username, tweet_mode='extended').items(count)
    col=["User ID","Username","Tweet ID","Created at","Text","Likes","Retweets","Is Quote","Is Retweet"]
    user_df=pd.DataFrame(columns=col)
    for i in range(len(l)):
        user_tweets = api.search(q='"ICICI Bank" OR @ICICIBank from:{0}'.format(l[i]))
        for tweet in user_tweets:
            x=api.get_status(tweet._json['id_str'])
            user_df=user_df.append({"User ID":tweet._json['user']['id'],"Username":tweet._json['user']['screen_name'],"Tweet ID":tweet._json['id'],"Created at":tweet._json['created_at'],"Text":x.text,"Likes":tweet._json['favorite_count'],"Retweets":tweet._json['retweet_count'],"Is Quote":int(tweet._json['is_quote_status']),"Is Retweet":int(tweet._json['retweeted'])},ignore_index=True)
            
    user_df.set_index("Tweet ID")
    c=cnxn.cursor()
    # Pulling information from tweets iterable object
    tweets_list = [[tweet.id, tweet.created_at.astimezone(pytz.timezone('Asia/Kolkata')), tweet.full_text,tweet._json['favorite_count'],tweet._json['retweet_count'],(datetime.datetime.utcnow()-tweet.created_at).total_seconds()/3600.00,3600.00*tweet._json['favorite_count']/(datetime.datetime.utcnow()-tweet.created_at).total_seconds(),3600.00*tweet._json['retweet_count']/(datetime.datetime.utcnow()-tweet.created_at).total_seconds()] for tweet in tweets]
    # Creation of dataframe from tweets list
    # Add or remove columns as you remove tweet information
    tweets_df = pd.DataFrame(tweets_list)
    
    tweets_df.columns = ["Tweet_ID","Created_Date","Text","Likes","Retweets","Elapsed (Hours)","Like Score","Retweet Score"]
    tweets_df = tweets_df[~tweets_df.Text.str.contains("RT")].reset_index(drop=True)
    for i,row in tweets_df.iterrows():
        c.execute('SELECT TweetID FROM LIGHTHOUSE_BANKING.dbo.ZSG_Client_Tweets WHERE TweetID=?',(row["Tweet_ID"],))
        d=c.fetchall()
        if len(d)!=0:
            continue
        c.execute('SET IDENTITY_INSERT LIGHTHOUSE_BANKING.dbo.ZSG_Client_Tweets ON')
        c.execute("INSERT INTO LIGHTHOUSE_BANKING.dbo.ZSG_Client_Tweets(TweetID,CreatedAt,Text,Likes,Retweets,Elapsed,LikeScore,RetweetScore)values(?,?,?,?,?,?,?,?)",(row["Tweet_ID"],row["Created_Date"],row["Text"],row["Likes"],row["Retweets"],row["Elapsed (Hours)"],row["Like Score"],row["Retweet Score"]))
        cnxn.commit()
    for i,row in user_df.iterrows():
        c.execute('SELECT TweetID FROM LIGHTHOUSE_BANKING.dbo.ZSG_User_Tweets WHERE TweetID=?',(row["Tweet ID"],))
        d=c.fetchall()
        if len(d)!=0:
            continue
        c.execute('SET IDENTITY_INSERT LIGHTHOUSE_BANKING.dbo.ZSG_User_Tweets ON')
        c.execute("INSERT INTO LIGHTHOUSE_BANKING.dbo.ZSG_User_Tweets(UserID,Username,TweetID,CreatedAt,Text,Likes,Retweets,IsQuote,IsRetweet)values(?,?,?,?,?,?,?,?,?)",(row["User ID"],row["Username"],row["Tweet ID"],row["Created at"],row["Text"],row["Likes"],row["Retweets"],row["Is Quote"],row["Is Retweet"]))
        cnxn.commit()
except BaseException as e:
    print('failed on_status,', str(e))
    time.sleep(3)
tweets_df.to_csv("Score.csv", encoding="utf-8", index=False)
X_axis = np.arange(len(tweets_df["Tweet_ID"]))
plt.figure(1)
plt.bar(X_axis, tweets_df["Like Score"], 0.4, label = 'Like Score (Avg Likes/hour)')
plt.xticks(X_axis, tweets_df["Tweet_ID"], rotation=90)
plt.xlabel("Tweet ID")
plt.ylabel("Like Score")
plt.title("Like Score (Avg Likes/hour)")
plt.legend()
plt.savefig('LikeScore.png')
plt.show()
plt.figure(2)
plt.bar(X_axis, tweets_df["Retweet Score"], 0.4, label = 'Retweet Score (Avg Retweets/hour)')
plt.xticks(X_axis, tweets_df["Tweet_ID"], rotation=90)
plt.xlabel("Tweet ID")
plt.ylabel("Retweet Score")
plt.title("Retweet Score (Avg Retweets/hour)")
plt.legend()
plt.savefig('RetweetScore.png')
plt.show()
