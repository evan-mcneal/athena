import json
import os
import pandas as pd
import time

from typing import Tuple

import s3fs
import tweepy


TWITTER_KEY_PATH = "./twitter_keys.txt"
TWEET_DF_PATH = "https://twitterathena.s3.us-east-2.amazonaws.com/tweet_df.csv"
CITIES_DF_PATH = "https://twitterathena.s3.us-east-2.amazonaws.com/cities.csv"


def create_twitter_api(twitter_key_path: str):
    with open(TWITTER_KEY_PATH) as json_file:
        keys = json.load(json_file)

    API_KEY = keys["API_KEY"]
    API_SECRET_KEY = keys["API_SECRET_KEY"]

    ACCESS_TOKEN = keys["ACCESS_TOKEN"]
    ACCESS_TOKEN_SECRET = keys["ACCESS_TOKEN_SECRET"]

    # Authenticate to Twitter
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)

    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    api = tweepy.API(
        auth,
        wait_on_rate_limit=True,
        wait_on_rate_limit_notify=True
    )

    api.verify_credentials()

    return api


def create_tweet_df(
        api,
        radius="100mi",
        most_recent_id=False, 
        max_id=False
        ) -> Tuple[pd.DataFrame, int]:
    
    tweet_tuples = []

    for tweet in tweepy.Cursor(
        api.search,
        q="*",
        lang="en",
        result_type="popular",
        max_id=max_id,
        tweet_mode="extended",
        count = 100
    ).items():

        tweet_info = (
            tweet.full_text, 
            tweet.favorite_count, 
            tweet.retweet_count, 
            tweet.user.followers_count, 
            tweet.created_at,
            tweet.id
        )

        tweet_tuples.append(tweet_info)

    tweet_df = pd.DataFrame(
        tweet_tuples, 
        columns =[
            'Tweet', 
            'Num_Favorites', 
            'Num_Retweets', 
            'Num_Followers',
            'Created_At',
            'id'
        ])
    
    tweet_df.sort_values('Created_At', inplace = True)
    
    return tweet_df


def tweet_pull(event, context):

    api = create_twitter_api(TWITTER_KEY_PATH)

    all_tweets_df = pd.read_csv(TWEET_DF_PATH).sort_values('Created_At')

    most_recent_id = all_tweets_df['id'].iloc[-1]

    max_tweets = 1000000
    num_tweets = all_tweets_df.shape[0]

    while num_tweets < max_tweets:
        
        new_tweets_df = create_tweet_df(api, most_recent_id=most_recent_id)

        all_tweets_df = all_tweets_df.append(new_tweets_df, ignore_index=True)

        all_tweets_df.drop_duplicates(inplace = True)

        if num_tweets != all_tweets_df.shape[0]:
            num_tweets = all_tweets_df.shape[0]

        else: # If there are no new tweets
            break

    all_tweets_df.to_csv(TWEET_DF_PATH, index=False)
