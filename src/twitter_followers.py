import pandas as pd
from dotenv import load_dotenv
import os

import tweepy

load_dotenv()

tw_consumer_key = os.getenv("TWEEPY_CONSUMER_KEY")
tw_consumer_secret = os.getenv("TWEEPY_CONSUMER_SECRET")
tw_access_token = os.getenv("TWEEPY_ACCESS_TOKEN")
tw_access_token_secret = os.getenv("TWEEPY_ACCESS_TOKEN_SECRET")

auth = tweepy.OAuthHandler(tw_consumer_key, tw_consumer_secret)
auth.set_access_token(tw_access_token, tw_access_token_secret)
api = tweepy.API(auth)


def get_twitter_followers(conn):
    # Get list of player names from DB
    df_players = pd.read_sql_query(
        """
        SELECT
        player_id,
        twitter_username
        FROM
        laliga.Players
        where twitter_username != "None"
        ;
        """, conn
    )

    df_players["followers"] = df_players['twitter_username'].apply(
        get_username_followers)

    followers_list = df_players.values.tolist()
    print(followers_list)
    return followers_list


def get_username_followers(name):
    user = api.get_user(name)
    return user.followers_count


def update_followers_db(conn, followers_list):

    for player in followers_list:
        p_id = player[0]
        followers = player[2]
        conn.execute(f'''
                        UPDATE Players
                        SET
                        twitter_followers = {followers}
                        WHERE
                        player_id = {p_id}
                        ''')
    return "Finished getting number of Twitter Followers and adding them to the DB!"
