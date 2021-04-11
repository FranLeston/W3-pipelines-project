import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from dotenv import load_dotenv
import requests
import os
import pymysql
import sys
import json
import tweepy

# My functions
import src.build_db as db
import src.twitter_followers as tf
import src.data_cleaning as clean

load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
twitter_api_key = os.getenv("TWITTER_API_KEY")
football_api_key = os.getenv("FOOTBALL_API_KEY")
football_api_url = os.getenv("FOOTBALL_API_URL")


teams_list = list()
players_list = list()


def build_and_seed_db():
    is_schema_done = db.create_schemas(conn)
    if is_schema_done:
        teams_list = db.get_liga_teams()
        players_list = db.get_liga_players()

    if teams_list and players_list:
        # Lets add the information to our database
        status = db.add_data_to_db(conn, players_list, teams_list)
        print(status)
        trophies_response = db.get_player_trophies(conn, players_list)
        print(trophies_response)
    return


if __name__ == '__main__':
    conn = db.connect_to_mysql()
    redo_db = input(
        "Do you wish to drop all tables and repopulate Database? (y)/(n): ")

    if redo_db.lower() == "y":
        build_and_seed_db()

    redo_followers = input(
        "Do you want to repopulate twitter followers usig Tweepy? (y)/(n): ")

    if redo_followers.lower() == "y":
        print("Getting twitter followers for known players")
        followers_list = tf.get_twitter_followers(conn)
        result = tf.update_followers_db(conn, followers_list)
        print(result)

    try:
        print("Preparing Data from Database for use in DataFrame and Jupyter Notebook..")
        result = clean.get_dfs_and_clean(conn)
        print(result)
    except Exception as error:
        print(error)
        print("Something went wrong. Did you run database creation first? Make you have all .env variables defined. Exiting")
        sys.exit()
