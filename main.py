import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from dotenv import load_dotenv
import requests
import os
import pymysql
import sys
import json

# My functions
import src.build_db as db


load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
twitter_api_key = os.getenv("TWITTER_API_KEY")
football_api_key = os.getenv("FOOTBALL_API_KEY")
football_api_url = os.getenv("FOOTBALL_API_URL")

teams_list = list()
players_list = list()


if __name__ == '__main__':
    conn = db.connect_to_mysql()
    redo_db = input(
        "Do you wish to drop all tables and repopulate Database y/n ")
    if redo_db == "y" or redo_db == "Y":
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
    print("Ready for next phase")