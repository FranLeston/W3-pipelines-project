import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from dotenv import load_dotenv
import requests
import os
import pymysql
import sys
import json


load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
football_api_key = os.getenv("FOOTBALL_API_KEY")
football_api_url = os.getenv("FOOTBALL_API_URL")

teams_list = list()
players_list = list()


def connect_to_mysql():
    connectionData = f"mysql+pymysql://{db_user}:{db_password}@localhost/laliga"
    try:

        engine = create_engine(connectionData, echo=False)
        if not database_exists(engine.url):
            create_database(engine.url)

        print(database_exists(engine.url))
        print("Great..we have connected to the DB")
        conn = engine.connect()

        return conn
    except Exception as error:
        print("Oh no..could not connect to the DB. Exiting...")
        print(error)
        sys.exit()


def create_schemas(conn):
    # drop all tables and re create
    # Teams Schema
    try:
        print("Creating Teams table...")
        conn.execute('DROP TABLE IF EXISTS Trophies;')
        conn.execute('DROP TABLE IF EXISTS Players;')
        conn.execute('DROP TABLE IF EXISTS Teams;')

        conn.execute('CREATE TABLE IF NOT EXISTS Teams('
                     'id int not null auto_increment primary key,'
                     'team_id int not null unique,'
                     'name varchar(50),'
                     'founded varchar(50))'
                     'ENGINE=INNODB;')
        print("Finished!")
    except Exception as error:
        print("There was an error creating the Teams Table...exiting")
        print(error)
        sys.exit()

    # Player Schema

    try:
        print("Creating the Players Table")
        conn.execute('CREATE TABLE IF NOT EXISTS Players('
                     'id int not null auto_increment primary key,'
                     'player_id int not null unique,'
                     'team_id int,'
                     'position varchar(50),'
                     'name varchar(50),'
                     'first_name varchar(50),'
                     'last_name varchar(50),'
                     'twitter_username varchar(50),'
                     'twitter_followers int,'
                     'age tinyint,'
                     'nationality varchar(50),'
                     'height varchar(50),'
                     'weight varchar(50),'
                     'minutes int,'
                     'goals int,'
                     'assists int,'
                     'saves int,'
                     'rating float,'
                     'FOREIGN KEY(team_id) REFERENCES Teams(team_id)) ENGINE=INNODB;'
                     )

    except Exception as error:
        print("There was an error creating the Player Table...exiting")
        print(error)
        sys.exit()

    # Player Trophies
    try:
        print("Creating the Trophies Table")
        conn.execute('CREATE TABLE IF NOT EXISTS Trophies('
                     'id int not null auto_increment primary key,'
                     'player_id int,'
                     'league varchar(50),'
                     'region varchar(50),'
                     'season varchar(50),'
                     'place varchar(50),'
                     'is_winner boolean,'
                     'FOREIGN KEY(player_id) REFERENCES Players(player_id)) ENGINE=INNODB;'
                     )

    except Exception as error:
        print("There was an error creating the Trophies Table...exiting")
        print(error)
        sys.exit()

    return True


def get_liga_teams():
    if not football_api_key:
        raise ValueError(
            "Please provide a valid Football API Token in the .env file")

    headers = {
        'x-rapidapi-host': "v3.football.api-sports.io",
        'x-rapidapi-key': f"{football_api_key}"
    }
    parameters = {"league": "140", "season": "2020"}
    res = requests.request(
        "GET", football_api_url + "/teams", params=parameters, headers=headers).json()
    teams_list = res["response"]

    return teams_list


def get_liga_players():
    if not football_api_key:
        raise ValueError(
            "Please provide a valid Football API Token in the .env file")

    headers = {
        'x-rapidapi-host': "v3.football.api-sports.io",
        'x-rapidapi-key': f"{football_api_key}"
    }
    parameters = {"league": "140", "season": "2020", "page": 1}
    print("Getting Page 1 of the players list from the API:")
    res = requests.request(
        "GET", football_api_url + "/players", params=parameters, headers=headers).json()

    total_pages = int(res["paging"]["total"])
    players_list.append(res["response"])

    for page in range(2, total_pages + 1):
        print(f"Getting Page {page} of the players list from the API:")

        parameters = {"league": "140", "season": "2020", "page": page}
        res = requests.request(
            "GET", football_api_url + "/players", params=parameters, headers=headers).json()
        players_list.append(res["response"])

    print("Flattening list of lists of players...")
    flat_list = [item for sublist in players_list for item in sublist]
    return flat_list


def add_data_to_db(conn, players_list, teams_list):
    try:
        # Teams first
        for ele in teams_list:
            print(f"Adding Team {ele['team']['name']} info to the Database...")

            team_id = int(ele['team']['id'])
            team_name = str(ele['team']['name'])
            team_founded = str(ele['team']['founded'])

            conn.execute("INSERT INTO Teams (team_id, name, founded) VALUES"
                         f"({team_id}, '{team_name}', '{team_founded}')"
                         )

        # Players
        for ele in players_list:
            print(
                f"Adding player {ele['player']['name']} info to the Database...")

            player_id = int(ele['player']['id'])
            player_team_id = int(ele['statistics'][0]['team']['id'])
            player_position = str(ele['statistics'][0]['games']['position'])
            player_name = str(ele['player']['name'])
            player_first_name = str(ele['player']['firstname'])
            player_last_name = str(ele['player']['lastname'])
            try:
                player_age = int(ele['player']['age'])
            except:
                player_age = 'NULL'

            player_nationality = str(ele['player']['nationality'])

            player_nationality = player_nationality.replace("'", " ")

            player_weight = str(ele['player']['weight'])
            player_height = str(ele['player']['height'])

            try:
                player_minutes = int(ele['statistics'][0]['games']['minutes'])
            except:
                player_minutes = 0

            try:
                player_goals = int(ele['statistics'][0]['goals']['total'])
            except:
                player_goals = 0

            try:
                player_assists = int(ele['statistics'][0]['goals']['assists'])
            except:
                player_assists = 0

            try:
                player_saves = int(ele['statistics'][0]['goals']['saves'])
            except:
                player_saves = 0

            player_rating = ele['statistics'][0]['games']['rating']
            if not player_rating:
                player_rating = float(0.0)
            else:
                player_rating = float(player_rating)

            # Verified Twitter UserName of known PLayers
            if player_id == 154:
                player_twitter_username = "TeamMessi"
            elif player_id == 136:
                player_twitter_username = "3gerardpique"
            elif player_id == 738:
                player_twitter_username = "SergioRamos"
            elif player_id == 144:
                player_twitter_username = "5sergiob"
            elif player_id == 743:
                player_twitter_username = "MarceloM12"
            elif player_id == 137:
                player_twitter_username = "SergiRoberto10"
            elif player_id == 139:
                player_twitter_username = "samumtiti"
            elif player_id == 157:
                player_twitter_username = "LuisSuarez9"
            elif player_id == 128:
                player_twitter_username = "JordiAlba"
            elif player_id == 759:
                player_twitter_username = "Benzema"
            elif player_id == 752:
                player_twitter_username = "ToniKroos"
            else:
                player_twitter_username = "None"

            conn.execute("INSERT INTO Players (player_id, team_id, position, name, first_name, last_name, twitter_username, age, nationality, height, weight, minutes, goals, assists, saves, rating) VALUES"
                         f"({player_id},{player_team_id},'{player_position}','{player_name}', '{player_first_name}', '{player_last_name}', '{player_twitter_username}',{player_age},'{player_nationality}','{player_height}','{player_weight}',{player_minutes},{player_goals},{player_assists},{player_saves},{player_rating})"
                         )

    except Exception as error:
        print(error)
        print("Something went wrong adding the data to the db")
        sys.exit()
    return "Added player and teams to DB Succesfully!"


def get_player_trophies(conn, players_list):
    print("Getting list of trophies for each player and adding them to DB..this can take a few minutes...")
    for ele in players_list:
        player_id = int(ele['player']['id'])

        if not football_api_key:
            raise ValueError(
                "Please provide a valid Football API Token in the .env file")

        headers = {
            'x-rapidapi-host': "v3.football.api-sports.io",
            'x-rapidapi-key': f"{football_api_key}"
        }
        parameters = {"player": player_id}
        print(f"Getting Trophies belonging to player id: {player_id}...")
        try:
            res = requests.request(
                "GET", football_api_url + "/trophies", params=parameters, headers=headers).json()

            player_trophies_list = res["response"]
            for trophy in player_trophies_list:
                trophy_league = trophy["league"]
                trophy_league = trophy_league.replace("'", " ")

                trophy_region = trophy["country"]
                trophy_season = trophy["season"]
                trophy_place = trophy["place"]
                trophy_is_winner = 0
                if trophy_place == "Winner":
                    trophy_is_winner = 1

                conn.execute("INSERT INTO Trophies (player_id, league, region, season, place, is_winner) VALUES"
                             f"({player_id}, '{trophy_league}', '{trophy_region}', '{trophy_season}', '{trophy_place}', '{trophy_is_winner}')"
                             )
        except Exception as error:
            print(error)
            print("Could not get the player trophies. Exiting...")
    return "Finished Getting all the player trophies and added them to the trophies table!"
