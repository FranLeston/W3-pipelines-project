import pandas as pd
import numpy as np


def get_dfs_and_clean(conn):

    # first do the player

    df_laliga_players = pd.read_sql_query(
        """
              SELECT 
              Player.*, Team.name AS team_name
              FROM
              Players AS Player
              LEFT JOIN
              Teams AS Team ON Team.team_id = Player.team_id
              """, conn
    )

    # clean columns
    df_laliga_players["height"] = df_laliga_players["height"].apply(
        clean_height)
    df_laliga_players["weight"] = df_laliga_players["weight"].apply(
        clean_weight)
    df_laliga_players["goals_minutes_coeff"] = df_laliga_players["goals"] / \
        df_laliga_players["minutes"]
    df_laliga_players["saves_minutes_coeff"] = df_laliga_players["saves"] / \
        df_laliga_players["minutes"]
    df_laliga_players["assists_minutes_coeff"] = df_laliga_players["assists"] / \
        df_laliga_players["minutes"]
    df_laliga_players["rating_minutes_coeff"] = df_laliga_players["rating"] / \
        df_laliga_players["minutes"]

    # Remove columns I wont need
    df_cleaned = df_laliga_players.drop(columns=["player_id", "team_id"])

    # Set Id as new index
    df_cleaned.set_index('id', inplace=True)

    df = df_cleaned.sort_values(
        by=['rating'], ascending=False)

    # num of titles
    df_titles = pd.read_sql_query(
        """
        SELECT 
        Player.id, COUNT(*) AS titles
        FROM
        Players AS Player
        LEFT JOIN
        Teams AS Team ON Team.team_id = Player.team_id
        LEFT JOIN
        Trophies AS Trophy ON Trophy.player_id = Player.player_id
        WHERE
        Trophy.is_winner = 1
        GROUP BY Player.player_id
        ORDER BY titles DESC
        """, conn)
    df = df.join(df_titles.set_index('id'), on='id', )
    df["titles"] = df["titles"].fillna(0)

    df.to_csv("data/la_liga_players.csv")
    print("Cleaned Players DF from DB-->")
    print(df.head(20))
    print(df.shape)

    # Second do the Trophies
    df_laliga_trophies = pd.read_sql_query(
        """
        SELECT Trophies.id, Trophies.league, Trophies.region, Trophies.season,Players.name
        FROM Trophies
        LEFT JOIN Players
        ON Trophies.player_id = Players.player_id
        where Trophies.is_winner = 1
              """, conn
    )

    df_laliga_trophies.set_index('id', inplace=True)
    df_laliga_trophies.to_csv("data/player_trophies.csv")
    print("Cleaned Trophies DF from DB-->")
    print(df_laliga_trophies.head(20))
    print(df_laliga_trophies.shape)
    print("Exported .csv files to the Data folder.")
    return "Finished all tasks...CSV FILES READY FOR USE"


def clean_height(height):
    if height == "None":
        return np.nan
    else:
        return int(height[:-3])


def clean_weight(weight):
    if weight == "None":
        return np.nan
    else:
        return int(weight[:-3])
