from cfg import DBCONN, TOKEN
import pandas as pd 
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import sqlite3
import requests
import json
from datetime import datetime
import datetime
import logging


logging.basicConfig(level=logging.INFO, filename="info.log", filemode="w",
                    format= "%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger()


def extract() -> pd.DataFrame:
    """Extract data from Spotify API for developers

    Returns:
        pd.DataFrame: DataFrame with all the recently played songs
    """    
    
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    log.info("Extracting data from the API")

    today = datetime.datetime.now()
    week = today - datetime.timedelta(weeks=1)
    week_unix_timestamp = int(week.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=week_unix_timestamp), headers = headers)

    api_data = r.json()

    #Create empty lists to append data
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in api_data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
        
    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

    return song_df    


def validate(df: pd.DataFrame) -> bool:
    """Validate data withind df, check for empty df, null values or pk violation

    Args:
        df (pd.DataFrame): song_df

    Raises:
        Exception: _description_
        Exception: _description_

    Returns:
        bool: _description_
    """    

    if df.empty:
        log.info("DataFrame is empty")
        return False 

    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        log.warning("Primary key violated")
        raise Exception("Primary Key check is violated")

    if df.isnull().values.any():
        log.warning("Founded null values, finishing execution")
        raise Exception("Null values found")

    return True


def load(df: pd.DataFrame):
    """Load data into the DB from the dataframe created at the extract function

    Args:
        df (pd.DataFrame): song dataframe
    """    
    engine = sqlalchemy.create_engine(DBCONN)
    conn = sqlite3.connect('spotify_data.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS recently_played(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    log.info("Opening database")

    try:
        df.to_sql("recently_played", engine, index=False, if_exists='replace')
        log.info("Loading data")
    except:
        log.info("Data exists in db")

    conn.close()
    log.info("Closing the database")


if __name__ == '__main__':
    final_df = extract()
    
    if validate(final_df):
        log.info("Data is valid, proceeding to load")
        load(final_df)