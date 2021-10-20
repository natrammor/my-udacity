import os
import glob
import psycopg2
import pandas as pd

from sql_queries import *


def process_song_file(cur, filepath):
    """
    This function gathers the information from JSON files containing metadata about the songs,
    processes this information and saves the data in the dimension tables "songs" and "artists"
    """
    # Read the JSON files from the data/song_data folder
    df = pd.read_json(filepath, lines=True)

    # Insert song records
    song_id = df["song_id"].values[0]
    title = df["title"].values[0]
    artist_id = df["artist_id"].values[0]
    year = df["year"].values.astype(float)[0]
    duration = df["duration"].values[0]
    song_data = (song_id, title, artist_id, year, duration)
    cur.execute(song_table_insert, song_data)
    
    # Insert artist records
    artist_id = df["artist_id"].values[0]
    artist_name = df["artist_name"].values[0]
    location = df["artist_location"].values[0]
    latitude = df["artist_latitude"].values.astype(float)[0]  # Otherwise it gives error due to formatting
    longitude = df["artist_longitude"].values.astype(float)[0]  # Otherwise it gives error due to formatting
    artist_data = (artist_id, artist_name, location, latitude, longitude)
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function gathers the information from JSON files containing log data, processes this 
    information and saves the data in the dimension tables "time" and "users".
    Fetching the necessary data from other tables, it also fills the *songplays* table
    """
    # Read the JSON files from the data/log_data folder
    df = pd.read_json(filepath, lines=True)

    # Filter by NextSong action
    df = df.loc[df['page'] == "NextSong"]

    # Convert timestamp column to datetime
    t_timestamp = df['ts']
    t_datetime = pd.to_datetime(df['ts'], unit='ms')  # Be careful! The unit needs to be given
    
    # Insert time data records: timestamp, hour, day, week of year, month, year, and weekday
    time_data = {"timestamp": t_timestamp, "hour": t_datetime.dt.hour, "day": t_datetime.dt.day, "week": t_datetime.dt.week,\
                 "month": t_datetime.dt.month, "year": t_datetime.dt.year, "weekday": t_datetime.dt.weekday}
    time_df = pd.DataFrame.from_dict(time_data, orient='columns') 
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # Load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    # Insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # Insert songplay records: songid and artistid are fetched from songs and artists tables
    for index, row in df.iterrows():
        cur.execute(song_select, (row.artist, row.song, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # Select the timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent and set to songplay_data
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # Get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # Get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # Iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    conn.set_session(autocommit=True)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    # Validation: check that only one row in songplays is different from None in artist_id and song_id
    cur.execute("""SELECT COUNT(artist_id) from songplays WHERE artist_id IS NOT NULL AND song_id IS NOT NULL""")
    count_var = cur.fetchone()
    print(count_var)
    if count_var != 0:
        print("!!! The pipeline seems good")
    else:
        print("!!! CHECK THE ETL PIPELINE, SOMETHING SEEMS TO BE WRONG!!")

    conn.close()


if __name__ == "__main__":
    main()