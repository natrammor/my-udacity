import configparser


# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh-cluster.cfg'))
ARN = config.get('IAM_ROLE','ARN')

# DROP TABLES
staging_events_table_drop = """DROP TABLE IF EXISTS staging_events"""
staging_songs_table_drop = """DROP TABLE IF EXISTS staging_songs"""
songplays_table_drop = """DROP TABLE IF EXISTS songplays"""
users_table_drop = """DROP TABLE IF EXISTS users"""
songs_table_drop = """DROP TABLE IF EXISTS songs"""
artists_table_drop = """DROP TABLE IF EXISTS artists"""
time_table_drop = """DROP TABLE IF EXISTS time"""


# CREATE TABLES
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
    event_id         INT IDENTITY(0,1) PRIMARY KEY, 
    artist           VARCHAR,
    auth             VARCHAR,
    firstName        VARCHAR,
    gender           VARCHAR, 
    itemInSession    INT, 
    lastName         VARCHAR, 
    length           NUMERIC, 
    level            VARCHAR, 
    location         VARCHAR, 
    method           VARCHAR, 
    page             VARCHAR, 
    registration     NUMERIC, 
    sessionId        INT, 
    song             VARCHAR, 
    status           INT, 
    ts               BIGINT, 
    userAgent        VARCHAR, 
    userId           VARCHAR    
)""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    stage_song_id    INT IDENTITY(0,1) PRIMARY KEY,
    num_songs        INT, 
    artist_id        VARCHAR, 
    artist_latitude  NUMERIC, 
    artist_longitude NUMERIC, 
    artist_location  VARCHAR, 
    artist_name      VARCHAR, 
    song_id          VARCHAR, 
    title            VARCHAR, 
    duration         NUMERIC, 
    year             INT
)""")

songplays_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
    songplay_id      INT IDENTITY(0,1) PRIMARY KEY, 
    start_time       BIGINT NOT NULL REFERENCES time(start_time), 
    user_id          VARCHAR REFERENCES users(user_id), 
    level            VARCHAR sortkey, 
    song_id          VARCHAR REFERENCES songs(song_id), 
    artist_id        VARCHAR REFERENCES artists(artist_id), 
    session_id       INT NOT NULL, 
    location         VARCHAR distkey, 
    user_agent       VARCHAR
)""")

users_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_key     INT IDENTITY(0,1),
    user_id      VARCHAR PRIMARY KEY sortkey, 
    first_name   VARCHAR, 
    last_name    VARCHAR, 
    gender       VARCHAR, 
    level        VARCHAR distkey
)""")

songs_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id    VARCHAR PRIMARY KEY sortkey, 
    title      VARCHAR NOT NULL, 
    artist_id  VARCHAR NOT NULL, 
    year       INT, 
    duration   NUMERIC(10,5)
)
diststyle all;""")

artists_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
    artist_key  INT IDENTITY(0,1),
    artist_id   VARCHAR PRIMARY KEY sortkey, 
    name        VARCHAR NOT NULL, 
    location    VARCHAR, 
    latitude    NUMERIC, 
    longitude   NUMERIC
)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time   BIGINT NOT NULL PRIMARY KEY sortkey,
    hour         INT NOT NULL, 
    day          INT NOT NULL, 
    week         INT NOT NULL, 
    month        INT NOT NULL, 
    year         INT NOT NULL, 
    weekday      INT NOT NULL
)
diststyle all;""")


# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events FROM 's3://udacity-dend/log_data' 
    CREDENTIALS 'aws_iam_role={}'
    json 's3://udacity-dend/log_json_path.json';
""").format(ARN)

staging_songs_copy = ("""
    COPY staging_songs FROM 's3://udacity-dend/song_data' 
    CREDENTIALS 'aws_iam_role={}'
    json 'auto';
""").format(ARN)


# FINAL TABLES
songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT    staging_events.ts        AS start_time,
                       staging_events.userId    AS user_id,
                       staging_events.level     AS level,
                       songs.song_id            AS song_id,
                       artists.artist_id        AS artist_id,
                       staging_events.sessionId AS session_id,
                       staging_events.location  AS location,
                       staging_events.userAgent AS user_agent

    FROM staging_events 
    LEFT JOIN artists ON staging_events.artist = artists.name
    LEFT JOIN songs ON staging_events.song = songs.title
    WHERE page='NextSong';
                       
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId     AS user_id,
                    firstName  AS first_name,
                    lastName   AS last_name,
                    gender     AS gender,
                    level      AS level
            
    FROM staging_events;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT     song_id    AS song_id,
                        title      AS title,
                        artist_id  AS artist_id,
                        year       AS year,
                        duration   AS duration
            
    FROM staging_songs;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT  artist_id                AS artist_id,
                     artist_name              AS name,
                     artist_location          AS location,
                     artist_latitude          AS latitude,
                     artist_longitude         AS longitude
            
    FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT    ts                                                                       AS start_time,
                        EXTRACT(hour FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'))    AS hour,
                        EXTRACT(day FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'))     AS day,
                        EXTRACT(week FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'))    AS week,
                        EXTRACT(month FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'))   AS month,
                        EXTRACT(year FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'))    AS year,
                        EXTRACT(weekday FROM (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) AS weekday

    FROM staging_events;          
""")


# DELETING DUPLICATES:
artist_table_delete_duplicates = ("""DELETE artists 
    WHERE artist_key NOT IN 
    (SELECT MAX(artist_key)
    FROM artists
    GROUP BY artist_id); 
""")

user_table_delete_duplicates = ("""DELETE users 
    WHERE user_key NOT IN 
    (SELECT MAX(user_key)
    FROM users
    GROUP BY user_id); 
""")


# DELETE THE COLUMNS THAT HELPED CLEANING THE TABLES FROM DUPLICATES
artist_table_delete_column = ("""
    ALTER TABLE artists
    DROP COLUMN artist_key;
""")

user_table_delete_column = ("""
    ALTER TABLE users
    DROP COLUMN user_key;
""")


# QUERY LISTS
create_table_queries = [users_table_create, songs_table_create, artists_table_create, time_table_create, songplays_table_create, staging_events_table_create, staging_songs_table_create]
drop_table_queries = [songplays_table_drop, users_table_drop, songs_table_drop, artists_table_drop, time_table_drop, staging_events_table_drop, staging_songs_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
delete_duplicates_queries = [artist_table_delete_duplicates, user_table_delete_duplicates]
delete_columns_queries = [artist_table_delete_column, user_table_delete_column]
