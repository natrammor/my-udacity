# Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.

A startup called Sparkify has grown their database and want to move their processes and data onto the cloud. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Project Datasets:
    Song data: s3://udacity-dend/song_data
    Log data: s3://udacity-dend/log_data
    Log data json path: s3://udacity-dend/log_json_path.json

The goal of this project is to build an ETL pipeline that extracts the data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team.


# State and justify your database schema design.

The database is based on a star schema. The **fact table** is designed in a way, that the desired queries for the purpose of the anaylisis can be directly done without the need of JOINS. This will spare a significant amount of time when retrieving data as the content of the database will grow.

The **fact table** is called **songplays** and the **dimension tables** are **users**, **time**, **songs** and **artists**. An entity relationship diagram can be found under /home/workspace/ERD_star_schema.png. Sortkeys and distkeys have been set to some columns of the tables, according to the type of queries that the users of the database are likely to perform.

Apart from these tables, two staging tables are created in order to extract and transform the data from S3. To design the staging tables, it is important to understand the datatypes and format of the data in S3.

Example of LOG data:

`{"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}`

Example of SONGS data:

`{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}`

The two staging tables are **staging_events** (with a primary key called *event_id* of type INT IDENTITY(0,1)) and **staging_songs** (with a primary key called *stage_song_id* of type INT IDENTITY(0,1)).


# State and justify your ETL pipeline.

To fill the database according to this schema, an ETL pipeline has been designed (please check the python file *etl.py*). The pipeline loads data from S3 into staging tables on Redshift and then processes that data into the analytics tables on Redshift. Four functions have been created to that aim:

*load_staging_tables*: the pipeline first grabs data from the song_data directory and then from the log_data directory. For the log_data, since the objects don't correspond directly to column names, a JSONPaths file is used to map the JSON elements to columns.

*insert_tables*: the data is loaded from staging tables to the dimension tables (users, artists, songs and time) on Redshift.

*delete_duplicates*: since on Redshift, the PRIMARY and FOREIGN KEYS are informational only, they do not prevent tables from having duplicates. That is why, a cleaning process is needed at this stage. More specifically, the **users** and the **artists** tables are affected by this issue. For that goal, the original tables contain a unique field that will allow to identify the duplicates and delete them: **user_key** and **artist_key**. These columns will be later deleted, once the duplicates are erased.

*insert_songplays*: once all the dimension tables are cleaned, the data is inserted into the songplays table. The decision to do it this way, is to prevent errors due to the FOREIGN KEYS in the songplays linking to the **artists** and **users** tables.


# Instructions.
1.- Launch a redshift cluster and create an IAM role that has read access to S3.
2.- Add redshift database and IAM role info to dwh-cluster.cfg (lines 2 and 9 to be completed).
3.- In a shell terminal, run `python create_tables.py` to drop the tables (if existing) and create them from scratch in the redshift database
4.- Run the ETL pipeline as `python etl.py`


# Provide example queries and results for song play analysis.
`SELECT song_id from songplays GROUP BY song_id ORDER BY song_id;`
Output:
SOAAZWW12A8AE460E9
SOABIXP12A8C135F75
SOACGMN12A8C13AD65
SOACRBY12AB017C757
SOACXKV12AB0189328

`SELECT location, level, COUNT(level) FROM songplays GROUP by location, level ORDER BY level DESC;`
Here an extract of the output:

Janesville-Beloit, WI	paid	250
Portland-South Portland, ME	paid	708
Longview, TX	paid	18
Chicago-Naperville-Elgin, IL-IN-WI	paid	477
Lansing-East Lansing, MI	paid	577
San Francisco-Oakland-Hayward, CA	paid	677
Atlanta-Sandy Springs-Roswell, GA	paid	448
Lake Havasu City-Kingman, AZ	paid	364


# Data quality checks.
- Data completeness: comparing a record count of the source (staging table) and the target table (dimension table).
    *) Artists: 
SELECT COUNT(*) as TOTAL_ARTISTS FROM staging_songs GROUP BY(artist_id); --> 9553
SELECT COUNT(artist_id) as TOTAL_ARTISTS FROM artists; --> 9553

    *) Songs:
SELECT COUNT(song_id) as TOTAL_SONGS FROM staging_songs; --> 14896
SELECT COUNT(DISTINCT song_id) as TOTAL_SONGS FROM staging_songs; --> 14896
SELECT COUNT(song_id) as TOTAL_SONGS FROM songs --> 14896

    *) Users:
SELECT COUNT(user_id) AS TOTAL_USERS FROM users --> 98
SELECT COUNT(DISTINCT userId) AS TOTAL_USERS FROM staging_events --> 98
    
- No duplicates:
    *) Users:
SELECT COUNT(user_id) AS TOTAL_USERS FROM users
GROUP BY (user_id)
HAVING COUNT(user_id)>1;

gives 0 records with duplicates

    *) Time:
similar queries gives 0 records with duplicates

    *) Artists:
similar queries gives 0 records with duplicates
    
    *) Songs:
similar queries gives 0 records with duplicates


