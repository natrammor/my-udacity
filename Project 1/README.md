
# Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.

A startup called Sparkify has accumulated a great amount of data available in two types of JSON files: logs on user activity on the app and metadata about the songs. Now they want to use this data to analyze the user activity on their new music streaming app, and to get to know which songs are the users listening to.

The goal of this project is to create a Postgres database, with a star schema, in which a fact table helps to optimize queries and perform song play analysis. In order to fill the databases, an ETL pipeline is used to extract, transform and load the data from the available JSON files.


# State and justify your database schema design and ETL pipeline.

The database is based on a star schema. The **fact table** is designed in a way, that the desired queries for the purpose of the anaylisis can be directly done without the need of JOINS. This will spare a significant amount of time when retrieving data as the content of the database will grow.

The **fact table** is called **songplays** and contains the following columns:

    songplay_id bigserial PRIMARY KEY
    start_time bigint NOT NULL
    user_id varchar NOT NULL
    level varchar
    song_id varchar REFERENCES songs(song_id)
    artist_id varchar REFERENCES artists(artist_id)
    session_id varchar
    location varchar
    user_agent varchar

Each **dimension table** is related to one specific aspect (dimension) of the facts and measures related to the business. Four dimension tables have been defined: **users**, **songs**, **artists** and **time**. The **fact table** shows Foreign Keys to each of the dimension tables.

The table **users** contains the following columns:
    
    user_id varchar PRIMARY KEY
    first_name varchar
    last_name varchar NOT NULL
    gender varchar
    level varchar

The table **songs** contains the following columns:

    song_id varchar PRIMARY KEY
    title varchar
    artist_id varchar
    year int
    duration numeric(10,5)

The table **artists** contains the following columns:

    artist_id varchar PRIMARY KEY
    name varchar
    location varchar
    latitude numeric
    longitude numeric

The table **time** contains the following columns:

    time_id bigserial PRIMARY KEY
    start_time bigint
    hour int
    day int
    week int
    month int
    year int
    weekday int

To fill the database according to this schema, an ETL pipeline has been designed (please check the python file *etl.py*). The pipeline first grabs data from the song_data directory and then from the log_data directory. The process is as follows:
1. All the JSON files in the folders and subfolders in data/song_data are gathered.
2. Using Pandas, each file is read and the data to fill the fields in the **songs** table are extracted.
3. Once this data is in the correct format to be inserted in the database, the "INSERT" query is performed.
4. Steps 2 and 3 are repeated, in order to fill the **artists** table this time.
5. Next, all the JSON files in the folders and subfolders in data/log_data are gathered.
6. Using Pandas, the values of the dataframes are extracted in order to organize the data that will be inserted in the **time** table. Only the records in which page=="NextSong" will be taken into consideration.
7. The data from the logs is used to fill the **users** table.
8. Finally, in order to fill the **fact table (songplays)**, the information from the log_data is used to retrieve the title of the song, the name of the artist and the duration of the song. With this information, a query is done in order to  extract the artist_id and the song_id information that matches the information from the logs (this is done by joining **artists** and **songs** tables). 
9. Once this info is retrieved, the data is easily saved in the songplays table.

The steps to perform the database creation and ETL operations are:
1. Open a shell terminal
2. Execute: python create_tables.py
3. Execute: python etl.py
4. A message will inform you whether the data retrieval and insertion has been correctly accomplished


# Provide example queries and results for song play analysis.
` %sql SELECT song_id from songplays GROUP BY song_id ORDER BY song_id;`
Output:SOZCTXZ12AB0182364, None

` %sql SELECT location, level, COUNT(level) FROM songplays GROUP by location, level ORDER BY level DESC;`
Output: to be checked in *test.ipynb*. Here an extract of the output:

    location	level	count
    New York-Newark-Jersey City, NY-NJ-PA	paid	149
    Augusta-Richmond County, GA-SC	paid	140
    Waterloo-Cedar Falls, IA	paid	397
    San Francisco-Oakland-Hayward, CA	paid	650
    San Jose-Sunnyvale-Santa Clara, CA	paid	178
    Lake Havasu City-Kingman, AZ	paid	321
    Atlanta-Sandy Springs-Roswell, GA	paid	428
    Tampa-St. Petersburg-Clearwater, FL	paid	289
    Janesville-Beloit, WI	paid	241
    Red Bluff, CA
    .
    .
    .