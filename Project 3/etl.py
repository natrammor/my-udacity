import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, delete_duplicates_queries, delete_columns_queries, songplay_table_insert


def load_staging_tables(cur, conn):
    """
    Loads the data from S3 bucket into the staging tables in Redshift
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        
def insert_tables(cur, conn):
    """
    Inserts the data from the staging tables to the dimension tables: artists, users, time and songs
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def delete_duplicates(cur, conn):
    """
    Deletes duplicates from the artists and users tables. Then deletes the columns that were created in order to ease the deletion
    """
    for query in delete_duplicates_queries:
        cur.execute(query)
        conn.commit()
    for query in delete_columns_queries:
        cur.execute(query)
        conn.commit()
        
def insert_songplays(cur, conn):
    """
    Inserts the data into the fact table, songplays
    """
    cur.execute(songplay_table_insert)
    conn.commit()
    

def main():
    config = configparser.ConfigParser()
    config.read('dwh-cluster.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    delete_duplicates(cur, conn)
    insert_songplays(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()