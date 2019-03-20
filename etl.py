import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Prcoess the song_data and inserts the records into songs and artist tables. This funsiton runs for each song_data file
    :param curr: database cursor to execute the queries
    :param filepath: file path for songs data file to be read and insert into song and artist table.csv
    :return: None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data_columns=['song_id','title','artist_id','year','duration']
    song_data = (df[song_data_columns].values.tolist())[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data_columns=['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    artist_data = (df[artist_data_columns].values.tolist())[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Prcoess the log_data and filter for NextSong, converts the timestamp to datetime, insert the relevat values into time and user       tables
    At the very end, insert the records in songsplay table from records froms songs, artist and log tabel
    :param curr: database cursor to execute the queries
    :param filepath: file path for log data file to be read and insert into time and user table.csv
    :return: None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df=df.loc[df['page'] == 'NextSong']
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [df['ts'],t.dt.hour,t.dt.day,t.dt.weekofyear,t.dt.month,t.dt.year,t.dt.weekday]
    column_labels = ['ts','hour','day','weekofyear','month','year','weekday']
    dictionary = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(dictionary)
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_columns=['userId','firstName','lastName','gender','level']
    user_df = df[user_columns]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        songid, artistid=None,None
        for record in cur:
            songid, artistid = record[0],record[1] 
        # insert songplay record
        songplay_data = row.ts,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This functions loads the song_data and log_data file and then for each file call the function process_song_file,process_log_file     respectively for further  processing
    :param curr: database cursor to execute the queries
    :param filepath: file path for log data file to be read and insert into time and user table.csv
    :param conn: conn to be used to commit and close the transaction
    :param fucn: This is the function name to be called for processing song_data or log_data after the files have been loaded
    :return: None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Main strating point of the file
    """
    try: 
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    except psycopg2.Error as e: 
        print("Error: Could not make connection to the Postgres database")
        print(e)
        
    try:
        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
        process_data(cur, conn, filepath='data/log_data', func=process_log_file)
        conn.close()
    except:
        print("Error: Error in process_data fucntion, Generic error printed")   

if __name__ == "__main__":
    main()