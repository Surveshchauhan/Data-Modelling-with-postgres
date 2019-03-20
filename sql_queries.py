# DROP TABLES
from psycopg2 import sql
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL PRIMARY KEY, start_time bigint NOT NULL REFERENCES                         time, user_id varchar(255) NOT NULL REFERENCES users, level varchar(255), song_id varchar(255) REFERENCES                             songs, artist_id varchar(255) REFERENCES artists, session_id varchar(255), location varchar(255), user_agent                         varchar(255))")

user_table_create = ("CREATE TABLE IF NOT EXISTS users  (user_id varchar(255) PRIMARY KEY, first_name varchar(255), last_name                             varchar(255), gender varchar(255), level varchar(255))")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs   (song_id varchar(255) PRIMARY KEY, title varchar(255), artist_id                                 varchar(255), year int, duration float8);")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists   (artist_id varchar(255) PRIMARY KEY, name varchar(255) , location                             varchar(255), lattitude varchar(255), longitude varchar(255));")

time_table_create = ("CREATE TABLE IF NOT EXISTS time (start_time bigint PRIMARY KEY, hour int, day int, week int, month int, year                       int, weekday int);")
# INSERT RECORDS

songplay_table_insert = ("INSERT INTO songplays (start_time , user_id , level , song_id , artist_id , session_id , location ,                                 user_agent) VALUES (%s, %s, %s, %s, %s,%s,%s,%s)")

user_table_insert = ("INSERT INTO users (user_id, first_name, last_name, gender, level ) VALUES (%s, %s, %s, %s, %s) ON CONFLICT                          (user_id) DO UPDATE SET level=EXCLUDED.level")

song_table_insert = ("INSERT INTO songs (song_id , title , artist_id , year, duration  ) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO                        NOTHING")

artist_table_insert = ("INSERT INTO artists (artist_id , name , location , lattitude , longitude ) VALUES (%s, %s, %s, %s, %s) ON                           CONFLICT DO NOTHING")


time_table_insert = ("INSERT INTO time (start_time , hour , day , week , month , year , weekday  ) VALUES (%s, %s, %s, %s, %s,%s,%s)                     ON CONFLICT DO NOTHING")

# FIND SONGS

song_select = ("select c.song_id, c.artist_id from (select s.song_id, s.artist_id,a.name,s.title,s.duration from songs s, artists a                 where s.artist_id=a.artist_id)c where  c.title  = %s and c.name = %s and duration = %s")


# QUERY LISTS

create_table_queries = [time_table_create,user_table_create, song_table_create, artist_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]