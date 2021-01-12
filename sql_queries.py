import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = " DROP TABLE IF EXISTS staging_events; "
staging_songs_table_drop = " DROP TABLE IF EXISTS staging_songs; "
songplay_table_drop = " DROP TABLE IF EXISTS songplay; "
user_table_drop = " DROP TABLE IF EXISTS users; "
song_table_drop = " DROP TABLE IF EXISTS song; "
artist_table_drop = " DROP TABLE IF EXISTS artist; " 
time_table_drop = " DROP TABLE IF EXISTS time; "

# CREATE TABLES

staging_events_table_create= (""" create table staging_events
(
artist VARCHAR(500) ,
auth VARCHAR(500) ,
firstName VARCHAR(500) ,
gender VARCHAR(10) ,
itemInSession INTEGER ,
lastName VARCHAR(100) ,
length FLOAT ,
level VARCHAR(100) ,
location VARCHAR(500) ,
method VARCHAR(50) ,
page VARCHAR(50) , 
registration BIGINT ,
sessionId INTEGER ,
song VARCHAR(500),
status INTEGER ,
ts BIGINT ,
userAgent VARCHAR(500) ,
userId INTEGER )
diststyle all;
""")

staging_songs_table_create = ("""create table staging_songs(
artist_id VARCHAR(100) ,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location VARCHAR(1000) ,
artist_name VARCHAR(1000), 
song_id VARCHAR(100) , 
title VARCHAR(1000) , 
duration float ,
year INTEGER );""")

songplay_table_create = ("""
create table songplay (
songplay_id Integer IDENTITY(0,1) NOT NULL,
start_time VARCHAR(25) NOT NULL,  
user_id integer , 
level varchar(100) , 
song_id varchar(100) , 
artist_id varchar(100) ,
session_id integer , 
location varchar(1000) , 
user_agent varchar(500) 
)
""")

user_table_create = (""" create table users 
(
user_id integer , 
first_name varchar(100) , 
last_name varchar(100) , 
gender varchar(10) , 
level varchar(10) 
)
diststyle all;
""")

song_table_create = (""" 
create table song (
song_id varchar(100),
title varchar(1000) distkey ,
artist_id varchar(100) ,
year integer , 
duration float );
""")

artist_table_create = ("""create table artist
(
artist_id varchar(100) ,
name varchar(1000) distkey, 
location varchar(1000) , 
lattitude FLOAT, 
longitude FLOAT
);
""")

time_table_create = (""" create table time
(
start_time TIMESTAMP NOT NULL, 
hour Integer , 
day VARCHAR(20) , 
week Integer , 
month Integer , 
year Integer, 
weekday Integer)
diststyle all;

""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role {}
    json {} ;
    """).format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    json 'auto';
    """).format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES



user_table_insert = ("""
INSERT INTO users
SELECT distinct userId, firstName, lastName , gender, level FROM staging_events
;
""")

song_table_insert = ("""
INSERT INTO song
SELECT distinct song_id, title, artist_id, year, duration FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO ARTIST
SELECT distinct artist_id ,
artist_name,
artist_location, 
artist_latitude, 
artist_longitude  FROM staging_songs;
""")

time_table_insert = ("""

insert into time
SELECT a.start_time,

EXTRACT (HOUR FROM a.start_time), EXTRACT (DAY FROM a.start_time),

EXTRACT (WEEK FROM a.start_time), EXTRACT (MONTH FROM a.start_time),

EXTRACT (YEAR FROM a.start_time), EXTRACT (WEEKDAY FROM a.start_time) FROM

(SELECT distinct TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time FROM staging_events) a;
""")

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id ,level , song_id , artist_id ,session_id ,location , user_agent)
SELECT distinct ts, userId,level,song.song_id,artist.artist_id,sessionId,
staging_events.location,userAgent
from staging_events LEFT JOIN song ON staging_events.song=song.title
LEFT JOIN artist ON staging_events.artist=artist.name
;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
