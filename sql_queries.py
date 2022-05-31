import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS log_staging"
staging_songs_table_drop  = "DROP TABLE IF EXISTS song_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS song_staging (
    num_songs INTEGER NULL,
    artist_id VARCHAR NULL,
    artist_latitude FLOAT NULL,
    artist_longitude FLOAT NULL,
    artist_location VARCHAR(500) NULL,
    artist_name VARCHAR(500) NULL,
    song_id VARCHAR NULL,
    title VARCHAR(500) NULL,
    duration FLOAT NULL,
    year int NULL
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS log_staging (
    artist VARCHAR(500) NULL,
    auth VARCHAR NULL,
    firstName VARCHAR NULL,
    gender CHAR(1) NULL,
    itemInSession INTEGER NULL,
    lastName VARCHAR NULL,
    length FLOAT NULL,
    level VARCHAR NULL,
    location VARCHAR NULL,
    method VARCHAR NULL,
    page VARCHAR NULL,
    registration VARCHAR NULL,
    sessionId INTEGER NULL,
    song VARCHAR NULL,
    status INTEGER NULL,
    ts BIGINT NULL,
    userAgent VARCHAR NULL,
    userId INTEGER
);
""")


songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1) sortkey,
    start_time TIMESTAMP,	
    user_id INTEGER,
    level VARCHAR,	
    song_id VARCHAR,	
    artist_id VARCHAR,
    session_id integer,	
    location VARCHAR,	
    user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER distkey,	
    first_name VARCHAR,	
    last_name VARCHAR,	
    gender CHAR(1),	
    level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR distkey,	
    title  VARCHAR,	
    artist_id VARCHAR,	
    year INTEGER,	
    duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR distkey,	
    name VARCHAR,	
    location VARCHAR,	
    latitude FLOAT,	
    longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP sortkey distkey,	
    hour INTEGER,	
    day INTEGER,	
    week INTEGER,	
    month INTEGER,	
    year INTEGER,	
    weekday INTEGER
);
""")



# STAGING TABLES

staging_events_copy = ("""
copy log_staging
from {}
iam_role {}
json {}
region 'us-west-2'
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy=("""
copy song_staging 
from {} 
iam_role {}
json 'auto'
region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT timestamp 'epoch' + l.ts/1000 * INTERVAL '1 second' as start_time, l.userid, l.level, s.song_id, s.artist_id, l.sessionid, l.location, l.userAgent FROM log_staging l 
JOIN song_staging AS s ON (l.artist = s.artist_name)
WHERE l.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level FROM log_staging 
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration FROM song_staging
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM song_staging 
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts, EXTRACT(HOUR FROM ts), EXTRACT(DAY FROM ts), EXTRACT(WEEK FROM ts), EXTRACT(MONTH FROM ts), EXTRACT(YEAR FROM ts), EXTRACT(WEEKDAY FROM ts)
FROM (SELECT (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second') as ts FROM log_staging)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create,staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,  songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop,artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]



