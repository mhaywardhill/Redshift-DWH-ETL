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


# QUERY LISTS

create_table_queries = [staging_events_table_create,staging_songs_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
