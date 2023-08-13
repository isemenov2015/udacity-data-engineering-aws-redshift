import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
region = 'us-west-2'
LOG_DATA = config['S3']['LOG_DATA']
LOG_PATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']
IAM_ROLE = config['IAM_ROLE']['ARN']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS plays_stage"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_stage"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS plays_stage (
        artist TEXT,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INTEGER,
        lastName VARCHAR,
        length NUMERIC,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration NUMERIC,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts NUMERIC,
        userAgent VARCHAR,
        userId INTEGER
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs_stage (
        num_songs INTEGER,
        artist_id TEXT NOT NULL,
        artist_latitude NUMERIC,
        artist_longitude NUMERIC,
        artist_location VARCHAR(4096),
        artist_name VARCHAR(4096),
        song_id TEXT NOT NULL,
        title VARCHAR(4096),
        duration NUMERIC,
        year INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0, 1),
        start_time TIMESTAMP NOT NULL DISTKEY SORTKEY,
        user_id INTEGER,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY SORTKEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR(1),
        level VARCHAR(30)
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY SORTKEY,
        title VARCHAR(4096),
        artist_id TEXT,
        year INTEGER,
        duration NUMERIC
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY SORTKEY,
        name VARCHAR(4096),
        location VARCHAR(4096),
        latitude NUMERIC,
        longitude NUMERIC
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP NOT NULL PRIMARY KEY DISTKEY SORTKEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy plays_stage from {bucket}
        credentials 'aws_iam_role={role}'
        region      '{region}'
        format       as JSON {path}
""").format(bucket=LOG_DATA, region=region, role=IAM_ROLE, path=LOG_PATH)

staging_songs_copy = ("""
copy songs_stage from {bucket}
    credentials 'aws_iam_role={role}'
    region      '{region}'
    format       as JSON 'auto'
""").format(bucket=SONG_DATA, region=region, role=IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        TIMESTAMP 'epoch' + (p.ts / 1000 * INTERVAL '1 second') AS start_time,
        p.userId,
        p.level,
        s.song_id,
        s.artist_id,
        p.sessionId,
        p.location,
        p.userAgent
    FROM plays_stage p
    LEFT JOIN songs_stage s
        ON p.song = s.title and p.artist = s.artist_name and p.page = 'NextSong'
    WHERE s.song_id IS NOT NULL AND p.ts IS NOT NULL;
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT
        DISTINCT userId,
        firstName,
        lastName,
        gender,
        level
    FROM plays_stage
    WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT
        DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM songs_stage
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT
        DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM songs_stage
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    WITH tmp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM plays_stage)
    SELECT
        DISTINCT ts,
        EXTRACT(HOUR FROM ts) AS hour,
        EXTRACT(DAY FROM ts) AS day,
        EXTRACT(WEEK FROM ts) AS week,
        EXTRACT(MONTH FROM ts) AS month,
        EXTRACT(YEAR FROM ts) AS year,
        EXTRACT(WEEKDAY FROM ts) AS weekday
    FROM tmp_time
    WHERE ts IS NOT NULL;
""")

# DEMO queries

demo_query1 = """
    SELECT
        t.year,
        s.title,
        COUNT(DISTINCT sp.user_id) AS total_users_listened
    FROM songplays sp
    LEFT JOIN time t
        ON t.start_time = sp.start_time
    LEFT JOIN songs s
        ON sp.song_id = s.song_id
    WHERE t.year = 2018 AND s.title = 'Am I High (Feat. Malice)'
    GROUP BY t.year, s.title
"""

demo_query2 = """
    SELECT
        sp.user_id,
        u.first_name,
        u.last_name,
        COUNT(*) AS total_songs_listened
    FROM songplays sp
    LEFT JOIN users u
        ON sp.user_id = u.user_id
    GROUP BY 1, 2, 3
    ORDER BY 4 DESC
    LIMIT 10
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
demo_queries = [demo_query1, demo_query2]
