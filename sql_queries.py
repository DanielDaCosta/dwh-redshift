import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id int identity(0, 1),
        artist_name varchar(100),
        auth varchar(50),
        user_first_name varchar(50),
        user_gender varchar(1),
        item_ion_session integer,
        user_last_name varchar(50),
        song_length double precision,
        user_level varchar(5),
        location varchar(80),
        method varchar(25),
        page varchar(35),
        registration varchar(50),
        session_id integer,
        song_title varchar(100),
        status integer,
        ts varchar(50),
        user_agent text,
        user_id varchar(18),
        PRIMARY KEY (event_id)
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id varchar(18),
        num_songs integer, 
        artist_id varchar(18),
        artist_latitude double precision,
        artist_longitude double precision,
        artist_location varchar(80),
        artist_name varchar(100),
        title varchar(100),
        duration numeric(9,5),
        year smallint,
        PRIMARY KEY (song_id)
    );
""")

songplay_table_create = ("""
    create table if not exists songplays(
        songplay_id int identity(0, 1),
        start_time timestamptz references time(start_time) sortkey,
        user_id varchar(18) references users(user_id),
        level varchar(5) default 'free',
        song_id varchar(18) references songs(song_id),
        artist_id varchar(18) references artists(artist_id),
        session_id integer,
        location varchar(80),
        user_agent text,
        PRIMARY KEY (songplay_id)
    );  
""")

user_table_create = ("""
    create table if not exists users(
        user_id varchar(18) primary key,
        first_name varchar(50) not null,
        last_name varchar(50) not null,
        gender varchar(1) not null,
        level varchar(5) default 'free'
    )
    diststyle all;
""")

song_table_create = ("""
    create table if not exists songs(
        song_id varchar(18) primary key,
        title varchar(100) NOT NULL,
        artist_id varchar(18) NOT NULL,
        year smallint,
        duration numeric(9,5) NOT NULL
    )
    diststyle all;
""")

artist_table_create = ("""
    create table if not exists artists(
        artist_id varchar(18) primary key,
        name varchar(100) NOT NULL,
        location varchar(80) NOT NULL,
        latitude float8,
        longitude float8
    )
    diststyle all;
""")

time_table_create = ("""
    create table if not exists time(
        start_time timestamptz primary key sortkey,
        hour smallint not null,
        day smallint not null,
        week smallint not null,
        month smallint not null,
        year smallint not null,
        weekday smallint not null
    )
    distyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    iam_role {}
    json {};
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    json 'auto';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
