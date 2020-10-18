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

# TRUNCATE TABLES
staging_events_table_truncate = "TRUNCATE staging_events"
staging_songs_table_truncate = "TRUNCATE staging_songs"
songplay_table_truncate = "TRUNCATE songplays"
user_table_truncate = "TRUNCATE users"
song_table_truncate = "TRUNCATE songs"
artist_table_truncate = "TRUNCATE artists"
time_table_truncate = "TRUNCATE time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id int identity(0, 1),
        artist_name varchar(255),
        auth varchar(50),
        user_first_name varchar(50),
        user_gender varchar(1),
        item_in_session integer,
        user_last_name varchar(50),
        song_length double precision,
        user_level varchar(5),
        location varchar(80),
        method varchar(25),
        page varchar(35),
        registration varchar(50),
        session_id integer,
        song_title varchar(255),
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
        first_name varchar(50),
        last_name varchar(50),
        gender varchar(1),
        level varchar(5) default 'free'
    )
    diststyle all;
""")

song_table_create = ("""
    create table if not exists songs(
        song_id varchar(18) primary key,
        title varchar(255) NOT NULL,
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
    diststyle all;
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
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
     select timestamp 'epoch' + ts::INT8/1000 * INTERVAL '1 second' as start_time, 
           user_id, 
           user_level as level, 
           songs.song_id, 
           artists.artist_id,
		   session_id, 
           staging_events.location,
           user_agent
    from staging_events 
    inner join artists on artists.name = staging_events.artist_name
    inner join songs on songs.title = staging_events.song_title
    where page='NextSong';
""")

# Get distinct user_id. Get user last inserted infos
user_table_insert = ("""
    insert into users(user_id, first_name, last_name, gender, level)
    select distinct (user_id) user_id, user_first_name, user_last_name, user_gender, user_level
    from staging_events
    where page='NextSong'
    order by user_id, ts desc;
""")


song_table_insert = ("""
    insert into songs(song_id, title, artist_id, year, duration)
    select distinct song_id, title, artist_id, year, duration
    from staging_songs;
""")

artist_table_insert = ("""
    insert into artists(artist_id, name, location, latitude, longitude)
    select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    from staging_songs;
""")

time_table_insert = ("""
insert into time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT start_time,
                extract(hour from start_time) as hour,
                extract(day from start_time) as day,
                extract(week from start_time) as week,
                extract(month from start_time) as month,
                extract(year from start_time) as year,
                extract(weekday from start_time) as weekday
from songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, artist_table_create, song_table_create, user_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert, time_table_insert]
truncate_table_queries = [songplay_table_truncate, user_table_truncate, song_table_truncate, artist_table_truncate, time_table_truncate]
