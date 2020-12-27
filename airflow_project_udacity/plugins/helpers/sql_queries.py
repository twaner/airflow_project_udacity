class SqlQueries:
    """SQL queries for creating and inserting data into the tables
    Keyword arguments:
    """
    songplay_table_insert = ("""
        INSERT INTO {}.songplays (
        SELECT
                md5(events.sessionid || events.start_time) playid,
                events.start_time as start_time,
                events.userid,
                events.level,
                songs.song_id,
                songs.artist_id,
                events.sessionid,
                events.location,
                events.useragent
                FROM (
            SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM {}.staging_events
            WHERE page='NextSong') events
            LEFT JOIN {}.staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration)
    """)

    user_table_insert = ("""
        INSERT INTO {}.users (
        SELECT distinct userid, firstname, lastname, gender, level
        FROM {}.staging_events
        WHERE page='NextSong')
    """)

    song_table_insert = ("""
        INSERT INTO {}.songs (
        SELECT distinct song_id, title, artist_id, year, duration
        FROM {}.staging_songs)
    """)

    artist_table_insert = ("""
        INSERT INTO {}.artists (
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM {}.staging_songs)
    """)

    time_table_insert = ("""
        INSERT INTO {}.time (
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time),
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM {}.songplays)
    """)

    # Create statements

    create_artist_table = ("""CREATE TABLE IF NOT EXISTS {}.artists (
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)
        );""")

    create_songplays_table = ("""CREATE TABLE IF NOT EXISTS {}.songplays (
            playid varchar(32) NOT NULL,
            start_time timestamp NOT NULL,
            userid int4 NOT NULL,
            "level" varchar(256),
            songid varchar(256),
            artistid varchar(256),
            sessionid int4,
            location varchar(256),
            user_agent varchar(256),
            CONSTRAINT songplays_pkey PRIMARY KEY (playid)
        );""")

    create_song_table = ("""CREATE TABLE IF NOT EXISTS {}.songs (
            songid varchar(256) NOT NULL,
            title varchar(256),
            artistid varchar(256),
            "year" int4,
            duration numeric(18,0),
            CONSTRAINT songs_pkey PRIMARY KEY (songid)
        );""")

    create_staging_events_table = ("""CREATE TABLE IF NOT EXISTS {}.staging_events (
            artist VARCHAR(255),
            auth VARCHAR(20),
            firstName VARCHAR(40),
            gender VARCHAR(1),
            itemInSession INTEGER,
            lastName  VARCHAR(40),
            length DECIMAL,
            level  VARCHAR(20),
            location  VARCHAR(100),
            method  VARCHAR(4),
            page  VARCHAR(30),
            registration VARCHAR(16),
            sessionId INTEGER,
            song  VARCHAR(255),
            status VARCHAR(4),
            ts BIGINT,
            userAgent VARCHAR(255),
            userId INTEGER
        );""")

    create_staging_songs_table = ("""CREATE TABLE IF NOT EXISTS {}.staging_songs (
            num_songs int4,
            artist_id varchar(256),
            artist_name varchar(256),
            artist_latitude numeric(18,0),
            artist_longitude numeric(18,0),
            artist_location varchar(256),
            song_id varchar(256),
            title varchar(256),
            duration numeric(18,0),
            "year" int4
        );""")

    create_time_table = ("""CREATE TABLE IF NOT EXISTS {}."time" (
            start_time timestamp NOT NULL,
            "hour" int4,
            "day" int4,
            week int4,
            "month" varchar(256),
            "year" int4,
            weekday varchar(256),
            CONSTRAINT time_pkey PRIMARY KEY (start_time)
        );""")

    create_users_table = ("""CREATE TABLE IF NOT EXISTS {}.users (
            userid int4 NOT NULL,
            first_name varchar(256),
            last_name varchar(256),
            gender varchar(256),
            "level" varchar(256),
            CONSTRAINT users_pkey PRIMARY KEY (userid)
        );""")
    # A List of tables that will be used.
    tables = ['staging_events', 'staging_songs', 'songplays', 'artists', 'users', 'songs', 'time']
