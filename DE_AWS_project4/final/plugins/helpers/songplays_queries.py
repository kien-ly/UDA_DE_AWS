class SongplaysQueries:
    create_tables = ("""
        DROP TABLE IF EXISTS staging_events;
        DROP TABLE IF EXISTS staging_songs;
        DROP TABLE IF EXISTS songplays;
        DROP TABLE IF EXISTS users;
        DROP TABLE IF EXISTS songs;
        DROP TABLE IF EXISTS artists;
        DROP TABLE IF EXISTS time;
                     
        CREATE TABLE IF NOT EXISTS staging_events(
            event_id          BIGINT IDENTITY(0,1)      NOT NULL,
            artist            TEXT                      NULL,
            auth              TEXT                      NULL,
            firstName         TEXT                      NULL,
            gender            TEXT                      NULL,
            itemInSession     INTEGER                   NULL,
            lastName          TEXT                      NULL,
            length            FLOAT                     NULL,
            level             TEXT                      NULL,
            location          TEXT                      NULL,
            method            TEXT                      NULL,
            page              TEXT                      NULL,
            registration      FLOAT                     NULL,
            sessionId         INTEGER                   NOT NULL,
            song              TEXT                      NULL,
            status            INTEGER                   NULL,
            ts                BIGINT                    NOT NULL,
            userAgent         TEXT                      NULL,
            userId            INTEGER                   NULL
        );

        CREATE TABLE IF NOT EXISTS staging_songs(
            num_songs         INTEGER                   NULL,
            artist_id         TEXT                      NOT NULL,
            artist_latitude   FLOAT                     NULL,
            artist_longitude  FLOAT                     NULL,
            artist_location   TEXT                      NULL,
            artist_name       TEXT                      NULL,
            song_id           TEXT                      NULL,
            title             TEXT                      NULL,
            duration          FLOAT                     NULL,
            year              INTEGER                   NULL
        );

        CREATE TABLE IF NOT EXISTS songplays(
            songplay_id       INTEGER                   IDENTITY(0,1),
            start_time        TIMESTAMP                 NOT NULL,
            user_id           INTEGER                   NOT NULL,
            level             TEXT                      NOT NULL,
            song_id           TEXT                      NOT NULL,
            artist_id         TEXT                      NOT NULL, 
            session_id        TEXT                      NOT NULL,
            location          TEXT                      NULL,
            user_agent        TEXT                      NULL
        );

        CREATE TABLE IF NOT EXISTS users(
            user_id           INTEGER                   PRIMARY KEY,
            first_name        TEXT                      NULL,
            last_name         TEXT                      NULL,
            gender            TEXT                      NULL,
            level             TEXT                      NULL
        );

        CREATE TABLE IF NOT EXISTS songs(
            song_id            TEXT                      PRIMARY KEY,
            title              TEXT                      NOT NULL,
            artist_id          TEXT                      NOT NULL,
            year               INT                       NOT NULL,
            duration           FLOAT                     NOT NULL 
        );

        CREATE TABLE IF NOT EXISTS artists(
            artist_id         TEXT                      PRIMARY KEY,
            name              TEXT                      NULL,
            location          TEXT                      NULL,
            latitude          FLOAT                     NULL,
            longitude         FLOAT                     NULL
        );

        CREATE TABLE IF NOT EXISTS time(
            start_time        TIMESTAMP                 PRIMARY KEY,
            hour              INTEGER                   NULL,
            day               INTEGER                   NULL,
            week              INTEGER                   NULL,
            month             INTEGER                   NULL,
            year              INTEGER                   NULL,
            weekday           INTEGER                   NULL
        );
    """)


    # INSERT TO FINAL TABLES
    songplay_table_insert = ("""
        INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT
            DISTINCT TIMESTAMP 'epoch' + (se.ts/1000) * INTERVAL '1 second',
            se.userId,
            se.level,
            ss.song_id,
            ss.artist_id, 
            se.sessionId,
            se.location,
            se.userAgent
        FROM staging_events AS se
        JOIN staging_songs AS ss
            ON se.song = ss.title AND se.artist = ss.artist_name
        WHERE se.userId IS NOT NULL AND se.page = 'NextSong';
    """)


    user_table_insert = ("""
        INSERT INTO users(user_id, first_name, last_name, gender, level)
        SELECT
            DISTINCT se.userId,
            se.firstName,
            se.lastName,
            se.gender,
            se.level
        FROM staging_events AS se
        WHERE se.userId IS NOT NULL AND se.page = 'NextSong';
    """)

    song_table_insert = ("""
        INSERT INTO songs(song_id, title, artist_id, year, duration)
        SELECT
            DISTINCT ss.song_id,
            ss.title,
            ss.artist_id,
            ss.year,
            ss.duration
        FROM staging_songs AS ss;
    """)

    artist_table_insert = ("""
        INSERT INTO artists(artist_id, name, location, latitude, longitude)
        SELECT
            DISTINCT ss.artist_id,
            ss.artist_name,
            ss.artist_location,
            ss.artist_latitude,
            ss.artist_longitude
        FROM staging_songs AS ss;
    """)

    time_table_insert = ("""
        INSERT INTO time(start_time, hour, day, week, month, year, weekday)
        SELECT
            DISTINCT TIMESTAMP 'epoch' + (se.ts/1000) * INTERVAL '1 second' as start_time,
            EXTRACT(HOUR FROM start_time) AS hour,
            EXTRACT(DAY FROM start_time) AS day,
            EXTRACT(WEEK FROM start_time) AS week,
            EXTRACT(MONTH FROM start_time) AS month,
            EXTRACT(YEAR FROM start_time) AS year,
            EXTRACT(weekday FROM start_time) AS weekday
        FROM staging_events AS se;
    """)