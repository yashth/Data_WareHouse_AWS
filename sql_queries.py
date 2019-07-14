import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop =  "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop =       "DROP TABLE IF EXISTS songplay"
user_table_drop =           "DROP TABLE IF EXISTS users"
song_table_drop =           "DROP TABLE IF EXISTS songs"
artist_table_drop =         "DROP TABLE IF EXISTS artists"
time_table_drop =           "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events 
                                   (
                                    artist        text, 
                                    auth          text, 
                                    firstName     text, 
                                    gender        text, 
                                    itemInSession int PRIMARY KEY,
                                    lastName      text, 
                                    length        float8, 
                                    level         text, 
                                    location      text, 
                                    method        text,
                                    page          text, 
                                    registration  text, 
                                    sessionId     int, 
                                    song          text, 
                                    status        int,
                                    ts            bigint, 
                                    userAgent     text, 
                                    userId       int
                                    )
                                   
                            """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs 
                                (
                                 artist_id         varchar, 
                                 artist_latitude   varchar,
                                 artist_location   varchar,
                                 artist_longitude  varchar,
                                 artist_name       varchar,
                                 duration          varchar, 
                                 num_songs         varchar, 
                                 song_id           varchar   PRIMARY KEY,
                                 title             varchar, 
                                 year              int
                                 )
                                 
                            """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay
                            (
                            songplay_id     bigint     IDENTITY(0,1) PRIMARY KEY, 
                            start_time      timestamp  NOT NULL, 
                            user_id         int        NOT NULL, 
                            level           varchar, 
                            song_id         varchar, 
                            artist_id       varchar, 
                            session_id      int, 
                            location        varchar, 
                            user_agent      varchar
                            )
                            
                    """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
                        (
                        user_id      int PRIMARY KEY, 
                        first_name   varchar, 
                        last_name    varchar, 
                        gender       varchar, 
                        level        varchar
                        );
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
                        (
                        song_id varchar PRIMARY KEY, 
                        title varchar, 
                        artist_id varchar, 
                        year int, 
                        duration float
                        );
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
                          (
                          artist_id   varchar PRIMARY KEY, 
                          name        varchar, 
                          location    varchar, 
                          latitude    float, 
                          longitude   float
                          );
                        """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                        (
                        ts          bigint,
                        start_time  timestamp PRIMARY KEY, 
                        hour        int, 
                        day         int, 
                        week        int, 
                        month       int, 
                        year        int, 
                        weekday     varchar
                        );
                    """)

# STAGING TABLES

staging_events_copy = (""" COPY staging_events FROM {}
                           credentials 'aws_iam_role={}'
                           json {}
                           region 'us-west-2'
                    """).format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = (""" COPY staging_songs FROM {}
                          credentials 'aws_iam_role={}'
                          region 'us-west-2'
                          json 'auto' truncatecolumns;
                      """).format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay
                            (
                            start_time, 
                            user_id, 
                            level, 
                            song_id, 
                            artist_id, 
                            session_id, 
                            location, 
                            user_agent
                            )
                              SELECT ts_start_time,
                                     userId,
                                     level,
                                     song_id,
                                     artist_id,
                                     sessionId,
                                     location,
                                     userAgent 
                                     FROM  (SELECT '1970-01-01'::date + se.ts/1000 * interval '1 second' as ts_start_time,
                                            se.userId, 
                                            se.level, 
                                            sa.song_id, 
                                            sa.artist_id,
                                            se.sessionId, 
                                            se.location, 
                                            se.userAgent 
                                            FROM staging_events se
                                            JOIN
                                            (SELECT songs.song_id, 
                                                    artists.artist_id, 
                                                    songs.title, 
                                                    artists.name,
                                                    songs.duration 
                                                    FROM songs
                                                    JOIN
                                                    artists 
                                                    ON songs.artist_id = artists.artist_id) AS sa
                                            ON (sa.title = se.song AND 
                                                sa.name = se.artist AND 
                                                sa.duration = se.length)
                                            WHERE se.page='NextSong');
                            
                            
                            """)

user_table_insert = ("""INSERT INTO users 
                        (
                        user_id, 
                        first_name, 
                        last_name, 
                        gender, 
                        level
                        ) 
                        (SELECT DISTINCT userId,
                                firstName,
                                lastName,
                                gender,
                                level 
                                FROM staging_events
                                WHERE page='NextSong')
                    """)

song_table_insert = ("""INSERT INTO songs 
                        (
                         song_id, 
                         title, 
                         artist_id, 
                         year, 
                         duration
                         ) (SELECT DISTINCT song_id, 
                                   title, 
                                   artist_id, 
                                   cast(year as integer), 
                                   cast(duration as float8) 
                                   FROM staging_songs)
                        """)

artist_table_insert = ("""INSERT INTO artists 
                         (
                          artist_id, 
                          name, 
                          location, 
                          latitude, 
                          longitude
                          ) (SELECT DISTINCT artist_id, 
                                    artist_name, 
                                    artist_location, 
                                    cast(artist_latitude as float8), 
                                    cast(artist_longitude as float8) 
                                    FROM staging_songs)
                        """)

time_table_insert = ("""INSERT INTO time 
                        (
                          ts,start_time, 
                          hour, 
                          day, 
                          week, 
                          month, 
                          year, 
                          weekday
                          ) (SELECT DISTINCT ts, 
                                             ts_start_time, 
                                             EXTRACT(HOUR FROM ts_start_time), 
                                             EXTRACT(DAY FROM ts_start_time), 
                                             EXTRACT(WEEK FROM ts_start_time), 
                                             EXTRACT(MONTH FROM ts_start_time), 
                                             EXTRACT(YEAR FROM ts_start_time), 
                                             EXTRACT(DOW FROM ts_start_time) 
                                             FROM (SELECT distinct ts, 
                                                   '1970-01-01'::date + ts/1000 * interval '1 second' as ts_start_time    
                                                   FROM staging_events))
                    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
