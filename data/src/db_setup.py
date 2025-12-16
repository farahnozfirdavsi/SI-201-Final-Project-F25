import os
import sqlite3

# Always use the database file that lives in the same folder as this script (src/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "afa.db")


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def create_tables(db_path: str = DB_PATH) -> None:
    """
    Create all tables needed for the AFA: Anxiety, Frequency & APIs project.

    Tables:
      - Artists
      - ScrapedSongs
      - Songs
      - SpotifyAudioFeatures
      - Popularity
      - CDCGroup
      - CDCState
      - CDCIndicator
      - CDCTimePeriod
      - CDCRaw (normalized fact table)
      - MentalHealthTrends
    """
    conn = get_connection(db_path)
    cur = conn.cursor()

    # 0. Artists (normalize artist strings)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS Artists (
            artist_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            artist_name TEXT UNIQUE NOT NULL
        );
        """
    )

    # 1. Songs scraped from Billboard / Pitchfork etc.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ScrapedSongs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            song_title TEXT NOT NULL,
            artist_id  INTEGER NOT NULL,
            genre      TEXT,
            chart_date TEXT NOT NULL,
            FOREIGN KEY (artist_id)
                REFERENCES Artists(artist_id)
                ON DELETE CASCADE
        );
        """
    )

    # 2. Core Spotify song info (one row per song)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS Songs (
            song_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_song_id  INTEGER NOT NULL,
            spotify_track_id TEXT NOT NULL,
            genre            TEXT,
            popularity       INTEGER,
            release_year     INTEGER,
            FOREIGN KEY (scraped_song_id)
                REFERENCES ScrapedSongs(id)
                ON DELETE CASCADE
        );
        """
    )

    # 3. Spotify audio features
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS SpotifyAudioFeatures (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            song_id          INTEGER NOT NULL,
            valence          REAL,
            energy           REAL,
            danceability     REAL,
            tempo            REAL,
            acousticness     REAL,
            instrumentalness REAL,
            FOREIGN KEY (song_id)
                REFERENCES Songs(song_id)
                ON DELETE CASCADE
        );
        """
    )

    # 4. Last.fm popularity metrics
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS Popularity (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            song_id        INTEGER NOT NULL,
            listener_count INTEGER,
            playcount      INTEGER,
            FOREIGN KEY (song_id)
                REFERENCES Songs(song_id)
                ON DELETE CASCADE
        );
        """
    )

    # NORMALIZED CDC LOOKUP TABLES
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS CDCGroup (
            group_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            group_name TEXT NOT NULL UNIQUE
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS CDCState (
            state_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            state_name TEXT NOT NULL UNIQUE
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS CDCIndicator (
            indicator_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            indicator_name TEXT NOT NULL UNIQUE
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS CDCTimePeriod (
            time_id               INTEGER PRIMARY KEY AUTOINCREMENT,
            time_period_start_date TEXT NOT NULL UNIQUE
        );
        """
    )

    # Normalized CDC fact table (no duplicate strings here)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS CDCRaw (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id     INTEGER NOT NULL,
            state_id     INTEGER NOT NULL,
            indicator_id INTEGER NOT NULL,
            time_id      INTEGER NOT NULL,
            value        REAL,
            FOREIGN KEY (group_id) REFERENCES CDCGroup(group_id),
            FOREIGN KEY (state_id) REFERENCES CDCState(state_id),
            FOREIGN KEY (indicator_id) REFERENCES CDCIndicator(indicator_id),
            FOREIGN KEY (time_id) REFERENCES CDCTimePeriod(time_id),
            UNIQUE (group_id, state_id, indicator_id, time_id)
        );
        """
    )

    #  CDC weekly mental health trends (derived/summary)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS MentalHealthTrends (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            week               TEXT NOT NULL,
            anxiety_percent    REAL,
            depression_percent REAL
        );
        """
    )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    create_tables()
    print(f"All tables created (or already existed) in database: {DB_PATH}")
