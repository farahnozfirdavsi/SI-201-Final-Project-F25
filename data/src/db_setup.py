import sqlite3
import os

# Default database name – you can change this if you want
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "afa.db")
DB_NAME = DB_PATH

def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn



def create_tables(db_name: str = DB_NAME) -> None:
    """
    Create all tables needed for the AFA: Anxiety, Frequency & APIs project.
    Tables:
      - Artists
      - ScrapedSongs
      - Songs
      - SpotifyAudioFeatures
      - Popularity
      - CDCRaw
      - MentalHealthTrends
    """
    conn = get_connection(db_name)
    cur = conn.cursor()

    # 0. Artists
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS Artists (
            artist_id INTEGER PRIMARY KEY AUTOINCREMENT,
            artist_name TEXT UNIQUE NOT NULL
        );
        """
    )

    # 1. Songs scraped from Billboard / Pitchfork etc.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ScrapedSongs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            song_title   TEXT NOT NULL,
            artist_id INTEGER NOT NULL,
            FOREIGN KEY (artist_id) REFERENCES Artists(artist_id),
            genre        TEXT,
            chart_date   TEXT NOT NULL
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

    # 3. Spotify audio features (can be separated for clarity)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS SpotifyAudioFeatures (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            song_id         INTEGER NOT NULL,
            valence         REAL,
            energy          REAL,
            danceability    REAL,
            tempo           REAL,
            acousticness    REAL,
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

    # 5. Raw CDC mental health data (many rows from the API)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS CDCRaw (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_name TEXT,
            state TEXT,
            indicator TEXT,
            time_period_start_date TEXT,
            value REAL
        );
        """
    )

    # 6. CDC mental health weekly trends 
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS MentalHealthTrends (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            week             TEXT NOT NULL,
            anxiety_percent  REAL,
            depression_percent REAL
        );
        """
    )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    create_tables()
    print(f"All tables created (or already existed) in database: {DB_NAME}")
