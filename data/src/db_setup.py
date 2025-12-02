import sqlite3

# Default database name – you can change this if you want
DB_NAME = "afa.db"


def get_connection(db_name: str = DB_NAME) -> sqlite3.Connection:
    """
    Create a SQLite connection with foreign key support enabled.
    """
    conn = sqlite3.connect(db_name)
    # Make sure foreign key constraints actually work in SQLite
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def create_tables(db_name: str = DB_NAME) -> None:
    """
    Create all tables needed for the AFA: Anxiety, Frequency & APIs project.
    Tables:
      - ScrapedSongs
      - Songs
      - SpotifyAudioFeatures
      - Popularity
      - MentalHealthTrends
    """
    conn = get_connection(db_name)
    cur = conn.cursor()

    # 1. Songs scraped from Billboard / Pitchfork etc.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ScrapedSongs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            song_title   TEXT NOT NULL,
            artist_name  TEXT NOT NULL,
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

    # 5. CDC mental health weekly trends
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
