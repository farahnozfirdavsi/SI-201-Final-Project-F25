import os
import sqlite3
import requests
from keys import LASTFM_API_KEY

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 
DB_PATH = os.path.join(BASE_DIR, "afa.db")


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def debug_db():
    """
    Print which DB we are using and what tables exist
    This helps avoid 'no such table' errors
    """
    conn = get_connection()
    cur = conn.cursor()
    print("Using DB:", DB_PATH)
    print("Tables in this DB:")
    for (name,) in cur.execute("SELECT name FROM sqlite_master WHERE type='table';"):
        print(" -", name)
    conn.close()


def get_lastfm_popularity(title, artist):
    """
    Call Last.fm track.getInfo for (artist, title).
    Returns dict with listeners + playcount, or None if not found.
    """
    url = "http://ws.audioscrobbler.com/2.0/"

    params = {
        "method": "track.getInfo",
        "api_key": LASTFM_API_KEY,
        "artist": artist,
        "track": title,
        "format": "json",
    }

    resp = requests.get(url, params=params)
    data = resp.json()

    if "track" not in data:
        return None

    track = data["track"]
    listeners = track.get("listeners")
    playcount = track.get("playcount")

    if listeners is None or playcount is None:
        return None

    try:
        listeners = int(listeners)
        playcount = int(playcount)
    except ValueError:
        return None

    return {
        "listeners": listeners,
        "playcount": playcount,
    }


def store_popularity(conn, song_id, pop):
    """
    Insert one row into Popularity for the given song_id.
    Assumes table:
      Popularity(id, song_id, listener_count, playcount)
    """
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO Popularity (song_id, listener_count, playcount)
        VALUES (?, ?, ?)
        """,
        (song_id, pop["listeners"], pop["playcount"]),
    )
    conn.commit()


def populate_lastfm(limit=25):
    """
    For up to `limit` songs that do NOT yet have a Popularity row,
    look up Last.fm track info and store listener_count + playcount.
    """

    debug_db()  # show which DB and tables we're using

    conn = get_connection()
    cur = conn.cursor()

    # If Songs table truly doesn't exist, fail with a clear message
    try:
        rows = cur.execute(
            """
            SELECT s.song_id, ss.song_title, a.artist_name
            FROM Songs s
            JOIN ScrapedSongs ss ON ss.id = s.scraped_song_id
            JOIN Artists a ON ss.artist_id = a.artist_id
            LEFT JOIN Popularity p ON p.song_id = s.song_id
            WHERE p.id IS NULL
            LIMIT ?;
            """,
            (limit,),
        ).fetchall()
    except sqlite3.OperationalError as e:
        print("\n[ERROR] While querying Songs/Popularity:")
        print(e)
        print(
            "\nIt looks like this afa.db does not have a Songs table.\n"
            "Make sure you have already run spotify_api.py and that you're using "
            "the same data/src/afa.db that your other scripts use."
        )
        conn.close()
        return

    print(f"\nFound {len(rows)} songs needing Last.fm data.")

    for song_id, title, artist in rows:
        print(f"\nSong_id={song_id} | {title} — {artist}")

        pop = get_lastfm_popularity(title, artist)
        if pop is None:
            print("  Last.fm: Track not found or missing counts. Skipping.")
            continue

        store_popularity(conn, song_id, pop)
        print(
            f"  Stored Last.fm popularity: listeners={pop['listeners']}, plays={pop['playcount']}"
        )

    conn.close()
    print("\nDone populating Last.fm popularity.")


if __name__ == "__main__":
    populate_lastfm(limit=25)
