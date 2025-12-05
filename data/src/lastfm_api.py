import os
import requests
import sqlite3


# CONFIG
BASE_URL = "http://ws.audioscrobbler.com/2.0/"
DB_NAME = "afa.db"


def get_connection(db_name=DB_NAME):
    """Return SQLite connection."""
    conn = sqlite3.connect(db_name)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


# LAST.FM REQUEST FUNCTION
def get_lastfm_popularity(song_title: str, artist: str):
    """
    Query Last.fm for listener_count and playcount.
    Returns a dictionary or None if not found.
    """

    api_key = os.getenv("LASTFM_API_KEY")

    if not api_key:
        raise RuntimeError(
            "ERROR: LASTFM_API_KEY environment variable not set.\n"
            "Please run:\n"
            "export LASTFM_API_KEY=\"yourkeyhere\""
        )

    params = {
        "method": "track.getInfo",
        "api_key": api_key,
        "artist": artist,
        "track": song_title,
        "format": "json"
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        data = response.json()
    except Exception as e:
        print(f"Request error for {song_title} - {artist}: {e}")
        return None

    # Last.fm returns error in JSON sometimes
    if "error" in data:
        print(f"Last.fm returned error for {song_title} - {artist}: {data.get('message')}")
        return None

    track = data.get("track")
    if not track:
        print(f"No Last.fm track data found for {song_title} - {artist}")
        return None

    listeners = track.get("listeners")
    playcount = track.get("playcount")

    if listeners is None or playcount is None:
        print(f"Missing popularity data for {song_title} - {artist}")
        return None

    try:
        return {
            "listener_count": int(listeners),
            "playcount": int(playcount)
        }
    except:
        print(f"Could not convert popularity values for {song_title} - {artist}")
        return None


# STORE ROW INTO Popularity TABLE
def store_popularity(song_id: int, listeners: int, plays: int, db_name=DB_NAME):
    conn = get_connection(db_name)
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO Popularity (song_id, listener_count, playcount)
        VALUES (?, ?, ?)
        """,
        (song_id, listeners, plays)
    )

    conn.commit()
    conn.close()


# MAIN POPULATE FUNCTION — RUNS THROUGH Songs TABLE
def populate_lastfm(limit=150):
    """
    Fetch Last.fm popularity for up to `limit` songs
    that do NOT already have a row in Popularity.
    """
    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT s.song_id, ss.song_title, ss.artist_name
        FROM Songs s
        JOIN ScrapedSongs ss ON ss.id = s.scraped_song_id
        WHERE s.song_id NOT IN (SELECT song_id FROM Popularity)
        ORDER BY s.song_id
        LIMIT ?
    """

    rows = cur.execute(query, (limit,)).fetchall()
    conn.close()

    print(f"Found {len(rows)} songs without Last.fm data (limit={limit}).")

    for song_id, title, artist in rows:
        print("\n-----------------------------------------")
        print(f"Song_id={song_id} | {title} — {artist}")

        popularity = get_lastfm_popularity(title, artist)
        if popularity is None:
            print("  ❌ Skipping (not found).")
            continue

        store_popularity(song_id, popularity["listener_count"], popularity["playcount"])
        print(f"  ✅ Stored Last.fm popularity: listeners={popularity['listener_count']}, plays={popularity['playcount']}")


def main():
    # Up to 150 songs – change this number if you want
    populate_lastfm(limit=150)


if __name__ == "__main__":
    main()
