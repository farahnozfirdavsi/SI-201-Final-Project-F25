import os
import sqlite3
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from keys import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 
DB_PATH = os.path.join(BASE_DIR, "afa.db")


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_spotify_client():
    """
    Initialize Spotipy client using keys from keys.py
    """
    auth_manager = SpotifyClientCredentials(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
    )
    return spotipy.Spotify(auth_manager=auth_manager)


def get_spotify_track(sp, title, artist):
    """
    Search for a track on Spotify by title + artist.
    Returns a dict with track_id, popularity, release_year or None if not found.
    """
    query = f"track:{title} artist:{artist}"
    results = sp.search(q=query, type="track", limit=1)

    items = results.get("tracks", {}).get("items", [])
    if not items:
        return None

    track = items[0]
    release_date = track["album"]["release_date"]  
    release_year = int(release_date[:4])

    return {
        "track_id": track["id"],
        "popularity": track["popularity"],
        "release_year": release_year,
    }


def store_song_row(conn, scraped_song_id, track_info):
    """
    Insert one row into Songs
    """
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO Songs (scraped_song_id, spotify_track_id, genre, popularity, release_year)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            scraped_song_id,
            track_info["track_id"],
            None,  # genre (we're not pulling Spotify genres here)
            track_info["popularity"],
            track_info["release_year"],
        ),
    )

    conn.commit()
    return cur.lastrowid


def populate_spotify_data(limit=25):
    """
    Grab up to `limit` rows from ScrapedSongs that do NOT yet have a Songs row,
    fetch Spotify track info for each, and store in Songs.

    Audio features (valence, energy, etc.) will be filled separately via the Kaggle merge
    """
    conn = get_connection()
    cur = conn.cursor()
    sp = init_spotify_client()

    print("Using database:", DB_PATH)

    rows = cur.execute(
    """
    SELECT ss.id, ss.song_title, a.artist_name
    FROM ScrapedSongs ss
    JOIN Artists a ON ss.artist_id = a.artist_id
    LEFT JOIN Songs s ON s.scraped_song_id = ss.id
    WHERE s.song_id IS NULL
    LIMIT ?;
    """,
    (limit,),
).fetchall()

    print(f"Found {len(rows)} scraped songs needing Spotify track info.")

    for scraped_id, title, artist in rows:
        print(f"\nProcessing ScrapedSongs.id={scraped_id} | {title} — {artist}")

        track_info = get_spotify_track(sp, title, artist)
        if track_info is None:
            print("  No Spotify match. Skipping.")
            continue

        song_id = store_song_row(conn, scraped_id, track_info)
        print(
            f"  Stored in Songs as song_id={song_id}, "
            f"popularity={track_info['popularity']}, "
            f"year={track_info['release_year']}"
        )

    conn.close()
    print("\nDone populating Spotify Songs (IDs + popularity + year).")


if __name__ == "__main__":
    populate_spotify_data()
