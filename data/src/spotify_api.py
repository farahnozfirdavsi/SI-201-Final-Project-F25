import os
import sqlite3
import time

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "afa.db")


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def get_spotify_client():
    client_id = os.getenv("SPOTIPY_CLIENT_ID")
    client_secret = os.getenv("SPOTIPY_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise RuntimeError(
            "SPOTIPY_CLIENT_ID and/or SPOTIPY_CLIENT_SECRET are not set.\n"
            "Run:\n"
            '  export SPOTIPY_CLIENT_ID="your_client_id"\n'
            '  export SPOTIPY_CLIENT_SECRET="your_client_secret"'
        )

    auth_manager = SpotifyClientCredentials()
    sp = spotipy.Spotify(auth_manager=auth_manager)
    return sp


def get_spotify_track(sp, song_title, artist_name):
    """
    Use Spotify search to find a track and return basic metadata:
    track_id, canonical title/artist, popularity, release_year.

    Returns None if no suitable result is found.
    """

    query = f"track:{song_title} artist:{artist_name}"
    try:
        results = sp.search(q=query, type="track", limit=1)
    except Exception as e:
        print(f"Spotify search error for {song_title} - {artist_name}: {e}")
        return None

    items = results.get("tracks", {}).get("items", [])
    if not items:
        # Try a looser search: just the title
        try:
            results = sp.search(q=song_title, type="track", limit=1)
            items = results.get("tracks", {}).get("items", [])
        except Exception as e:
            print(f"Spotify loose search error for {song_title}: {e}")
            return None

        if not items:
            print(f"No Spotify results for: {song_title} - {artist_name}")
            return None

    track = items[0]
    track_id = track["id"]
    popularity = track.get("popularity", None)

    # release_date can be "YYYY-MM-DD", "YYYY-MM", or "YYYY"
    release_date = track["album"].get("release_date", "")
    release_year = None
    if len(release_date) >= 4 and release_date[:4].isdigit():
        release_year = int(release_date[:4])

    track_title = track["name"]
    main_artist = track["artists"][0]["name"]

    return {
        "spotify_track_id": track_id,
        "song_title": track_title,
        "artist_name": main_artist,
        "popularity": popularity,
        "release_year": release_year,
    }


def insert_song_and_audio_row(cur, scraped_id, track_info):
    """
    Insert into Songs and create a placeholder row in SpotifyAudioFeatures
    with null audio features (to be filled by Kaggle later).
    """
    cur.execute(
        """
        INSERT INTO Songs (scraped_song_id, spotify_track_id, popularity, release_year)
        VALUES (?, ?, ?, ?)
        """,
        (
            scraped_id,
            track_info["spotify_track_id"],
            track_info["popularity"],
            track_info["release_year"],
        ),
    )
    song_id = cur.lastrowid

    # Ensure a matching row in SpotifyAudioFeatures
    cur.execute(
        """
        INSERT INTO SpotifyAudioFeatures (song_id, valence, energy, danceability, tempo, acousticness, instrumentalness)
        VALUES (?, NULL, NULL, NULL, NULL, NULL, NULL)
        """,
        (song_id,),
    )
    return song_id


def populate_spotify_for_scraped_songs(limit=200):
    """
    For ScrapedSongs rows that do not yet have Songs entries,
    query Spotify and create rows in Songs + SpotifyAudioFeatures.
    """

    sp = get_spotify_client()
    conn = get_connection()
    cur = conn.cursor()

    # Select scraped songs that do NOT yet have a Songs row
    query = """
        SELECT s.id, s.song_title, s.artist_name
        FROM ScrapedSongs s
        LEFT JOIN Songs t ON t.scraped_song_id = s.id
        WHERE t.song_id IS NULL
        ORDER BY s.id
    """

    if limit is not None:
        query += f" LIMIT {int(limit)}"

    rows = cur.execute(query).fetchall()
    print(f"Found {len(rows)} scraped songs without Spotify metadata.")

    processed = 0

    for scraped_id, song_title, artist_name in rows:
        print("\n------------------------------------")
        print(f"ScrapedSongs.id={scraped_id} | {song_title} — {artist_name}")

        track_info = get_spotify_track(sp, song_title, artist_name)
        if track_info is None:
            print("  Could not find a suitable Spotify track. Skipping.")
            continue

        print(
            f"  Spotify match: {track_info['song_title']} — {track_info['artist_name']} "
            f"(id={track_info['spotify_track_id']}, pop={track_info['popularity']}, year={track_info['release_year']})"
        )

        song_id = insert_song_and_audio_row(cur, scraped_id, track_info)
        conn.commit()
        processed += 1
        print(f"  Inserted as Songs.song_id={song_id} and created placeholder SpotifyAudioFeatures row.")

        # Optional small delay to be polite to the API
        time.sleep(0.1)

    conn.close()
    print(f"\nDone. Inserted Spotify metadata for {processed} songs.")


def main():
    # Adjust limit as needed; 150–300 is plenty for the project
    populate_spotify_for_scraped_songs(limit=800)


if __name__ == "__main__":
    main()
