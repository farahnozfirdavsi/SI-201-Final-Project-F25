import os
import sqlite3
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException

DB_NAME = "afa.db"


# ---------- DB HELPERS ----------

def get_connection(db_name: str = DB_NAME) -> sqlite3.Connection:
    conn = sqlite3.connect(db_name)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def store_spotify_song(track_info: dict, scraped_song_id: int, db_name: str = DB_NAME) -> int:
    """
    Insert a Spotify track into Songs + SpotifyAudioFeatures tables.

    track_info should come from get_spotify_track and look like:
    {
      "track_id": str,
      "song_title": str,
      "artist_name": str,
      "release_year": int,
      "popularity": int,
      "valence": float | None,
      "energy": float | None,
      "danceability": float | None,
      "tempo": float | None,
      "acousticness": float | None,
      "instrumentalness": float | None,
      "genre": None or str
    }

    Returns the song_id (PK in Songs).
    """
    conn = get_connection(db_name)
    cur = conn.cursor()

    # Insert into Songs
    cur.execute(
        """
        INSERT INTO Songs (scraped_song_id, spotify_track_id, genre, popularity, release_year)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            scraped_song_id,
            track_info["track_id"],
            track_info.get("genre"),
            track_info.get("popularity"),
            track_info.get("release_year"),
        ),
    )

    song_id = cur.lastrowid

    # Insert into SpotifyAudioFeatures
    cur.execute(
        """
        INSERT INTO SpotifyAudioFeatures (
            song_id, valence, energy, danceability, tempo,
            acousticness, instrumentalness
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            song_id,
            track_info.get("valence"),
            track_info.get("energy"),
            track_info.get("danceability"),
            track_info.get("tempo"),
            track_info.get("acousticness"),
            track_info.get("instrumentalness"),
        ),
    )

    conn.commit()
    conn.close()
    return song_id


# ---------- SPOTIFY API HELPERS ----------

def get_spotify_client():
    """
    Create and return a Spotipy client using client credentials.

    Make sure you have set:
      export SPOTIPY_CLIENT_ID="your_client_id"
      export SPOTIPY_CLIENT_SECRET="your_client_secret"
    """
    client_id = os.getenv("SPOTIPY_CLIENT_ID")
    client_secret = os.getenv("SPOTIPY_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise RuntimeError(
            "Spotify credentials not found. "
            "Set SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET as environment variables."
        )

    auth_manager = SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret,
    )
    sp = spotipy.Spotify(auth_manager=auth_manager)
    return sp


def get_spotify_track(song_title: str, artist: str):
    """
    Search for a track on Spotify and return basic info + audio features.

    Strategy:
      1. Try strict search: track:{title} artist:{artist}
      2. If that fails, fall back to title-only search: {title}

    Returns a dict or None if track not found.
    """
    sp = get_spotify_client()

    # 1) Strict search
    query = f"track:{song_title} artist:{artist}"
    results = sp.search(q=query, type="track", limit=1)
    items = results.get("tracks", {}).get("items", [])

    # 2) Fallback: title-only search
    if not items:
        print(f"No strict match for: {song_title} - {artist}. Trying title-only search...")
        fallback_results = sp.search(q=song_title, type="track", limit=1)
        items = fallback_results.get("tracks", {}).get("items", [])

    if not items:
        print(f"No Spotify results found even with title-only search for: {song_title}")
        return None

    track = items[0]
    track_id = track["id"]
    track_name = track["name"]
    artist_name = track["artists"][0]["name"]
    popularity = track.get("popularity", None)

    # Extract release year from album release_date
    release_date = track["album"].get("release_date")
    release_year = None
    if release_date:
        release_year = int(release_date.split("-")[0])

    # Default result (even if we can't get audio features)
    result = {
        "track_id": track_id,
        "song_title": track_name,
        "artist_name": artist_name,
        "release_year": release_year,
        "popularity": popularity,
        "valence": None,
        "energy": None,
        "danceability": None,
        "tempo": None,
        "acousticness": None,
        "instrumentalness": None,
        "genre": None,
    }

    # Try to get audio features, but don't crash if forbidden
    try:
        audio_features_list = sp.audio_features([track_id])
        audio_features = audio_features_list[0] if audio_features_list else None
    except SpotifyException as e:
        print(f"Could not get audio features for {track_name} ({track_id}): {e}")
        audio_features = None

    if audio_features:
        result.update(
            {
                "valence": audio_features.get("valence"),
                "energy": audio_features.get("energy"),
                "danceability": audio_features.get("danceability"),
                "tempo": audio_features.get("tempo"),
                "acousticness": audio_features.get("acousticness"),
                "instrumentalness": audio_features.get("instrumentalness"),
            }
        )

    return result


# ---------- POPULATE MANY SONGS ----------

def populate_spotify_for_scraped_songs(limit: int | None = None, db_name: str = DB_NAME):
    """
    For scraped songs in ScrapedSongs, fetch Spotify info
    and insert into Songs + SpotifyAudioFeatures if not already present.

    If limit is None, process all scraped songs.
    """
    conn = get_connection(db_name)
    cur = conn.cursor()

    base_query = """
        SELECT id, song_title, artist_name
        FROM ScrapedSongs
        ORDER BY id
    """
    if limit is not None:
        base_query += " LIMIT ?"
        cur.execute(base_query, (limit,))
    else:
        cur.execute(base_query)

    rows = cur.fetchall()
    conn.close()

    print(f"Found {len(rows)} scraped songs to process.")

    for scraped_id, title, artist in rows:
        print("\n-------------------------------")
        print(f"ScrapedSongs.id={scraped_id} | {title} - {artist}")

        # Check if this scraped song already has a row in Songs
        conn = get_connection(db_name)
        cur = conn.cursor()
        cur.execute(
            "SELECT song_id FROM Songs WHERE scraped_song_id = ?",
            (scraped_id,),
        )
        existing = cur.fetchone()
        conn.close()

        if existing:
            print(f"  Already has Songs entry (song_id={existing[0]}). Skipping.")
            continue

        # Fetch from Spotify
        track_info = get_spotify_track(title, artist)
        if track_info is None:
            print("  Could not find this song on Spotify. Skipping.")
            continue

        # Store in DB
        song_id = store_spotify_song(track_info, scraped_id, db_name=db_name)
        print(f"  Inserted into Songs with song_id={song_id}.")


def main():
    # Process ALL scraped songs (or set a limit like 50 if you want to test)
    populate_spotify_for_scraped_songs(limit=150)


if __name__ == "__main__":
    main()
