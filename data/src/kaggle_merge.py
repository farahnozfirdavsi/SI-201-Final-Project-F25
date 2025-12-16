import os
import sqlite3
import pandas as pd

# DB PATH  
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "afa.db")

#  Kaggle CSV  
KAGGLE_CSV = os.path.join(BASE_DIR, "spotify_kaggle_audio.csv")


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    # timeout helps with occasional "database is locked"
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def normalize(s) -> str:
    if s is None:
        return ""
    return (
        str(s)
        .lower()
        .strip()
        .replace("&", "and")
        .replace("’", "'")
        .replace("feat.", "ft.")
    )


def ensure_audiofeatures_unique_songid(conn: sqlite3.Connection) -> None:
    """
    Needed so ON CONFLICT(song_id) works for UPSERT.
    Safe: does not delete anything.
    """
    cur = conn.cursor()
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_audiofeatures_songid
        ON SpotifyAudioFeatures(song_id);
    """)
    conn.commit()


def load_project_songs(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Pull song_id + song_title + artist_name from your normalized schema:
    Songs -> ScrapedSongs -> Artists
    """
    q = """
    SELECT
        so.song_id,
        ss.song_title,
        ar.artist_name
    FROM Songs so
    JOIN ScrapedSongs ss ON ss.id = so.scraped_song_id
    JOIN Artists ar ON ar.artist_id = ss.artist_id;
    """
    return pd.read_sql_query(q, conn)


def main():
    print("Loading Kaggle audio features from:", KAGGLE_CSV)
    kag = pd.read_csv(KAGGLE_CSV)
    print("Kaggle audio features loaded:", len(kag), "rows")

    # Validate required columns based on YOUR Kaggle file
    required = {"track_name", "track_artist", "valence", "energy", "danceability", "tempo", "acousticness", "instrumentalness"}
    missing = required - set(kag.columns)
    if missing:
        raise ValueError(f"Kaggle CSV missing columns: {missing}")

    conn = get_connection(DB_PATH)
    ensure_audiofeatures_unique_songid(conn)

    proj = load_project_songs(conn)
    print("Loaded project songs from DB:", len(proj), "rows")

    # Normalize join keys
    proj["key_title"] = proj["song_title"].map(normalize)
    proj["key_artist"] = proj["artist_name"].map(normalize)

    kag["key_title"] = kag["track_name"].map(normalize)
    kag["key_artist"] = kag["track_artist"].map(normalize)

    # Many Kaggle rows repeat the same song across playlists.
    # Collapse to one row per (title, artist) to avoid duplicates during merge.
    kag_small = (
        kag.sort_values("track_popularity", ascending=False)
           .drop_duplicates(subset=["key_title", "key_artist"])
           [["key_title", "key_artist", "valence", "energy", "danceability", "tempo", "acousticness", "instrumentalness"]]
    )

    merged = proj.merge(kag_small, how="left", on=["key_title", "key_artist"])

    matched = merged[merged["valence"].notna()].copy()

    print("\nMerge result:")
    print("Total songs in Songs table:", len(proj))
    print("Matched rows with non-null valence:", len(matched))

    print("\nSample matched rows:")
    print(matched[["song_title", "artist_name", "valence", "energy", "danceability"]].head())

    # Prepare rows for UPSERT
    rows = []
    for _, r in matched.iterrows():
        rows.append((
            int(r["song_id"]),
            float(r["valence"]) if pd.notna(r["valence"]) else None,
            float(r["energy"]) if pd.notna(r["energy"]) else None,
            float(r["danceability"]) if pd.notna(r["danceability"]) else None,
            float(r["tempo"]) if pd.notna(r["tempo"]) else None,
            float(r["acousticness"]) if pd.notna(r["acousticness"]) else None,
            float(r["instrumentalness"]) if pd.notna(r["instrumentalness"]) else None,
        ))

    cur = conn.cursor()
    cur.executemany("""
        INSERT INTO SpotifyAudioFeatures
            (song_id, valence, energy, danceability, tempo, acousticness, instrumentalness)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(song_id) DO UPDATE SET
            valence = excluded.valence,
            energy = excluded.energy,
            danceability = excluded.danceability,
            tempo = excluded.tempo,
            acousticness = excluded.acousticness,
            instrumentalness = excluded.instrumentalness;
    """, rows)

    conn.commit()
    conn.close()

    print(f"\nUpserted SpotifyAudioFeatures for {len(rows)} songs.")


if __name__ == "__main__":
    main()
