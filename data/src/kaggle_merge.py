import os
import re
import sqlite3
import pandas as pd

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "afa.db")

# Change this to your actual Kaggle file name in data/src
KAGGLE_CSV_PATH = os.path.join(BASE_DIR, "spotify_kaggle_audio.csv")


def normalize_title(text: str) -> str:
    if not isinstance(text, str):
        return ""
    t = text.lower()

    # Remove text in parentheses: "Song (Remastered)" -> "Song "
    t = re.sub(r"\([^)]*\)", " ", t)

    # Cut off after " - " ("Song - From The Movie")
    t = re.split(r"\s+-\s+", t)[0]

    # Cut off after feat/ft/featuring
    t = re.split(r"\s+feat\.|\s+ft\.|\s+featuring", t)[0]

    # Keep only letters, numbers, spaces
    t = re.sub(r"[^a-z0-9\s]", " ", t)

    # Collapse spaces
    t = re.sub(r"\s+", " ", t).strip()
    return t


def normalize_artist(text: str) -> str:
    if not isinstance(text, str):
        return ""
    t = text.lower()

    # Keep only the first primary artist before &, and, feat, ft, etc.
    t = re.split(r"\s+feat\.|\s+ft\.|\s+featuring|\s+and\s+|&", t)[0]

    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def load_kaggle_audio():
    print(f"Loading Kaggle audio features from: {KAGGLE_CSV_PATH}")
    df = pd.read_csv(KAGGLE_CSV_PATH)

    keep_cols = [
        "track_name", "track_artist",
        "danceability", "energy", "speechiness",
        "acousticness", "instrumentalness", "liveness",
        "valence", "tempo", "track_popularity",
        "playlist_genre", "playlist_subgenre",
    ]
    df = df[keep_cols]

    df["track_name_norm"] = df["track_name"].apply(normalize_title)
    df["track_artist_norm"] = df["track_artist"].apply(normalize_artist)

    print("Kaggle audio features loaded:", len(df), "rows")
    return df


def load_project_songs():
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT
            s.id AS scraped_id,
            s.song_title,
            s.artist_name,
            t.song_id
        FROM ScrapedSongs s
        JOIN Songs t ON t.scraped_song_id = s.id
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    print("Loaded project songs from DB:", len(df), "rows")

    df["song_title_norm"] = df["song_title"].apply(normalize_title)
    df["artist_name_norm"] = df["artist_name"].apply(normalize_artist)

    return df


def merge_kaggle_with_db_songs(kaggle_df, songs_df):
    merged = songs_df.merge(
        kaggle_df,
        left_on=["song_title_norm", "artist_name_norm"],
        right_on=["track_name_norm", "track_artist_norm"],
        how="left",
        suffixes=("_proj", "_kaggle"),
    )

    print("\nMerge result:")
    print("Total songs in Songs table:", len(merged))
    print("Matched rows with non-null valence:", merged["valence"].notna().sum())

    print("\nSample matched rows:")
    print(
        merged[merged["valence"].notna()][
            [
                "song_title",
                "artist_name",
                "track_name",
                "track_artist",
                "valence",
                "energy",
                "danceability",
            ]
        ].head()
    )

    return merged


def update_spotify_audiofeatures(merged_df):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    updated_count = 0

    for _, row in merged_df.iterrows():
        if pd.isna(row["valence"]):
            continue  # nothing from Kaggle, skip

        song_id = int(row["song_id"])
        valence = float(row["valence"])
        energy = float(row["energy"])
        danceability = float(row["danceability"])
        tempo = float(row["tempo"])
        acousticness = float(row["acousticness"])
        instrumentalness = float(row["instrumentalness"])

        cur.execute(
            """
            UPDATE SpotifyAudioFeatures
            SET
                valence = ?,
                energy = ?,
                danceability = ?,
                tempo = ?,
                acousticness = ?,
                instrumentalness = ?
            WHERE song_id = ?
            """,
            (
                valence,
                energy,
                danceability,
                tempo,
                acousticness,
                instrumentalness,
                song_id,
            ),
        )
        updated_count += 1

    conn.commit()
    conn.close()

    print(f"\nUpdated SpotifyAudioFeatures for {updated_count} songs.")


def main():
    kaggle_df = load_kaggle_audio()
    songs_df = load_project_songs()
    merged = merge_kaggle_with_db_songs(kaggle_df, songs_df)
    update_spotify_audiofeatures(merged)


if __name__ == "__main__":
    main()
