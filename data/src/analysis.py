import os
import sqlite3
import pandas as pd


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "afa.db")


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def get_weekly_mood_and_anxiety():
    """
    Weekly Average Music Mood vs Mental Health

    - JOIN ScrapedSongs + Songs + SpotifyAudioFeatures
    - Compute average valence and energy per chart_date
    - Align with CDC weekly anxiety/depression via nearest-date merge
    """
    conn = get_connection()

    # Song-level mood data (only where we have valence)
    songs_df = pd.read_sql_query(
        """
        SELECT
            ss.chart_date AS chart_date,
            saf.valence,
            saf.energy,
            saf.danceability,
            saf.tempo,
            saf.acousticness
        FROM ScrapedSongs ss
        JOIN Songs s
          ON s.scraped_song_id = ss.id
        JOIN SpotifyAudioFeatures saf
          ON saf.song_id = s.song_id
        WHERE saf.valence IS NOT NULL
        """,
        conn,
    )

    # CDC weekly anxiety/depression
    mh_df = pd.read_sql_query(
        """
        SELECT
            week,
            anxiety_percent,
            depression_percent
        FROM MentalHealthTrends
        """,
        conn,
    )

    conn.close()

    # Convert date strings → datetime
    songs_df["chart_date"] = pd.to_datetime(songs_df["chart_date"])
    mh_df["week"] = pd.to_datetime(mh_df["week"])

    # Average valence/energy per chart date
    weekly_mood = (
        songs_df.groupby("chart_date", as_index=False)
        .agg(
            avg_valence=("valence", "mean"),
            avg_energy=("energy", "mean"),
            avg_danceability=("danceability", "mean"),
            avg_tempo=("tempo", "mean")
        )
    )


    mh_df = mh_df.sort_values("week")

    # Align Billboard chart dates to nearest CDC week date
    merged = pd.merge_asof(
        left=weekly_mood.rename(columns={"chart_date": "date"}),
        right=mh_df.rename(columns={"week": "date"}),
        on="date",
        direction="nearest",
    )

    return merged


def get_valence_vs_popularity():
    """
    Listening Behavior vs Mood

    - JOIN Songs + SpotifyAudioFeatures
    - Return rows with non-null valence and popularity
    - Used for scatter(popularity, valence)
    """
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT
            saf.valence,
            s.popularity
        FROM SpotifyAudioFeatures saf
        JOIN Songs s
          ON s.song_id = saf.song_id
        WHERE saf.valence IS NOT NULL
          AND s.popularity IS NOT NULL
        """,
        conn,
    )
    conn.close()
    return df


def get_artist_emotional_profile(min_songs=2, top_n=15):
    """
    Genre or Artist Emotional Profiles (we'll do ARTIST profiles)

    - For each artist:
        * avg valence
        * avg energy
        * number of songs we have with features
    - Filter to artists with at least min_songs
    - Return top_n artists by n_songs
    """
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT
            ss.artist_name,
            saf.valence,
            saf.energy
        FROM ScrapedSongs ss
        JOIN Songs s
          ON s.scraped_song_id = ss.id
        JOIN SpotifyAudioFeatures saf
          ON saf.song_id = s.song_id
        WHERE saf.valence IS NOT NULL
        """,
        conn,
    )
    conn.close()

    grouped = (
        df.groupby("artist_name")
        .agg(
            avg_valence=("valence", "mean"),
            avg_energy=("energy", "mean"),
            n_songs=("valence", "count"),
        )
        .reset_index()
    )

    filtered = grouped[grouped["n_songs"] >= min_songs]
    filtered = filtered.sort_values(
        by=["n_songs", "avg_valence"], ascending=[False, False]
    )

    return filtered.head(top_n)


def compute_correlations():
    """
    Correlation Between Music Energy & Depression Levels (and valence & anxiety)

    - correlation(valence, anxiety_percent)
    - correlation(energy, depression_percent)
    - correlation(valence, popularity)
    """
    weekly = get_weekly_mood_and_anxiety()
    vp = get_valence_vs_popularity()

    print("Weekly mood + anxiety/depression (first rows):")
    print(weekly.head(), "\n")

    # Correlation between weekly mood and mental health
    corr_weekly = weekly[["avg_valence", "avg_energy", "anxiety_percent", "depression_percent"]].corr()
    print("Correlation matrix (weekly averages):")
    print(corr_weekly, "\n")

    # Correlation between valence and popularity
    corr_vp = vp[["valence", "popularity"]].corr()
    print("Correlation valence vs popularity:")
    print(corr_vp, "\n")

    # Optionally: return these to use in report if you want
    return corr_weekly, corr_vp


def main():
    weekly = get_weekly_mood_and_anxiety()
    print("Weekly merged mood + anxiety (first 10 rows):")
    print(weekly.head(10), "\n")

    vp = get_valence_vs_popularity()
    print("Valence vs popularity sample (first 10 rows):")
    print(vp.head(10), "\n")

    artist_profile = get_artist_emotional_profile()
    print("Artist emotional profile sample:")
    print(artist_profile, "\n")

    compute_correlations()


if __name__ == "__main__":
    main()

