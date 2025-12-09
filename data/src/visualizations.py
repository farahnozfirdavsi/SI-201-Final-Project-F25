import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "afa.db")

PLOTS_DIR = os.path.join(BASE_DIR, "plots")
os.makedirs(PLOTS_DIR, exist_ok=True)


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def get_weekly_mood_and_anxiety():
    """
    Returns a DataFrame with:
      date, avg_valence, avg_energy, anxiety_percent, depression_percent
    """
    conn = get_connection()

    songs_df = pd.read_sql_query(
        """
        SELECT 
            ss.chart_date AS chart_date,
            saf.valence,
            saf.energy
        FROM ScrapedSongs ss
        JOIN Songs s ON s.scraped_song_id = ss.id
        JOIN SpotifyAudioFeatures saf ON saf.song_id = s.song_id
        WHERE saf.valence IS NOT NULL
        """,
        conn,
    )

    mh_df = pd.read_sql_query(
        """
        SELECT week, anxiety_percent, depression_percent
        FROM MentalHealthTrends
        """,
        conn,
    )

    conn.close()

    songs_df["chart_date"] = pd.to_datetime(songs_df["chart_date"])
    mh_df["week"] = pd.to_datetime(mh_df["week"])

    weekly_mood = (
        songs_df.groupby("chart_date")
        .agg(
            avg_valence=("valence", "mean"),
            avg_energy=("energy", "mean"),
        )
        .reset_index()
    )
    weekly_mood = weekly_mood.rename(columns={"chart_date": "date"})

    mh_df = mh_df.rename(columns={"week": "date"})

    merged = pd.merge_asof(
        left=weekly_mood.sort_values("date"),
        right=mh_df.sort_values("date"),
        on="date",
        direction="nearest",
    )

    return merged


def plot_valence_vs_popularity():
    """
    X = valence
    Y = Spotify popularity (0–100)
    Title: Do Happier Songs Get More Popular?
    """
    print("\n[STEP] Plot 1: Valence vs Spotify Popularity")

    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT 
            saf.valence,
            s.popularity
        FROM SpotifyAudioFeatures saf
        JOIN Songs s ON s.song_id = saf.song_id
        WHERE saf.valence IS NOT NULL
          AND s.popularity IS NOT NULL
        """,
        conn,
    )
    conn.close()

    print("[DEBUG] valence vs popularity shape:", df.shape)
    if df.empty:
        print("⚠ No valence/popularity data available.")
        return

    plt.figure(figsize=(8, 6))
    sns.scatterplot(data=df, x="valence", y="popularity", alpha=0.6)
    plt.title("Do Happier Songs Get More Popular?")
    plt.xlabel("Valence (Happiness Score)")
    plt.ylabel("Spotify Popularity (0–100)")
    plt.tight_layout()

    out_path = os.path.join(PLOTS_DIR, "scatter_valence_vs_popularity.png")
    plt.savefig(out_path)
    print(f"[SAVED] {out_path}")
    plt.show()


def plot_weekly_mood_vs_anxiety():
    """
    X = date (week)
    Y1 = avg_valence (left axis)
    Y2 = anxiety_percent (right axis)
    Title: Music Mood vs Anxiety Over Time
    """
    print("\n[STEP] Plot 2: Weekly Mood vs Anxiety")

    df = get_weekly_mood_and_anxiety()
    print("[DEBUG] weekly mood+anxiety shape:", df.shape)
    if df.empty:
        print("⚠ No weekly mood/anxiety data found.")
        return

    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x="date", y="avg_valence", label="Average Valence")

    ax2 = plt.twinx()
    sns.lineplot(
        data=df,
        x="date",
        y="anxiety_percent",
        color="red",
        label="Anxiety %",
        ax=ax2,
    )

    plt.title("Music Mood vs Anxiety Over Time")
    plt.xlabel("Week")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    out_path = os.path.join(PLOTS_DIR, "line_mood_vs_anxiety.png")
    plt.savefig(out_path)
    print(f"[SAVED] {out_path}")
    plt.show()



def plot_artist_emotional_profile(min_songs=2, top_n=15):
    """
    X = artist_name
    Y = avg_valence
    Title: Emotional Profile of Artists
    Only includes artists with at least min_songs tracks.
    """
    print("\n[STEP] Plot 3: Artist Emotional Profile")

    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT 
            ss.artist_name,
            saf.valence,
            saf.energy
        FROM ScrapedSongs ss
        JOIN Songs s ON s.scraped_song_id = ss.id
        JOIN SpotifyAudioFeatures saf ON saf.song_id = s.song_id
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
    filtered = filtered.sort_values("avg_valence", ascending=False).head(top_n)

    print("[DEBUG] artist emotional profile shape:", filtered.shape)
    if filtered.empty:
        print("⚠ No artist groups with enough songs to plot.")
        return

    plt.figure(figsize=(12, 6))
    sns.barplot(data=filtered, x="artist_name", y="avg_valence")
    plt.title("Emotional Profile of Artists (Average Valence)")
    plt.xlabel("Artist")
    plt.ylabel("Average Valence")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    out_path = os.path.join(PLOTS_DIR, "bar_artist_emotional_profile.png")
    plt.savefig(out_path)
    print(f"[SAVED] {out_path}")
    plt.show()



def plot_high_low_anxiety_valence():
    """
    X = anxiety group (High vs Low)
    Y = avg_valence
    Title: Music Mood in High-Anxiety vs Low-Anxiety Weeks
    """
    print("\n[STEP] Plot 4: High vs Low Anxiety Weeks")

    df = get_weekly_mood_and_anxiety()
    if df.empty:
        print("⚠ No weekly data for grouped bar chart.")
        return

    threshold = df["anxiety_percent"].mean()
    df["anxiety_group"] = df["anxiety_percent"].apply(
        lambda x: "High Anxiety Week" if x >= threshold else "Low Anxiety Week"
    )

    grouped = (
        df.groupby("anxiety_group")["avg_valence"]
        .mean()
        .reset_index()
    )

    print("[DEBUG] grouped anxiety valence:")
    print(grouped)

    plt.figure(figsize=(8, 6))
    sns.barplot(data=grouped, x="anxiety_group", y="avg_valence")
    plt.title("Music Mood in High-Anxiety vs Low-Anxiety Weeks")
    plt.xlabel("")
    plt.ylabel("Average Valence")
    plt.tight_layout()

    out_path = os.path.join(PLOTS_DIR, "grouped_high_low_anxiety_valence.png")
    plt.savefig(out_path)
    print(f"[SAVED] {out_path}")
    plt.show()



def main():
    plot_valence_vs_popularity()
    plot_weekly_mood_vs_anxiety()
    plot_artist_emotional_profile()
    plot_high_low_anxiety_valence()


if __name__ == "__main__":
    main()
