import os
import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# ==========================
# Cute global pastel theme 🌸
# ==========================

sns.set_theme(
    style="whitegrid",
    rc={
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.facecolor": "#FDF6F9",    # pale pink
        "figure.facecolor": "#FFFFFF",  # white
        "grid.color": "#F3DDE8",        # light pastel grid
        "lines.linewidth": 2,
        "font.size": 11,
    },
)

PASTEL_COLORS = [
    "#FFB7C5",  # baby pink
    "#A7D2CB",  # mint
    "#C5A3FF",  # lavender
    "#F7D488",  # peachy yellow
    "#9AD1D4",  # soft teal
]

# Where to save plots
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # data/src
DB_PATH = os.path.join(BASE_DIR, "afa.db")
PLOTS_DIR = os.path.join(BASE_DIR, "plots")
os.makedirs(PLOTS_DIR, exist_ok=True)


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


# ==========================
# Data helper functions
# ==========================

def get_weekly_mood_and_anxiety():
    """
    Returns a DataFrame with:
      date, avg_valence, avg_energy, anxiety_percent, depression_percent
    Uses:
      ScrapedSongs, Songs, SpotifyAudioFeatures, MentalHealthTrends
    """
    conn = get_connection()

    # Songs + audio features
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

    # CDC weekly mental health
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
        songs_df.groupby("chart_date", as_index=False)
        .agg(
            avg_valence=("valence", "mean"),
            avg_energy=("energy", "mean"),
        )
    )
    weekly_mood = weekly_mood.rename(columns={"chart_date": "date"})
    mh_df = mh_df.rename(columns={"week": "date"})

    # nearest-week merge (because Billboard weeks & CDC weeks don't match exactly)
    merged = pd.merge_asof(
        left=weekly_mood.sort_values("date"),
        right=mh_df.sort_values("date"),
        on="date",
        direction="nearest",
    )

    return merged


def get_valence_vs_popularity():
    """
    Returns DataFrame with valence + Spotify popularity
    (for scatter plot: Do happier songs get more popular?)
    """
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
    return df


def get_valence_vs_listeners():
    """
    Returns DataFrame with valence + Last.fm listener_count
    (for scatter plot: Do happier songs get more listens?)
    """
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT 
            saf.valence,
            p.listener_count
        FROM SpotifyAudioFeatures saf
        JOIN Popularity p ON p.song_id = saf.song_id
        WHERE saf.valence IS NOT NULL
          AND p.listener_count IS NOT NULL
        """,
        conn,
    )
    conn.close()
    return df


def get_artist_emotional_profile(min_songs=2, top_n=15):
    """
    Returns DataFrame grouped by artist_name with:
      artist_name, avg_valence, avg_energy, n_songs
    Filters to artists with at least min_songs, then keeps top_n by avg_valence.
    """
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
        df.groupby("artist_name", as_index=False)
        .agg(
            avg_valence=("valence", "mean"),
            avg_energy=("energy", "mean"),
            n_songs=("valence", "count"),
        )
    )

    filtered = grouped[grouped["n_songs"] >= min_songs]
    filtered = filtered.sort_values("avg_valence", ascending=False).head(top_n)
    return filtered


def get_high_low_anxiety_valence():
    """
    Uses the weekly merged data to compute average valence
    for 'High Anxiety' vs 'Low Anxiety' weeks.
    """
    df = get_weekly_mood_and_anxiety()
    if df.empty:
        return df

    # Use median anxiety as the split point
    threshold = df["anxiety_percent"].median()
    df["anxiety_group"] = np.where(
        df["anxiety_percent"] >= threshold,
        "High Anxiety Weeks",
        "Low Anxiety Weeks",
    )

    grouped = (
        df.groupby("anxiety_group", as_index=False)
        .agg(avg_valence=("avg_valence", "mean"))
    )
    return grouped


# ==========================
# Pretty plot functions 💖
# ==========================

def plot_weekly_mood_vs_anxiety():
    """
    Dual-axis line chart:
      X = date
      Left Y = avg_valence (music mood, 0–1)
      Right Y = anxiety_percent (CDC, %)

    Also prints the Pearson correlation between avg_valence
    and anxiety_percent to the console.
    """
    print("\n[STEP] Plot: Weekly Music Mood vs Anxiety")
    df = get_weekly_mood_and_anxiety()
    if df.empty:
        print("⚠ No weekly mood/anxiety data.")
        return

    # Correlation for the report
    corr = df["avg_valence"].corr(df["anxiety_percent"])
    print(f"Correlation between avg valence and anxiety %: {corr:.3f}")

    fig, ax1 = plt.subplots(figsize=(11, 6))

    color_valence = PASTEL_COLORS[0]   # pink
    color_anxiety = PASTEL_COLORS[2]   # lavender

    # ---- Left axis line: avg valence ----
    line1 = ax1.plot(
        df["date"],
        df["avg_valence"],
        color=color_valence,
        marker="o",
        markersize=6,
        label="Avg Valence (Music Mood)",
    )[0]

    ax1.set_xlabel("Week")
    ax1.set_ylabel("Average Valence (0–1)", color=color_valence)
    ax1.tick_params(axis="y", colors=color_valence)

    # ---- Right axis line: anxiety % ----
    ax2 = ax1.twinx()
    line2 = ax2.plot(
        df["date"],
        df["anxiety_percent"],
        color=color_anxiety,
        marker="D",
        markersize=5,
        label="Anxiety % (CDC)",
    )[0]

    ax2.set_ylabel("Anxiety %", color=color_anxiety)
    ax2.tick_params(axis="y", colors=color_anxiety)

    # ---- Correct combined legend ----
    fig.legend(
        handles=[line1, line2],
        labels=["Avg Valence (Music Mood)", "Anxiety % (CDC)"],
        loc="upper left",
        frameon=False,
    )

    plt.title("Music Mood vs Anxiety Over Time (Two Y-Axes)", fontsize=14)
    plt.xticks(rotation=45, ha="right")
    fig.tight_layout()

    out_path = os.path.join(PLOTS_DIR, "line_mood_vs_anxiety.png")
    plt.savefig(out_path)
    print(f"[SAVED] {out_path}")
    plt.show()


def plot_valence_vs_popularity():
    """
    Scatter plot:
      X = valence (happiness)
      Y = Spotify popularity
    """
    print("\n[STEP] Plot: Valence vs Spotify Popularity")
    df = get_valence_vs_popularity()
    if df.empty:
        print("⚠ No valence/popularity data.")
        return

    plt.figure(figsize=(9, 6))
    sns.scatterplot(
        data=df,
        x="valence",
        y="popularity",
        color=PASTEL_COLORS[1],  # mint
        s=60,
        alpha=0.7,
        edgecolor="white",
    )

    plt.title("Do Happier Songs Get More Popular?", fontsize=14)
    plt.xlabel("Valence (Happiness Score)")
    plt.ylabel("Spotify Popularity (0–100)")
    plt.tight_layout()

    out_path = os.path.join(PLOTS_DIR, "scatter_valence_vs_popularity.png")
    plt.savefig(out_path)
    print(f"[SAVED] {out_path}")
    plt.show()


def plot_valence_vs_listeners():
    """
    Scatter plot:
      X = valence (happiness)
      Y = Last.fm listener_count
    """
    print("\n[STEP] Plot: Valence vs Last.fm Listener Count")
    df = get_valence_vs_listeners()
    if df.empty:
        print("⚠ No valence/listener_count data.")
        return

    plt.figure(figsize=(9, 6))
    sns.scatterplot(
        data=df,
        x="valence",
        y="listener_count",
        color=PASTEL_COLORS[3],  # peach
        s=70,
        alpha=0.7,
        edgecolor="white",
    )

    plt.title("Do Happier Songs Get More Listens?", fontsize=14)
    plt.xlabel("Valence (Happiness Score)")
    plt.ylabel("Last.fm Listener Count")
    plt.tight_layout()

    out_path = os.path.join(PLOTS_DIR, "scatter_valence_vs_listener_count.png")
    plt.savefig(out_path)
    print(f"[SAVED] {out_path}")
    plt.show()


def plot_artist_emotional_profile():
    """
    Bar chart:
      X = artist_name
      Y = average valence
    """
    print("\n[STEP] Plot: Artist Emotional Profile")
    df = get_artist_emotional_profile()
    if df.empty:
        print("⚠ No artist emotional profile data.")
        return

    plt.figure(figsize=(12, 6))
    sns.barplot(
        data=df,
        x="artist_name",
        y="avg_valence",
        palette=PASTEL_COLORS,
        edgecolor="white",
    )
    plt.title("Emotional Profile of Artists (Average Valence)", fontsize=14)
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
    Grouped bar chart:
      X = Week Type (High vs Low Anxiety)
      Y = Average Song Valence (Happiness Score)
    """
    print("\n[STEP] Plot: High vs Low Anxiety Weeks (Avg Valence)")
    df = get_high_low_anxiety_valence()
    if df.empty:
        print("⚠ No data for high/low anxiety comparison.")
        return

    plt.figure(figsize=(7, 5))
    sns.barplot(
        data=df,
        x="anxiety_group",
        y="avg_valence",
        palette=[PASTEL_COLORS[4], PASTEL_COLORS[0]],
        edgecolor="white",
    )

    plt.title("Music Mood in High-Anxiety vs Low-Anxiety Weeks", fontsize=14)
    plt.xlabel("Week Type")
    plt.ylabel("Average Valence (0–1)")
    plt.tight_layout()

    out_path = os.path.join(PLOTS_DIR, "bar_high_low_anxiety_valence.png")
    plt.savefig(out_path)
    print(f"[SAVED] {out_path}")
    plt.show()


# ==========================
# Main
# ==========================

def main():
    print("Plotting weekly mood vs anxiety...")
    plot_weekly_mood_vs_anxiety()

    print("Plotting valence vs Spotify popularity...")
    plot_valence_vs_popularity()

    print("Plotting valence vs Last.fm listener count...")
    plot_valence_vs_listeners()

    print("Plotting artist emotional profile...")
    plot_artist_emotional_profile()

    print("Plotting high vs low anxiety valence...")
    plot_high_low_anxiety_valence()


if __name__ == "__main__":
    main()
