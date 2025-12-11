import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from analysis import (
    get_weekly_mood_and_anxiety,
    get_valence_vs_popularity,
    get_artist_emotional_profile,
)


def plot_weekly_mood_vs_anxiety():
    """
    Line chart:
      X = date
      Y1 = avg_valence (music mood)
      Y2 = anxiety_percent (CDC)
    """
    df = get_weekly_mood_and_anxiety()

    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x="date", y="avg_valence", label="Avg Valence (Music Mood)")

    ax2 = plt.twinx()
    sns.lineplot(
        data=df,
        x="date",
        y="anxiety_percent",
        label="Anxiety % (CDC)",
        ax=ax2,
    )

    plt.title("Music Mood vs Anxiety Over Time")
    plt.xlabel("Week")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()


def plot_valence_vs_popularity():
    """
    Scatter plot:
      X = valence (happiness)
      Y = Spotify popularity

    This answers:
    "Do happier songs get more playcount / popularity?"
    """
    df = get_valence_vs_popularity()

    plt.figure(figsize=(8, 6))
    sns.scatterplot(data=df, x="valence", y="popularity", alpha=0.6)
    plt.title("Do Happier Songs Get More Popular?")
    plt.xlabel("Valence (Happiness Score)")
    plt.ylabel("Spotify Popularity (0–100)")
    plt.tight_layout()
    plt.show()


def plot_artist_emotional_profile():
    """
    Bar chart:
      X = artist_name
      Y = average valence

    This implements:
    "Genre or Artist Emotional Profiles"
    """
    df = get_artist_emotional_profile()

    plt.figure(figsize=(10, 6))
    sns.barplot(
        data=df,
        x="artist_name",
        y="avg_valence",
    )
    plt.title("Emotional Profile of Artists (Average Valence)")
    plt.xlabel("Artist")
    plt.ylabel("Average Valence")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()



def main():
    print("Plotting weekly mood vs anxiety...")
    plot_weekly_mood_vs_anxiety()

    print("Plotting valence vs popularity...")
    plot_valence_vs_popularity()

    print("Plotting artist emotional profile...")
    plot_artist_emotional_profile()

if __name__ == "__main__":
    main()
