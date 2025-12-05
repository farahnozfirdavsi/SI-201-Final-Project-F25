import sqlite3
import os
import pandas as pd

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "afa.db")

def show_table(name, limit=10):
    print(f"\n===== {name} (first {limit} rows) =====")
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(f"SELECT * FROM {name} LIMIT {limit}", conn)
    print(df)
    conn.close()

def show_nulls(table, column):
    print(f"\nNULL check for {table}.{column}:")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
    print("NULL count:", cur.fetchone()[0])
    conn.close()

def main():
    show_table("ScrapedSongs")
    show_table("Songs")
    show_table("SpotifyAudioFeatures")
    show_table("Popularity")
    show_table("MentalHealthTrends")

    # Null checks for Spotify features
    for col in ["valence", "energy", "danceability", "tempo"]:
        show_nulls("SpotifyAudioFeatures", col)

if __name__ == "__main__":
    main()
