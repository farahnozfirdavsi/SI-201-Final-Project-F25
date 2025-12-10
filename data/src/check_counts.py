import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "afa.db")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

tables = [
    "ScrapedSongs",
    "Songs",
    "SpotifyAudioFeatures",
    "Popularity",
    "CDCRaw", 
    "MentalHealthTrends"
]

for t in tables:
    try:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        count = cur.fetchone()[0]
        print(f"{t}: {count} rows")
    except Exception as e:
        print(f"Error reading {t}: {e}")

conn.close()
