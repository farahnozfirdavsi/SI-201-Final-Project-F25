import sqlite3

DB_NAME = "afa.db"

conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()

print("Row counts:")
for table in ["ScrapedSongs", "Songs", "SpotifyAudioFeatures", "MentalHealthTrends"]:
    cur.execute(f"SELECT COUNT(*) FROM {table};")
    count = cur.fetchone()[0]
    print(f"{table}: {count}")

conn.close()
