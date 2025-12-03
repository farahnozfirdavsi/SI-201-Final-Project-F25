import sqlite3

DB_NAME = "afa.db"

conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()

print("Total rows in SpotifyAudioFeatures:")
cur.execute("SELECT COUNT(*) FROM SpotifyAudioFeatures;")
print(cur.fetchone()[0])

print("\nRows with non-null valence:")
cur.execute("SELECT COUNT(*) FROM SpotifyAudioFeatures WHERE valence IS NOT NULL;")
print(cur.fetchone()[0])

print("\nSample rows with valence:")
cur.execute("""
    SELECT s.song_title, s.artist_name, f.valence, f.energy, f.danceability
    FROM ScrapedSongs s
    JOIN Songs t ON t.scraped_song_id = s.id
    JOIN SpotifyAudioFeatures f ON f.song_id = t.song_id
    WHERE f.valence IS NOT NULL
    LIMIT 10;
""")
for row in cur.fetchall():
    print(row)

conn.close()
