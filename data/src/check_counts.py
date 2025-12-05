import sqlite3
import os

# Always locate afa.db relative to THIS file
BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "afa.db")

def count_rows(table):
    """Return row count for a given table, or 0 if table doesn't exist."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        conn.close()
        return count
    except Exception as e:
        return f"Table not found ({e})"

def main():
    print("📊 DATABASE CHECK — AFA Project")
    print(f"Database path: {DB_PATH}\n")

    tables = [
        "ScrapedSongs",
        "Songs",
        "SpotifyAudioFeatures",
        "Popularity",
        "MentalHealthTrends"
    ]

    for table in tables:
        count = count_rows(table)
        print(f"{table:25} → {count}")

    print("\nDone ✔")

if __name__ == "__main__":
    main()
