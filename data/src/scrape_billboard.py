import os
import requests
from bs4 import BeautifulSoup
import sqlite3
from urllib.parse import urlparse

# --- Always use the DB file located in the SAME folder as this script (src/) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "afa.db")

def get_or_create_artist(cur, artist_name):
    cur.execute(
        "SELECT artist_id FROM Artists WHERE artist_name = ?",
        (artist_name,),
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        "INSERT INTO Artists (artist_name) VALUES (?)",
        (artist_name,),
    )
    return cur.lastrowid

def extract_chart_date_from_url(url: str) -> str:
    """
    Extract the chart date from a Billboard chart URL.
    Example URL: https://www.billboard.com/charts/hot-100/2024-01-06/
    Returns: '2024-01-06'
    """
    path = urlparse(url).path.rstrip("/")
    parts = path.split("/")
    if parts:
        return parts[-1]  # last part should be the date
    return "unknown-date"


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """
    Helper to connect to the SQLite database with foreign keys on.
    Uses an absolute path so we never accidentally create a new DB elsewhere.
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def scrape_billboard(url: str):
    """
    Scrape a Billboard Hot 100 chart page and return a list of dicts:
    [{song_title, artist_name, chart_date, genre}, ...]
    """
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    response = requests.get(url, headers=headers, timeout=20)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    chart_date = extract_chart_date_from_url(url)
    songs_data = []

    entries = soup.select("li.o-chart-results-list__item")

    for entry in entries:
        title_tag = entry.select_one("h3#title-of-a-story")
        if not title_tag:
            continue
        song_title = title_tag.get_text(strip=True)

        artist_tag = entry.select_one("span.c-label")
        artist_name = artist_tag.get_text(strip=True) if artist_tag else "Unknown Artist"

        songs_data.append(
            {
                "song_title": song_title,
                "artist_name": artist_name,
                "chart_date": chart_date,
                "genre": None,
            }
        )

    return songs_data


def store_scraped_songs(songs_list, db_path: str = DB_PATH):
    """
    Insert scraped songs into ScrapedSongs.
    """
    conn = get_connection(db_path)
    cur = conn.cursor()

    for song in songs_list:
        artist_id = get_or_create_artist(cur, song["artist_name"])

        cur.execute(
            """
            INSERT INTO ScrapedSongs (song_title, artist_id, genre, chart_date)
            VALUES (?, ?, ?, ?)
            """,
            (
                song["song_title"],
                artist_id,
                song["genre"],
                song["chart_date"],
            )
        )

    conn.commit()
    conn.close()


def main():
    print("Using DB file:", DB_PATH)

    BILLBOARD_WEEKS_2020 = [
        "2020-04-25",
        "2020-05-02",
        "2020-05-09",
    ]


def main():
    print("Using DB file:", DB_PATH)

    BILLBOARD_WEEKS_2020 = [
        "2020-04-25",
        "2020-05-02",
        "2020-05-09",
        "2020-05-16",
        "2020-05-23",
        "2020-05-30",
        "2020-06-06",
        "2020-06-13",
        "2020-06-20",
        "2020-06-27",
        "2020-07-04",
        "2020-07-11",
        "2020-07-18",
        "2020-07-25",
        "2020-08-01",
        "2020-08-08",
        "2020-08-15",
        "2020-08-22",
        "2020-08-29",
    ]

    total_inserted = 0
    MAX_PER_RUN = 25

    for week in BILLBOARD_WEEKS_2020:
        if total_inserted >= MAX_PER_RUN:
            break

        url = f"https://www.billboard.com/charts/hot-100/{week}/"
        print(f"\nScraping Billboard Hot 100 for {week} ...")

        songs = scrape_billboard(url)

        remaining = MAX_PER_RUN - total_inserted
        songs_to_store = songs[:remaining]

        print(f"  Storing {len(songs_to_store)} songs in database...")
        store_scraped_songs(songs_to_store)

        total_inserted += len(songs_to_store)

    print(f"\nDone scraping. Inserted {total_inserted} songs this run.")


if __name__ == "__main__":
    main()
