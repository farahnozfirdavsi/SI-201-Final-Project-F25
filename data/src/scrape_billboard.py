import requests
from bs4 import BeautifulSoup
import sqlite3


from urllib.parse import urlparse

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



DB_NAME = "afa_v2.db"


def get_connection(db_name: str = DB_NAME) -> sqlite3.Connection:
    """
    Helper to connect to the SQLite database with foreign keys on.
    """
    conn = sqlite3.connect(db_name)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def scrape_billboard(url: str):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # NEW: always get a date from the URL
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

        song_info = {
            "song_title": song_title,
            "artist_name": artist_name,
            "chart_date": chart_date,
            "genre": None
        }
        songs_data.append(song_info)

    return songs_data

def store_scraped_songs(songs_list, db_name: str = DB_NAME):
    """
    Take the list of dicts from scrape_billboard and insert them into ScrapedSongs.
    """
    conn = get_connection(db_name)
    cur = conn.cursor()

    for song in songs_list:
        cur.execute(
            """
            INSERT INTO ScrapedSongs (song_title, artist_name, genre, chart_date)
            VALUES (?, ?, ?, ?)
            """,
            (
                song["song_title"],
                song["artist_name"],
                song["genre"],
                song["chart_date"],
            ),
        )

    conn.commit()
    conn.close()


def main():
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

        # Only take the remaining allowed songs this run
        remaining = MAX_PER_RUN - total_inserted
        songs_to_store = songs[:remaining]

        print(f"  Storing {len(songs_to_store)} songs in database...")
        store_scraped_songs(songs_to_store)
        total_inserted += len(songs_to_store)

    print(f"\nDone scraping. Inserted {total_inserted} songs this run.")


if __name__ == "__main__":
    main()