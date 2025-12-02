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



DB_NAME = "afa.db"


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
    # Example: Hot 100 for a specific week.
    # Replace the URL with whatever week your instructor wants you to use.
    url = "https://www.billboard.com/charts/hot-100/2024-01-06/"

    print("Scraping Billboard chart...")
    songs = scrape_billboard(url)
    print(f"Found {len(songs)} songs.")

    # Print first 5 to sanity-check
    for s in songs[:5]:
        print(s)

    if songs:
        print("Storing in database...")
        store_scraped_songs(songs)
        print("Done inserting scraped songs into ScrapedSongs table.")


if __name__ == "__main__":
    main()
