import os
import requests
import sqlite3

# --- DB PATH: exactly like your setup ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "afa.db")
DB_NAME = DB_PATH

CDC_API_URL = "https://data.cdc.gov/resource/8pt5-q6wp.json"
RAW_BATCH_SIZE = 25  # <-- ONLY 25 raw rows per run


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


# ---------- TABLES (copied from your setup, so they ALWAYS exist) ----------
def create_tables(db_name: str = DB_NAME) -> None:
    conn = get_connection(db_name)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ScrapedSongs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            song_title   TEXT NOT NULL,
            artist_name  TEXT NOT NULL,
            genre        TEXT,
            chart_date   TEXT NOT NULL
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS Songs (
            song_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_song_id  INTEGER NOT NULL,
            spotify_track_id TEXT NOT NULL,
            genre            TEXT,
            popularity       INTEGER,
            release_year     INTEGER,
            FOREIGN KEY (scraped_song_id)
                REFERENCES ScrapedSongs(id)
                ON DELETE CASCADE
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS SpotifyAudioFeatures (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            song_id          INTEGER NOT NULL,
            valence          REAL,
            energy           REAL,
            danceability     REAL,
            tempo            REAL,
            acousticness     REAL,
            instrumentalness REAL,
            FOREIGN KEY (song_id)
                REFERENCES Songs(song_id)
                ON DELETE CASCADE
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS Popularity (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            song_id        INTEGER NOT NULL,
            listener_count INTEGER,
            playcount      INTEGER,
            FOREIGN KEY (song_id)
                REFERENCES Songs(song_id)
                ON DELETE CASCADE
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS CDCRaw (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_name TEXT,
            state TEXT,
            indicator TEXT,
            time_period_start_date TEXT,
            value REAL
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS MentalHealthTrends (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            week               TEXT NOT NULL,
            anxiety_percent    REAL,
            depression_percent REAL
        );
        """
    )

    conn.commit()
    conn.close()


def ensure_indexes(db_name: str = DB_NAME) -> None:
    """
    Make week unique so we can UPSERT into MentalHealthTrends cleanly.
    Safe to run every time.
    """
    conn = get_connection(db_name)
    cur = conn.cursor()

    # This is the key: ON CONFLICT(week) only works if week is UNIQUE.
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_mentalhealthtrends_week
        ON MentalHealthTrends(week);
    """)

    conn.commit()
    conn.close()


# ---------- RAW CDC (25 per run, accumulate) ----------
def fetch_cdc_raw(limit=RAW_BATCH_SIZE):
    params = {"$limit": limit}
    response = requests.get(CDC_API_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    raw_rows = []
    for row in data:
        indicator = row.get("indicator", "")
        if indicator not in (
            "Symptoms of Anxiety Disorder",
            "Symptoms of Depressive Disorder",
        ):
            continue

        group_name = row.get("group")
        state = row.get("state")

        time_period_start_raw = row.get("time_period_start_date")
        time_period_start = time_period_start_raw.split("T")[0] if time_period_start_raw else None

        value_str = row.get("value")
        try:
            value = float(value_str) if value_str is not None else None
        except ValueError:
            continue

        raw_rows.append((group_name, state, indicator, time_period_start, value))

    return raw_rows


def store_cdc_raw(raw_rows, db_name: str = DB_NAME):
    """
    APPEND MODE: no DELETE.
    Inserts only what this run fetched (<=25 rows after filtering).
    """
    conn = get_connection(db_name)
    cur = conn.cursor()

    cur.executemany(
        """
        INSERT INTO CDCRaw (group_name, state, indicator, time_period_start_date, value)
        VALUES (?, ?, ?, ?, ?)
        """,
        raw_rows,
    )

    conn.commit()
    conn.close()


# ---------- WEEKLY NATIONAL SUMMARY (same logic as before) ----------
def get_cdc_mental_health(api_url: str = CDC_API_URL):
    params = {"$limit": 5000}
    response = requests.get(api_url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    weeks = {}

    for row in data:
        if row.get("group") != "National Estimate":
            continue
        if row.get("state") != "United States":
            continue

        indicator = row.get("indicator", "")
        if indicator not in (
            "Symptoms of Anxiety Disorder",
            "Symptoms of Depressive Disorder",
        ):
            continue

        week_start_raw = row.get("time_period_start_date")
        if not week_start_raw:
            continue
        week = week_start_raw.split("T")[0]

        if week not in weeks:
            weeks[week] = {"week": week, "anxiety_percent": None, "depression_percent": None}

        value_str = row.get("value")
        try:
            value = float(value_str) if value_str is not None else None
        except ValueError:
            continue

        if indicator == "Symptoms of Anxiety Disorder":
            weeks[week]["anxiety_percent"] = value
        else:
            weeks[week]["depression_percent"] = value

    records = []
    for week_key in sorted(weeks.keys()):
        rec = weeks[week_key]
        if rec["anxiety_percent"] is None and rec["depression_percent"] is None:
            continue
        records.append(rec)

    return records


def store_mental_health(records, db_name: str = DB_NAME):
    """
    No DELETE. Uses UPSERT so it's one row per week.
    """
    conn = get_connection(db_name)
    cur = conn.cursor()

    for rec in records:
        cur.execute(
            """
            INSERT INTO MentalHealthTrends (week, anxiety_percent, depression_percent)
            VALUES (?, ?, ?)
            ON CONFLICT(week) DO UPDATE SET
                anxiety_percent = excluded.anxiety_percent,
                depression_percent = excluded.depression_percent
            """,
            (rec["week"], rec["anxiety_percent"], rec["depression_percent"]),
        )

    conn.commit()
    conn.close()


def main():
    # 0) Always ensure tables exist in *this* afa.db
    create_tables()
    ensure_indexes()

    # 1) Raw batch (25)
    print("Fetching raw CDC mental health data (batch of 25)...")
    raw_rows = fetch_cdc_raw(limit=RAW_BATCH_SIZE)
    print(f"Fetched {len(raw_rows)} raw rows after indicator filtering.")
    store_cdc_raw(raw_rows)
    print("Appended into CDCRaw.")

    # 2) Weekly national summary
    print("Fetching weekly national CDC mental health summary...")
    records = get_cdc_mental_health()
    print(f"Prepared {len(records)} weekly records.")
    store_mental_health(records)
    print("Upserted into MentalHealthTrends (one row per week).")

    print(f"\nUsing DB file: {DB_NAME}")


if __name__ == "__main__":
    main()
