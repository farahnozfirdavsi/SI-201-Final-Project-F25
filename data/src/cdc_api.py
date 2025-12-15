import os
import requests
import sqlite3
from typing import List, Dict, Any

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "afa_v2.db")
DB_NAME = DB_PATH

CDC_API_URL = "https://data.cdc.gov/resource/8pt5-q6wp.json"
BATCH_SIZE = 25 

def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


# ---------- FETCH ----------
def fetch_cdc_rows(limit: int = BATCH_SIZE) -> List[Dict[str, Any]]:
    """

    """
    params = {"$limit": limit}
    response = requests.get(CDC_API_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


# ---------- STORE ----------
def store_cdc_rows(rows: List[Dict[str, Any]]) -> int:
    """
    Append CDC rows into CDCRaw.
    Does not delete or overwrite existing rows.
    """
    conn = get_connection()
    cur = conn.cursor()

    for r in rows:
        group_name = r.get("group")
        state = r.get("state")
        indicator = r.get("indicator")
        time_period_start_date = r.get("time_period_start_date")
        value = r.get("value")

        # Convert value safely to float if possible
        try:
            value = float(value) if value is not None else None
        except (TypeError, ValueError):
            value = None

        cur.execute(
            """
            INSERT INTO CDCRaw (
                group_name,
                state,
                indicator,
                time_period_start_date,
                value
            )
            VALUES (?, ?, ?, ?, ?);
            """,
            (
                group_name,
                state,
                indicator,
                time_period_start_date,
                value
            )
        )

    conn.commit()
    conn.close()
    return len(rows)


# ---------- RUNNER ----------
def main() -> None:
    rows = fetch_cdc_rows(BATCH_SIZE)
    inserted = store_cdc_rows(rows)

    print(f"Fetched {len(rows)} rows from CDC API.")
    print(f"Inserted {inserted} rows into CDCRaw.")
    print("Run this script again to accumulate more data.")


if __name__ == "__main__":
    main()
