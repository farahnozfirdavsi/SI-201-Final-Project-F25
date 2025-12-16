import os
import requests
import sqlite3
from typing import List, Dict, Optional, Tuple

# Always use DB next to this script (src/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "afa.db")

CDC_API_URL = "https://data.cdc.gov/resource/8pt5-q6wp.json"

INDICATORS = (
    "Symptoms of Anxiety Disorder",
    "Symptoms of Depressive Disorder",
)

MAX_FACT_ROWS_PER_RUN = 25


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def get_or_create_id(cur, table: str, id_col: str, value_col: str, value: str) -> int:
    """
    Insert value into lookup table if missing, then return its integer id.
    """
    cur.execute(f"INSERT OR IGNORE INTO {table} ({value_col}) VALUES (?)", (value,))
    cur.execute(f"SELECT {id_col} FROM {table} WHERE {value_col} = ?", (value,))
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Could not get id for {table}.{value_col}={value}")
    return int(row[0])


def fetch_cdc_rows_page(limit: int = 500, offset: int = 0) -> List[Dict]:
    """
    Fetch a page of CDC API results.
    """
    params = {"$limit": limit, "$offset": offset}
    r = requests.get(CDC_API_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def normalize_raw_row(row: Dict) -> Optional[Dict]:
    """
    Convert a raw CDC API row dict into our normalized-friendly dict.
    Returns None if the row is missing required fields or not one of our indicators.
    """
    indicator = row.get("indicator", "")
    if indicator not in INDICATORS:
        return None

    group_name = row.get("group")
    state_name = row.get("state")

    t_raw = row.get("time_period_start_date")
    time_period_start_date = t_raw.split("T")[0] if t_raw else None

    value_str = row.get("value")
    try:
        value = float(value_str) if value_str is not None else None
    except ValueError:
        return None

    if not (group_name and state_name and time_period_start_date):
        return None

    return {
        "group_name": group_name,
        "state_name": state_name,
        "indicator_name": indicator,
        "time_period_start_date": time_period_start_date,
        "value": value,
    }


def cdcr_fact_exists(cur, group_id: int, state_id: int, indicator_id: int, time_id: int) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM CDCRaw
        WHERE group_id = ? AND state_id = ? AND indicator_id = ? AND time_id = ?
        LIMIT 1
        """,
        (group_id, state_id, indicator_id, time_id),
    )
    return cur.fetchone() is not None


def insert_cdcr_fact(cur, group_id: int, state_id: int, indicator_id: int, time_id: int, value: Optional[float]) -> bool:
    """
    Insert a CDCRaw fact row if not already present.
    Returns True if inserted, False if skipped.
    """

    if cdcr_fact_exists(cur, group_id, state_id, indicator_id, time_id):
        return False

    cur.execute(
        """
        INSERT INTO CDCRaw (group_id, state_id, indicator_id, time_id, value)
        VALUES (?, ?, ?, ?, ?)
        """,
        (group_id, state_id, indicator_id, time_id, value),
    )
    return True


def populate_cdc_raw_normalized(max_rows: int = MAX_FACT_ROWS_PER_RUN, page_limit: int = 500) -> int:
    """
    Inserts up to `max_rows` NEW CDCRaw rows per run, normalized via lookup tables.
    We keep paging through the API until we insert enough NEW facts or run out.
    """
    conn = get_connection()
    cur = conn.cursor()

    inserted = 0
    offset = 0

    while inserted < max_rows:
        page = fetch_cdc_rows_page(limit=page_limit, offset=offset)
        if not page:
            break

        for raw in page:
            if inserted >= max_rows:
                break

            norm = normalize_raw_row(raw)
            if norm is None:
                continue

            # Lookup IDs (normalization)
            group_id = get_or_create_id(cur, "CDCGroup", "group_id", "group_name", norm["group_name"])
            state_id = get_or_create_id(cur, "CDCState", "state_id", "state_name", norm["state_name"])
            indicator_id = get_or_create_id(cur, "CDCIndicator", "indicator_id", "indicator_name", norm["indicator_name"])
            time_id = get_or_create_id(cur, "CDCTimePeriod", "time_id", "time_period_start_date", norm["time_period_start_date"])

            # Insert FACT row (counts toward the 25 limit)
            did_insert = insert_cdcr_fact(cur, group_id, state_id, indicator_id, time_id, norm["value"])
            if did_insert:
                inserted += 1

        offset += page_limit

    conn.commit()
    conn.close()
    return inserted


def get_cdc_weekly_national_summary(limit: int = 5000) -> List[Dict]:
    """
    National Estimate + United States weekly summary for the two indicators.
    (This is not the "25 limit" table—it's an aggregated table used for analysis.)
    """
    params = {"$limit": limit}
    r = requests.get(CDC_API_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    weeks = {}
    for row in data:
        if row.get("group") != "National Estimate":
            continue
        if row.get("state") != "United States":
            continue

        indicator = row.get("indicator", "")
        if indicator not in INDICATORS:
            continue

        t_raw = row.get("time_period_start_date")
        if not t_raw:
            continue
        week = t_raw.split("T")[0]

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
    for wk in sorted(weeks.keys()):
        rec = weeks[wk]
        if rec["anxiety_percent"] is None and rec["depression_percent"] is None:
            continue
        records.append(rec)

    return records


def refresh_mental_health_trends() -> int:
    """
    Refresh MentalHealthTrends with the latest weekly national summary.
    Safe to rebuild each run because it's a small derived table.
    """
    records = get_cdc_weekly_national_summary(limit=5000)

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("DELETE FROM MentalHealthTrends;")
    for rec in records:
        cur.execute(
            """
            INSERT INTO MentalHealthTrends (week, anxiety_percent, depression_percent)
            VALUES (?, ?, ?)
            """,
            (rec["week"], rec["anxiety_percent"], rec["depression_percent"]),
        )

    conn.commit()
    conn.close()
    return len(records)


def main():
    print("Using DB file:", DB_PATH)

    print(f"\n[STEP 1] Inserting up to {MAX_FACT_ROWS_PER_RUN} NEW normalized CDCRaw rows...")
    inserted = populate_cdc_raw_normalized(max_rows=MAX_FACT_ROWS_PER_RUN, page_limit=500)
    print(f"Inserted {inserted} new CDCRaw rows this run.")

    print("\n[STEP 2] Refreshing MentalHealthTrends weekly national summary...")
    n_weeks = refresh_mental_health_trends()
    print(f"MentalHealthTrends now has {n_weeks} weekly rows.")


if __name__ == "__main__":
    main()
