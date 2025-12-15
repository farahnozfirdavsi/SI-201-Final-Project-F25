import os
import requests
import sqlite3

# Always use the database file that lives in the same folder as this script (src/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "afa_v2.db")
DB_NAME = DB_PATH

# CDC Household Pulse Survey: Anxiety / Depression indicators
CDC_API_URL = "https://data.cdc.gov/resource/8pt5-q6wp.json"


def get_connection(db_name: str = DB_NAME) -> sqlite3.Connection:
    conn = sqlite3.connect(db_name)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


# ---------- NEW: FETCH + STORE RAW CDC ROWS ----------

def fetch_cdc_raw(limit=5000):
    """
    Fetch raw CDC rows from the API.

    We still focus only on the two mental health indicators we care about:
      - Symptoms of Anxiety Disorder
      - Symptoms of Depressive Disorder

    BUT we do NOT restrict to just National / United States here,
    so we will get many rows across states & groups (well over 100).
    """
    params = {"$limit": limit}
    response = requests.get(CDC_API_URL, params=params)
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
        if time_period_start_raw:
            time_period_start = time_period_start_raw.split("T")[0]
        else:
            time_period_start = None

        value_str = row.get("value")
        try:
            value = float(value_str) if value_str is not None else None
        except ValueError:
            continue

        raw_rows.append(
            {
                "group_name": group_name,
                "state": state,
                "indicator": indicator,
                "time_period_start_date": time_period_start,
                "value": value,
            }
        )

    return raw_rows


def store_cdc_raw(raw_rows, db_name: str = DB_NAME):
    """
    Insert many raw rows into CDCRaw.

    This table is mainly to satisfy the "100+ rows from an API" requirement.
    """
    conn = get_connection(db_name)
    cur = conn.cursor()

    # Optional: clear old data so repeated runs don't endlessly duplicate
    cur.execute("DELETE FROM CDCRaw;")

    for r in raw_rows:
        cur.execute(
            """
            INSERT INTO CDCRaw (group_name, state, indicator, time_period_start_date, value)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                r["group_name"],
                r["state"],
                r["indicator"],
                r["time_period_start_date"],
                r["value"],
            ),
        )

    conn.commit()
    conn.close()


# ---------- EXISTING WEEKLY NATIONAL SUMMARY LOGIC ----------

def get_cdc_mental_health(api_url: str = CDC_API_URL):
    """
    Call the CDC mental health API and return a list of dicts like:
    [
      {
        "week": "2020-04-23",
        "anxiety_percent": 27.2,
        "depression_percent": 23.5
      },
      ...
    ]

    We:
      - Filter to National Estimate, United States
      - Use two indicators:
          "Symptoms of Anxiety Disorder"
          "Symptoms of Depressive Disorder"
      - Group by the week start date
    """
    # Get up to 5000 rows (default is 1000)
    params = {"$limit": 5000}
    response = requests.get(api_url, params=params)
    response.raise_for_status()

    data = response.json()

    # Dict keyed by week start date: "YYYY-MM-DD"
    weeks = {}

    for row in data:
        # Only keep national US estimates
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
            weeks[week] = {
                "week": week,
                "anxiety_percent": None,
                "depression_percent": None,
            }

        # Convert value string to float
        value_str = row.get("value")
        try:
            value = float(value_str) if value_str is not None else None
        except ValueError:
            continue

        # Assign to the correct field
        if indicator == "Symptoms of Anxiety Disorder":
            weeks[week]["anxiety_percent"] = value
        elif indicator == "Symptoms of Depressive Disorder":
            weeks[week]["depression_percent"] = value

    # Turn dict into sorted list
    records = []
    for week_key in sorted(weeks.keys()):
        rec = weeks[week_key]
        # keeps weeks where at least one measure exists
        if rec["anxiety_percent"] is None and rec["depression_percent"] is None:
            continue
        records.append(rec)

    return records


def store_mental_health(records, db_name: str = DB_NAME):
    """
    Insert mental health records into the MentalHealthTrends table.

    Expects records as a list of dicts with keys:
      "week", "anxiety_percent", "depression_percent"
    """
    conn = get_connection(db_name)
    cur = conn.cursor()

    # Optional: clear old weekly data so it stays one row per week
    cur.execute("DELETE FROM MentalHealthTrends;")

    for rec in records:
        cur.execute(
            """
            INSERT INTO MentalHealthTrends (week, anxiety_percent, depression_percent)
            VALUES (?, ?, ?)
            """,
            (
                rec["week"],
                rec["anxiety_percent"],
                rec["depression_percent"],
            ),
        )

    conn.commit()
    conn.close()


def main():
    # Step 1: RAW CDC DATA → CDCRaw
    print("Fetching raw CDC mental health data...")
    raw_rows = fetch_cdc_raw(limit=5000)
    print(f"Fetched {len(raw_rows)} raw rows for indicators of interest.")
    print("Storing raw rows in CDCRaw table...")
    store_cdc_raw(raw_rows)
    print("Done storing CDCRaw.")

    # Step 2: WEEKLY NATIONAL SUMMARY → MentalHealthTrends
    print("\nFetching weekly national CDC mental health summary...")
    records = get_cdc_mental_health()
    print(f"Prepared {len(records)} weekly records.")

    # Print first 5 to sanity-check
    for r in records[:5]:
        print(r)

    print("Storing weekly records in MentalHealthTrends table...")
    store_mental_health(records)
    print("Done.")


if __name__ == "__main__":
    main()
