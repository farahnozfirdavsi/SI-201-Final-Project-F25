import requests
import sqlite3

DB_NAME = "afa.db"

# CDC Household Pulse Survey: Anxiety / Depression indicators
CDC_API_URL = "https://data.cdc.gov/resource/8pt5-q6wp.json"


def get_connection(db_name: str = DB_NAME) -> sqlite3.Connection:
    """
    Helper to connect to the SQLite database with foreign keys on.
    """
    conn = sqlite3.connect(db_name)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


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

        # Example: '2020-04-23T00:00:00.000' -> '2020-04-23'
        week_start_raw = row.get("time_period_start_date")
        if not week_start_raw:
            continue
        week = week_start_raw.split("T")[0]

        # Initialize record for this week if not present
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
        # keep weeks where at least one measure exists
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

    # Optional: clear old data so repeated runs don't duplicate rows
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
    print("Fetching CDC mental health data...")
    records = get_cdc_mental_health()
    print(f"Prepared {len(records)} weekly records.")

    # Print first 5 to sanity-check
    for r in records[:5]:
        print(r)

    print("Storing records in MentalHealthTrends table...")
    store_mental_health(records)
    print("Done.")


if __name__ == "__main__":
    main()
