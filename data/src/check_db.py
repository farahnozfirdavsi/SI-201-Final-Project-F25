import sqlite3
import os



# Directory where *this file* lives (data/src)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Path to the database inside data/src
DB_PATH = os.path.join(BASE_DIR, "afa.db")

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn




def main():
    conn = get_connection()
    cur = conn.cursor()

    print("Using database:", DB_PATH)

    try:
        cur.execute("SELECT week, anxiety_percent, depression_percent FROM MentalHealthTrends LIMIT 10;")
        rows = cur.fetchall()

        print("\nFirst 10 rows from MentalHealthTrends:\n")
        for row in rows:
            print(row)

    except sqlite3.OperationalError as e:
        print("\nERROR:", e)
        print("This means you're NOT connected to the correct DB or the table doesn't exist.")

    conn.close()


if __name__ == "__main__":
    main()
