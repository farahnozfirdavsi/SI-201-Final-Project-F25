import sqlite3

DB_NAME = "afa.db"

conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()

cur.execute("SELECT week, anxiety_percent, depression_percent FROM MentalHealthTrends LIMIT 10;")
rows = cur.fetchall()

for row in rows:
    print(row)

conn.close()
