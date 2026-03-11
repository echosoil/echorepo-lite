#!/usr/bin/env python3
import os
import sqlite3

db = os.environ.get("SQLITE_PATH", "data/db/data.db")
conn = sqlite3.connect(db)
cur = conn.cursor()
cur.executescript("""
CREATE INDEX IF NOT EXISTS idx_samples_collectedAt ON samples(collectedAt);
CREATE INDEX IF NOT EXISTS idx_samples_gps ON samples(GPS_lat, GPS_long);
""")
conn.commit()
conn.close()
print("Indexes ensured.")
