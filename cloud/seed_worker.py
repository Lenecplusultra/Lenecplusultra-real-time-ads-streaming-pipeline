# cloud/seed_worker.py
"""
Upsert recent fake aggregates so the dashboard shows data even when producers are off.

Env:
  NEON_HOST_DIRECT  (direct host, NOT -pooler)
  NEON_DB           (e.g. adsdb)
  NEON_USER         (e.g. neondb_owner)
  NEON_PASS
  NUM_WINDOWS       (optional int; default 36 => ~6 minutes of 10s windows)
"""

import os
import random
from datetime import datetime, timedelta, timezone

import psycopg  # pip install "psycopg[binary]==3.2.1"

def require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val

def main():
    host = require("NEON_HOST_DIRECT")  # direct host (no -pooler)
    db   = os.getenv("NEON_DB", "adsdb")
    user = require("NEON_USER")
    pwd  = require("NEON_PASS")
    nwin = int(os.getenv("NUM_WINDOWS", "36"))

    dsn = f"host={host} port=5432 dbname={db} user={user} password={pwd} sslmode=require"

    # A few campaigns
    campaigns = ["cmp-001", "cmp-002", "cmp-003", "cmp-004"]

    # Build rows for the last N 10-second windows
    now = datetime.now(timezone.utc)
    rows = []
    for i in range(nwin):
        end = now - timedelta(seconds=10 * i)
        end = end.replace(microsecond=0)
        end_aligned = end - timedelta(seconds=end.second % 10)
        start = end_aligned - timedelta(seconds=10)

        for cid in campaigns:
            clicks = random.randint(0, 8)
            users  = max(0, clicks - random.randint(0, 3))
            rows.append((start, end_aligned, cid, clicks, users))

    create_sql = """
    CREATE TABLE IF NOT EXISTS campaign_agg (
      window_start timestamptz NOT NULL,
      window_end   timestamptz NOT NULL,
      campaign_id  text        NOT NULL,
      clicks       bigint      NOT NULL,
      unique_users bigint      NOT NULL,
      ctr          double precision,
      PRIMARY KEY (window_start, window_end, campaign_id)
    );
    """

    # *** IMPORTANT: psycopg3 placeholders are %s ***
    upsert_sql = """
    INSERT INTO campaign_agg
      (window_start, window_end, campaign_id, clicks, unique_users, ctr)
    VALUES (%s, %s, %s, %s, %s, NULL)
    ON CONFLICT (window_start, window_end, campaign_id)
    DO UPDATE SET
      clicks       = EXCLUDED.clicks,
      unique_users = EXCLUDED.unique_users,
      ctr          = EXCLUDED.ctr;
    """

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(create_sql)
            # executemany uses the %s placeholders above
            cur.executemany(upsert_sql, rows)
        conn.commit()

    print(f"Upserted {len(rows)} rows")

if __name__ == "__main__":
    main()
