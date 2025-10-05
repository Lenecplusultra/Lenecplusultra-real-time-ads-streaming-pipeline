"""
Upsert a few recent 10s windows into Neon so the dashboard has fresh data,
even when the local producer/Spark job is off.

Env vars expected (set in GitHub Actions secrets or workflow env):
- NEON_HOST_DIRECT  (no -pooler; direct writer host)
- NEON_DB
- NEON_USER
- NEON_PASS
- NUM_WINDOWS       (optional; default 12 => last 2 minutes)
"""

import os
import time
import random
from datetime import datetime, timedelta, timezone

import psycopg  # psycopg 3

def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

def main():
    host = env("NEON_HOST_DIRECT")
    db   = env("NEON_DB", "adsdb")
    user = env("NEON_USER")
    pwd  = env("NEON_PASS")
    n    = int(os.getenv("NUM_WINDOWS", "12"))  # last 12 windows (~2 mins)

    dsn = f"host={host} port=5432 dbname={db} user={user} password={pwd} sslmode=require"

    # Make up some small synthetic aggregates for a few campaigns.
    campaigns = ["cmp-001", "cmp-002", "cmp-003", "cmp-004"]
    now = datetime.now(timezone.utc)

    rows = []
    for i in range(n):
        # align to 10s windows
        end = now - timedelta(seconds=10 * i)
        end = end.replace(microsecond=0)
        end_aligned = end - timedelta(seconds=end.second % 10)
        start = end_aligned - timedelta(seconds=10)

        for cid in campaigns:
            clicks = random.randint(0, 8)
            users  = max(0, clicks - random.randint(0, 3))
            rows.append((start, end_aligned, cid, clicks, users))

    sql = """
    INSERT INTO campaign_agg
      (window_start, window_end, campaign_id, clicks, unique_users, ctr)
    VALUES (%s, %s, %s, %s, %s, NULL)
    ON CONFLICT (window_start, window_end, campaign_id)
    DO UPDATE SET
      clicks       = EXCLUDED.clicks,
      unique_users = EXCLUDED.unique_users,
      ctr          = EXCLUDED.ctr;
    """

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

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(create_sql)
            # Upsert in batches
            for batch_start in range(0, len(rows), 500):
                cur.executemany(sql, rows[batch_start: batch_start + 500])
        conn.commit()

    print(f"Upserted {len(rows)} rows into campaign_agg")

if __name__ == "__main__":
    main()
