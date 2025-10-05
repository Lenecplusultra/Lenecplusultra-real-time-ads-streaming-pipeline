# cloud/seed_worker.py
import os, random
from datetime import datetime, timedelta, timezone
import psycopg  # psycopg[binary]

def need(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var {name}")
    return v

def main():
    host = need("NEON_HOST_DIRECT")  # direct writer host (NO -pooler in the name)
    db   = os.getenv("NEON_DB", "adsdb")
    user = need("NEON_USER")
    pwd  = need("NEON_PASS")
    nwin = int(os.getenv("NUM_WINDOWS", "36"))

    dsn = f"host={host} port=5432 dbname={db} user={user} password={pwd} sslmode=require"

    campaigns = ["cmp-001", "cmp-002", "cmp-003", "cmp-004"]

    now = datetime.now(timezone.utc).replace(microsecond=0)
    rows = []
    for i in range(nwin):
        end = now - timedelta(seconds=10 * i)
        end = end.replace(second=(end.second // 10) * 10)   # align to 10s
        start = end - timedelta(seconds=10)
        for cid in campaigns:
            clicks = random.randint(0, 8)
            users  = max(0, clicks - random.randint(0, 3))
            rows.append((start, end, cid, clicks, users))

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

    # *** psycopg3 placeholders are %s ***
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
            cur.executemany(upsert_sql, rows)
        conn.commit()

    print(f"Upserted {len(rows)} rows")

if __name__ == "__main__":
    main()
