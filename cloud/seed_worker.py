import os, time, random, math, psycopg
from datetime import datetime, timezone, timedelta

NEON_HOST   = os.environ["NEON_HOST_DIRECT"]  # ep-...c-2.us-east-1.aws.neon.tech
NEON_DB     = os.environ.get("NEON_DB", "adsdb")
NEON_USER   = os.environ.get("NEON_USER", "neondb_owner")
NEON_PASS   = os.environ["NEON_PASS"]

DSN = f"host={NEON_HOST} port=5432 dbname={NEON_DB} user={NEON_USER} password={NEON_PASS} sslmode=require"

CAMPAIGNS = ["cmp-001", "cmp-002", "cmp-003", "cmp-004"]
WINDOW_SECONDS = 10

UPSERT_SQL = """
INSERT INTO campaign_agg (window_start, window_end, campaign_id, clicks, unique_users, ctr)
VALUES ($1, $2, $3, $4, $5, NULL)
ON CONFLICT (window_start, window_end, campaign_id)
DO UPDATE SET
  clicks       = EXCLUDED.clicks,
  unique_users = EXCLUDED.unique_users,
  ctr          = EXCLUDED.ctr;
"""

def align_to_window(ts, seconds=10):
    epoch = int(ts.timestamp())
    aligned = epoch - (epoch % seconds)
    start = datetime.fromtimestamp(aligned, tz=timezone.utc)
    end = start + timedelta(seconds=seconds)
    return start, end

def make_metrics(c):
    # simple variability so charts “move”
    clicks = random.randint(1, 7) + int(3 * math.sin(time.time()/20.0 + hash(c)%7))
    clicks = max(clicks, 1)
    unique_users = max(1, clicks - random.randint(0, 3))
    return clicks, unique_users

def main():
    print("Seed worker starting…")
    with psycopg.connect(DSN) as conn:
        while True:
            now = datetime.now(timezone.utc)
            wstart, wend = align_to_window(now, WINDOW_SECONDS)
            with conn.cursor() as cur:
                for c in CAMPAIGNS:
                    clicks, users = make_metrics(c)
                    cur.execute(UPSERT_SQL, (wstart, wend, c, clicks, users))
            conn.commit()
            time.sleep(3)

if __name__ == "__main__":
    main()
