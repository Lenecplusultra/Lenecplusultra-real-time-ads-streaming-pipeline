# cloud/seed_once.py
import os, random, math, psycopg
from datetime import datetime, timezone, timedelta

NEON_HOST   = os.environ["NEON_HOST_DIRECT"]      # ep-...c-2.us-east-1.aws.neon.tech
NEON_DB     = os.environ.get("NEON_DB", "adsdb")
NEON_USER   = os.environ.get("NEON_USER", "neondb_owner")
NEON_PASS   = os.environ["NEON_PASS"]

CAMPAIGNS = os.environ.get("CAMPAIGNS", "cmp-001,cmp-002,cmp-003,cmp-004").split(",")
WINDOW_SECONDS = int(os.environ.get("WINDOW_SECONDS", "10"))
N_WINDOWS = int(os.environ.get("N_WINDOWS", "12"))  # how many 10s windows to upsert (e.g., last 2 minutes)

DSN = (
    f"host={NEON_HOST} port=5432 dbname={NEON_DB} "
    f"user={NEON_USER} password={NEON_PASS} sslmode=require"
)

UPSERT_SQL = """
INSERT INTO campaign_agg (window_start, window_end, campaign_id, clicks, unique_users, ctr)
VALUES ($1, $2, $3, $4, $5, NULL)
ON CONFLICT (window_start, window_end, campaign_id)
DO UPDATE SET
  clicks       = EXCLUDED.clicks,
  unique_users = EXCLUDED.unique_users,
  ctr          = EXCLUDED.ctr;
"""

def align_to_window(ts: datetime, seconds=10):
    epoch = int(ts.timestamp())
    aligned = epoch - (epoch % seconds)
    start = datetime.fromtimestamp(aligned, tz=timezone.utc)
    end = start + timedelta(seconds=seconds)
    return start, end

def clicks_users(seed: int):
    rnd = random.Random(seed)
    base = rnd.randint(1, 7)
    wobble = int(3 * math.sin(seed / 13.0))
    clicks = max(1, base + wobble)
    users = max(1, clicks - rnd.randint(0, 3))
    return clicks, users

def main():
    now = datetime.now(timezone.utc)
    _, aligned_end = align_to_window(now, WINDOW_SECONDS)

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        # upsert N_WINDOWS windows ending at aligned_end
        for i in range(N_WINDOWS, 0, -1):
            w_end = aligned_end - timedelta(seconds=(i - 1) * WINDOW_SECONDS)
            w_start = w_end - timedelta(seconds=WINDOW_SECONDS)
            for c in CAMPAIGNS:
                seed = int(w_start.timestamp()) ^ hash(c)
                clicks, users = clicks_users(seed)
                cur.execute(UPSERT_SQL, (w_start, w_end, c, clicks, users))
        conn.commit()

if __name__ == "__main__":
    main()
