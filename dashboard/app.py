import time
import pandas as pd
from sqlalchemy import create_engine, text
import streamlit as st

st.set_page_config(page_title="Real-Time Ads", layout="wide")
st.title("📈 Real-Time Ads Streaming — Campaign Metrics")

import os, urllib.parse
from sqlalchemy import create_engine

# Prefer a full DATABASE_URL if provided
db_url = os.getenv("DATABASE_URL", "").strip()

if not db_url:
    # Build from components (recommended if your password has special chars)
    host = os.getenv("DB_HOST_POOLED", "ep-dry-shape-ad0cywr3-pooler.c-2.us-east-1.aws.neon.tech")
    user = os.getenv("DB_USER", "neondb_owner")
    pwd  = os.getenv("DB_PASS", "")
    db   = os.getenv("DB_NAME", "adsdb")
    enc_pwd = urllib.parse.quote_plus(pwd)
    db_url = f"postgresql+psycopg2://{user}:{enc_pwd}@{host}/{db}?sslmode=require"

ENGINE = create_engine(db_url, pool_pre_ping=True, pool_recycle=300)


@st.cache_data(ttl=5)
def load_data(minutes: int = 15) -> pd.DataFrame:
    q = text("""
        SELECT window_start, window_end, campaign_id, clicks, unique_users
        FROM campaign_agg
        WHERE window_end > now() - interval :mins
        ORDER BY window_start
    """)
    with ENGINE.begin() as conn:
        return pd.read_sql(q, conn, params={"mins": f"{minutes} minutes"})

auto = st.sidebar.toggle("Auto-refresh (5s)", value=True)
mins = st.sidebar.slider("Time window (minutes)", 5, 120, 15, step=5)

df = load_data(mins)

if df.empty:
    st.info("No data yet. Make sure the Python producer and Spark job are running.")
else:
    k1, k2, k3 = st.columns(3)
    k1.metric("Total rows", f"{len(df):,}")
    k2.metric("Clicks (sum)", f"{int(df['clicks'].sum()):,}")
    k3.metric("Unique users (latest window)", 
              f"{int(df[df['window_end']==df['window_end'].max()]['unique_users'].sum()):,}")

    st.subheader("Clicks over time")
    line = (df.rename(columns={"window_start":"time"})
              .pivot_table(index="time", columns="campaign_id", values="clicks", aggfunc="sum")
              .fillna(0))
    st.line_chart(line)

    st.subheader("Latest window (top campaigns)")
    latest_end = df["window_end"].max()
    latest = df[df["window_end"] == latest_end].sort_values("clicks", ascending=False)
    st.dataframe(latest.reset_index(drop=True))

st.caption("Auto-refreshes every 5s while enabled.")
if auto:
    time.sleep(5)
    st.rerun()
