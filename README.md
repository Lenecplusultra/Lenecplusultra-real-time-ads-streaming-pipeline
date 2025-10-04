# Real-Time Ads Streaming Pipeline (Kafka + Spark + PostgreSQL)

Ingest ad-click events → aggregate near real-time campaign metrics → serve a simple dashboard.

**Stack:** Kafka (KRaft), Spark Structured Streaming (Scala), PostgreSQL, Python (producer), Streamlit/Flask (dashboard).

## Quickstart (local)
- `docker compose -f docker/compose.local.yml up -d`
- `python producer-python/producer.py`  # send events
- Run Spark job: `sbt "run"` in `streaming-scala/`
- Open dashboard: `http://localhost:8501`  (if Streamlit)

## Architecture
Producer → Kafka → Spark → Postgres → Dashboard
EOF
