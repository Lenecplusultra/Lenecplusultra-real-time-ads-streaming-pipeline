# Real-Time Ads Streaming Pipeline
Producer (Python) → Kafka → Spark Structured Streaming → Postgres → Streamlit.

```mermaid
flowchart LR
  P[Python Producer] --> K[(Kafka)]
  K --> S[Spark Structured Streaming]
  S --> DB[(Postgres)]
  DB --> UI[Streamlit Dashboard]
