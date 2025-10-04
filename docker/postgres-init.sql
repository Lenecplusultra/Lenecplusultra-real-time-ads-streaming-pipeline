CREATE TABLE IF NOT EXISTS campaign_agg (
  window_start TIMESTAMPTZ NOT NULL,
  window_end   TIMESTAMPTZ NOT NULL,
  campaign_id  TEXT        NOT NULL,
  clicks       BIGINT      NOT NULL,
  unique_users BIGINT      NOT NULL,
  ctr          DOUBLE PRECISION,
  PRIMARY KEY (window_start, window_end, campaign_id)
);
