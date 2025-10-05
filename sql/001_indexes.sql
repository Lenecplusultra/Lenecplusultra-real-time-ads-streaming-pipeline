-- Helpful indexes for queries
CREATE INDEX IF NOT EXISTS idx_campaign_agg_end
  ON campaign_agg(window_end);

CREATE INDEX IF NOT EXISTS idx_campaign_agg_campaign
  ON campaign_agg(campaign_id);
