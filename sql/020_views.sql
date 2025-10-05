CREATE OR REPLACE VIEW latest_campaign_window AS
SELECT *
FROM campaign_agg
WHERE window_end = (SELECT MAX(window_end) FROM campaign_agg);

CREATE OR REPLACE VIEW campaign_totals AS
SELECT campaign_id,
       SUM(clicks) AS clicks,
       SUM(unique_users) AS unique_users
FROM campaign_agg
GROUP BY campaign_id
ORDER BY clicks DESC;
