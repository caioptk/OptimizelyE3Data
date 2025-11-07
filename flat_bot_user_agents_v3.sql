
-- ============================================================================
-- File: flat_bot_user_agents_v3.sql
-- Purpose: Flat list of likely bot User-Agents across decisions_raw + events.
--          Uses WITH params (no DECLARE) to avoid script-mode issues.
-- Author: Caio / M365 Copilot
-- Last updated: 2025-10-24
-- ============================================================================

WITH params AS (
  SELECT DATE '2024-10-30' AS start_date,
         DATE '2025-10-29' AS end_date
),
base AS (
  SELECT LOWER(COALESCE(user_agent, '')) AS ua,
         visitor_id,
         TIMESTAMP(timestamp) AS ts,
         DATE_TRUNC(DATE(timestamp), MONTH) AS month_start
  FROM `caio-sandbox-468412.optimizely_e3.decisions_raw`
  WHERE DATE(timestamp) BETWEEN (SELECT start_date FROM params) AND (SELECT end_date FROM params)

  UNION ALL

  SELECT LOWER(COALESCE(user_agent, '')) AS ua,
         visitor_id,
         TIMESTAMP(timestamp) AS ts,
         DATE_TRUNC(DATE(timestamp), MONTH) AS month_start
  FROM `caio-sandbox-468412.optimizely_e3.events`
  WHERE DATE(timestamp) BETWEEN (SELECT start_date FROM params) AND (SELECT end_date FROM params)
)
SELECT
  ua AS user_agent,
  COUNT(*) AS hits,
  COUNT(DISTINCT CONCAT(visitor_id, '|', CAST(month_start AS STRING))) AS visitor_months,
  MIN(ts) AS first_seen,
  MAX(ts) AS last_seen
FROM base
WHERE ua != ''
  AND (
        REGEXP_CONTAINS(ua, r'(petalbot|headlesschrome|google-read[- ]?aloud|phantomjs|ahrefs|semrush|mj12)')
        OR REGEXP_CONTAINS(ua, r'(bot|crawl|spider|slurp|crawler|scrap(e|er)|fetch)')
      )
GROUP BY user_agent
ORDER BY visitor_months DESC, hits DESC
LIMIT 2000;
