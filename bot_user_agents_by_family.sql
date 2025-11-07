
-- ============================================================================
-- File: bot_user_agents_by_family.sql
-- Purpose: Classify and aggregate bot traffic by "bot family" across
--          Optimizely E3 decisions + events, and surface representative
--          user-agent exemplars for Cloudflare Worker/WAF rules.
-- Author: Caio / M365 Copilot
-- Last updated: 2025-10-24
-- Notes:
--   * Adjust the date window in the params CTE below.
--   * Tables targeted: `caio-sandbox-468412.optimizely_e3.decisions_raw` and `events`.
--   * This script returns two result sets when run as a script (BigQuery UI):
--       1) Aggregation by bot_family (impact over time + sample UAs)
--       2) Top UA exemplars per bot_family (ranked)
-- ============================================================================

DECLARE start_date DATE DEFAULT DATE '2024-10-30';
DECLARE end_date   DATE DEFAULT DATE '2025-10-29';
DECLARE cap_at_today BOOL DEFAULT TRUE;

CREATE TEMP TABLE _bounds AS
SELECT start_date AS start_date,
       IF(cap_at_today, LEAST(end_date, CURRENT_DATE()), end_date) AS end_date;

-- 1) Normalize decisions/events
CREATE TEMP TABLE _base AS
WITH bounds AS (SELECT * FROM _bounds)
SELECT 'decisions' AS source,
       visitor_id,
       TIMESTAMP(`timestamp`) AS ts,
       DATE_TRUNC(DATE(`timestamp`), MONTH) AS month_start,
       LOWER(COALESCE(user_agent, '')) AS ua,
       LOWER(COALESCE(referer, ''))    AS ref
FROM `caio-sandbox-468412.optimizely_e3.decisions_raw`
WHERE DATE(`timestamp`) BETWEEN (SELECT start_date FROM bounds) AND (SELECT end_date FROM bounds)

UNION ALL

SELECT 'events' AS source,
       visitor_id,
       TIMESTAMP(`timestamp`) AS ts,
       DATE_TRUNC(DATE(`timestamp`), MONTH) AS month_start,
       LOWER(COALESCE(user_agent, '')) AS ua,
       LOWER(COALESCE(referer, ''))    AS ref
FROM `caio-sandbox-468412.optimizely_e3.events`
WHERE DATE(`timestamp`) BETWEEN (SELECT start_date FROM bounds) AND (SELECT end_date FROM bounds);

-- 2) Label bot families
CREATE TEMP TABLE _labeled AS
SELECT
  source, visitor_id, ts, month_start, ua, ref,
  CASE
    -- Readers / TTS
    WHEN REGEXP_CONTAINS(ua,  r'google-read[- ]?aloud') THEN 'google-read-aloud'
    WHEN REGEXP_CONTAINS(ua,  r'read[- ]?aloud|speechify') THEN 'tts-reader'

    -- Translation / proxy referers
    WHEN REGEXP_CONTAINS(ref, r'translate\.google|translate\.goog|googleweblight|webcache\.googleusercontent') THEN 'google-translation/proxy'
    WHEN REGEXP_CONTAINS(ref, r'translatetheweb\.com|translate\.bing\.com') THEN 'microsoft-translation/proxy'

    -- Search / SEO / crawlers
    WHEN REGEXP_CONTAINS(ua,  r'googlebot') THEN 'googlebot'
    WHEN REGEXP_CONTAINS(ua,  r'bingbot') THEN 'bingbot'
    WHEN REGEXP_CONTAINS(ua,  r'bingpreview') THEN 'bingpreview'
    WHEN REGEXP_CONTAINS(ua,  r'duckduckbot') THEN 'duckduckbot'
    WHEN REGEXP_CONTAINS(ua,  r'yandex(bot)?') THEN 'yandexbot'
    WHEN REGEXP_CONTAINS(ua,  r'ahrefs') THEN 'ahrefs'
    WHEN REGEXP_CONTAINS(ua,  r'semrush') THEN 'semrush'
    WHEN REGEXP_CONTAINS(ua,  r'mj12') THEN 'mj12bot'
    WHEN REGEXP_CONTAINS(ua,  r'petalbot') THEN 'petalbot'
    WHEN REGEXP_CONTAINS(ua,  r'screaming\s?frog') THEN 'screaming-frog'
    WHEN REGEXP_CONTAINS(ua,  r'applebot') THEN 'applebot'

    -- Headless & automation
    WHEN REGEXP_CONTAINS(ua,  r'phantomjs') THEN 'phantomjs'
    WHEN REGEXP_CONTAINS(ua,  r'headlesschrome') THEN 'headlesschrome'
    WHEN REGEXP_CONTAINS(ua,  r'puppeteer|playwright') THEN 'headless-automation'

    -- Libraries / CLI
    WHEN REGEXP_CONTAINS(ua,  r'python-requests|aiohttp|urllib') THEN 'python-http'
    WHEN REGEXP_CONTAINS(ua,  r'okhttp') THEN 'okhttp'
    WHEN REGEXP_CONTAINS(ua,  r'node\.js|axios') THEN 'node-http'
    WHEN REGEXP_CONTAINS(ua,  r'httpclient|libwww') THEN 'httpclient'
    WHEN REGEXP_CONTAINS(ua,  r'curl') THEN 'curl'
    WHEN REGEXP_CONTAINS(ua,  r'wget') THEN 'wget'
    WHEN REGEXP_CONTAINS(ua,  r'go-http-client') THEN 'go-http-client'

    -- Generic catch-all
    WHEN REGEXP_CONTAINS(ua,  r'(bot|crawl|spider|slurp|crawler|scrap(e|er)|fetch)') THEN 'other-bot-generic'
    ELSE NULL
  END AS bot_family
FROM _base;

-- 3) Keep only rows classified as a bot family
CREATE TEMP TABLE _bots AS
SELECT * FROM _labeled WHERE bot_family IS NOT NULL AND ua != '';

-- ========================================================================
-- Result set 1: Impact by bot family (overall + per-source + sample UAs)
-- ========================================================================
SELECT
  bot_family,
  COUNT(DISTINCT CONCAT(visitor_id, '|', CAST(month_start AS STRING))) AS visitor_months_impacted,
  COUNT(DISTINCT visitor_id)                                          AS distinct_visitors_bw,
  COUNT(*)                                                            AS rows_flagged,
  COUNTIF(source = 'decisions') AS rows_decisions,
  COUNTIF(source = 'events')    AS rows_events,
  COUNT(DISTINCT IF(source = 'decisions', CONCAT(visitor_id, '|', CAST(month_start AS STRING)), NULL)) AS vm_decisions,
  COUNT(DISTINCT IF(source = 'events',    CONCAT(visitor_id, '|', CAST(month_start AS STRING)), NULL)) AS vm_events,
  COUNT(DISTINCT IF(source = 'decisions', visitor_id, NULL)) AS visitors_decisions,
  COUNT(DISTINCT IF(source = 'events',    visitor_id, NULL)) AS visitors_events,
  MIN(ts) AS first_seen,
  MAX(ts) AS last_seen,
  STRING_AGG(DISTINCT ua, ' | ' ORDER BY ua LIMIT 3) AS sample_uas
FROM _bots
GROUP BY bot_family
ORDER BY visitor_months_impacted DESC, bot_family;

-- ========================================================================
-- Result set 2: Top UA exemplars per family (ranked by distinct visitor-months)
-- ========================================================================
SELECT
  bot_family,
  ua AS user_agent,
  COUNT(*) AS hits,
  COUNT(DISTINCT CONCAT(visitor_id, '|', CAST(month_start AS STRING))) AS visitor_months
FROM _bots
GROUP BY bot_family, user_agent
QUALIFY ROW_NUMBER() OVER (PARTITION BY bot_family ORDER BY visitor_months DESC, hits DESC) <= 50
ORDER BY bot_family, visitor_months DESC, hits DESC;
