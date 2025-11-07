-- Bot family list across Decisions + Events (event-time window; cap at today = TRUE)
WITH params AS (
  SELECT DATE '2024-10-30' AS start_date,
         DATE '2025-10-29' AS end_date,
         TRUE AS cap_at_today
),
bounds AS (
  SELECT start_date,
         IF(cap_at_today, LEAST(end_date, CURRENT_DATE()), end_date) AS end_date
  FROM params
),

-- 1) Normalize decisions/events to the same shape; use column name `source` (not `src`)
base AS (
  SELECT 'decisions' AS source,
         visitor_id,
         DATE_TRUNC(DATE(`timestamp`), MONTH) AS month_start,
         LOWER(COALESCE(user_agent, '')) AS ua,
         LOWER(COALESCE(referer, ''))    AS ref,
         TIMESTAMP(`timestamp`) AS ts
  FROM `caio-sandbox-468412.optimizely_e3.decisions_raw`
  WHERE visitor_id IS NOT NULL
    AND DATE(`timestamp`) BETWEEN (SELECT start_date FROM bounds) AND (SELECT end_date FROM bounds)

  UNION ALL

  SELECT 'events' AS source,
         visitor_id,
         DATE_TRUNC(DATE(`timestamp`), MONTH) AS month_start,
         LOWER(COALESCE(user_agent, '')) AS ua,
         LOWER(COALESCE(referer, ''))    AS ref,
         TIMESTAMP(`timestamp`) AS ts
  FROM `caio-sandbox-468412.optimizely_e3.events`
  WHERE visitor_id IS NOT NULL
    AND DATE(`timestamp`) BETWEEN (SELECT start_date FROM bounds) AND (SELECT end_date FROM bounds)
),

-- 2) Label bot families (extend as needed)
labeled AS (
  SELECT
    source, visitor_id, month_start, ua, ref, ts,
    CASE
      WHEN REGEXP_CONTAINS(ua,  r'google-read[- ]?aloud') THEN 'google-read-aloud'
      WHEN REGEXP_CONTAINS(ref, r'translate\.google|translate\.goog|googleweblight|webcache\.googleusercontent') THEN 'google-translation/proxy'
      WHEN REGEXP_CONTAINS(ua,  r'googlebot') THEN 'googlebot'
      WHEN REGEXP_CONTAINS(ua,  r'bingbot') THEN 'bingbot'
      WHEN REGEXP_CONTAINS(ua,  r'bingpreview') THEN 'bingpreview'
      WHEN REGEXP_CONTAINS(ua,  r'duckduckbot') THEN 'duckduckbot'
      WHEN REGEXP_CONTAINS(ua,  r'yandexbot') THEN 'yandexbot'
      WHEN REGEXP_CONTAINS(ua,  r'ahrefs') THEN 'ahrefs'
      WHEN REGEXP_CONTAINS(ua,  r'semrush') THEN 'semrush'
      WHEN REGEXP_CONTAINS(ua,  r'mj12') THEN 'mj12bot'
      WHEN REGEXP_CONTAINS(ua,  r'petalbot') THEN 'petalbot'
      WHEN REGEXP_CONTAINS(ua,  r'screaming\s?frog') THEN 'screaming-frog'
      WHEN REGEXP_CONTAINS(ua,  r'python-requests') THEN 'python-requests'
      WHEN REGEXP_CONTAINS(ua,  r'curl') THEN 'curl'
      WHEN REGEXP_CONTAINS(ua,  r'httpclient') THEN 'httpclient'
      WHEN REGEXP_CONTAINS(ua,  r'phantomjs') THEN 'phantomjs'
      WHEN REGEXP_CONTAINS(ua,  r'headlesschrome') THEN 'headlesschrome'
      WHEN REGEXP_CONTAINS(ua,  r'\b(bot|crawl|spider|slurp|crawler)\b') THEN 'other-bot-generic'
      ELSE NULL
    END AS bot_family
  FROM base
),

-- 3) Keep only rows classified as a bot family
bots AS (
  SELECT * FROM labeled WHERE bot_family IS NOT NULL
)

-- 4) Aggregate per family, with per-source breakdown (no STRING_AGG)
SELECT
  bot_family,

  -- Overall counts
  COUNT(DISTINCT CONCAT(visitor_id, '|', CAST(month_start AS STRING))) AS visitor_months_impacted,
  COUNT(DISTINCT visitor_id)                                          AS distinct_visitors_bw,
  COUNT(*)                                                            AS rows_flagged,

  -- Per-source row counts
  COUNTIF(source = 'decisions') AS rows_decisions,
  COUNTIF(source = 'events')    AS rows_events,

  -- Per-source visitor-month counts (distinct visitor_id x month per source)
  COUNT(DISTINCT IF(source = 'decisions', CONCAT(visitor_id, '|', CAST(month_start AS STRING)), NULL)) AS vm_decisions,
  COUNT(DISTINCT IF(source = 'events',    CONCAT(visitor_id, '|', CAST(month_start AS STRING)), NULL)) AS vm_events,

  -- Per-source distinct visitors in the whole window
  COUNT(DISTINCT IF(source = 'decisions', visitor_id, NULL)) AS visitors_decisions,
  COUNT(DISTINCT IF(source = 'events',    visitor_id, NULL)) AS visitors_events,

  -- Helpful metadata
  MIN(ts) AS first_seen,
  MAX(ts) AS last_seen,

  -- ✅ fixed: aggregate directly; no UNNEST, no subquery
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT ua ORDER BY ua LIMIT 3), ' | ') AS sample_uas

FROM bots
GROUP BY bot_family
ORDER BY visitor_months_impacted DESC, bot_family;