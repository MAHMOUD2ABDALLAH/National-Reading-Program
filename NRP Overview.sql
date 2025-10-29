-- ======================================================================
-- NATIONAL READING PROGRAM (NRP) - EGYPT BRANCH
-- COMPREHENSIVE BIGQUERY ANALYTICS PLATFORM
-- ======================================================================

-- CONFIGURATION PARAMETERS
DECLARE START_DATE STRING DEFAULT '20251001';
DECLARE END_DATE STRING DEFAULT '20251028';
DECLARE PROJECT_ID STRING DEFAULT 'nrp-egypt-analytics';
DECLARE DATASET_ID STRING DEFAULT 'nrp_ga4_data';

-- 1. CORE USER PLATFORM ANALYSIS
WITH platform_detailed AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    user_pseudo_id,
    device.category AS device_category,
    device.operating_system AS operating_system,
    device.web_info.browser AS browser,
    device.mobile_brand_name AS mobile_brand,
    device.mobile_model_name AS mobile_model,
    device.language,
    device.screen_resolution,
    geo.continent,
    geo.country,
    geo.region,
    geo.city,
    traffic_source.source AS acquisition_source,
    traffic_source.medium AS acquisition_medium,
    traffic_source.name AS campaign_name,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') AS page_title,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_url,
    event_name,
    event_timestamp,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') AS engagement_time_msec
  FROM
    `{{PROJECT_ID}}.{{DATASET_ID}}.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN START_DATE AND END_DATE
),

-- 2. USER SESSIONS ENRICHMENT
user_sessions AS (
  SELECT
    user_pseudo_id,
    session_id,
    MIN(TIMESTAMP_MICROS(event_timestamp)) AS session_start,
    MAX(TIMESTAMP_MICROS(event_timestamp)) AS session_end,
    TIMESTAMP_DIFF(MAX(TIMESTAMP_MICROS(event_timestamp)), 
                   MIN(TIMESTAMP_MICROS(event_timestamp)), SECOND) AS session_duration_sec,
    COUNT(DISTINCT event_name) AS unique_events,
    SUM(IF(event_name = 'page_view', 1, 0)) AS page_views,
    SUM(IF(event_name = 'user_engagement', 1, 0)) AS engagement_events,
    MAX(device_category) AS device_category,
    MAX(operating_system) AS operating_system,
    MAX(acquisition_source) AS acquisition_source,
    MAX(page_title) AS landing_page_title,
    MAX(page_url) AS landing_page_url
  FROM
    platform_detailed
  WHERE
    session_id IS NOT NULL
  GROUP BY
    user_pseudo_id, session_id
),

-- 3. DAILY ACTIVITY METRICS
daily_metrics AS (
  SELECT
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS activity_date,
    COUNT(DISTINCT user_pseudo_id) AS daily_active_users,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) AS daily_sessions,
    SUM(IF(event_name = 'page_view', 1, 0)) AS daily_page_views,
    SUM(IF(event_name = 'first_visit', 1, 0)) AS daily_new_users,
    AVG(COALESCE(engagement_time_msec, 0)) / 60000 AS avg_engagement_minutes,
    COUNT(DISTINCT IF(event_name IN ('page_view', 'user_engagement'), user_pseudo_id, NULL)) AS engaged_users
  FROM
    platform_detailed
  GROUP BY
    activity_date
),

-- 4. CONTENT PERFORMANCE DEEP DIVE
content_performance AS (
  SELECT
    page_title,
    page_url,
    COUNT(*) AS total_views,
    COUNT(DISTINCT user_pseudo_id) AS unique_viewers,
    AVG(COALESCE(engagement_time_msec, 0)) / 60000 AS avg_time_on_page_minutes,
    COUNT(DISTINCT session_id) AS sessions_with_page,
    -- Content categorization based on Arabic keywords
    CASE
      WHEN REGEXP_CONTAINS(LOWER(page_title), r'(تسجيل|تسجيل الدخول|register|login)') THEN 'Registration'
      WHEN REGEXP_CONTAINS(LOWER(page_title), r'(طالب|طالب جامعة|student)') THEN 'Student Portal'
      WHEN REGEXP_CONTAINS(LOWER(page_title), r'(معلم|مدرس|teacher)') THEN 'Teacher Portal'
      WHEN REGEXP_CONTAINS(LOWER(page_title), r'(مكتبة|كتب|library|books)') THEN 'Library'
      WHEN REGEXP_CONTAINS(LOWER(page_title), r'(قراءة|reading)') THEN 'Reading Content'
      ELSE 'Other'
    END AS content_category
  FROM
    platform_detailed
  WHERE
    event_name = 'page_view'
    AND page_title IS NOT NULL
  GROUP BY
    page_title, page_url
),

-- 5. ACQUISITION CHANNEL ANALYSIS
acquisition_analysis AS (
  SELECT
    acquisition_source,
    acquisition_medium,
    campaign_name,
    COUNT(DISTINCT user_pseudo_id) AS total_users,
    COUNT(DISTINCT IF(event_name = 'first_visit', user_pseudo_id, NULL)) AS new_users,
    COUNT(DISTINCT session_id) AS total_sessions,
    SUM(IF(event_name = 'page_view', 1, 0)) AS total_page_views,
    AVG(COALESCE(engagement_time_msec, 0)) / 60000 AS avg_engagement_minutes,
    -- Channel grouping
    CASE
      WHEN acquisition_source = '(direct)' AND acquisition_medium = '(none)' THEN 'Direct'
      WHEN acquisition_medium = 'organic' THEN 'Organic Search'
      WHEN REGEXP_CONTAINS(acquisition_source, r'(facebook|instagram|twitter)') THEN 'Organic Social'
      WHEN acquisition_medium = 'referral' THEN 'Referral'
      WHEN acquisition_medium = 'email' THEN 'Email'
      ELSE 'Other'
    END AS channel_group
  FROM
    platform_detailed
  WHERE
    acquisition_source IS NOT NULL
  GROUP BY
    acquisition_source, acquisition_medium, campaign_name
),

-- 6. USER RETENTION COHORT ANALYSIS
user_retention_cohorts AS (
  WITH user_first_visit AS (
    SELECT
      user_pseudo_id,
      DATE(MIN(TIMESTAMP_MICROS(event_timestamp))) AS first_visit_date
    FROM
      platform_detailed
    WHERE
      event_name = 'first_visit'
    GROUP BY
      user_pseudo_id
  ),
  user_daily_activity AS (
    SELECT
      user_pseudo_id,
      DATE(TIMESTAMP_MICROS(event_timestamp)) AS activity_date
    FROM
      platform_detailed
    WHERE
      event_name = 'session_start'
    GROUP BY
      user_pseudo_id, activity_date
  )
  SELECT
    ufv.first_visit_date AS cohort_date,
    ufv.user_pseudo_id,
    uda.activity_date,
    DATE_DIFF(uda.activity_date, ufv.first_visit_date, DAY) AS days_since_first_visit
  FROM
    user_first_visit ufv
  LEFT JOIN
    user_daily_activity uda
  ON
    ufv.user_pseudo_id = uda.user_pseudo_id
    AND uda.activity_date >= ufv.first_visit_date
),

-- 7. DEVICE & TECHNOLOGY STACK ANALYSIS
technology_stack AS (
  SELECT
    device_category,
    operating_system,
    browser,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT session_id) AS total_sessions,
    AVG(COALESCE(engagement_time_msec, 0)) / 60000 AS avg_engagement_minutes,
    SUM(IF(event_name = 'page_view', 1, 0)) AS total_page_views,
    -- Device performance score
    ROUND(
      (COUNT(DISTINCT user_pseudo_id) * 0.3 +
       AVG(COALESCE(engagement_time_msec, 0)) / 1000 * 0.4 +
       COUNT(DISTINCT session_id) * 0.3), 2
    ) AS device_performance_score
  FROM
    platform_detailed
  WHERE
    device_category IS NOT NULL
  GROUP BY
    device_category, operating_system, browser
),

-- 8. READING ENGAGEMENT PATTERNS
reading_engagement AS (
  SELECT
    user_pseudo_id,
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS engagement_date,
    EXTRACT(HOUR FROM TIMESTAMP_MICROS(event_timestamp)) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP_MICROS(event_timestamp)) AS day_of_week,
    COUNT(DISTINCT session_id) AS daily_sessions,
    SUM(IF(event_name = 'page_view', 1, 0)) AS daily_page_views,
    SUM(COALESCE(engagement_time_msec, 0)) / 60000 AS total_engagement_minutes,
    COUNT(DISTINCT page_title) AS unique_pages_viewed
  FROM
    platform_detailed
  WHERE
    event_name IN ('page_view', 'user_engagement')
  GROUP BY
    user_pseudo_id, engagement_date, hour_of_day, day_of_week
),

-- 9. USER SEGMENTATION BY ENGAGEMENT
user_segmentation AS (
  WITH user_engagement_metrics AS (
    SELECT
      user_pseudo_id,
      COUNT(DISTINCT DATE(TIMESTAMP_MICROS(event_timestamp))) AS active_days,
      COUNT(DISTINCT session_id) AS total_sessions,
      SUM(IF(event_name = 'page_view', 1, 0)) AS total_page_views,
      SUM(COALESCE(engagement_time_msec, 0)) / 60000 AS total_engagement_minutes,
      COUNT(DISTINCT page_title) AS unique_content_viewed,
      DATE_DIFF(
        PARSE_DATE('%Y%m%d', END_DATE),
        DATE(MIN(TIMESTAMP_MICROS(event_timestamp))),
        DAY
      ) AS days_since_first_visit
    FROM
      platform_detailed
    GROUP BY
      user_pseudo_id
  )
  SELECT
    user_pseudo_id,
    active_days,
    total_sessions,
    total_page_views,
    total_engagement_minutes,
    unique_content_viewed,
    -- User segmentation
    CASE
      WHEN active_days >= 7 AND total_engagement_minutes > 60 THEN 'Power Reader'
      WHEN active_days >= 3 AND total_engagement_minutes > 30 THEN 'Active Reader'
      WHEN active_days >= 1 AND total_engagement_minutes > 10 THEN 'Casual Reader'
      WHEN active_days = 1 AND total_engagement_minutes <= 10 THEN 'One-Time Visitor'
      ELSE 'Inactive'
    END AS user_segment,
    -- Engagement score (0-100)
    ROUND(
      (LEAST(active_days, 10) * 10 + 
       LEAST(total_sessions, 20) * 2.5 +
       LEAST(total_engagement_minutes, 120) * 0.5 +
       LEAST(unique_content_viewed, 50) * 0.5), 2
    ) AS engagement_score
  FROM
    user_engagement_metrics
)

-- ======================================================================
-- MAIN ANALYSIS QUERIES - COMPREHENSIVE REPORTING
-- ======================================================================

-- A. EXECUTIVE SUMMARY DASHBOARD
SELECT 
  'EXECUTIVE_SUMMARY' AS report_type,
  COUNT(DISTINCT pd.user_pseudo_id) AS total_active_users,
  COUNT(DISTINCT IF(pd.event_name = 'first_visit', pd.user_pseudo_id, NULL)) AS total_new_users,
  COUNT(DISTINCT pd.session_id) AS total_sessions,
  SUM(IF(pd.event_name = 'page_view', 1, 0)) AS total_page_views,
  ROUND(AVG(COALESCE(pd.engagement_time_msec, 0)) / 60000, 2) AS avg_engagement_minutes,
  COUNT(DISTINCT cp.page_title) AS unique_content_pieces,
  ROUND(COUNT(DISTINCT pd.session_id) / COUNT(DISTINCT pd.user_pseudo_id), 2) AS avg_sessions_per_user
FROM 
  platform_detailed pd
LEFT JOIN 
  content_performance cp ON pd.page_title = cp.page_title;

-- B. PLATFORM PERFORMANCE DETAILED BREAKDOWN
SELECT
  'PLATFORM_PERFORMANCE' AS report_type,
  device_category,
  operating_system,
  browser,
  COUNT(DISTINCT pd.user_pseudo_id) AS active_users,
  COUNT(DISTINCT pd.session_id) AS total_sessions,
  ROUND(AVG(us.session_duration_sec) / 60, 2) AS avg_session_minutes,
  ROUND(SUM(COALESCE(pd.engagement_time_msec, 0)) / 60000, 2) AS total_engagement_hours,
  ts.device_performance_score
FROM
  platform_detailed pd
JOIN
  user_sessions us ON pd.user_pseudo_id = us.user_pseudo_id AND pd.session_id = us.session_id
JOIN
  technology_stack ts ON pd.device_category = ts.device_category 
                      AND pd.operating_system = ts.operating_system 
                      AND pd.browser = ts.browser
GROUP BY
  device_category, operating_system, browser, ts.device_performance_score
ORDER BY
  active_users DESC;

-- C. ACQUISITION CHANNEL EFFECTIVENESS
SELECT
  'ACQUISITION_ANALYSIS' AS report_type,
  channel_group,
  acquisition_source,
  acquisition_medium,
  campaign_name,
  total_users,
  new_users,
  total_sessions,
  total_page_views,
  avg_engagement_minutes,
  ROUND(total_sessions / total_users, 2) AS sessions_per_user,
  ROUND(total_page_views / total_sessions, 2) AS pages_per_session
FROM
  acquisition_analysis
ORDER BY
  total_users DESC;

-- D. CONTENT PERFORMANCE & READER PREFERENCES
SELECT
  'CONTENT_PERFORMANCE' AS report_type,
  content_category,
  page_title,
  total_views,
  unique_viewers,
  ROUND(avg_time_on_page_minutes, 2) AS avg_time_minutes,
  ROUND(total_views / unique_viewers, 2) AS views_per_user,
  ROUND(UNIX_SECONDS(CURRENT_TIMESTAMP()) - UNIX_SECONDS(MAX(TIMESTAMP_MICROS(pd.event_timestamp)))) AS seconds_since_last_view
FROM
  content_performance cp
JOIN
  platform_detailed pd ON cp.page_title = pd.page_title
GROUP BY
  content_category, page_title, total_views, unique_viewers, avg_time_on_page_minutes
ORDER BY
  total_views DESC;

-- E. USER RETENTION & ENGAGEMENT TRENDS
SELECT
  'RETENTION_ANALYSIS' AS report_type,
  cohort_date,
  COUNT(DISTINCT user_pseudo_id) AS cohort_size,
  ROUND(COUNT(DISTINCT IF(days_since_first_visit = 1, user_pseudo_id, NULL)) / COUNT(DISTINCT user_pseudo_id) * 100, 2) AS day_1_retention,
  ROUND(COUNT(DISTINCT IF(days_since_first_visit = 7, user_pseudo_id, NULL)) / COUNT(DISTINCT user_pseudo_id) * 100, 2) AS day_7_retention,
  ROUND(COUNT(DISTINCT IF(days_since_first_visit = 30, user_pseudo_id, NULL)) / COUNT(DISTINCT user_pseudo_id) * 100, 2) AS day_30_retention
FROM
  user_retention_cohorts
WHERE
  cohort_date >= DATE_SUB(PARSE_DATE('%Y%m%d', START_DATE), INTERVAL 30 DAY)
GROUP BY
  cohort_date
ORDER BY
  cohort_date DESC;

-- F. USER SEGMENTATION FOR TARGETED ENGAGEMENT
SELECT
  'USER_SEGMENTATION' AS report_type,
  user_segment,
  COUNT(DISTINCT us.user_pseudo_id) AS segment_size,
  ROUND(AVG(us.active_days), 2) AS avg_active_days,
  ROUND(AVG(us.total_sessions), 2) AS avg_sessions,
  ROUND(AVG(us.total_engagement_minutes), 2) AS avg_engagement_minutes,
  ROUND(AVG(us.engagement_score), 2) AS avg_engagement_score,
  COUNT(DISTINCT pd.acquisition_source) AS acquisition_channels
FROM
  user_segmentation us
LEFT JOIN
  platform_detailed pd ON us.user_pseudo_id = pd.user_pseudo_id
GROUP BY
  user_segment
ORDER BY
  segment_size DESC;

-- G. DAILY PERFORMANCE TRENDS
SELECT
  'DAILY_TRENDS' AS report_type,
  dm.activity_date,
  dm.daily_active_users,
  dm.daily_new_users,
  dm.daily_sessions,
  dm.daily_page_views,
  dm.avg_engagement_minutes,
  ROUND(dm.daily_sessions / NULLIF(dm.daily_active_users, 0), 2) AS sessions_per_user,
  ROUND(dm.daily_page_views / NULLIF(dm.daily_sessions, 0), 2) AS pages_per_session,
  COUNT(DISTINCT re.user_pseudo_id) AS engaged_readers
FROM
  daily_metrics dm
LEFT JOIN
  reading_engagement re ON dm.activity_date = re.engagement_date
GROUP BY
  dm.activity_date, dm.daily_active_users, dm.daily_new_users, dm.daily_sessions, 
  dm.daily_page_views, dm.avg_engagement_minutes
ORDER BY
  dm.activity_date;

-- H. READING PATTERNS BY TIME OF DAY
SELECT
  'TIME_ANALYSIS' AS report_type,
  hour_of_day,
  day_of_week,
  COUNT(DISTINCT user_pseudo_id) AS active_readers,
  COUNT(DISTINCT session_id) AS total_sessions,
  SUM(IF(event_name = 'page_view', 1, 0)) AS page_views,
  ROUND(AVG(COALESCE(engagement_time_msec, 0)) / 60000, 2) AS avg_engagement_minutes,
  CASE day_of_week
    WHEN 1 THEN 'Sunday'
    WHEN 2 THEN 'Monday'
    WHEN 3 THEN 'Tuesday'
    WHEN 4 THEN 'Wednesday'
    WHEN 5 THEN 'Thursday'
    WHEN 6 THEN 'Friday'
    WHEN 7 THEN 'Saturday'
  END AS day_name
FROM
  platform_detailed
WHERE
  event_name IN ('session_start', 'page_view', 'user_engagement')
GROUP BY
  hour_of_day, day_of_week
ORDER BY
  day_of_week, hour_of_day;

-- ======================================================================
-- ADVANCED ANALYTICS: PREDICTIVE METRICS
-- ======================================================================

-- I. USER LIFETIME VALUE PREDICTION
WITH user_lifetime_metrics AS (
  SELECT
    us.user_pseudo_id,
    us.user_segment,
    us.engagement_score,
    us.total_engagement_minutes,
    us.active_days,
    COUNT(DISTINCT pd.session_id) AS total_sessions,
    COUNT(DISTINCT pd.page_title) AS unique_content_viewed,
    DATE_DIFF(
      PARSE_DATE('%Y%m%d', END_DATE),
      DATE(MIN(TIMESTAMP_MICROS(pd.event_timestamp))),
      DAY
    ) AS user_tenure_days
  FROM
    user_segmentation us
  JOIN
    platform_detailed pd ON us.user_pseudo_id = pd.user_pseudo_id
  GROUP BY
    us.user_pseudo_id, us.user_segment, us.engagement_score, 
    us.total_engagement_minutes, us.active_days
)
SELECT
  'LIFETIME_VALUE' AS report_type,
  user_segment,
  COUNT(*) AS users_in_segment,
  ROUND(AVG(user_tenure_days), 2) AS avg_tenure_days,
  ROUND(AVG(total_engagement_minutes), 2) AS avg_total_engagement,
  ROUND(AVG(unique_content_viewed), 2) AS avg_content_consumed,
  -- Simple LTV prediction based on engagement patterns
  ROUND(
    (AVG(engagement_score) * 0.3 +
     AVG(user_tenure_days) * 0.2 +
     AVG(unique_content_viewed) * 0.2 +
     AVG(total_engagement_minutes) * 0.3), 2
  ) AS predicted_ltv_score
FROM
  user_lifetime_metrics
GROUP BY
  user_segment
ORDER BY
  predicted_ltv_score DESC;

-- J. CONTENT RECOMMENDATION ENGINE BASE
SELECT
  'CONTENT_RECOMMENDATIONS' AS report_type,
  cp.content_category,
  cp.page_title,
  cp.unique_viewers,
  cp.avg_time_on_page_minutes,
  ROUND(cp.unique_viewers / NULLIF((
    SELECT COUNT(DISTINCT user_pseudo_id) 
    FROM platform_detailed 
    WHERE event_name = 'session_start'
  ), 0) * 100, 2) AS penetration_rate,
  -- Content popularity score
  ROUND(
    (cp.total_views * 0.4 +
     cp.unique_viewers * 0.3 +
     cp.avg_time_on_page_minutes * 20 * 0.3), 2
  ) AS content_popularity_score
FROM
  content_performance cp
WHERE
  cp.total_views > 10  -- Minimum threshold for recommendations
ORDER BY
  content_popularity_score DESC
LIMIT 50;

-- ======================================================================
-- DATA QUALITY & COMPLETENESS CHECKS
-- ======================================================================

SELECT
  'DATA_QUALITY' AS report_type,
  COUNT(*) AS total_events,
  COUNT(DISTINCT user_pseudo_id) AS distinct_users,
  COUNT(DISTINCT session_id) AS distinct_sessions,
  SUM(IF(event_name IS NULL, 1, 0)) AS missing_event_names,
  SUM(IF(device_category IS NULL, 1, 0)) AS missing_device_info,
  SUM(IF(acquisition_source IS NULL, 1, 0)) AS missing_acquisition_data,
  ROUND(SUM(IF(engagement_time_msec IS NULL, 1, 0)) / COUNT(*) * 100, 2) AS pct_missing_engagement_time
FROM
  platform_detailed;