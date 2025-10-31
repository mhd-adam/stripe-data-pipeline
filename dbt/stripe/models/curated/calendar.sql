{{ config(
    materialized="table",
) }}

SELECT
  date_day,
  EXTRACT(YEAR FROM date_day) AS year,
  EXTRACT(WEEK FROM date_day) AS week_of_year,
  EXTRACT(DAY FROM date_day) AS day_of_year,
  FORMAT_DATE('%Q', date_day) as quarter_of_year,
  EXTRACT(MONTH FROM date_day) AS month_of_year,
  FORMAT_DATE('%B', date_day) AS month_name,
  CAST(FORMAT_DATE('%u', date_day) AS INT64) AS day_of_week,
  FORMAT_DATE('%A', date_day) AS day_of_week_name,
  IF(FORMAT_DATE('%A', date_day) IN ('Saturday', 'Sunday'), FALSE, TRUE) AS is_weekday,
  COUNT(date_day) OVER (PARTITION BY EXTRACT(YEAR FROM date_day), EXTRACT(MONTH FROM date_day)) AS days_in_month,
  IF(COUNT(date_day) OVER (PARTITION BY EXTRACT(YEAR FROM date_day)) = 365, FALSE, TRUE) AS is_leap_year, 
FROM UNNEST(
  GENERATE_DATE_ARRAY('2020-01-01', CURRENT_DATE, INTERVAL 1 DAY)
  ) AS date_day