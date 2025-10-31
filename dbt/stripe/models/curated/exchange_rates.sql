{{ config(
    materialized="table"
) }}



-- This is a demo table
-- in reality this should be a table that is updated daily
-- with the latest exchange rates from a reliable source

WITH base_rates AS (
    SELECT 'USD' AS from_currency, 'USD' AS to_currency, 1.0 AS exchange_rate, CURRENT_DATE() AS rate_date
    UNION ALL
    SELECT 'GBP' AS from_currency, 'USD' AS to_currency, 1.27 AS exchange_rate, CURRENT_DATE() AS rate_date
    UNION ALL
    SELECT 'EUR' AS from_currency, 'USD' AS to_currency, 1.08 AS exchange_rate, CURRENT_DATE() AS rate_date
    UNION ALL
    SELECT 'USD' AS from_currency, 'GBP' AS to_currency, 0.79 AS exchange_rate, CURRENT_DATE() AS rate_date
    UNION ALL
    SELECT 'EUR' AS from_currency, 'GBP' AS to_currency, 0.85 AS exchange_rate, CURRENT_DATE() AS rate_date
    UNION ALL
    SELECT 'GBP' AS from_currency, 'EUR' AS to_currency, 1.17 AS exchange_rate, CURRENT_DATE() AS rate_date
    UNION ALL
    SELECT 'USD' AS from_currency, 'EUR' AS to_currency, 0.93 AS exchange_rate, CURRENT_DATE() AS rate_date
    UNION ALL
    SELECT 'EUR' AS from_currency, 'EUR' AS to_currency, 1.0 AS exchange_rate, CURRENT_DATE() AS rate_date
    UNION ALL
    SELECT 'GBP' AS from_currency, 'GBP' AS to_currency, 1.0 AS exchange_rate, CURRENT_DATE() AS rate_date
)

SELECT
    from_currency,
    to_currency,
    exchange_rate,
    rate_date
FROM base_rates

