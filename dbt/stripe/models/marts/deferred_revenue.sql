{{ config(
    materialized="incremental",
    unique_key=["line_item_id", "as_of_date"],
    incremental_strategy="merge",
    partition_by={"field": "as_of_date", "data_type": "date"},
    cluster_by=["customer_id", "subscription_id"]
) }}

WITH line_items AS (
    SELECT * FROM {{ ref('invoice_line_items') }}
    {% if is_incremental() %}
    WHERE invoice_created_at > (SELECT MAX(invoice_created_at) FROM {{ this }})
    {% endif %}
),

exchange_rates AS (
    SELECT * FROM {{ ref('exchange_rates') }}
),

calendar AS (
    SELECT date_day FROM {{ ref('calendar') }}
),

revenue_calculated AS (
    SELECT  
        *,

        -- amount without tax
        CASE 
            WHEN is_tax_inclusive THEN amount - tax_amount
            ELSE amount
        END AS amount_without_tax,

        -- amount with tax
        CASE 
            WHEN is_tax_inclusive THEN amount
            ELSE amount + tax_amount
        END AS amount_with_tax

    FROM line_items
),

rate_exchanged AS (
    SELECT 
        rc.*,

        -- convert amounts and taxes to USD, or any currency of choice
        rc.amount_without_tax * er.exchange_rate AS amount_without_tax_usd,
        rc.amount_with_tax * er.exchange_rate AS amount_with_tax_usd,
        rc.tax_amount * er.exchange_rate AS tax_amount_usd

    FROM revenue_calculated rc
    JOIN exchange_rates er 
        ON rc.currency = er.from_currency 
        AND er.to_currency = 'USD'
),

service_period_calculated AS (
    SELECT  
        *,
        -- effective service period
        DATE_DIFF(period_end_date, period_start_date, DAY) AS effective_service_period_days,

        -- daily revenue amount
        CASE
            WHEN DATE_DIFF(period_end_date, period_start_date, DAY) > 0 THEN
                amount_without_tax_usd / DATE_DIFF(period_end_date, period_start_date, DAY)
            ELSE amount_without_tax_usd
        END AS daily_revenue_usd

    FROM rate_exchanged
),

deferred_revenue_by_date AS (
    SELECT  
        spc.line_item_id,
        spc.invoice_id,
        spc.customer_id,
        spc.subscription_id,
        
        cal.date_day AS as_of_date,
        
        spc.currency,
        spc.amount_without_tax,
        spc.amount_without_tax_usd,
        
        spc.period_start_date,
        spc.period_end_date,
        spc.effective_service_period_days,

        spc.daily_revenue_usd,
        
        spc.invoice_created_date,
        spc.invoice_created_at,

        -- deferred revenue
        CASE
            WHEN cal.date_day < spc.period_start_date THEN spc.amount_without_tax_usd
            WHEN cal.date_day >= spc.period_end_date THEN 0
            ELSE 
                spc.daily_revenue_usd * DATE_DIFF(spc.period_end_date, cal.date_day, DAY)
        END AS deferred_revenue_usd,

        -- recognized revenue
        CASE
            WHEN cal.date_day < spc.period_start_date THEN 0
            WHEN cal.date_day >= spc.period_end_date THEN spc.amount_without_tax_usd
            ELSE 
                spc.daily_revenue_usd * DATE_DIFF(cal.date_day, spc.period_start_date, DAY)
        END AS recognized_revenue_usd,

    FROM service_period_calculated spc
    CROSS JOIN calendar cal

    -- invoice created (all deferred revenue, no recognized revenue)
    -- service start (some deferred revenue, some recognized revenue)
    -- service end (no deferred revenue, all recognized revenue)
    WHERE
     -- use invoice created date as the start of calculations
    cal.date_day >= spc.invoice_created_date
    -- use service end date as the end date of calculations
        AND cal.date_day <= spc.period_end_date
)

SELECT
    *,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM deferred_revenue_by_date

