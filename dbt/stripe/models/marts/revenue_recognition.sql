{{ config(
    materialized="incremental",
    unique_key=["line_item_id", "recognition_date"],
    incremental_strategy="merge",
    partition_by={"field": "recognition_date", "data_type": "date"},
    cluster_by=["customer_id", "line_item_id"]
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
    SELECT calendar_date FROM {{ ref('calendar') }}
),

revenue_calculated AS (

SELECT  *,

        CASE WHEN  is_tax_inclusive THEN amount - tax_amount
        ELSE amount
        END AS amount_without_tax,

        CASE WHEN is_tax_inclusive THEN amount
        ELSE amount + tax_amount
        END AS amount_with_tax,
from line_items
)

rate_exchanged AS (

SELECT rc.*,

       amount_without_tax * er.exchange_rate AS amount_without_tax_usd,
       amount_with_tax * er.exchange_rate AS amount_with_tax_usd,
       tax_amount * er.exchange_rate AS tax_amount_usd

FROM revenue_calculated rc
JOIN exchange_rates er on rc.currency = er.from_currency AND er.to_currency='USD'

),

service_period_calculated AS (
SELECT  *,
        DATE_DIFF(
            period_end_date,
            period_start_date,
            DAY
        ) AS effective_service_period_days,

        CASE
            --todo, this is not optimal, find reasons why (end-start) could be <=0
            WHEN DATE_DIFF(period_end_date,period_start_date,DAY) > 0 THEN
                amount_without_tax_usd / DATE_DIFF(period_end_date,period_start_date,DAY)
            ELSE amount_without_tax_usd
            END AS daily_revenue_usd
        )
FROM exchange_rate

),

daily_revenue AS (

SELECT  spc.line_item_id,
        spc.invoice_id,
        spc.customer_id,
        spc.subscription_id,

        cal.date_day AS revenue_date,

        spc.currency,
        spc.amount_without_tax,
        spc.amount_without_tax_usd,

        spc.period_start_date,
        spc.period_end_date,
        spc.effective_service_period_days,

        spc.daily_revenue_usd,

        spc.invoice_created_date,
        spc.invoice_created_at,

FROM service_period_calculated spc
join calendar cal on cal.date_day >= spc.period_start_date
    and cal.date_date < spc.service_period_end
        -- todo, could there be a negative item amount due to refund?
        -- todo, do we need to filter on amount >0?
)

SELECT
   *,

    CURRENT_TIMESTAMP() AS _loaded_at

FROM daily_revenue

