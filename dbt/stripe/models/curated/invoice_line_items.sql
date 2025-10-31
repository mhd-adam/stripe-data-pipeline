{{ config(
    materialized="incremental",
    unique_key="line_item_id",
    incremental_strategy="merge",
    partition_by={"field": "invoice_created_date", "data_type": "date"},
    cluster_by=["invoice_id", "subscription_id"]
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_invoices') }}
    {% if is_incremental() %}
    WHERE created_at_date > (SELECT MAX(invoice_created_date) FROM {{ this }})
    {% endif %}
),

flattened_lines AS (
    SELECT
        s.id AS invoice_id,
        s.customer AS customer_id,
        s.subscription AS subscription_id,
        s.status AS invoice_status,
        s.currency AS invoice_currency,

        automatic_tax,

        TIMESTAMP_SECONDS(CAST(s.created AS INT64)) AS invoice_created_at,
        s.created_at_date AS invoice_created_date,

        line_item
    FROM source s,
    UNNEST(JSON_EXTRACT_ARRAY(s.lines, '$.data')) AS line_item
    WHERE s.status = 'paid' -- there's a filter in the source Stripe query, leaving this here for reference
),

transformed AS (
    SELECT
        fl.invoice_id,
        fl.automatic_tax,
        fl.customer_id,
        COALESCE(fl.subscription_id, JSON_EXTRACT_SCALAR(fl.line_item, '$.subscription')) AS subscription_id,
        fl.invoice_status,
        fl.invoice_created_at,
        fl.invoice_created_date,

        JSON_EXTRACT_SCALAR(fl.line_item, '$.id') AS line_item_id,
        JSON_EXTRACT_SCALAR(fl.line_item, '$.type') AS line_item_type,
        JSON_EXTRACT_SCALAR(fl.line_item, '$.description') AS description,

        CAST(JSON_EXTRACT_SCALAR(fl.line_item, '$.amount') AS FLOAT64) / 100 AS amount,
        JSON_EXTRACT_SCALAR(fl.line_item, '$.currency') AS currency,
        CAST(JSON_EXTRACT_SCALAR(fl.line_item, '$.quantity') AS INT64) AS quantity,

        -- period_start
        CAST(JSON_EXTRACT_SCALAR(fl.line_item, '$.period.start') AS INT64) AS period_start_timestamp,
        TIMESTAMP_SECONDS(CAST(JSON_EXTRACT_SCALAR(fl.line_item, '$.period.start') AS INT64)) AS period_start_at,
        DATE(TIMESTAMP_SECONDS(CAST(JSON_EXTRACT_SCALAR(fl.line_item, '$.period.start') AS INT64))) AS period_start_date,

        -- period_end
        CAST(JSON_EXTRACT_SCALAR(fl.line_item, '$.period.end') AS INT64) AS period_end_timestamp,
        TIMESTAMP_SECONDS(CAST(JSON_EXTRACT_SCALAR(fl.line_item, '$.period.end') AS INT64)) AS period_end_at,
        DATE(TIMESTAMP_SECONDS(CAST(JSON_EXTRACT_SCALAR(fl.line_item, '$.period.end') AS INT64))) AS period_end_date,


        -- assuming all taxes have the same tax_behavior: inclusive or exclusive
        COALESCE(
            (
                SELECT SUM(CAST(JSON_EXTRACT_SCALAR(tax_item, '$.amount') AS FLOAT64)) / 100
                FROM UNNEST(JSON_EXTRACT_ARRAY(fl.line_item, '$.taxes')) AS tax_item
            ),
            0
        ) AS tax_amount,

        (
            SELECT JSON_EXTRACT_SCALAR(tax_item, '$.tax_behavior')
            FROM UNNEST(JSON_EXTRACT_ARRAY(fl.line_item, '$.taxes')) AS tax_item
            LIMIT 1
        ) AS tax_behavior,

        COALESCE(
            (
                SELECT JSON_EXTRACT_SCALAR(tax_item, '$.tax_behavior') = 'inclusive'
                FROM UNNEST(JSON_EXTRACT_ARRAY(fl.line_item, '$.taxes')) AS tax_item
                LIMIT 1
            ),
            FALSE
        ) AS is_tax_inclusive,

        JSON_EXTRACT(fl.line_item, '$.metadata') AS metadata

    FROM flattened_lines fl
)

SELECT
    *,

    DATE_DIFF(
        COALESCE(period_end_date, invoice_created_date),
        COALESCE(period_start_date, invoice_created_date),
        DAY
    ) AS service_period_days,

    CASE
    WHEN period_end_date IS NOT NULL THEN period_end_date

    -- TODO: options (need more info):
    -- 1. Infer from subscription's billing period
    -- 2. Read the invoice's end date, although invoice end date might not always be service item end date
    -- 3. Consider one-time charges as 1 day service period (below)
    ELSE
        -- Last resort: assume one-time charge (same day)
        -- use 1 day as service period to include the line item in revenue recognition
        DATE_ADD(period_start_date, INTERVAL 1 DAY)
END AS period_end_date_inferred,

    -- Flag to indicate if we used fallback
    period_end_date IS NULL AS is_missing_period_end,

    CURRENT_TIMESTAMP() AS _loaded_at

FROM transformed

