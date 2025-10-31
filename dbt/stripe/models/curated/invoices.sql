{{ config(
    materialized="incremental",
    unique_key="invoice_id",
    incremental_strategy="merge",
    partition_by={"field": "created_at_date", "data_type": "date"},
    cluster_by=["customer_id"]
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_invoices') }}
    {% if is_incremental() %}
    WHERE created_at_date > (SELECT MAX(created_at_date) FROM {{ this }})
    {% endif %}
),

renamed AS (
    SELECT
        id AS invoice_id,
        customer AS customer_id,
        subscription AS subscription_id,

        created_at_date,
        TIMESTAMP_SECONDS(CAST(created AS INT64)) AS created_at,

        status,
        currency,

        CAST(amount_due AS FLOAT64) / 100 AS amount_due,
        CAST(amount_paid AS FLOAT64) / 100 AS amount_paid,
        CAST(amount_remaining AS FLOAT64) / 100 AS amount_remaining,
        CAST(subtotal AS FLOAT64) / 100 AS subtotal,
        CAST(total AS FLOAT64) / 100 AS total,
        CAST(tax AS FLOAT64) / 100 AS tax,

        automatic_tax,
        collection_method,

        -- period_start
        CAST(period_start AS INT64) AS period_start_timestamp,
        TIMESTAMP_SECONDS(CAST(period_start AS INT64)) AS period_start_at,
        DATE(TIMESTAMP_SECONDS(CAST(period_start AS INT64))) AS period_start_date,

        -- period_end
        CAST(period_end AS INT64) AS period_end_timestamp,
        TIMESTAMP_SECONDS(CAST(period_end AS INT64)) AS period_end_at,
        DATE(TIMESTAMP_SECONDS(CAST(period_end AS INT64))) AS period_end_date,

        metadata,

        CURRENT_TIMESTAMP() AS _loaded_at

    FROM source
)

SELECT * FROM renamed

