{{ config(
    materialized="incremental",
    unique_key="id",
    incremental_strategy="merge",
    partition_by={"field": "created_at_date", "data_type": "date"}
) }}

WITH
base AS (
  SELECT
    s.*,
    DATE(TIMESTAMP_SECONDS(CAST(s.created AS INT64))) AS created_at_date
  FROM `stripe_data.invoices_staging` AS s
)

SELECT
  *
FROM base