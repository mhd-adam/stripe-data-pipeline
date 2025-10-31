SELECT
    (SELECT COUNT(*) FROM {{ ref('invoice_line_items') }}) as total_count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {{ ref('invoice_line_items') }}) as missing_pct
FROM {{ ref('invoice_line_items') }}
WHERE period_end_date IS NULL
HAVING missing_pct > 3.0