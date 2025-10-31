# Stripe Revenue Recognition Pipeline

This is a demo pipeline for Stripe revenue recognition. It is built on top of the following tools:

- Stripe API
- Google Cloud Storage
- Google BigQuery
- Airflow
- DBT


Please note that this is **not** a complete solution but rather a demo of what the pipeline could look like.
It is kept as simple as possible to focus on the core requirements in the technical document.

## Architecture

### Data Flow

![img.png](data_flow_diagram.png)

**GCS → Staging → Curated → Marts**

1. **GCS**: Raw Stripe data stored in Google Cloud Storage (replaced on each DAG run)
2. **Staging**: Pulls latest data from GCS builds historical data
3. **Curated**: Incremental processing with business logic
4. **Marts**: Final fact tables for analytics and reporting

Each DAG run will extract the data from Stripe and load it to GCS. The write mode is overwrite on GCS so DBT can read the latest data into the staging layer. 
The staging layer will keep the historical data.

Another approach to this is partition the data in GCS and read the latest partition in the staging layer. 
The benefit of this approach is we can archive historical data in GCS to reduce cost. 
However, this requires more complex logic to read the latest partition in the staging layer.

### Models


#### Staging Layer
- `stg_invoices` - Latest invoices from GCS
- `stg_subscriptions` - Latest subscriptions from GCS
- `stg_subscription_updates` - Latest subscription updates from GCS

#### Curated Layer
- `invoice_line_items` - Extracts and flattens line items from invoices
- `revenue_recognition_schedule` - Tax calculations, currency normalization, daily revenue schedule
- `calendar` - Date dimension
- `exchange_rates` - Currency conversion rates
- `invoices` - Invoice dimension

#### Marts Layer
- `fct_recognized_revenue_daily` - Daily recognized revenue
- `fct_deferred_revenue` - Deferred revenue tracking

## Revenue Recognition Logic

To recognize revenue, we need to understand the service period of each line item. 
This allows us to calculate the daily revenue recognition schedule. 

The following digram illustrates the logic for revenue recognition.

For a service item **SI** with amount **M** spanning a service period of **S** to **E**, with (**E** - **S**) representing the total number of days:
- **Service Start (S)** to **Service End (E)** defines the total service period
- **Before Service Start**: Deferred Revenue = M, Recognized Revenue = 0
- **During Service Period**:
  - Recognized Revenue = X * M / (E - S), where X is number of days recognized
  - Deferred Revenue = Y * M / (E - S), where Y is remaining days of service
- **After Service End**: Deferred Revenue = 0, Recognized Revenue = M

Revenue is recognized daily over the service period, with deferred revenue decreasing proportionally as time progresses.

![img.png](revenue_recognition.png)

### Tax Handling
- Extracts tax data from JSON fields
- Separates tax-inclusive vs tax-exclusive amounts
- Calculates revenue excluding tax

### Currency Normalization
- Converts all amounts to USD using exchange rates
- Enables cross-currency revenue aggregation

## Example Queries

### Total Deferred Revenue as of Today
```sql
SELECT
    SUM(deferred_revenue_usd) AS total_deferred_revenue_usd
FROM deferred_revenue
WHERE as_of_date = CURRENT_DATE()
```

### Deferred Revenue by Customer
```sql
SELECT
    customer_id,
    SUM(deferred_revenue_usd) AS total_deferred_revenue_usd
FROM deferred_revenue
WHERE as_of_date = CURRENT_DATE()
GROUP BY customer_id
ORDER BY total_deferred_revenue_usd DESC
```

### Deferred Revenue Trend Over Time
```sql
SELECT
    as_of_date,
    SUM(deferred_revenue_usd) AS total_deferred_revenue_usd
FROM deferred_revenue
GROUP BY as_of_date
ORDER BY as_of_date
```

### Recognized Revenue for Q2 2025
```sql
SELECT
    SUM(f.recognized_revenue_usd) AS total_recognized_revenue_usd
FROM fct_recognized_revenue_daily f
JOIN calendar c ON f.recognition_date = c.date_day
WHERE c.year = 2025
  AND c.quarter_of_year = '2'
```
