# Stripe Revenue Recognition Pipeline

## Architecture

### Data Flow

**GCS → Staging → Curated → Marts**

1. **GCS**: Raw Stripe data stored in Google Cloud Storage
2. **Staging**: Pulls latest data from GCS (replaced on each DAG run)
3. **Curated**: Incremental processing, builds historical data with business logic
4. **Marts**: Final fact tables for analytics and reporting

### Models

#### Staging Layer
- `stg_invoices` - Latest invoices from GCS
- `stg_subscriptions` - Latest subscriptions from GCS
- `stg_subscription_updates` - Latest subscription updates from GCS

#### Curated Layer
- `int_invoice_line_items` - Extracts and flattens line items from invoices
- `int_revenue_recognition_schedule` - Tax calculations, currency normalization, daily revenue schedule
- `calendar` - Date dimension
- `exchange_rates` - Currency conversion rates
- `invoices` - Invoice dimension

#### Marts Layer
- `fct_recognized_revenue_daily` - Daily recognized revenue
- `fct_deferred_revenue` - Deferred revenue tracking

## Revenue Recognition Logic

### Tax Handling
- Extracts tax data from JSON fields
- Separates tax-inclusive vs tax-exclusive amounts
- Calculates revenue excluding tax (tax is not revenue)

### Currency Normalization
- Converts all amounts to USD using exchange rates
- Enables cross-currency revenue aggregation

### Revenue Recognition
- Items with service periods > 31 days are recognized daily over the period
- Daily revenue = Total amount / Service period days
- Calendar join generates one row per recognition date

## Setup

### Prerequisites
- Google Cloud Platform account
- BigQuery dataset
- Stripe API key
- Python 3.8+
- DBT installed

### Configuration
1. Set up GCS buckets for Stripe data
2. Configure BigQuery external tables in `models/external_tables.yml`
3. Update Stripe API key in extraction scripts
4. Configure DBT profiles for BigQuery connection

## Running the Pipeline

### Data Extraction
```bash
python scripts/extract_stripe_data.py
```

### DBT Models
```bash
dbt run
dbt test
```

