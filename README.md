# Financial Analytics Platform

## Enterprise-Grade Data Analytics with Medallion Architecture & Kimball Dimensional Modeling

> A production-ready, end-to-end financial analytics platform implementing modern data engineering best practices with comprehensive visualization capabilities.

[![Status](https://img.shields.io/badge/status-production_ready-brightgreen)]()
[![Python](https://img.shields.io/badge/python-3.10+-blue)]()
[![dbt](https://img.shields.io/badge/dbt-1.7+-orange)]()
[![License](https://img.shields.io/badge/license-MIT-green)]()

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Analytics Suite](#analytics-suite)
- [Data Models](#data-models)
- [Visualizations](#visualizations)
- [Deployment](#deployment)
- [Configuration](#configuration)
- [Usage Examples](#usage-examples)
- [Contributing](#contributing)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## 🎯 Overview

This platform provides a complete financial analytics solution from data generation to interactive visualizations, implementing:

- **Medallion Architecture** (Bronze/Silver/Gold layers)
- **Kimball Dimensional Modeling** (Star schema with SCD Type 2)
- **8 Analytics Types** (Descriptive, Diagnostic, Exploratory, Inferential, Predictive, Prescriptive, Causal, Real-Time)
- **Interactive Visualizations** (28+ Plotly charts with HTML reports)
- **90+ ML Features** for predictive modeling
- **Production-Ready** deployment scripts and automation

### Key Capabilities

✅ **Data Generation**: Synthetic financial data generator (1000 customers, 50K+ transactions)  
✅ **ETL Pipeline**: dbt-powered transformation (30+ models)  
✅ **Dimensional Model**: 4 dimensions, 3 facts, 8 analytics views  
✅ **Visual Analytics**: 8 Python scripts generating interactive HTML reports  
✅ **ML Ready**: Feature store with 90+ pre-engineered features  
✅ **Real-Time**: Live monitoring dashboard with auto-refresh

---

## 🏗️ Architecture

### Three-Layer Medallion Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA GENERATION                          │
│              Python Synthetic Data Generator                │
│         (11 tables: customers, transactions, etc.)          │
└────────────────────┬────────────────────────────────────────┘
                     │ CSV Files
                     ↓
┌─────────────────────────────────────────────────────────────┐
│              Ingestion LAYER (Raw Data)                     │
│  PostgreSQL Schema: bronze | Materialization: Table         │
│  • products       • merchants      • customers              │
│  • accounts       • transactions   • credit_applications    │
│  • fraud_alerts   • interactions   • economic_indicators    │
│  • campaigns      • loan_payments                           │
└────────────────────┬────────────────────────────────────────┘
                     │ dbt run --select silver.*
                     ↓
┌─────────────────────────────────────────────────────────────┐
│          Transform LAYER (Cleaned & Validated)              │
│  PostgreSQL Schema: silver | Materialization: Table/Inc     │
│  • stg_customers         • stg_transactions                 │
│  • stg_accounts          • stg_credit_applications          │
│  • stg_fraud_alerts      • stg_customer_interactions        │
│  • stg_loan_payments     • stg_economic_indicators          │
│  • stg_marketing_campaigns                                  │
└────────────────────┬────────────────────────────────────────┘
                     │ dbt run --select gold.*
                     ↓
┌─────────────────────────────────────────────────────────────┐
│                Serving LAYER (Business Ready)               │
│       PostgreSQL Schema: gold | Kimball Star Schema         │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  DIMENSIONS (SCD Type 2)                             │   │
│  │  • dim_customer   • dim_product                      │   │
│  │  • dim_merchant   • dim_date (2020-2030)             │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  FACTS (Transactional & Snapshots)                   │   │
│  │  • fact_transactions (incremental)                   │   │
│  │  • fact_account_daily_snapshot                       │   │
│  │  • fact_customer_monthly (aggregates)                │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  ANALYTICS VIEWS (8 Types)                           │   │
│  │  • descriptive_analytics    • diagnostic_analytics   │   │
│  │  • exploratory_analytics    • inferential_analytics  │   │
│  │  • predictive_analytics     • prescriptive_analytics │   │
│  │  • causal_analytics         • realtime_analytics     │   │
│  └──────────────────────────────────────────────────────┘   │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────┐
│              CONSUMPTION LAYER                              │
│  • Python Analytics Scripts (8 scripts, 28 charts)          │
│  • Interactive HTML Reports (Plotly visualizations)         │
│  • BI Dashboards (Tableau, Power BI, Looker)                │
│  • ML Models (Predictive features ready)                    │
│  • APIs & Applications                                      │
└─────────────────────────────────────────────────────────────┘
```

---

## ✨ Features

### Data Platform

- **Synthetic Data Generation**

  - 1,000 customers with realistic demographics
  - 50,000+ transactions across multiple channels
  - 11 interconnected tables with referential integrity
  - Configurable parameters for volume and characteristics

- **ETL Pipeline (dbt)**

  - 30+ transformation models
  - Incremental loading for large tables
  - Data quality tests (50+ automated checks)
  - Documentation generation
  - Lineage tracking

- **Dimensional Model**
  - 4 dimension tables (customer, product, merchant, date)
  - 3 fact tables (transactions, account snapshots, monthly aggregates)
  - SCD Type 2 for customer dimension
  - Pre-aggregated metrics for performance

### Analytics Suite

- **8 Analytics Types Implemented**

  - Descriptive: What happened? (KPIs, trends, summaries)
  - Diagnostic: Why did it happen? (Root cause analysis)
  - Exploratory: What patterns exist? (EDA, clustering)
  - Inferential: Can we generalize? (Statistical tests)
  - Predictive: What will happen? (ML features, risk scores)
  - Prescriptive: What should we do? (Recommendations)
  - Causal: What causes what? (Attribution, impact)
  - Real-Time: What's happening now? (Live monitoring)

- **Visual Analytics**
  - 8 Python scripts with Plotly charts
  - 28+ interactive visualizations
  - Professional HTML reports
  - Export capabilities (PNG, SVG)
  - Mobile-responsive design

### Machine Learning Ready

- **ML Features** for:
  - Churn prediction
  - Fraud detection
  - Credit risk scoring
  - Customer lifetime value prediction
  - Next best offer recommendations

---

## 🚀 Quick Start

### Prerequisites

```bash
# System Requirements
- Python 3.8+
- PostgreSQL 12+
- 4GB RAM minimum
- 10GB disk space

# Software
- Git
- pip
- psql (PostgreSQL client)
```

### Installation (5 Minutes)

```bash
# 1. Clone repository
git clone <your-repo-url>
cd financial-analytics-platform

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Install dbt packages
dbt deps

# 5. Setup environment variables
cat > .env <<EOF
DB_HOST=localhost
DB_USER=postgres
DB_PASSWORD=your_password
DB_NAME=financial_analytics
EOF

# 6. Create database and schemas
psql -U postgres -c "CREATE DATABASE financial_analytics;"
psql -U postgres -d financial_analytics -f setup_postgres.sql

# 7. Generate synthetic data
python generate_financial_data.py

# 8. Run dbt transformations
dbt run

```

### Verify Installation

```bash
# Check data in Gold layer
psql -d financial_analytics -c "SELECT COUNT(*) FROM gold.fact_transactions;"

# View generated reports
ls -lh reports/

```

---

## 📁 Project Structure

```
financial-analytics-platform/
│
├── README.md                          # This file
├── requirements.txt                   # Python dependencies
├── .env                               # Environment variables
├── .gitignore                         # Git ignore rules
├── Makefile                           # Common commands
│
├── dbt_project.yml                    # dbt configuration
├── profiles.yml                       # Database connections
├── packages.yml                       # dbt packages
│
├── generate_financial_data.py         # ✅ Enhanced data generator
├── setup_postgres.sql                 # ✅ Database setup script
│
├── models/                            # dbt models
│   ├── ingestion/
│   │   └── sources.yml                # ✅ Source definitions
│   │
│   ├── transform/                        # ✅ Staging models (9 files)
│   │   ├── stg_customers.sql
│   │   ├── stg_transactions.sql
│   │   ├── stg_accounts.sql
│   │   ├── stg_credit_applications.sql
│   │   ├── stg_fraud_alerts.sql
│   │   ├── stg_customer_interactions.sql
│   │   ├── stg_loan_payments.sql
│   │   ├── stg_economic_indicators.sql
│   │   └── stg_marketing_campaigns.sql
│   │
│   ├── serving/                          # Gold layer models
│   │   ├── dimensions/                # ✅ Dimension tables (4 files)
│   │   │   ├── dim_customer.sql
│   │   │   ├── dim_date.sql
│   │   │   ├── dim_product.sql
│   │   │   └── dim_merchant.sql
│   │   │
│   │   ├── facts/                     # ✅ Fact tables (3 files)
│   │   │   ├── fact_transactions.sql
│   │   │   ├── fact_account_daily_snapshot.sql
│   │   │   └── fact_customer_monthly.sql
│   │   │
│   │   └── analytics/                 # ✅ Analytics views (8 files)
│   │       ├── descriptive_analytics.sql
│   │       ├── diagnostic_analytics.sql
│   │       ├── exploratory_analytics.sql
│   │       ├── inferential_analytics.sql
│   │       ├── predictive_analytics.sql
│   │       ├── prescriptive_analytics.sql
│   │       ├── causal_analytics.sql
│   │       └── realtime_analytics.sql
│   │
│   └── schema.yml                     # ✅ Model tests & documentation
│
├── macros/
│   └── custom_macros.sql              # ✅ Custom dbt macros
│
├── tests/                             # Custom dbt tests
│   └── custom/
│
├── analyses/                          # Ad-hoc SQL analyses
│
├── seeds/                             # Static reference data
│
├── scripts/                           # ✅ Utility scripts
│   ├── generate.py                    # Initial setup
│
├── analytics_viz/                     # ✅ Visual analytics (8 scripts)
│   ├── 01_descriptive_analytics.py
│   ├── 02_diagnostic_analytics.py
│   ├── 03_exploratory_analytics.py
│   ├── 04_inferential_analytics.py
│   ├── 05_predictive_analytics.py
│   ├── 06_prescriptive_analytics.py
│   ├── 07_causal_analytics.py
│   ├── 08_realtime_analytics.py                # Master runner
│
│
├── reports/                           # 📊 Generated HTML reports
│   └── *.html                         # Interactive visualizations
│
│
```

**Total Files**: 50+ production files  
**Lines of Code**: 5,000+ (Python + SQL)  
**Data Models**: 30+ dbt models  
**Visualizations**: 28 interactive charts

---

## 📊 Analytics Suite

### 1. Descriptive Analytics

**Question**: What happened?

**Outputs**:

- Key performance indicators (KPIs)
- Transaction volume trends
- Customer demographics
- Product performance
- Channel distribution
- Top merchants
- Account status

**Visualizations** (9 charts):

- KPI Dashboard
- Monthly trends
- Channel pie/bar charts
- Customer segmentation
- Product treemap
- Merchant rankings

**Run**: `python analytics_viz/01_descriptive_analytics.py`

---

### 2. Diagnostic Analytics

**Question**: Why did it happen?

**Outputs**:

- Fraud pattern analysis
- Churn root causes
- Transaction decline reasons
- Delinquency factors
- Customer service issues

**Visualizations** (5 charts):

- Fraud by channel
- Fraud heatmap by hour
- Churn by tenure
- Decline reasons
- Service issues

**Run**: `python analytics_viz/02_diagnostic_analytics.py`

---

### 3. Exploratory Analytics (EDA)

**Question**: What patterns exist?

**Outputs**:

- Spending behavior patterns
- Customer clustering
- Feature correlations
- Geographic patterns
- Outlier detection

**Visualizations** (3 charts):

- Spending treemap
- Customer cluster scatter
- Correlation heatmap

**Run**: `python analytics_viz/03_exploratory_analytics.py`

---

### 4. Inferential Analytics

**Question**: Can we generalize?

**Outputs**:

- Statistical significance tests
- Confidence intervals
- Cohort analysis
- A/B test results

**Visualizations** (2 charts):

- 95% confidence intervals
- Cohort retention heatmap

**Run**: `python analytics_viz/04_inferential_analytics.py`

---

### 5. Predictive Analytics

**Question**: What will happen?

**Outputs**:

- Churn risk scores
- Fraud predictions
- Credit risk assessment
- Customer lifetime value
- 90+ ML features

**Visualizations** (4 charts):

- Churn risk distribution
- Risk scatter plot
- Risk categories
- Feature importance

**Run**: `python analytics_viz/05_predictive_analytics.py`

---

### 6. Prescriptive Analytics

**Question**: What should we do?

**Outputs**:

- Next best offer recommendations
- Retention action plans
- Intervention budgets
- Expected ROI

**Visualizations** (3 charts):

- Next best offer matrix
- Retention waterfall
- Budget allocation

**Run**: `python analytics_viz/06_prescriptive_analytics.py`

---

### 7. Causal Analytics

**Question**: What causes what?

**Outputs**:

- Marketing attribution
- Economic impact analysis
- Product impact on CLV

**Visualizations** (2 charts):

- Campaign attribution
- Economic impact trends

**Run**: `python analytics_viz/07_causal_analytics.py`

---

### 8. Real-Time Analytics

**Question**: What's happening now?

**Outputs**:

- Live transaction monitoring
- System health indicators
- Active fraud alerts
- Performance metrics

**Visualizations** (1 dashboard):

- Real-time dashboard (auto-refresh)

**Run**: `python analytics_viz/08_realtime_analytics.py`

---

## 🗄️ Data Models

### Bronze Layer (11 Tables)

Raw data loaded from CSV files

```sql
bronze.products              -- Product catalog
bronze.merchants             -- Merchant master
bronze.customers             -- Customer demographics
bronze.accounts              -- Account information
bronze.transactions          -- Transaction details (partitioned)
bronze.credit_applications   -- Credit history
bronze.fraud_alerts          -- Fraud detection
bronze.customer_interactions -- Service interactions
bronze.economic_indicators   -- Macro indicators
bronze.marketing_campaigns   -- Campaign data
bronze.loan_payments         -- Payment history
```

### Silver Layer (9 Models)

Cleaned and validated data

```sql
silver.stg_customers              -- Enhanced customer data
silver.stg_transactions           -- Validated transactions
silver.stg_accounts               -- Account metrics
silver.stg_credit_applications    -- Credit staging
silver.stg_fraud_alerts           -- Fraud staging
silver.stg_customer_interactions  -- Interaction staging
silver.stg_loan_payments          -- Payment staging
silver.stg_economic_indicators    -- Economic staging
silver.stg_marketing_campaigns    -- Campaign staging
```

### Gold Layer - Dimensions (4 Tables)

```sql
gold.dim_customer    -- SCD Type 2, 30+ attributes
gold.dim_date        -- 2020-2030, 25+ attributes
gold.dim_product     -- Product hierarchy
gold.dim_merchant    -- Geographic & risk attributes
```

### Gold Layer - Facts (3 Tables)

```sql
gold.fact_transactions            -- Transaction details (40+ measures)
gold.fact_account_daily_snapshot  -- Daily account balances
gold.fact_customer_monthly        -- Monthly aggregates (30+ metrics)
```

### Gold Layer - Analytics (8 Views)

```sql
gold.descriptive_analytics   -- KPIs & summaries
gold.diagnostic_analytics    -- Root cause analysis
gold.exploratory_analytics   -- Pattern discovery
gold.inferential_analytics   -- Statistical tests
gold.predictive_analytics    -- ML features (90+)
gold.prescriptive_analytics  -- Recommendations
gold.causal_analytics        -- Attribution
gold.realtime_analytics      -- Live monitoring
```

---

## 📈 Visualizations

### Chart Types

| Type              | Count | Use Cases                  |
| ----------------- | ----- | -------------------------- |
| **Bar Charts**    | 8     | Comparisons, rankings      |
| **Line Charts**   | 4     | Trends over time           |
| **Pie Charts**    | 3     | Proportions, distributions |
| **Scatter Plots** | 3     | Correlations, clusters     |
| **Heatmaps**      | 3     | Patterns, cohorts          |
| **Treemaps**      | 2     | Hierarchical data          |
| **Histograms**    | 2     | Distributions              |
| **Indicators**    | 2     | KPIs, metrics              |
| **Waterfall**     | 1     | Sequential impact          |

**Total**: 28 interactive charts

### Report Features

✅ **Interactive**: Zoom, pan, hover for details  
✅ **Responsive**: Works on desktop/tablet/mobile  
✅ **Exportable**: Save charts as PNG/SVG  
✅ **Professional**: Clean, modern design  
✅ **Self-contained**: Single HTML file per report

### Sample Output

```bash
$ python analytics_viz/01_descriptive_analytics.py

Fetching data from Gold layer...
Creating visualizations...
✓ Report generated successfully!
📄 Saved to: reports/01_descriptive_analytics_20250101_120530.html
🌐 Open in browser to view interactive charts
```

---

## 🚀 Deployment

### Production Deployment

```bash

# With validation
dbt run --target prod --full-refresh
dbt test --target prod
dbt docs generate --target prod
```

## ⚙️ Configuration

### Database Configuration

**File**: `profiles.yml`

```yaml
financial_analytics:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: "{{ env_var('DB_USER') }}"
      password: "{{ env_var('DB_PASSWORD') }}"
      port: 5432
      dbname: financial_analytics
      schema: public
      threads: 4

    prod:
      type: postgres
      host: "{{ env_var('PROD_DB_HOST') }}"
      user: "{{ env_var('PROD_DB_USER') }}"
      password: "{{ env_var('PROD_DB_PASSWORD') }}"
      port: 5432
      dbname: financial_analytics_prod
      schema: public
      threads: 8
```

### Environment Variables

**File**: `.env`

```bash
# Database
DB_HOST=localhost
DB_USER=postgres
DB_PASSWORD=your_password
DB_NAME=financial_analytics
DB_PORT=5432

# Production
PROD_DB_HOST=prod-server.example.com
PROD_DB_USER=prod_user
PROD_DB_PASSWORD=prod_password

# Analytics
REPORT_OUTPUT_DIR=reports
ENABLE_EMAIL_REPORTS=false
EMAIL_RECIPIENTS=team@company.com
```

### Data Generation Configuration

**File**: `generate_financial_data.py`

```python
generator = EnhancedFinancialDataGenerator(
    start_date='2023-01-01',      # Start date for data
    num_customers=1000             # Number of customers
)

data = generator.generate_all(
    num_transactions=50000         # Number of transactions
)
```

### Querying Data

```sql
-- Get high-risk churn customers
SELECT
    customer_id,
    churn_risk_score,
    customer_lifetime_value,
    total_transactions_90d
FROM gold.predictive_analytics
WHERE churn_risk_score > 0.7
ORDER BY customer_lifetime_value DESC;

-- Fraud analysis by merchant
SELECT
    m.merchant_name,
    m.category,
    COUNT(*) as fraud_count,
    SUM(f.transaction_amount_abs) as fraud_amount
FROM gold.fact_transactions f
JOIN gold.dim_merchant m ON f.merchant_key = m.merchant_key
WHERE f.is_fraud = TRUE
  AND f.transaction_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY m.merchant_name, m.category
ORDER BY fraud_count DESC;

-- Monthly KPIs
SELECT
    TO_CHAR(transaction_date, 'YYYY-MM') as month,
    COUNT(*) as transactions,
    COUNT(DISTINCT customer_key) as active_customers,
    SUM(transaction_amount_abs) as volume,
    AVG(transaction_amount_abs) as avg_transaction
FROM gold.fact_transactions
GROUP BY TO_CHAR(transaction_date, 'YYYY-MM')
ORDER BY month DESC;
```

### dbt Commands

```bash
# Run all models
dbt run

# Run specific layer
dbt run --select silver.*
dbt run --select gold.dimensions.*
dbt run --select gold.facts.*

# Run specific model
dbt run --select fact_transactions

# Run with full refresh
dbt run --full-refresh

# Run modified models only
dbt run --select state:modified+

# Run tests
dbt test
dbt test --select silver.*

# Generate documentation
dbt docs generate
dbt docs serve

# Compile without running
dbt compile

# Debug connection
dbt debug
```

---

## 🤝 Contributing

### Development Workflow

1. **Create Feature Branch**

```bash
git checkout -b feature/your-feature-name
```

2. **Make Changes**

```bash
# Edit files
# Add tests
# Update documentation
```

3. **Test Locally**

```bash
dbt run
dbt test
python analytics_viz/run_all_viz.py
```

4. **Commit & Push**

```bash
git add .
git commit -m "feat: your feature description"
git push origin feature/your-feature-name
```

5. **Create Pull Request**

- Describe changes
- Link related issues
- Request review

### Code Style

- **Python**: Follow PEP 8
- **SQL**: Lowercase keywords, 4-space indent
- **dbt**: Use Jinja formatting
- **Documentation**: Update README for new features

### Adding New Analytics

1. Create SQL view in `models/gold/analytics/`
2. Add Python visualization script in `analytics_viz/`
3. Update `run_all_viz.py`
4. Add documentation
5. Create tests

---

## 🐛 Troubleshooting

### Common Issues

#### Issue: Database Connection Failed

```bash
# Solution 1: Check PostgreSQL is running
sudo systemctl status postgresql

# Solution 2: Verify credentials
psql -h localhost -U postgres -d financial_analytics -c "SELECT 1;"

# Solution 3: Check .env file
cat .env | grep DB_
```

#### Issue: dbt Models Failing

```bash
# Solution 1: Check compiled SQL
dbt compile
cat target/compiled/financial_analytics/models/...

# Solution 2: Run with debug logging
dbt run --debug --select failing_model

# Solution 3: Check for missing dependencies
dbt deps
```

#### Issue: Visualizations Not Generating

```bash
# Solution 1: Check Python dependencies
pip list | grep plotly

# Solution 2: Verify database connection
python -c "import psycopg2; conn = psycopg2.connect('dbname=financial_analytics user=postgres'); print('OK')"

# Solution 3: Check Gold layer data
psql -d financial_analytics -c "SELECT COUNT(*) FROM gold.fact_transactions;"
```

#### Issue: Out of Memory

```python
# Solution: Limit data in queries
query = """
SELECT * FROM gold.fact_transactions
LIMIT 10000  -- Add LIMIT
"""
```

### Getting Help

- **Documentation**: Check `docs/` directory
- **Email**: ikigamidevs@gmail.com

---

## 📚 Additional Resources

### Documentation

- [Quick Start Guide](docs/QUICKSTART.md) - 5-minute setup
- [Complete Project Guide](docs/COMPLETE_PROJECT_GUIDE.md) - Detailed documentation
- [Project Summary](docs/PROJECT_SUMMARY.md) - Executive overview
- [Analytics Summary](docs/ANALYTICS_SUMMARY.md) - Analytics details

### External Resources

- **dbt**: https://docs.getdbt.com/
- **Plotly**: https://plotly.com/python/
- **Kimball Method**: https://www.kimballgroup.com/
- **Medallion Architecture**: https://databricks.com/glossary/medallion-architecture

---
