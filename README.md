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
- [Configuration](#configuration)
- [Usage Examples](#usage-examples)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## 🎯 Overview

This platform provides a complete financial analytics solution implementing:

- **Medallion Architecture** (Bronze → Silver → Gold layers)
- **Kimball Dimensional Modeling** (Star schema with SCD Type 2)
- **8 Analytics Types** (60 SQL models covering all analytics categories)
- **92 Interactive Visualizations** (8 Python scripts with Plotly charts)
- **13 Fact Tables** + **9 Dimension Tables**
- **Production-Ready** with complete dbt transformations

### Key Capabilities

✅ **ETL Pipeline**: dbt-powered transformation (22 staging models)  
✅ **Dimensional Model**: 9 dimensions, 13 facts, 60 analytics models  
✅ **Visual Analytics**: 8 Python scripts generating 92 interactive charts  
✅ **Star Schema**: Full Kimball methodology implementation  
✅ **Real-Time Analytics**: Live monitoring with multiple dashboards

---

## 🏗️ Architecture

### Three-Layer Medallion Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              BRONZE LAYER (Raw Data Ingestion)              │
│  PostgreSQL Schema: ingestion_raw_data                      │
│  Materialization: External Tables / Raw CSVs                │
│                                                             │
│  • products              • merchants         • customers    │
│  • accounts              • transactions                     │
│  • credit_applications   • fraud_alerts                     │
│  • customer_interactions • loan_payments                    │
│  • economic_indicators   • marketing_campaigns              │
│  • branch_locations      • atm_locations                    │
│  • account_events        • customer_segments_history        │
│  • regulatory_reports    • risk_assessments                 │
└────────────────────┬────────────────────────────────────────┘
                     │ dbt run --select tag:silver
                     ↓
┌─────────────────────────────────────────────────────────────┐
│         SILVER LAYER (Cleaned & Validated Data)             │
│  PostgreSQL Schema: silver | Materialization: Table/Inc     │
│                                                             │
│  STAGING MODELS (22 models):                                │
│  • stg_customers              • stg_transactions            │
│  • stg_accounts               • stg_products                │
│  • stg_merchants              • stg_credit_applications     │
│  • stg_fraud_alerts           • stg_customer_interactions   │
│  • stg_loan_payments          • stg_economic_indicators     │
│  • stg_marketing_campaigns    • stg_branch_locations        │
│  • stg_atm_locations          • stg_account_events          │
│  • stg_customer_segments_history                            │
│  • stg_regulatory_reports     • stg_risk_assessments        │
└────────────────────┬────────────────────────────────────────┘
                     │ dbt run --select tag:gold
                     ↓
┌─────────────────────────────────────────────────────────────┐
│           GOLD LAYER (Business-Ready Star Schema)           │
│       PostgreSQL Schema: gold | Kimball Star Schema         │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  DIMENSIONS (9 tables - SCD Type 1 & Type 2)         │   │
│  │  • dim_customer (SCD Type 2)                         │   │
│  │  • dim_product                                       │   │
│  │  • dim_merchant                                      │   │
│  │  • dim_date (2020-2030 conformed dimension)          │   │
│  │  • dim_account (SCD Type 2)                          │   │
│  │  • dim_location (branches + ATMs)                    │   │
│  │  • dim_economic_indicators                           │   │
│  │  • dim_campaign                                      │   │
│  │  • dim_agent                                         │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  FACTS (13 tables - Multiple grain types)            │   │
│  │  • fact_transactions (atomic, incremental)           │   │
│  │  • fact_account_daily_snapshot (periodic)            │   │
│  │  • fact_customer_monthly_summary (aggregated)        │   │
│  │  • fact_loan_payments                                │   │
│  │  • fact_fraud_alerts (accumulating snapshot)         │   │
│  │  • fact_credit_applications                          │   │
│  │  • fact_customer_interactions                        │   │
│  │  • fact_account_events                               │   │
│  │  • fact_customer_segment_history (SCD Type 2)        │   │
│  │  • fact_regulatory_reports                           │   │
│  │  • fact_risk_assessments                             │   │
│  │  • fact_marketing_campaigns                          │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  ANALYTICS (60 models across 8 categories)           │   │
│  │                                                       │   │
│  │  📊 01_descriptive_analytics/ (16 models)            │   │
│  │     Customer overview, transactions, accounts, etc.  │   │
│  │                                                       │   │
│  │  🔍 02_diagnostic_analytics/ (9 models)              │   │
│  │     Churn, fraud patterns, loan defaults, etc.       │   │
│  │                                                       │   │
│  │  🔬 03_exploratory_analytics/ (8 models)             │   │
│  │     Behavior clusters, time patterns, cross-sell     │   │
│  │                                                       │   │
│  │  📈 04_inferential_analytics/ (7 models)             │   │
│  │     Statistical tests, A/B tests, confidence         │   │
│  │                                                       │   │
│  │  🔮 05_predictive_analytics/ (5 models)              │   │
│  │     Churn prediction, forecasts, risk scores         │   │
│  │                                                       │   │
│  │  💡 06_prescriptive_analytics/ (5 models)            │   │
│  │     Retention actions, recommendations, optimization │   │
│  │                                                       │   │
│  │  🎯 07_causal_analytics/ (4 models)                  │   │
│  │     Impact analysis, elasticity, attribution         │   │
│  │                                                       │   │
│  │  ⚡ 08_realtime_analytics/ (6 models)                │   │
│  │     Live monitoring, fraud alerts, system health     │   │
│  └──────────────────────────────────────────────────────┘   │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────┐
│              CONSUMPTION LAYER (Visualizations)             │
│  • Python Analytics Scripts (8 files, 92 charts total)      │
│  • Interactive HTML Reports (Plotly)                        │
│  • BI Tool Integration (Tableau, Power BI, Looker)          │
│  • ML Model Features Ready                                  │
└─────────────────────────────────────────────────────────────┘
```

---

## ✨ Features

### Data Platform

- **ETL Pipeline (dbt)**

  - 22 staging transformation models
  - 9 dimension tables (SCD Type 1 & Type 2)
  - 13 fact tables (atomic, snapshot, aggregated)
  - 60 analytics models across 8 categories
  - Incremental loading for large tables
  - Full Kimball star schema implementation

- **Dimensional Model**
  - 9 dimension tables with surrogate keys
  - 13 fact tables with multiple grain types
  - SCD Type 2 for customer & account dimensions
  - Conformed date dimension (2020-2030)
  - Pre-aggregated metrics for performance

### Analytics Suite (60 Models)

- **8 Analytics Categories**

  1. **Descriptive** (16 models): KPIs, trends, distributions
  2. **Diagnostic** (9 models): Root cause, drill-downs
  3. **Exploratory** (8 models): Patterns, clusters, correlations
  4. **Inferential** (7 models): Statistical tests, confidence intervals
  5. **Predictive** (5 models): Forecasts, risk scores
  6. **Prescriptive** (5 models): Recommendations, optimization
  7. **Causal** (4 models): Impact analysis, attribution
  8. **Real-Time** (6 models): Live monitoring, alerts

- **92 Interactive Visualizations**
  - 8 Python scripts using Plotly
  - Professional HTML reports
  - Mobile-responsive design
  - Export capabilities (PNG, SVG)

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
- dbt-core
- psycopg2
- plotly
- pandas
```

### Installation

```bash
# 1. Clone repository
git clone <your-repo-url>
cd financial-analytics-platform

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure database connection
# Edit profiles.yml with your PostgreSQL credentials

# 5. Install dbt packages
dbt deps

# 6. Run transformations
dbt run                          # Run all models
dbt run --select tag:silver      # Run silver layer only
dbt run --select tag:gold        # Run gold layer only

# 7. Generate visualizations
cd visualizations
python 01_descriptive_analytics.py
python 02_diagnostic_analytics.py
# ... run other scripts

# 8. View reports
open outputs/descriptive/*.html
```

---

## 📁 Project Structure

```
financial-analytics-platform/
│
├── README.md                          # This file
├── dbt_project.yml                    # dbt configuration
├── profiles.yml                       # Database connections
│
├── models/                            # dbt models (104+ files)
│   │
│   ├── ingestion/
│   │   └── sources.yml
│   │
│   │
│   ├── transform/                        # Silver schema layer (22 models)
│   │   ├── stg_customers.sql
│   │   ├── stg_transactions.sql
│   │   ├── stg_accounts.sql
│   │   ├── stg_products.sql
│   │   ├── stg_merchants.sql
│   │   ├── stg_credit_applications.sql
│   │   ├── stg_fraud_alerts.sql
│   │   ├── stg_customer_interactions.sql
│   │   ├── stg_loan_payments.sql
│   │   ├── stg_economic_indicators.sql
│   │   ├── stg_marketing_campaigns.sql
│   │   ├── stg_branch_locations.sql
│   │   ├── stg_atm_locations.sql
│   │   ├── stg_account_events.sql
│   │   ├── stg_customer_segments_history.sql
│   │   ├── stg_regulatory_reports.sql
│   │   └── stg_risk_assessments.sql
│   │   └── schema.yml
│   │
│   └── gold/                          # Gold layer (82 models)
│       │
│       ├── dimensions/                # 9 dimension tables
│       │   ├── dim_customer.sql
│       │   ├── dim_product.sql
│       │   ├── dim_merchant.sql
│       │   ├── dim_date.sql
│       │   ├── dim_account.sql
│       │   ├── dim_location.sql
│       │   ├── dim_economic_indicators.sql
│       │   ├── dim_campaign.sql
│       │   └── dim_agent.sql
│       │
│       ├── facts/                     # 13 fact tables
│       │   ├── fact_transactions.sql
│       │   ├── fact_account_daily_snapshot.sql
│       │   ├── fact_customer_monthly_summary.sql
│       │   ├── fact_loan_payments.sql
│       │   ├── fact_fraud_alerts.sql
│       │   ├── fact_credit_applications.sql
│       │   ├── fact_customer_interactions.sql
│       │   ├── fact_account_events.sql
│       │   ├── fact_customer_segment_history.sql
│       │   ├── fact_regulatory_reports.sql
│       │   ├── fact_risk_assessments.sql
│       │   └── fact_marketing_campaigns.sql
│       │
│       └── analytics/                 # 60 analytics models
│           ├── 01_descriptive_analytics/       (16 models)
│           ├── 02_diagnostic_analytics/        (9 models)
│           ├── 03_exploratory_analytics/       (8 models)
│           ├── 04_inferential_analytics/       (7 models)
│           ├── 05_predictive_analytics/        (5 models)
│           ├── 06_prescriptive_analytics/      (5 models)
│           ├── 07_causal_analytics/            (4 models)
│           └── 08_realtime_analytics/          (6 models)
│
├── visualizations/                    # Python visualization scripts
│   ├── 01_descriptive_analytics.py       (14 charts)
│   ├── 02_diagnostic_analytics.py        (13 charts)
│   ├── 03_exploratory_analytics.py       (13 charts)
│   ├── 04_inferential_analytics.py       (10 charts)
│   ├── 05_predictive_analytics.py        (10 charts)
│   ├── 06_prescriptive_analytics.py      (10 charts)
│   ├── 07_causal_analytics.py            (8 charts)
│   └── 08_realtime_analytics.py          (11 charts)
│
└── outputs/                           # Generated HTML reports
    ├── descriptive/                   # 14 HTML files
    ├── diagnostic/                    # 13 HTML files
    ├── exploratory/                   # 13 HTML files
    ├── inferential/                   # 10 HTML files
    ├── predictive/                    # 10 HTML files
    ├── prescriptive/                  # 10 HTML files
    ├── causal/                        # 8 HTML files
    └── realtime/                      # 11 HTML files
```

**Total**:

- **104 dbt Models** (22 silver + 9 dimensions + 13 facts + 60 analytics)
- **8 Python Visualization Scripts**
- **92 Interactive Charts** (89 + 3 engagement charts)

---

## 📊 Analytics Suite

### Complete Model List

#### 01 Descriptive Analytics (16 models)

1. analytics_customer_overview
2. analytics_customer_by_segment
3. analytics_customer_by_age_group
4. analytics_customer_by_geography
5. analytics_transaction_summary
6. analytics_transaction_by_channel
7. analytics_transaction_by_category
8. analytics_daily_transaction_trend
9. analytics_monthly_transaction_trend
10. analytics_account_summary
11. analytics_account_by_product
12. analytics_account_balance_distribution
13. analytics_product_summary
14. analytics_fraud_summary
15. analytics_loan_payment_summary
16. analytics_customer_engagement_metrics ✨ NEW

#### 02 Diagnostic Analytics (9 models)

1. analytics_churn_analysis
2. analytics_fraud_pattern_analysis
3. analytics_loan_default_drivers
4. analytics_transaction_decline_analysis
5. analytics_customer_satisfaction_drivers
6. analytics_credit_application_rejection_analysis
7. analytics_account_closure_analysis
8. analytics_revenue_variance_analysis
9. analytics_marketing_campaign_performance_drivers

#### 03 Exploratory Analytics (8 models)

1. analytics_customer_behavior_clusters
2. analytics_transaction_time_patterns
3. analytics_product_cross_sell_patterns
4. analytics_merchant_spending_patterns
5. analytics_credit_score_behavior_correlation
6. analytics_geographic_clustering
7. analytics_seasonal_patterns
8. analytics_account_lifecycle_patterns

#### 04 Inferential Analytics (7 models)

1. analytics_customer_segment_statistical_comparison
2. analytics_ab_test_channel_performance
3. analytics_credit_approval_hypothesis_test
4. analytics_fraud_detection_accuracy
5. analytics_customer_lifetime_value_distribution
6. analytics_transaction_amount_normality_test
7. analytics_churn_rate_confidence_intervals

#### 05 Predictive Analytics (5 models)

1. analytics_customer_churn_prediction
2. analytics_transaction_volume_forecast
3. analytics_loan_default_prediction
4. analytics_customer_lifetime_value_prediction
5. analytics_fraud_risk_forecast

#### 06 Prescriptive Analytics (5 models)

1. analytics_customer_retention_actions
2. analytics_product_recommendation
3. analytics_credit_limit_optimization
4. analytics_marketing_budget_allocation
5. analytics_branch_staffing_optimization

#### 07 Causal Analytics (4 models)

1. analytics_credit_score_impact_analysis
2. analytics_campaign_causal_impact
3. analytics_interest_rate_behavior_impact
4. analytics_branch_proximity_impact

#### 08 Real-time Analytics (6 models)

1. analytics_realtime_transaction_monitoring
2. analytics_realtime_fraud_alerts
3. analytics_realtime_customer_activity
4. analytics_realtime_system_health
5. analytics_realtime_trending_merchants
6. analytics_realtime_account_alerts

---

## 🗄️ Data Models

### Gold Layer Summary

**Dimensions (9 tables)**:

- dim_customer (SCD Type 2)
- dim_product
- dim_merchant
- dim_date (conformed, 2020-2030)
- dim_account (SCD Type 2)
- dim_location
- dim_economic_indicators
- dim_campaign
- dim_agent

**Facts (13 tables)**:

- fact_transactions (atomic, incremental)
- fact_account_daily_snapshot (periodic snapshot)
- fact_customer_monthly_summary (aggregated)
- fact_loan_payments
- fact_fraud_alerts (accumulating snapshot)
- fact_credit_applications
- fact_customer_interactions
- fact_account_events
- fact_customer_segment_history (SCD Type 2)
- fact_regulatory_reports
- fact_risk_assessments
- fact_marketing_campaigns

**Analytics (60 models across 8 categories)**

---

## 📈 Visualizations

### Chart Distribution

| Analytics Type   | Charts | Description                             |
| ---------------- | ------ | --------------------------------------- |
| **Descriptive**  | 17     | KPIs, trends, distributions             |
| **Diagnostic**   | 13     | Root cause analysis, drill-downs        |
| **Exploratory**  | 13     | Patterns, clusters, correlations        |
| **Inferential**  | 10     | Statistical tests, confidence intervals |
| **Predictive**   | 10     | Forecasts, predictions, risk scores     |
| **Prescriptive** | 10     | Recommendations, optimizations          |
| **Causal**       | 8      | Causal effects, attribution             |
| **Real-time**    | 11     | Live monitoring, alerts                 |

**Total: 92 Interactive Charts**

### Chart Types Used

✅ KPI Cards & Indicators  
✅ Bar Charts (grouped, stacked, horizontal)  
✅ Line & Area Charts  
✅ Pie & Donut Charts  
✅ Scatter & Bubble Plots  
✅ 3D Scatter Plots  
✅ Heatmaps & Correlation Matrices  
✅ Box & Violin Plots  
✅ Waterfall Charts  
✅ Funnel Charts  
✅ Sankey Diagrams  
✅ Network Graphs  
✅ Treemaps & Sunburst  
✅ Radar Charts  
✅ Gauge Charts  
✅ Error Bars & Forest Plots  
✅ Geographic Maps (Choropleth, Bubble)

---

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
      user: postgres
      password: your_password
      port: 5432
      dbname: banking_db
      schema: public
      threads: 4
```

### Environment Setup

```bash
# Set connection string for visualizations
export CONNECTION_STRING="postgresql://user:password@localhost:5432/banking_db"
```

---

## 💡 Usage Examples

### Running dbt Models

```bash
# Run all models
dbt run

# Run specific layer
dbt run --select tag:silver
dbt run --select tag:gold

# Run specific analytics category
dbt run --select gold.analytics.01_descriptive_analytics.*

# Run with full refresh
dbt run --full-refresh --select fact_transactions

# Run tests
dbt test
```

### Generating Visualizations

```bash
# Navigate to visualizations folder
cd visualizations

# Set database connection
export CONNECTION_STRING="postgresql://postgres:password@localhost:5432/banking_db"

# Run individual analytics
python 01_descriptive_analytics.py
python 02_diagnostic_analytics.py
python 03_exploratory_analytics.py
python 04_inferential_analytics.py
python 05_predictive_analytics.py
python 06_prescriptive_analytics.py
python 07_causal_analytics.py
python 08_realtime_analytics.py

# View generated reports
open outputs/descriptive/*.html
```

### Querying Analytics

```sql
-- Get customer churn predictions
SELECT
    customer_natural_key,
    predicted_churn_risk_pct,
    clv_at_risk,
    days_since_login,
    transaction_count_90d
FROM analytics_customer_churn_prediction
WHERE predicted_churn_risk_pct > 70
ORDER BY clv_at_risk DESC
LIMIT 100;

-- Monthly transaction trends
SELECT * FROM analytics_monthly_transaction_trend
ORDER BY year DESC, month DESC;

-- Real-time fraud alerts
SELECT * FROM analytics_realtime_fraud_alerts
ORDER BY transaction_date DESC
LIMIT 50;
```

---

## 🐛 Troubleshooting

### Common Issues

**Issue: dbt connection failed**

```bash
# Test connection
dbt debug

# Check profiles.yml
cat ~/.dbt/profiles.yml
```

**Issue: Visualization script fails**

```bash
# Check database connection
python -c "import psycopg2; print('OK')"

# Verify data exists
psql -d banking_db -c "SELECT COUNT(*) FROM fact_transactions;"
```

**Issue: Missing analytics tables**

```bash
# Run gold layer models
dbt run --select tag:gold

# Check created tables
psql -d banking_db -c "\dt gold.*"
```

---

## 📝 License

MIT License - See LICENSE file for details

---

## 📧 Contact

**Email**: ikigamidevs@gmail.com

---

**Built with ❤️ using dbt, PostgreSQL, Python & Plotly**
