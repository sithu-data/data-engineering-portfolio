# 🛒 Brazilian E-Commerce Analytics Platform
### End-to-End Data Engineering & Analytics Engineering on Olist Dataset

![Status](https://img.shields.io/badge/Status-In%20Progress-yellow)
![Stack](https://img.shields.io/badge/Stack-Python%20%7C%20Snowflake%20%7C%20dbt%20Core%20%7C%20Power%20BI-blue)
![Architecture](https://img.shields.io/badge/Architecture-Medallion%20%28RAW%20→%20BRONZE%20→%20SILVER%20→%20GOLD%29-green)

---

## 📌 Project Overview

This project demonstrates a production-style data platform built on the [Brazilian Olist E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (~1.5M rows across 9 source tables).

It covers the **full analytics stack** — from raw data ingestion through to business-ready dashboards — following industry best practices in both **data engineering** (pipeline reliability, layered architecture, environment separation) and **analytics engineering** (modular dbt modeling, business logic encapsulation, semantic layer design).

---

## 🏗️ Architecture

```
Kaggle CSV Files
      │
      ▼
Python Ingestion Script
(pandas → Snowflake bulk load)
      │
      ▼
ECOMMERCE_RAW.RAW_SCHEMA          ← Raw source tables, untouched
      │
      ▼
ECOMMERCE_ANALYTICS.BRONZE        ← Typed, renamed, no business logic
      │
      ▼
ECOMMERCE_ANALYTICS.SILVER        ← Enriched, joined, business logic applied
      │
      ▼
ECOMMERCE_ANALYTICS.GOLD          ← Aggregated, business-facing semantic layer
      │
      ▼
Power BI                          ← Self-serve analytics & KPI reporting
```

---

## 🔧 Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Python 3.11, pandas, Snowflake Connector |
| Data Warehouse | Snowflake (multi-database, multi-role) |
| Transformation | dbt Core |
| Version Control | GitHub |
| Development | VS Code |

---

## ❄️ Snowflake Environment Design

A key engineering decision in this project was separating **raw storage** from **analytics workloads** using two databases, two warehouses, and two roles — mirroring how production data platforms are structured in enterprise environments.

```
Databases:
  ECOMMERCE_RAW          → stores unmodified source data
  ECOMMERCE_ANALYTICS    → stores all dbt-transformed layers

Schemas:
  RAW_SCHEMA             → raw Olist CSV loads
  BRONZE                 → dbt staging models
  SILVER                 → dbt intermediate/enriched models
  GOLD                   → dbt mart/semantic models
  DBT_ARTIFACTS          → dbt metadata and run artifacts

Warehouses:
  DBT_WH                 → used by dbt transformations (DBT_ROLE)
  REPORTER_WH            → used by BI tools (REPORTER_ROLE)

Roles:
  DBT_ROLE               → full write access for dbt transformations
  REPORTER_ROLE          → read-only access to GOLD schema
```

---

## 🐍 Phase 1 — Data Ingestion

**Script:** `ingestion/load_olist_to_snowflake.py`

- Loads all 9 Olist CSV source tables (~1.5M rows total) into `ECOMMERCE_RAW.RAW_SCHEMA`
- Uses `pandas` for type-safe loading and Snowflake bulk copy for performance
- Idempotent design: truncates and reloads on each run (suitable for full-refresh sources)

**Source tables loaded:**

| Table | Rows (approx.) |
|---|---|
| olist_orders | 99,441 |
| olist_order_items | 112,650 |
| olist_products | 32,951 |
| olist_customers | 99,441 |
| olist_sellers | 3,095 |
| olist_order_payments | 103,886 |
| olist_order_reviews | 99,224 |
| olist_geolocation | 1,000,163 |
| product_category_name_translation | 71 |

---

## 🥉 Phase 2 — Bronze Layer (dbt Staging)

**Models:** `models/bronze/`

Bronze is a **clean, typed mirror** of the raw source. No business logic lives here — only:
- Column renaming to `snake_case` standards
- Explicit data type casting
- Removal of source-system artefacts

All 8 Bronze staging models pass **15 dbt data tests** (not_null, unique, accepted_values, relationships).

**Key design decision:** Business logic (date calculations, CASE WHEN enrichment, status derivations) is deliberately excluded from Bronze. This makes Bronze models reusable and stable regardless of downstream business rule changes.

---

## 🥈 Phase 3 — Silver Layer (dbt Intermediate) ✅

**Models:** `models/silver/`

Silver applies **business logic and enrichment** to produce analysis-ready datasets that feed multiple Gold marts.

| Model | Description |
|---|---|
| `int_orders_enriched.sql` | Enriches raw orders with `delivery_days` (datediff), `delivered_on_time` flag, and `order_status_rank` for funnel analysis |
| `int_customer_orders.sql` | One row per order joining orders, customers, items, payments, and reviews — reviews deduped to most recent per order via `QUALIFY ROW_NUMBER()` |
| `int_product_revenue.sql` | Product-level revenue for delivered orders only, with English category name resolution |

**Silver design principles:**
- Models are designed for reuse across multiple Gold marts
- Reviews deduplication handled here to keep downstream marts clean

---

## 🥇 Phase 4 — Gold Layer (dbt Marts) ✅

**Models:** `models/gold/` — organized by business domain

**`finance/`**
| Model | Description |
|---|---|
| `mart_revenue_trends.sql` | Daily revenue, order volume, AOV, on-time rate, and avg review score for delivered orders — primary time series dashboard source |
| `mart_product_category.sql` | Monthly performance by product category: revenue, order volume, pricing, freight, and review scores with revenue share % |

**`marketing/`**
| Model | Description |
|---|---|
| `mart_customer_segments.sql` | RFM-based segmentation (Champions, Loyal, At Risk, Lost, etc.) — one row per customer with recency, frequency, monetary scores |
| `mart_cohort_retention.sql` | Monthly cohort retention analysis showing what % of customers from each acquisition cohort return in subsequent months |

**`operations/`**
| Model | Description |
|---|---|
| `mart_delivery_operations.sql` | Monthly delivery ops by customer state: on-time rate, avg/median/P95 delivery days, and impact of late delivery on review scores |
| `mart_seller_performance.sql` | Seller-level metrics: revenue, order volume, on-time rate, avg review score, first and last sale dates |

**`ml/`**
| Model | Description |
|---|---|
| `mart_churn_features.sql` | ML-ready feature table for churn prediction with behavioural features and binary `is_churned` label (180-day threshold) |

---

## 📊 Phase 5 — Dashboard *(In Progress)*

Power BI report connecting to `GOLD` schema via `REPORTER_ROLE` / `REPORTER_WH`.

KPIs powered by Gold marts:
- 📦 Daily/monthly order volume, GMV, and AOV trends (`mart_revenue_trends`)
- 🚚 Delivery performance by state — on-time rate, median & P95 delivery days (`mart_delivery_operations`)
- 🏷️ Product category revenue share and review score breakdown (`mart_product_category`)
- 👤 RFM customer segments — Champions, At Risk, Lost, etc. (`mart_customer_segments`)
- 🔁 Cohort retention curves by acquisition month (`mart_cohort_retention`)
- 🏪 Seller performance leaderboard — revenue, on-time rate, review score (`mart_seller_performance`)

---

## 🧪 Testing Strategy

| Test Type | Layer | Tool |
|---|---|---|
| Not null, unique | Bronze | dbt tests |
| Referential integrity | Bronze | dbt relationship tests |
| Accepted values | Bronze | dbt accepted_values |
| Business rule validation | Silver | dbt custom tests *(planned)* |
| Row count reconciliation | All layers | dbt audit_helper *(planned)* |

---

## 📁 Repository Structure

```
data-engineering-portfolio/
├── ingestion/
│   └── load_olist_to_snowflake.py
├── olist_dbt/
│   ├── models/
│   │   ├── bronze/               ← 8 staging models
│   │   ├── silver/               ← 3 intermediate enrichment models
│   │   └── gold/
│   │       ├── finance/          ← mart_revenue_trends, mart_product_category
│   │       ├── marketing/        ← mart_customer_segments, mart_cohort_retention
│   │       ├── operations/       ← mart_delivery_operations, mart_seller_performance
│   │       └── ml/               ← mart_churn_features
│   ├── tests/
│   ├── dbt_project.yml
│   └── profiles.yml        ← (gitignored)
├── dashboards/             ← Power BI .pbix files (planned)
└── README.md
```

---

## 🔑 Key Engineering Decisions

| Decision | Rationale |
|---|---|
| Two-database Snowflake design | Separates raw storage from analytics, mirrors enterprise architecture |
| Python 3.11 for ingestion | Snowflake connector incompatible with Python 3.13+ |
| Warehouse separation (DBT_WH / REPORTER_WH) | Prevents BI query contention blocking pipeline runs |
| Business logic in Silver, not Bronze | Bronze remains stable and reusable; logic changes isolated to Silver |

---

*Dataset: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — Kaggle*