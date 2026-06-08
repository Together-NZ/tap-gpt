# wcet — Meltano project

Impact CI data pipeline for **wcet** (Beervana) on GCP project `wcet-main`.

## Setup

```bash
cd wcet
cp .env.example .env   # fill secrets, TAP_TIKTOK_ADVERTISER_ID, TAP_DV360_ADVERTISER_ID
meltano install
```

## Extract + transform

```bash
# Facebook → facebook_raw__beervana → facebook_transformed__beervana.facebook__beervana
export BQ_DATASET=facebook_raw__beervana
export DBT_BIGQUERY_DATASET=facebook_transformed__beervana
meltano run --environment=prod tap-facebook target-bigquery dbt-bigquery:facebook_models

# TikTok → tiktok_raw__beervana → tiktok_transformed__beervana.tiktok__beervana
export BQ_DATASET=tiktok_raw__beervana
export DBT_BIGQUERY_DATASET=tiktok_transformed__beervana
meltano run --environment=prod tap-tiktok target-bigquery dbt-bigquery:tiktok_models

# DV360 → dv360_raw__beervana → dv360_transformed__beervana
export BQ_DATASET=dv360_raw__beervana
export DBT_BIGQUERY_DATASET=dv360_transformed__beervana
meltano run --environment=prod tap-dv360 target-bigquery dbt-bigquery:dv360_models

# Dash union (after all channels)
export DBT_BIGQUERY_DATASET=dash_table__beervana
meltano invoke --environment=prod dbt-bigquery run --select dash_table__beervana dash_table_search__beervana
```

## BigQuery datasets (terraform)

| Purpose | Dataset |
|---------|---------|
| Facebook raw | `facebook_raw__beervana` |
| Facebook transformed | `facebook_transformed__beervana` |
| TikTok raw | `tiktok_raw__beervana` |
| TikTok transformed | `tiktok_transformed__beervana` |
| DV360 raw | `dv360_raw__beervana` |
| DV360 transformed | `dv360_transformed__beervana` |
| Dash table | `dash_table__beervana` |
| Dash table search | `dash_table_search__beervana` |

## Plugins

- **tap-facebook**, **tap-tiktok**, **tap-dv360**
- **target-bigquery** → `wcet-main`
- **dbt-bigquery** — `facebook__beervana`, `tiktok__beervana`, `dv360_standard__beervana`, `dv360_youtube__beervana`, `dash_table__beervana`, `dash_table_search__beervana`

## Airflow

`orchestrate/dags/airflow/wcet.py` — deploy to Composer; requires Variable `meltano_wcet_main` and image `meltano-wcet-main:prod` in Artifact Registry.
