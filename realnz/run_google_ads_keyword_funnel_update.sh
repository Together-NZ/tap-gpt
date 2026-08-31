#!/usr/bin/env bash
set -euo pipefail
cd /Users/peter/work/realnz

set -a
set +u
source .env
set -u
set +a

export MELTANO_ENVIRONMENT=prod
unset GOOGLE_ADS_SEARCH_SUFFIX

DBT=".meltano/transformers/dbt-bigquery/venv/bin/dbt"

for brand in tourism mountain; do
  echo "=== realnz: google_ads_keyword__${brand} ==="
  export DBT_BIGQUERY_DATASET="google_ads_search_transformed__${brand}"
  "$DBT" run \
    --project-dir transform \
    --profiles-dir transform/profiles/bigquery \
    --profile meltano --target prod \
    --select "google_ads_keyword__${brand}" \
    --no-use-colors

  echo "=== funnel distribution: ${brand} ==="
  "$DBT" show \
    --project-dir transform \
    --profiles-dir transform/profiles/bigquery \
    --profile meltano --target prod \
    --inline "SELECT funnel, COUNT(*) AS row_count FROM \`real-nz-main.google_ads_search_transformed__${brand}.google_ads_keyword__${brand}\` GROUP BY 1 ORDER BY 2 DESC" \
    --limit 10 --no-use-colors
done
