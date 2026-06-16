{{ config(
    materialized='table',
) }}
with dash_table AS (
SELECT * FROM `kiwibank-main.google_ads_search_transformed__fraud_and_scams.google_ads_search__fraud_and_scams`
U)
{{ dash_table_general_process.dash_table_search_general_process('Choose') }}