{{ config(
    materialized='table',
) }}
with dash_table AS (
SELECT * FROM `kiwibank-main.google_ads_search_transformed__everyday_banking_join_kiwibank.google_ads_search__everyday_banking_join_kiwibank`
)
{{ dash_table_general_process.dash_table_search_general_process('Choose') }}