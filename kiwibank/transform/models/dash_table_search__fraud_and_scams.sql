{{ config(
    materialized='table',
) }}
with dash_table AS (
{{dash_table_general_process.google_ads_search(source_name='google_ads__fraud_and_scams', table_name='google_ads_search__fraud_and_scams')}}
),
{{ dash_table_general_process.dash_table_search_general_process('Choose') }}
