{{ config(
    materialized='table',
) }}
with dash_table AS (
{{dash_table_general_process.google_ads_search(source_name='google_ads__home_loans', table_name='google_ads_search__home_loans')}}
UNION ALL
{{dash_table_general_process.bing_ads_search(source_name='google_ads__home_loans', table_name='bing_ads_search__home_loans')}}
),
{{ dash_table_general_process.dash_table_search_general_process('Choose') }}
