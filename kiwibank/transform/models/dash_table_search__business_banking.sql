{{ config(
    materialized='table',
) }}
with dash_table AS (
{{dash_table_general_process.google_ads_search(source_name='google_ads__business_banking', table_name='google_ads_search__business_banking')}}
),
{{ dash_table_general_process.dash_table_search_general_process('Choose') }}
