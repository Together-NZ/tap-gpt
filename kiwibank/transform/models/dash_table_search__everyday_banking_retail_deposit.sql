{{ config(
    materialized='table',
) }}
with dash_table AS (
{{dash_table_general_process.google_ads_search(source_name='google_ads__everyday_banking_retail_deposit', table_name='google_ads_search__everyday_banking_retail_deposit1')}}
UNION ALL
{{dash_table_general_process.google_ads_search(source_name='google_ads__everyday_banking_retail_deposit', table_name='google_ads_search__everyday_banking_retail_deposit2')}}
UNION ALL
{{dash_table_general_process.bing_ads_search(source_name='google_ads__everyday_banking_retail_deposit', table_name='bing_ads_search__everyday_banking_retail_deposit')}}
),
{{ dash_table_general_process.dash_table_search_general_process('Choose') }}
