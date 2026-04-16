{{ config(
    materialized='table',
) }}
{{ dash_table_general_process.dash_table_search(source_name='dash_union__everyday_banking_retail_deposit', table_name='dash_table__everyday_banking_retail_deposit') }}
