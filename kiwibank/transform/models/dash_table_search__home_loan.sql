{{ config(
    materialized='table',
) }}
{{ dash_table_general_process.dash_table_search(source_name='dash_union__home_loan', table_name='dash_table__home_loan') }}
