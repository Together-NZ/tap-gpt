{{ config(
    materialized='table',
) }}
{{ dash_table_general_process.dash_table_search(source_name='dash_union__fraud_and_scams', table_name='dash_table__fraud_and_scams') }}
