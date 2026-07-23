{{ config(
    materialized='table',
) }}

{{ dash_table_general_process.dash_union_search(
    source_name='dash_table_search__hr_career',
    table_name='dash_table_search__hr_career',
    sub_brands='null'
) }}
