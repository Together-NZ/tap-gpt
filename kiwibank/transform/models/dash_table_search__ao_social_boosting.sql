{{ config(
    materialized='table',
) }}
{{ dash_table_general_process.dash_table_search(source_name='dash_union__ao_social_boosting', table_name='dash_table__ao_social_boosting') }}
