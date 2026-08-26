{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
) }}
{{ ga4.ga4_goal_a_contact_purchase(
    source_name='ga4_raw',
    table_name='contact_purchase_energy_report',
    dash_union_source_name='dash_union_energy',
    dash_union_table_name='dash_union__energy'
) }}