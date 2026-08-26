{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'date', 'data_type': 'date'},
) }}

WITH transformed AS (
    {{ ga4.ga4_goal_a(
        source_name='ga4_raw',
        table_name='goal',
        plan_code='con',
        dash_union_source_name='dash_table',
        dash_union_table_name='dash_union'
    ) }}
)
SELECT
    * EXCEPT(eventName),
    CASE
        WHEN LOWER(eventName) = 'purchase'
             AND LOWER(hostName) LIKE '%contactmobile%'
            THEN 'purchase_mobile'
        ELSE eventName
    END AS eventName
FROM transformed
