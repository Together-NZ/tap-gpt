{{ config(
    materialized='table',
) }}

WITH package_goal AS (
    {{ ga4.ga4_goal_a(
        source_name='ga4_raw',
        table_name='goal',
        plan_code='bri',
        dash_union_source_name='dash_table',
        dash_union_table_name='dash_union'
    ) }}
)
SELECT
    * EXCEPT (sessionManualAdContent),
    CASE
        WHEN LOWER(sessionManualAdContent) LIKE '%bri%'
            THEN SPLIT(sessionManualAdContent, '_')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(sessionManualAdContent, '_')) - 1)]
        ELSE sessionManualAdContent
    END AS sessionManualAdContent
FROM package_goal
