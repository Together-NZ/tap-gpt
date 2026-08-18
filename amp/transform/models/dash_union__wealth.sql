{{ config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}

SELECT *
FROM {{ source('dash_union', 'dash_union__centralized') }}
WHERE sub_brands = 'Wealth'
