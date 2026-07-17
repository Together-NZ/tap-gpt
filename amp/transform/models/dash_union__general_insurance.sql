{{ config(
    materialized='table',
) }}

SELECT *
FROM {{ source('dash_union', 'dash_union__centralized') }}
WHERE sub_brands = 'General Insurance'
