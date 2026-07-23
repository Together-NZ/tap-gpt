{{ config(
    materialized='table',
) }}

SELECT * FROM {{ source('dash_table_search__hr_career', 'dash_table_search__hr_career') }}
UNION ALL
SELECT * FROM {{ source('dash_table_search__beststart', 'dash_table_search__beststart') }}
