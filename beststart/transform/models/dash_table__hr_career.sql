{{ config(
    materialized='table',
    schema='dash_table__hr_career',
    alias='dash_table__hr_career',
) }}

WITH dash_table AS (
    {{ dash_table_general_process.google_ads(
        source_name='google_ads__hr_career',
        table_name='google_ads_demand__hr_career'
    ) }}
),
with_channel AS (
    SELECT
        dt.* EXCEPT (publisher),
        dc.publisher,
        dc.channel
    FROM dash_table AS dt
    JOIN `together-internal.channel.publisher_channel` AS dc
        ON LOWER(dt.publisher) = LOWER(dc.publisher)
),
{{ dash_table_general_process.dash_table_general_process_funnel(
    funnels=['awareness', 'consideration', 'intent']
) }}
