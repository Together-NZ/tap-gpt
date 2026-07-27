{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    {{ dash_table_general_process.meta(
        source_name='facebook_transformed__interislander',
        table_name='facebook__interislander'
    ) }}
    UNION ALL
    {{ dash_table_general_process.google_ads(
        source_name='google_ads__interislander',
        table_name='google_ads_demand__interislander'
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
{{ dash_table_general_process.dash_table_general_process() }}
