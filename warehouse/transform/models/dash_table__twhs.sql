{{ config(
    materialized='table',
    schema='dash_table__twhs',
    alias='dash_table__twhs',
) }}

WITH dash_table AS (
    {{ dash_table_general_process.dv360_standard(
        source_name='dv360_transformed__twhs',
        table_name='dv360_standard__twhs',
        yt_source_name='dv360_transformed__twhs',
        yt_table_name='dv360_youtube__twhs'
    ) }}
    UNION ALL
    {{ dash_table_general_process.dv360_youtube(
        source_name='dv360_transformed__twhs',
        table_name='dv360_youtube__twhs'
    ) }}
    UNION ALL
    {{ dash_table_general_process.meta(
        source_name='facebook_transformed__twhs',
        table_name='facebook__twhs'
    ) }}
    UNION ALL
    {{ dash_table_general_process.cm360(
        source_name='cm360_transformed__twhs',
        table_name='cm360_direct_buy__twhs'
    ) }}
    UNION ALL
    {{ dash_table_general_process.google_ads(
        source_name='google_ads__twhs',
        table_name='google_ads_demand__twhs'
    ) }}
),

with_channel AS (
    SELECT * EXCEPT (publisher, channel),
        dc.publisher,
        dc.channel
    FROM dash_table AS dt
    JOIN `together-internal.channel.publisher_channel` AS dc
        ON LOWER(TRIM(dt.publisher)) = LOWER(TRIM(dc.publisher))
),

{{ dash_table_general_process.dash_table_general_process() }}
