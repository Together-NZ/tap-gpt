{{ config(
    materialized='table',
    schema='dash_table__noel_leeming',
    alias='dash_table__noel_leeming',
) }}

WITH dash_table AS (
    {{ dash_table_general_process.dv360_standard(
        source_name='dv360_transformed__noel_leeming',
        table_name='dv360_standard__noel_leeming',
        yt_source_name='dv360_transformed__noel_leeming',
        yt_table_name='dv360_youtube__noel_leeming'
    ) }}
    UNION ALL
    {{ dash_table_general_process.dv360_youtube(
        source_name='dv360_transformed__noel_leeming',
        table_name='dv360_youtube__noel_leeming'
    ) }}
    UNION ALL
    {{ dash_table_general_process.meta(
        source_name='facebook_transformed__noel_leeming',
        table_name='facebook__noel_leeming'
    ) }}
    UNION ALL
    {{ dash_table_general_process.ttd(
        source_name='ttd_transformed__noel_leeming',
        table_name='ttd_transformed__noel_leeming'
    ) }}
    UNION ALL
    {{ dash_table_general_process.tiktok(
        source_name='tiktok_transformed__noel_leeming',
        table_name='tiktok__noel_leeming'
    ) }}
    UNION ALL
    {{ dash_table_general_process.pinterest(
        source_name='pinterest_transformed__noel_leeming',
        table_name='pinterest__noel_leeming'
    ) }}
    UNION ALL
    {{ dash_table_general_process.google_ads(
        source_name='google_ads__noel_leeming',
        table_name='google_ads_demand__noel_leeming'
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
