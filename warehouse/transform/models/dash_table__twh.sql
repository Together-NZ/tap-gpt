{{ config(
    materialized='table',
    schema='dash_table__twh',
    alias='dash_table__twh',
) }}

WITH dash_table AS (
    {{ dash_table_general_process.dv360_standard(
        source_name='dv360_transformed__twh',
        table_name='dv360_standard__twh',
        yt_source_name='dv360_transformed__twh',
        yt_table_name='dv360_youtube__twh'
    ) }}
    UNION ALL
    {{ dash_table_general_process.dv360_youtube(
        source_name='dv360_transformed__twh',
        table_name='dv360_youtube__twh'
    ) }}
    UNION ALL
    {{ dash_table_general_process.meta(
        source_name='facebook_transformed__twh',
        table_name='facebook__twh'
    ) }}
    UNION ALL
    {{ dash_table_general_process.cm360(
        source_name='cm360_transformed__twh',
        table_name='cm360_direct_buy__twh'
    ) }}
    UNION ALL
    {{ dash_table_general_process.hivestack(
        source_name='hivestack_transformed__twh',
        table_name='hivestack__twh'
    ) }}
    UNION ALL
    {{ dash_table_general_process.ttd(
        source_name='ttd_transformed__twh',
        table_name='ttd_transformed__twh'
    ) }}
    UNION ALL
    {{ dash_table_general_process.snapchat(
        source_name='snapchat_transformed__twh',
        table_name='snapchat__twh'
    ) }}
    UNION ALL
    {{ dash_table_general_process.tiktok(
        source_name='tiktok_transformed__twh',
        table_name='tiktok__twh'
    ) }}
    UNION ALL
    {{ dash_table_general_process.pinterest(
        source_name='pinterest_transformed__twh',
        table_name='pinterest__twh'
    ) }}
    UNION ALL
    {{ dash_table_general_process.google_ads(
        source_name='google_ads__twh',
        table_name='google_ads_demand__twh'
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
