{{ config(
    materialized='table',
    schema='dash_table__beststart',
    alias='dash_table__beststart',
) }}

WITH dash_table AS (
    {{ dash_table_general_process.dv360_standard(
        source_name='dv360_transformed',
        table_name='dv360_standard',
        yt_source_name='dv360_transformed',
        yt_table_name='dv360_youtube'
    ) }}
    UNION ALL
    {{ dash_table_general_process.dv360_youtube(
        source_name='dv360_transformed',
        table_name='dv360_youtube'
    ) }}
    UNION ALL
    {{ dash_table_general_process.google_ads(
        source_name='google_ads__beststart',
        table_name='google_ads_demand2'
    ) }}
    UNION ALL
    {{ dash_table_general_process.google_ads(
        source_name='google_ads__beststart',
        table_name='google_ads_demand__beststart'
    ) }}
    UNION ALL
    {{ dash_table_general_process.tiktok(
        source_name='tiktok_transformed',
        table_name='tiktok'
    ) }}
    UNION ALL
    {{ dash_table_general_process.meta(
        source_name='facebook_transformed',
        table_name='facebook'
    ) }}
    UNION ALL
    {{ dash_table_general_process.cm360(
        source_name='cm360_transformed',
        table_name='cm360_direct_buy'
    ) }}
    UNION ALL
    {{ dash_table_general_process.gpt(
        source_name='gpt_transformed',
        table_name='gpt'
    )}}
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
