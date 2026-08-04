{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    {{ dash_table_general_process.ttd(
        source_name='ttd_transformed__creativenz',
        table_name='ttd_transformed__creativenz'
    ) }}
    UNION ALL
    {{ dash_table_general_process.meta(
        source_name='facebook_transformed__creativenz',
        table_name='facebook__creativenz'
    ) }}
    UNION ALL
    {{ dash_table_general_process.cm360(
        source_name='cm360_transformed__creativenz',
        table_name='cm360_direct_buy__creativenz'
    ) }}
    UNION ALL
    {{ dash_table_general_process.tiktok(
        source_name='tiktok_transformed__creativenz',
        table_name='tiktok__creativenz'
    ) }}
    UNION ALL
    {{ dash_table_general_process.dv360_standard(
        source_name='dv360_transformed__creativenz',
        table_name='dv360_standard__creativenz',
        yt_source_name='dv360_transformed__creativenz',
        yt_table_name='dv360_youtube__creativenz'
    ) }}
    UNION ALL
    {{ dash_table_general_process.dv360_youtube(
        source_name='dv360_transformed__creativenz',
        table_name='dv360_youtube__creativenz'
    ) }}
    UNION ALL
    {{ dash_table_general_process.google_ads(
        source_name='google_ads__creativenz',
        table_name='google_ads_demand__creativenz'
    ) }}
),
with_channel AS (
SELECT * EXCEPT (publisher, channel),
dc.publisher,
dc.channel
FROM dash_table AS dt
JOIN `together-internal.channel.publisher_channel` AS dc
ON lower(dt.publisher) = lower(dc.publisher)
where lower(campaign_name) like '%cnz%'
),
{{ dash_table_general_process.dash_table_general_process_funnel(
    funnels=['awareness', 'consideration', 'intent']
) }}
