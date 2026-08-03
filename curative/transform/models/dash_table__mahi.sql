{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    {{ dash_table_general_process.ttd(
        source_name='ttd_transformed__mahi',
        table_name='ttd_transformed__mahi'
    ) }}
    UNION ALL
    {{ dash_table_general_process.dv360_standard(
        source_name='dv360_transformed__mahi',
        table_name='dv360_standard__mahi',
        yt_source_name='dv360_transformed__mahi',
        yt_table_name='dv360_youtube__mahi'
    ) }}
    UNION ALL
    {{ dash_table_general_process.dv360_youtube(
        source_name='dv360_transformed__mahi',
        table_name='dv360_youtube__mahi'
    ) }}
    UNION ALL
    {{ dash_table_general_process.tiktok(
        source_name='tiktok_transformed__mahi',
        table_name='tiktok__mahi'
    ) }}
    UNION ALL
    {{ dash_table_general_process.google_ads(
        source_name='google_ads__mahi',
        table_name='google_ads_demand__mahi'
    ) }}
),
with_channel AS (
SELECT * EXCEPT (publisher, channel),
dc.publisher,
dc.channel
FROM dash_table AS dt
JOIN `together-internal.channel.publisher_channel` AS dc
ON lower(dt.publisher) = lower(dc.publisher)
),
{{ dash_table_general_process.dash_table_general_process_funnel(
    funnels=['awareness', 'consideration', 'intent']
) }}
