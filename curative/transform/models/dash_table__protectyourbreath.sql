{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    {{ dash_table_general_process.ttd(
        source_name='ttd_transformed__protectyourbreath',
        table_name='ttd_transformed__protectyourbreath'
    ) }}
    UNION ALL
    {{ dash_table_general_process.dv360_standard(
        source_name='dv360_transformed__protectyourbreath',
        table_name='dv360_standard__protectyourbreath',
        yt_source_name='dv360_transformed__protectyourbreath',
        yt_table_name='dv360_youtube__protectyourbreath'
    ) }}
    UNION ALL
    {{ dash_table_general_process.dv360_youtube(
        source_name='dv360_transformed__protectyourbreath',
        table_name='dv360_youtube__protectyourbreath'
    ) }}
    UNION ALL
    {{ dash_table_general_process.meta(
        source_name='facebook_transformed__protectyourbreath',
        table_name='facebook__protectyourbreath'
    ) }}
    UNION ALL
    {{ dash_table_general_process.snapchat(
        source_name='snapchat_transformed__protectyourbreath',
        table_name='snapchat__protectyourbreath'
    ) }}
    UNION ALL
    {{ dash_table_general_process.tiktok(
        source_name='tiktok_transformed__protectyourbreath',
        table_name='tiktok__protectyourbreath'
    ) }}
    UNION ALL
    {{ dash_table_general_process.google_ads(
        source_name='google_ads__protectyourbreath',
        table_name='google_ads_demand__protectyourbreath'
    ) }}
    UNION ALL
    {{ dash_table_general_process.hivestack(
        source_name='hivestack_transformed__protectyourbreath',
        table_name='hivestack__protectyourbreath'
    ) }}
),
with_channel AS (
SELECT * EXCEPT (publisher, channel),
dc.publisher,
dc.channel
FROM dash_table AS dt
JOIN `together-internal.channel.publisher_channel` AS dc
ON lower(dt.publisher) = lower(dc.publisher)
where lower(campaign_name) like '%hac%'
),
{{ dash_table_general_process.dash_table_general_process_funnel(
    funnels=['awareness', 'consideration', 'intent']
) }}
