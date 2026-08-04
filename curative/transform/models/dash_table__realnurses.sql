{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    {{ dash_table_general_process.ttd(
        source_name='ttd_transformed__realnurses',
        table_name='ttd_transformed__realnurses'
    ) }}
    UNION ALL
    {{ dash_table_general_process.google_ads(
        source_name='google_ads__realnurses',
        table_name='google_ads_demand__realnurses'
    ) }}
),
with_channel AS (
SELECT * EXCEPT (publisher, channel),
dc.publisher,
dc.channel
FROM dash_table AS dt
JOIN `together-internal.channel.publisher_channel` AS dc
ON lower(dt.publisher) = lower(dc.publisher)
where lower(campaign_name) like '%nur%'
),
{{ dash_table_general_process.dash_table_general_process_funnel(
    funnels=['awareness', 'consideration', 'intent']
) }}
