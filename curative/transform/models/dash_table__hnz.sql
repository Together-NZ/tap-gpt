{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    {{ dash_table_general_process.tiktok(
        source_name='tiktok_transformed__hnz',
        table_name='tiktok__hnz'
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
