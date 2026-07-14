{{ config(
    materialized='table',
    alias='dash_table__mountain'
) }}

WITH dash_table AS (
    {{ dash_table_general_process.meta(source_name='facebook_transformed__mountain', table_name='facebook__mountain') }}
    UNION ALL
    {{ dash_table_general_process.dv360_standard(source_name='dv360_transformed__mountain', table_name='dv360_standard__mountain', yt_source_name='dv360_transformed__mountain', yt_table_name='dv360_youtube__mountain') }}
    UNION ALL
    {{ dash_table_general_process.dv360_youtube(source_name='dv360_transformed__mountain', table_name='dv360_youtube__mountain') }}
    UNION ALL
    {{ dash_table_general_process.cm360(source_name='cm360_transformed__mountain', table_name='cm360_direct_buy__mountain') }}
    UNION ALL
    {{ dash_table_general_process.ttd(source_name='ttd_transformed__mountain', table_name='ttd_transformed__mountain') }}
    UNION ALL
    {{ dash_table_general_process.tiktok(source_name='tiktok_transformed__mountain', table_name='tiktok__mountain') }} WHERE LOWER(campaign_name) LIKE '%mtn%'
    UNION ALL
    {{ dash_table_general_process.google_ads(source_name='google_ads_search_transformed__mountain', table_name='google_ads_demand__mountain') }}
    UNION ALL
    {{ dash_table_general_process.hivestack(source_name='hivestack_transformed__mountain', table_name='hivestack__mountain') }}
),
with_channel AS (
    SELECT * EXCEPT (publisher, channel),
        dc.publisher,
        dc.channel
    FROM dash_table AS dt
    JOIN `together-internal.channel.publisher_channel` AS dc
        ON LOWER(dt.publisher) = LOWER(dc.publisher)
),
{{ dash_table_general_process.dash_table_general_process_funnel(
    funnels=['explore', 'compare', 'book', 'dream']
) }}
