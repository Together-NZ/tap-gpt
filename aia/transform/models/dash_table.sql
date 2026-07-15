{{ config(
    materialized='table',
) }}

WITH dash_table AS (
    {{ dash_table_general_process.ttd(source_name='ttd_transformed', table_name='ttd_transformed') }}

    UNION ALL
    {{ dash_table_general_process.dv360_standard(source_name='dv360_transformed', table_name='dv360_standard', yt_source_name='dv360_transformed', yt_table_name='dv360_youtube') }}

    UNION ALL
    {{ dash_table_general_process.dv360_youtube(source_name='dv360_transformed', table_name='dv360_youtube') }}

    UNION ALL
    {{ dash_table_general_process.tiktok(source_name='tiktok_transformed', table_name='tiktok') }}

    UNION ALL
    {{ dash_table_general_process.google_ads(source_name='google_ads_search_transformed__brand', table_name='google_ads_demand_2') }}

    UNION ALL
    {{ dash_table_general_process.meta(source_name='facebook_transformed', table_name='facebook') }}

    UNION ALL
    {{ dash_table_general_process.linkedin(source_name='linkedin_transformed', table_name='linkedin') }}

    UNION ALL
    {{ dash_table_general_process.google_ads(source_name='google_ads_search_transformed__brand', table_name='google_ads_demand__brand') }}

    UNION ALL
    {{ dash_table_general_process.google_ads(source_name='google_ads_search_transformed__marketing', table_name='google_ads_demand__marketing') }}

    UNION ALL
    SELECT
        SAFE_CAST(media_cost AS FLOAT64) AS media_cost,
        SAFE_CAST(impressions AS INT64) AS impressions,
        SAFE_CAST(clicks AS INT64) AS clicks,
        creative_name,
        audience_name,
        ad_format,
        ad_format_detail,
        CAST(0 AS INT64) AS video_completion,
        CAST(0 AS INT64) AS video_25_completion,
        CAST(0 AS INT64) AS video_50_completion,
        CAST(0 AS INT64) AS video_75_completion,
        CAST(0 AS INT64) AS video_views,
        campaign_name,
        publisher,
        campaign_descr,
        creative_descr,
        DATE(date) AS date,
        CAST(NULL AS FLOAT64) AS conversions
    FROM {{ source('outbrain_transformed', 'outbrain') }}

    UNION ALL
    {{ dash_table_general_process.cm360(source_name='cm360_transformed', table_name='cm360_direct_buy') }}
),
with_channel AS (
    SELECT * EXCEPT (publisher, channel),
        dc.publisher,
        dc.channel
    FROM dash_table AS dt
    JOIN `together-internal.channel.publisher_channel` AS dc
        ON LOWER(TRIM(dt.publisher)) = LOWER(TRIM(dc.publisher))
    WHERE LOWER(campaign_name) LIKE '%aia%'
),
{{ dash_table_general_process.dash_table_general_process() }}
