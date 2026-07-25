{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    {{ dash_table_general_process.ttd(
        source_name='ttd_transformed',
        table_name='ttd_transformed'
    ) }}
    UNION ALL
    {{ dash_table_general_process.cm360(
        source_name='cm360_transformed',
        table_name='cm360_direct_buy'
    ) }}
    UNION ALL
    {{ dash_table_general_process.meta(
        source_name='facebook_transformed',
        table_name='facebook'
    ) }}
    UNION ALL
    SELECT
        SAFE_CAST(media_cost AS FLOAT64) AS media_cost,
        SAFE_CAST(impressions AS INT64) AS impressions,
        SAFE_CAST(clicks AS INT64) AS clicks,
        creative_name,
        audience_name,
        ad_format,
        ad_format_detail,
        SAFE_CAST(video_completion AS INT64) AS video_completion,
        SAFE_CAST(video_25_completion AS INT64) AS video_25_completion,
        SAFE_CAST(video_50_completion AS INT64) AS video_50_completion,
        SAFE_CAST(video_75_completion AS INT64) AS video_75_completion,
        SAFE_CAST(video_views AS INT64) AS video_views,
        campaign_name,
        publisher,
        campaign_descr,
        creative_descr,
        DATE(date) AS date,
        NULL AS conversions,
        'Pinterest' AS platform
    FROM {{ source('pinterest_transformed', 'pinterest') }}
    UNION ALL
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
),
with_channel AS (
    SELECT
        dt.* EXCEPT (publisher),
        dc.publisher,
        dc.channel
    FROM dash_table AS dt
    JOIN `together-internal.channel.publisher_channel` AS dc
        ON LOWER(TRIM(dt.publisher)) = LOWER(TRIM(dc.publisher))
),
{{ dash_table_general_process.dash_table_general_process() }}
