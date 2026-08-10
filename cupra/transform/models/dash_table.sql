{{ config(
    materialized='table',
) }}

SELECT * EXCEPT (ad_format),
    ad_format_detail AS ad_format,
    ad_format AS ad_format1
FROM (
    WITH dash_table AS (
        {{ dash_table_general_process.dv360_standard(source_name='dv360_transformed', table_name='dv360_standard', yt_source_name='dv360_transformed', yt_table_name='dv360_youtube') }}
        UNION ALL
        {{ dash_table_general_process.dv360_youtube(source_name='dv360_transformed', table_name='dv360_youtube') }}
        UNION ALL
        {{ dash_table_general_process.google_ads(source_name='google_ads', table_name='google_ads_demand') }}
        UNION ALL
        {{ dash_table_general_process.meta(source_name='facebook_transformed', table_name='facebook') }}
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
    ),
    {{ dash_table_general_process.dash_table_general_process_funnel(
        funnels=['cultural signal', 'high attention', 'public impact', 'consideration', 'traffic']
    ) }}
)
