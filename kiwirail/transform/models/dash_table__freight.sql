{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    {{ dash_table_general_process.linkedin(
        source_name='linkedin_transformed__freight',
        table_name='linkedin__freight'
    ) }}
    UNION ALL
    {{ dash_table_general_process.ttd(
        source_name='ttd_transformed__freight',
        table_name='ttd_transformed__freight'
    ) }}
    UNION ALL
    {{ dash_table_general_process.dv360_standard(
        source_name='dv360_transformed__freight',
        table_name='dv360_standard__freight',
        yt_source_name='dv360_transformed__freight',
        yt_table_name='dv360_youtube__freight'
    ) }}
    UNION ALL
    {{ dash_table_general_process.dv360_youtube(
        source_name='dv360_transformed__freight',
        table_name='dv360_youtube__freight'
    ) }}
    UNION ALL
    {{ dash_table_general_process.cm360(
        source_name='cm360_transformed__freight',
        table_name='cm360_direct_buy__freight'
    ) }}
    UNION ALL
    {{ dash_table_general_process.google_ads(
        source_name='google_ads__freight',
        table_name='google_ads_demand__freight'
    ) }}
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
{{ dash_table_general_process.dash_table_general_process() }}
