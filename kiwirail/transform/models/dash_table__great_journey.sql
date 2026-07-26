{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    {{ dash_table_general_process.meta(
        source_name='facebook_transformed__great_journey',
        table_name='facebook__great_journey'
    ) }}
    UNION ALL
    {{ dash_table_general_process.dv360_standard(
        source_name='dv360_transformed__great_journey',
        table_name='dv360_standard__great_journey',
        yt_source_name='dv360_transformed__great_journey',
        yt_table_name='dv360_youtube__great_journey'
    ) }}
    UNION ALL
    {{ dash_table_general_process.dv360_youtube(
        source_name='dv360_transformed__great_journey',
        table_name='dv360_youtube__great_journey'
    ) }}
    UNION ALL
    {{ dash_table_general_process.cm360(
        source_name='cm360_transformed__great_journey',
        table_name='cm360_direct_buy__great_journey'
    ) }}
    UNION ALL
    {{ dash_table_general_process.google_ads(
        source_name='google_ads__great_journey',
        table_name='google_ads_demand__great_journey'
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
