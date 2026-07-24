{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    {{ dash_table_general_process.meta(
        source_name='facebook_transformed__inghams',
        table_name='facebook__inghams'
    ) }}
    UNION ALL
    {{ dash_table_general_process.dv360_standard(
        source_name='dv360_transformed__inghams',
        table_name='dv360_standard__inghams',
        yt_source_name='dv360_transformed__inghams',
        yt_table_name='dv360_youtube__inghams'
    ) }}
    UNION ALL
    {{ dash_table_general_process.dv360_youtube(
        source_name='dv360_transformed__inghams',
        table_name='dv360_youtube__inghams'
    ) }}
    UNION ALL
    {{ dash_table_general_process.ttd(
        source_name='ttd_transformed__inghams',
        table_name='ttd__inghams'
    ) }}
    UNION ALL
    {{ dash_table_general_process.cm360(
        source_name='cm360_transformed__inghams',
        table_name='cm360_direct_buy__inghams'
    ) }}
    UNION ALL
    {{ dash_table_general_process.hivestack(
        source_name='hivestack_transformed__inghams',
        table_name='hivestack__inghams'
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
