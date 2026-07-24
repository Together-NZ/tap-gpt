{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    {{ dash_table_general_process.meta(
        source_name='facebook_transformed__waitoa',
        table_name='facebook__waitoa'
    ) }}
    UNION ALL
    {{ dash_table_general_process.tiktok(
        source_name='tiktok_transformed__waitoa',
        table_name='tiktok__waitoa'
    ) }}
    UNION ALL
    {{ dash_table_general_process.dv360_standard(
        source_name='dv360_transformed__waitoa',
        table_name='dv360_standard__waitoa',
        yt_source_name='dv360_transformed__waitoa',
        yt_table_name='dv360_youtube__waitoa'
    ) }}
    UNION ALL
    {{ dash_table_general_process.dv360_youtube(
        source_name='dv360_transformed__waitoa',
        table_name='dv360_youtube__waitoa'
    ) }}
    UNION ALL
    {{ dash_table_general_process.ttd(
        source_name='ttd_transformed__waitoa',
        table_name='ttd__waitoa'
    ) }}
    UNION ALL
    {{ dash_table_general_process.cm360(
        source_name='cm360_transformed__waitoa',
        table_name='cm360_direct_buy__waitoa'
    ) }}
    UNION ALL
    {{ dash_table_general_process.hivestack(
        source_name='hivestack_transformed__waitoa',
        table_name='hivestack__waitoa'
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
