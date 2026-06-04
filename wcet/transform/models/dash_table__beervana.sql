{{ config(
    materialized='table',
    schema='dash_table__beervana',
    alias='dash_table__beervana',
) }}

WITH dash_table AS (
    {{ dash_table_general_process.dv360_standard(source_name='dv360_transformed__beervana', table_name='dv360_standard__beervana', yt_source_name='dv360_transformed__beervana', yt_table_name='dv360_youtube__beervana') }}
    UNION ALL
    {{ dash_table_general_process.dv360_youtube(source_name='dv360_transformed__beervana', table_name='dv360_youtube__beervana') }}
    UNION ALL
    {{ dash_table_general_process.meta(source_name='facebook_transformed__beervana', table_name='facebook__beervana') }}
    UNION ALL
    {{ dash_table_general_process.tiktok(source_name='tiktok_transformed__beervana', table_name='tiktok__beervana') }}
),

with_channel as (
SELECT * EXCEPT (publisher,channel), 
dc.publisher,
dc.channel

FROM dash_table as dt join `together-internal.channel.publisher_channel` as dc
ON lower(trim(dt.publisher)) = lower(trim(dc.publisher))),
{{dash_table_general_process.dash_table_general_process()}}