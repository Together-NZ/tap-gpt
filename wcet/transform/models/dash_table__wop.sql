{{ config(
    materialized='table',
    schema='dash_table__wop',
    alias='dash_table__wop',
) }}

WITH dash_table AS (
    {{ dash_table_general_process.dv360_standard(source_name='dv360_transformed__wop', table_name='dv360_standard__wop', yt_source_name='dv360_transformed__wop', yt_table_name='dv360_youtube__wop') }}
    UNION ALL
    {{ dash_table_general_process.dv360_youtube(source_name='dv360_transformed__wop', table_name='dv360_youtube__wop') }}
    UNION ALL
    {{ dash_table_general_process.meta(source_name='facebook_transformed__wop', table_name='facebook__wop') }}
    UNION ALL
    {{ dash_table_general_process.google_ads(source_name='google_ads__wop', table_name='google_ads_demand__wop') }}

),

with_channel as (
SELECT * EXCEPT (publisher,channel), 
dc.publisher,
dc.channel

FROM dash_table as dt join `together-internal.channel.publisher_channel` as dc
ON lower(trim(dt.publisher)) = lower(trim(dc.publisher))),
{{dash_table_general_process.dash_table_general_process()}}