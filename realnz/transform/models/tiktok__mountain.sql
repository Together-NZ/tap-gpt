{{ config(
    materialized='table',
    schema='tiktok_transformed__mountain',
    alias='tiktok__mountain',
) }}

WITH
{{ tiktok.tk_ads(source_name='tiktok_raw__mountain', table_name='ads') }},
{{ tiktok.tk_basic_metrics_by_day(source_name='tiktok_raw__mountain', table_name='ads_basic_data_metrics_by_day') }},
{{ tiktok.tk_video_metrics_by_day(source_name='tiktok_raw__mountain', table_name='ads_video_play_metrics_by_day') }},
{{ tiktok.tk_final_calculation() }}
