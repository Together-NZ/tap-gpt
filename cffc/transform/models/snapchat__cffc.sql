{{ config(
    materialized='table',
) }}
{{ snapchat.ad_squad(source_name='snapchat_raw', table_name='ad_squads') }},
{{ snapchat.campaigns(source_name='snapchat_raw', table_name='campaigns') }},
{{ snapchat.ad_detail(source_name='snapchat_raw', table_name='ads') }},
{{ snapchat.merge_infos() }},
{{ snapchat.ad_stats_daily(source_name='snapchat_raw', table_name='ad_stats_daily') }},
{{ snapchat.final_calculation() }}
