{{ config(
    materialized='table',
) }}
WITH dash_table AS (
{{dash_table_general_process.tiktok(source_name='tiktok_transformed', table_name='tiktok')          }} WHERE campaign_name LIKE '%000001%'
UNION ALL
{{dash_table_general_process.meta(source_name='facebook_transformed', table_name='facebook')}} WHERE campaign_name LIKE '%000001%'
UNION ALL
{{dash_table_general_process.linkedin(source_name='linkedin_transformed', table_name='linkedin')}} WHERE campaign_name LIKE '%000001%'
UNION ALL
{{dash_table_general_process.dv360_standard(source_name='dv360_transformed', table_name='dv360_standard',yt_source_name='dv360_transformed',yt_table_name='dv360_youtube')}} AND campaign_name LIKE '%000001%'
UNION ALL
{{dash_table_general_process.dv360_youtube(source_name='dv360_transformed', table_name='dv360_youtube')}} WHERE campaign_name LIKE '%000001%'
UNION ALL
{{dash_table_general_process.hivestack(source_name='hivestack_transformed', table_name='hivestack')}} WHERE campaign_name LIKE '%000001%'
UNION ALL
{{dash_table_general_process.google_ads(source_name='google_ads__everyday_banking_join_kiwibank', table_name='google_ads_demand__everyday_banking_join_kiwibank')}}
),
with_channel AS (
SELECT * EXCEPT (publisher, channel),
dc.publisher,
dc.channel
FROM dash_table as dt JOIN `together-internal.channel.publisher_channel` as dc
ON lower(dt.publisher) = lower(dc.publisher) 
),
{{dash_table_general_process.dash_table_general_process()}}

